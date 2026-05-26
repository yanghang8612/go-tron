// Package freezer drives the background freezing goroutine that moves
// solidified chain data out of Pebble and into the slice-1 freezer's
// append-only flat files. It registers as a `node.Lifecycle` so cmd/gtron
// can start it alongside the other long-running services.
//
// The runner owns the "when" and "how much" of each pass:
//
//  1. Read the chain's latest solidified block number from the supplied
//     ChainSource.
//  2. Compute `freezeTo = solidified - cfg.MarginBlocks` (don't get any
//     closer to the live head than the configured margin).
//  3. `freezeFrom = freezer.AncientCount("bodies")` — the freezer's own
//     position is the canonical resume point; all three slice-1 tables
//     advance in lockstep via ModifyAncients, so `bodies` is enough.
//  4. Cap the per-pass range at `freezeFrom + cfg.BatchBlocks`.
//  5. Read each block's raw KV bytes (block proto, tx-infos-per-block,
//     state-root-by-hash-resolved-via-block-hash) and append them inside
//     a single ModifyAncients call. The freezer rolls back atomically on
//     error so a partial pass leaves no orphan ancient rows.
//  6. fsync the ancient (`freezer.Sync()`).
//  7. DeleteRange the now-frozen `b-<num>` and `tib-<num>` rows from
//     Pebble (hash-keyed `bh-<hash>`, `bsr-<hash>`, `tx-<hash>`,
//     `ti-<txid>` remain hot per the slice-1 design).
//  8. Compact the freed range so Pebble reclaims space promptly.
//
// Crash safety: every batch first appends to ancient (with fsync), then
// deletes from Pebble. If the process dies between (6) and (7) the
// ancient has rows we already wrote (idempotent — `freezeFrom` re-reads
// AncientCount on the next pass) and Pebble may still have some of those
// rows (next pass re-deletes, no-op). No data loss; worst case is small
// duplicate work.
package freezer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
)

var log = gtronlog.NewModule("core/freezer")

// Defaults applied when Config fields are zero. They mirror the spec's
// recommended production values: 30-second cadence, 128-block margin
// (keeps us well below the PBFT solidification line under steady-state
// 27-SR DPoS), and 30 000 blocks per pass (large enough to drain a
// fresh-install backlog in under an hour, small enough that one pass
// can't dominate Pebble's compaction queue).
const (
	defaultInterval     = 30 * time.Second
	defaultMarginBlocks = uint64(128)
	defaultBatchBlocks  = uint64(30_000)
)

// Config governs the freezing pass cadence and batch sizing.
//
// Zero-valued fields are filled in by Default() so callers can populate
// only the knobs they care about. Tests that drive the runner
// synchronously (via OnePass) typically only set Enabled and
// MarginBlocks; production code reads everything from the operator's
// TOML / CLI overrides.
type Config struct {
	// Enabled is the master switch. Default true. When false, OnePass
	// returns (0, nil) without touching either store — useful as an
	// operator escape hatch on tiny dev chains that don't benefit from
	// freezing and want the smallest possible datadir layout.
	Enabled bool

	// Interval is the period between freezing passes. Default
	// defaultInterval (30s). The loop fires once on Start so a fresh-
	// install backlog begins draining without waiting an interval.
	Interval time.Duration

	// MarginBlocks is the buffer below the solidified line beneath which
	// the freezer never goes. Default 128 — but only when constructed via
	// Default(); applyDefaults leaves an explicit 0 untouched so an operator
	// can freeze right up to the solidified line. The freezer trails
	// solidified by at least this much so reorgs (bounded by KhaosDB's
	// 1024-block window) never have to unfreeze. Solidified blocks are
	// already final, so 0 is reorg-safe; the 128-block default is extra
	// caution against an upstream solidification regression.
	MarginBlocks uint64

	// BatchBlocks caps the number of blocks frozen in one pass. Default
	// defaultBatchBlocks (30 000). Higher = catch up faster, but each
	// pass holds the freezer write lock for longer and produces a larger
	// burst of Pebble DeleteRange tombstones; the default is calibrated
	// so one pass fits comfortably under the Interval ceiling.
	BatchBlocks uint64
}

// Default returns the production defaults. Used by cmd/gtron when no
// operator overrides have been supplied.
func Default() Config {
	return Config{
		Enabled:      true,
		Interval:     defaultInterval,
		MarginBlocks: defaultMarginBlocks,
		BatchBlocks:  defaultBatchBlocks,
	}
}

// applyDefaults fills in zero fields with package defaults. Returns a
// copy so the caller's struct stays untouched.
//
// Two fields are intentionally NOT defaulted:
//   - Enabled: explicit false means "disabled" while explicit true (or the
//     constructor calling Default()) is the only path to "enabled".
//   - MarginBlocks: zero is a legitimate operator choice meaning "freeze
//     right up to the solidified line" (solidified blocks are final, so a
//     zero extra margin is reorg-safe). The production 128-block margin is
//     applied only via Default(); leaving an explicit 0 untouched here lets
//     callers — and the missing-block test — drive a true zero margin.
//
// WIRING CONTRACT: applyDefaults deliberately does NOT default MarginBlocks
// (an explicit 0 is a valid "freeze up to solidified" choice). The
// production 128-block cushion lives in Default(). The cmd/gtron config
// loader MUST therefore start from Default() and overlay operator values —
// starting from a zero-value Config{} would silently run with margin 0.
func (c Config) applyDefaults() Config {
	if c.Interval <= 0 {
		c.Interval = defaultInterval
	}
	if c.BatchBlocks == 0 {
		c.BatchBlocks = defaultBatchBlocks
	}
	return c
}

// ChainSource is the narrow contract the freezer needs from the chain.
// Extracting it into an interface lets unit tests drive the runner with
// a fake that can inject specific block layouts; production callers wire
// a thin adapter around *core.BlockChain.
//
// The accessor split mirrors the freezer's three slice-1 ancient tables:
// `bodies` reads the marshalled `corepb.Block` proto under `b-<num>`,
// `tx_infos` reads the marshalled `corepb.TransactionRet` under
// `tib-<num>`, and `state_roots` reads the 32-byte root under
// `bsr-<block-hash>` (the only hash-keyed one — the runner resolves the
// hash from the block proto on the fly). Returning raw bytes (not parsed
// types) skips a marshal round-trip and keeps the freezer hot loop
// allocation-free.
type ChainSource interface {
	// LatestSolidifiedBlockNum returns the most-recently-solidified
	// block number. The freezer cutoff is (solidified - MarginBlocks);
	// blocks at or above the cutoff stay hot.
	LatestSolidifiedBlockNum() int64

	// DB returns the disk KV store the freezer mutates during the
	// DeleteRange / Compact phases. Writes bypass the in-memory
	// applyBlock buffer because every row the freezer touches is
	// strictly below the solidified line, which the buffer flushes past
	// on every InsertBlock — the rows are already on disk by the time
	// freezing considers them.
	DB() ethdb.KeyValueStore

	// ReadBlockRaw returns the marshalled `corepb.Block` bytes under
	// `b-<num>`, or nil if the row is missing. A nil return is treated
	// as a hard error by the freezer pass: if a solidified block
	// disappeared from Pebble, something else is wrong upstream.
	ReadBlockRaw(number uint64) []byte

	// ReadTransactionInfosRaw returns the marshalled
	// `corepb.TransactionRet` bytes under `tib-<num>`, or nil if absent.
	// Empty blocks (no transactions) still have a row written by
	// applyBlock — see core.WriteTransactionInfosByBlock — so nil only
	// occurs in test fakes; the freezer pass treats nil as "no rows" and
	// appends an empty byte slice to preserve the per-num cardinality of
	// the ancient table.
	ReadTransactionInfosRaw(number uint64) []byte

	// ReadBlockHashByNumber returns the canonical block hash for the
	// given number. Used by the freezer to resolve the `bsr-<hash>`
	// state-root key. Returns the zero hash when the block is unknown.
	ReadBlockHashByNumber(number uint64) tcommon.Hash

	// ReadBlockStateRootRaw returns the raw state-root bytes under
	// `bsr-<hash>`, or nil if absent. Pre-AccountStateRoot fork blocks
	// don't have this row; the freezer pass writes an empty entry in
	// that case so per-num cardinality matches across all three tables.
	ReadBlockStateRootRaw(hash tcommon.Hash) []byte
}

// FreezerStore is the writer surface the runner needs from the freezer.
// Implemented by *freezer.Freezer (via rawdb.AncientWriter) — abstracted
// for the same testability reason as ChainSource: unit tests substitute
// an in-memory implementation that lets them assert on the rows the
// runner appended.
type FreezerStore interface {
	rawdb.AncientReader
	rawdb.AncientWriter
}

// Stats is a thread-safe snapshot of runner progress. Operators consume
// it via Runner.Snapshot; a future metrics layer (Prometheus / OTel) can
// translate it into gauges without the runner having a dep on a metrics
// package.
type Stats struct {
	// FrozenMin is the lowest block number currently in ancient. Slice 1
	// of the freezer spec never truncates the tail, so this is always 0
	// once any pass has succeeded. Kept on the struct for forward
	// compatibility with the eventual TruncateTail support.
	FrozenMin uint64
	// FrozenMax is the highest block number currently in ancient,
	// inclusive. Equivalent to AncientCount("bodies") - 1 (or absent if
	// the freezer is empty).
	FrozenMax uint64
	// HasFrozen distinguishes "FrozenMax = 0 because nothing is frozen"
	// from "FrozenMax = 0 because block #0 is the only frozen block".
	// Avoids the sentinel-value ambiguity around an empty ancient.
	HasFrozen bool
	// BlocksFrozen is the cumulative count of blocks moved into ancient
	// by this runner since it started.
	BlocksFrozen uint64
	// PassesCompleted is the count of completed (no-op + non-no-op)
	// pass iterations.
	PassesCompleted uint64
	// LastPassAt is the wall-clock time the most recent pass started.
	// Zero value if no pass has run yet.
	LastPassAt time.Time
	// LastPassDuration is the wall-clock duration of the most recent
	// pass. p99 latency dashboards layer over this.
	LastPassDuration time.Duration
	// PebbleSizeAfter is an approximate footprint of the still-hot
	// `b-<num>` + `tib-<num>` rows after the most recent pass. Sampled
	// via an iterator pass on the prefix; expensive enough that the
	// runner samples only at the end of each pass, not per-block.
	PebbleSizeAfter uint64
}

// Runner is the freezer's Lifecycle service. Construct with New, register
// the returned value with the node, and the loop fires on its own timer
// until Stop returns.
type Runner struct {
	chain   ChainSource
	freezer FreezerStore
	cfg     Config

	quit chan struct{}
	done chan struct{}
	once sync.Once

	// stats fields are atomics so Snapshot is lock-free against the running
	// goroutine.
	blocksFrozen     atomic.Uint64
	passesCompleted  atomic.Uint64
	lastPassUnixNano atomic.Int64
	lastPassDuration atomic.Int64 // nanoseconds
	pebbleSizeAfter  atomic.Uint64

	// reconciled guards the once-per-process crash-leftover sweep in
	// onePass. See the reconciliation block there.
	reconciled atomic.Bool

	// pauseCtx wraps the quit channel for callers that prefer a Context
	// API. Sealed only when Stop is called.
	pauseCtx    context.Context
	pauseCancel context.CancelFunc
}

// New constructs a Runner against the supplied chain source and freezer
// store. The Config is shallow-copied and zero fields are defaulted; the
// caller's struct stays untouched.
//
// Returns a nil runner when freezer == nil — callers (cmd/gtron) use this
// to skip Lifecycle registration when the freezer is disabled at the CLI
// level. Production wiring: if cfg.Enabled == false, cmd/gtron simply
// doesn't open a freezer and never calls New.
func New(chain ChainSource, fz FreezerStore, cfg Config) *Runner {
	if fz == nil {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Runner{
		chain:       chain,
		freezer:     fz,
		cfg:         cfg.applyDefaults(),
		quit:        make(chan struct{}),
		done:        make(chan struct{}),
		pauseCtx:    ctx,
		pauseCancel: cancel,
	}
}

// Start implements node.Lifecycle. Launches the freezing goroutine. If
// the runner is disabled (cfg.Enabled == false), it still takes the
// Lifecycle slot — Stop completes immediately and Snapshot reports zero
// counters — so an operator can disable freezing without rebuilding
// gtron's wiring graph.
func (r *Runner) Start() error {
	if !r.cfg.Enabled {
		log.Info("Freezer runner registered but disabled (Config.Enabled=false)")
		close(r.done)
		r.pauseCancel()
		return nil
	}
	go r.loop()
	log.Info("Freezer runner started",
		"interval", r.cfg.Interval,
		"margin", r.cfg.MarginBlocks,
		"batch", r.cfg.BatchBlocks)
	return nil
}

// Stop implements node.Lifecycle. Signals the loop to exit and waits for
// the in-flight pass (if any) to finish. Idempotent: safe to call from
// multiple goroutines / multiple times.
func (r *Runner) Stop() error {
	r.once.Do(func() {
		close(r.quit)
		r.pauseCancel()
	})
	<-r.done
	log.Info("Freezer runner stopped",
		"blocksFrozen", r.blocksFrozen.Load(),
		"passes", r.passesCompleted.Load())
	return nil
}

// Snapshot returns a thread-safe copy of the runner's current counters.
// Safe to call from any goroutine — every field is read from an atomic.
func (r *Runner) Snapshot() Stats {
	stats := Stats{
		BlocksFrozen:     r.blocksFrozen.Load(),
		PassesCompleted:  r.passesCompleted.Load(),
		LastPassDuration: time.Duration(r.lastPassDuration.Load()),
		PebbleSizeAfter:  r.pebbleSizeAfter.Load(),
	}
	if t := r.lastPassUnixNano.Load(); t > 0 {
		stats.LastPassAt = time.Unix(0, t)
	}
	// FrozenMin / FrozenMax come straight from the ancient store so the
	// caller always sees the canonical position even if a concurrent
	// pass is appending. Mismatches across the three tables would be
	// caught by AncientCount returning errors (handled lazily here).
	count, err := r.freezer.AncientCount(rawdbAncientBlocks)
	if err == nil && count > 0 {
		stats.HasFrozen = true
		stats.FrozenMax = count - 1
		// FrozenMin = 0 until TruncateTail support arrives.
	}
	return stats
}

// OnePass runs a single freezing pass synchronously and returns the
// number of blocks moved into ancient. Exported so tests can drive the
// pass deterministically without spinning up the loop.
//
// Returns nil error on success, even on no-op passes (e.g. chain hasn't
// produced enough blocks above the margin yet). Per-pass errors leave
// the freezer in a consistent state thanks to ModifyAncients' atomic
// rollback; the next pass simply retries.
func (r *Runner) OnePass() (uint64, error) {
	start := time.Now()
	defer func() {
		r.lastPassUnixNano.Store(start.UnixNano())
		r.lastPassDuration.Store(int64(time.Since(start)))
		r.passesCompleted.Add(1)
	}()

	if !r.cfg.Enabled {
		return 0, nil
	}

	solid := r.chain.LatestSolidifiedBlockNum()
	if solid <= 0 {
		// Pre-genesis or chain not yet producing — nothing to freeze.
		return 0, nil
	}
	if uint64(solid) < r.cfg.MarginBlocks {
		// Chain hasn't accumulated more than `MarginBlocks` solidified
		// blocks yet, so every block is still inside the reorg-safe
		// window.
		return 0, nil
	}
	freezeTo := uint64(solid) - r.cfg.MarginBlocks // inclusive upper bound

	// Resume from the freezer's own canonical position. Reading
	// AncientCount on every pass means we never need to persist a
	// separate cursor — the freezer table itself is the source of truth.
	freezeFromN, err := r.freezer.AncientCount(rawdbAncientBlocks)
	if err != nil {
		return 0, err
	}
	// Startup reconciliation (once per process). A crash that landed
	// between Phase 2 (ancient Sync) and Phase 3 (Pebble DeleteRange) of a
	// prior pass leaves blocks [x, freezeFromN) durably in ancient but
	// with their hot `b-`/`tib-` rows still in Pebble. No later pass would
	// ever revisit them — passes only delete the range they just froze
	// ([freezeFromN, cap)) — so the frozen-but-undeleted rows would leak
	// disk space forever. Detect the condition cheaply (the highest frozen
	// block's hot row still present) and sweep [0, freezeFromN) once. The
	// expensive DeleteRange+Compact only runs when a crash actually left
	// rows behind; the clean-restart path pays a single Get.
	if freezeFromN > 0 && !r.reconciled.Swap(true) {
		if len(r.chain.ReadBlockRaw(freezeFromN-1)) > 0 {
			leftoverHi := freezeFromN - 1
			if err := rawdb.DeleteFrozenBlockRange(r.chain.DB(), 0, leftoverHi); err != nil {
				return 0, err
			}
			start, limit := rawdb.BlockRangeBounds(0, leftoverHi)
			if err := r.chain.DB().Compact(start, limit); err != nil {
				log.Warn("Freezer: crash-leftover compact failed (rows still deleted)",
					"to", leftoverHi, "err", err)
			}
			log.Info("Freezer: swept crash-leftover hot rows", "upTo", leftoverHi)
		}
	}

	if freezeTo < freezeFromN {
		return 0, nil
	}
	// The freezer pass works in half-open [freezeFromN, capExclusive)
	// blocks. Cap at BatchBlocks so a multi-day backlog can't tie up
	// one pass.
	capExclusive := freezeTo + 1
	if r.cfg.BatchBlocks > 0 && capExclusive-freezeFromN > r.cfg.BatchBlocks {
		capExclusive = freezeFromN + r.cfg.BatchBlocks
	}
	if capExclusive <= freezeFromN {
		return 0, nil
	}

	// Phase 1: append every block's three blobs to ancient atomically.
	// ModifyAncients rolls every table back to its pre-call head if the
	// callback returns an error, so a partial pass never leaves orphan
	// rows in one table.
	if _, err := r.freezer.ModifyAncients(func(op rawdb.AncientWriteOp) error {
		for n := freezeFromN; n < capExclusive; n++ {
			blockRaw := r.chain.ReadBlockRaw(n)
			if len(blockRaw) == 0 {
				return errMissingBlock(n)
			}
			if err := op.AppendRaw(rawdbAncientBlocks, n, blockRaw); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdbAncientTxInfos, n, r.chain.ReadTransactionInfosRaw(n)); err != nil {
				return err
			}
			// State-root row is hash-keyed; resolve via the block proto.
			// Pre-AccountStateRoot fork blocks have no row, in which case
			// ReadBlockStateRootRaw returns nil — append nil so the
			// ancient table's per-num cardinality stays aligned with
			// `bodies` / `tx_infos`. Empty entries decode back to the
			// zero hash via the slice-2 read path, which matches the
			// pre-freezer Pebble miss → zero-hash behavior.
			hash := r.chain.ReadBlockHashByNumber(n)
			stateRoot := r.chain.ReadBlockStateRootRaw(hash)
			if err := op.AppendRaw(rawdbAncientStateRoots, n, stateRoot); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return 0, err
	}

	// Phase 2: explicit fsync. This is the durability barrier, NOT
	// belt-and-braces: freezerTableBatch.commit() only fsyncs periodically
	// (every ~30s past freezerTableFlushThreshold), so without this call a
	// Phase-3 Pebble delete could outrun the ancient write to stable
	// storage — exactly the ordering the crash-recovery contract forbids.
	// Do not remove.
	if err := r.freezer.Sync(); err != nil {
		return 0, err
	}

	// Phase 3: delete the now-frozen hot rows from Pebble. The two
	// DeleteRange calls cover `b-<num>` and `tib-<num>`; hash-keyed
	// `bh-<hash>` / `bsr-<hash>` stay hot per the slice-1 design.
	frozenHi := capExclusive - 1
	if err := rawdb.DeleteFrozenBlockRange(r.chain.DB(), freezeFromN, frozenHi); err != nil {
		return 0, err
	}

	// Phase 4: compact the freed range. Pebble turns DeleteRange into
	// range tombstones, which are O(1) on the write path but only reclaim
	// space when their containing SSTables get compacted. Explicit
	// Compact triggers a synchronous compaction so the operator sees the
	// datadir shrink without waiting for background compaction to roll
	// through (which can take hours on a healthy LSM).
	start1, limit1 := rawdb.BlockRangeBounds(freezeFromN, frozenHi)
	if err := r.chain.DB().Compact(start1, limit1); err != nil {
		// Compaction failure is non-fatal: the rows are deleted from a
		// correctness standpoint (range tombstones above them), the only
		// loss is that the operator's datadir won't shrink as quickly.
		// Log and proceed.
		log.Warn("Freezer: compact failed (rows still deleted)",
			"from", freezeFromN, "to", frozenHi, "err", err)
	}

	// Phase 5: update stats. PebbleSizeAfter is sampled by an iterator
	// pass on the still-hot `b-` prefix — cheap because after a successful
	// freeze the prefix only holds the post-margin window.
	frozen := capExclusive - freezeFromN
	r.blocksFrozen.Add(frozen)
	r.pebbleSizeAfter.Store(pebbleBlockNamespaceSize(r.chain.DB()))
	return frozen, nil
}

// loop is the goroutine. Fires once on Start so a fresh-install backlog
// begins draining without waiting an interval, then ticks on
// cfg.Interval until quit is signalled.
func (r *Runner) loop() {
	defer close(r.done)

	if frozen, err := r.OnePass(); err != nil {
		log.Warn("Freezer: initial pass failed", "err", err)
	} else if frozen > 0 {
		log.Info("Freezer: initial pass frozen", "blocks", frozen)
	}

	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if frozen, err := r.OnePass(); err != nil {
				log.Warn("Freezer: pass failed", "err", err)
			} else if frozen > 0 {
				log.Info("Freezer: pass frozen", "blocks", frozen)
			}
		case <-r.quit:
			return
		}
	}
}

// pebbleBlockNamespaceSize iterates `b-` rows and returns the cumulative
// key+value bytes. Approximate — Pebble's on-disk footprint after
// compression and block-overhead deduction is smaller — but accurate
// enough as an unbounded-growth detector. Called once at the end of each
// pass; cost is O(remaining-block-rows), which is bounded by
// MarginBlocks + BatchBlocks under steady state.
func pebbleBlockNamespaceSize(db ethdb.Iteratee) uint64 {
	it := db.NewIterator(blockNamespacePrefix, nil)
	defer it.Release()
	var size uint64
	for it.Next() {
		size += uint64(len(it.Key()) + len(it.Value()))
	}
	return size
}

// blockNamespacePrefix is the `b-` prefix mirrored from
// core/rawdb/schema.go. Duplicated as a package-private constant so the
// freezer package doesn't have to reach into rawdb's private symbol set.
// rawdb's `blockPrefix` is package-private and the slice-1 schema is
// stable enough that mirroring is safer than exposing it. A future slice
// that changes the prefix must update both places.
var blockNamespacePrefix = []byte("b-")

// rawdbAncient* mirrors core/rawdb's per-table ancient name constants.
// They are package-private in rawdb because each accessor file owns its
// own name; the runner needs all three. Mirrored here rather than
// exported because the table set is a runner-side concern (the freezer
// is content-agnostic).
const (
	rawdbAncientBlocks     = "bodies"
	rawdbAncientTxInfos    = "tx_infos"
	rawdbAncientStateRoots = "state_roots"
)

// FreezerTableSet returns the table-name/config map the runner expects
// when opening the freezer. Used by cmd/gtron in the NewFreezer call so
// the slice 3 wiring doesn't have to coin its own list — it must stay
// synced with the runner's table-name constants above.
//
// Compression is Snappy for `bodies` (proto blobs compress well) and
// `tx_infos`; raw bytes for `state_roots` because 32-byte payloads
// already sit below Snappy's per-row overhead.
func FreezerTableSet() map[string]rawdbfreezer.TableConfig {
	return map[string]rawdbfreezer.TableConfig{
		rawdbAncientBlocks:     {NoSnappy: false},
		rawdbAncientTxInfos:    {NoSnappy: false},
		rawdbAncientStateRoots: {NoSnappy: true},
	}
}

// errMissingBlock signals a solidified block missing from Pebble — a
// hard invariant violation that should never happen in a healthy node.
// Wrapped as a typed error so test assertions can distinguish it from a
// generic freezer-write failure.
func errMissingBlock(n uint64) error {
	return &MissingBlockError{Number: n}
}

// MissingBlockError is returned by OnePass when a solidified block's
// `b-<num>` row is missing from Pebble. Surfaces an actionable detail
// (the block number) rather than a generic error string so an operator
// reading logs can correlate with the rest of their state.
type MissingBlockError struct {
	Number uint64
}

func (e *MissingBlockError) Error() string {
	return "freezer: solidified block missing from KV store (num=" + itoa(e.Number) + ")"
}

// itoa avoids pulling fmt/strconv for the one error-format call site.
// Bounded loop — block numbers fit in 20 digits.
func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// Compile-time assertion that *Runner satisfies node.Lifecycle. Avoids a
// dep on the node package; the interface is two methods (Start + Stop)
// and Go's structural typing catches drift at the registration call site
// in cmd/gtron.
var _ interface {
	Start() error
	Stop() error
} = (*Runner)(nil)

// ErrRunnerDisabled is a sentinel returned by callers that want to
// signal "the runner was constructed but is operating in no-op mode".
// Slice 3 doesn't surface this anywhere internally — kept exported for a
// future RPC layer that wants to differentiate "no freezer attached"
// from "freezer attached but cfg.Enabled=false".
var ErrRunnerDisabled = errors.New("freezer runner disabled")
