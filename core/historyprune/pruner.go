// Package historyprune is the background pruner for the State History
// Index (SHI) when the chain runs in "full" retention mode.
//
// Slice 5 of the SHI plan introduces this loop. The pruner:
//
//  1. Computes a per-pass cutoff from the chain's latest solidified block
//     and the operator-configured retention window.
//  2. Range-deletes the per-block forward rows (sh-m-, sh-a-, sh-s-) for
//     every block strictly below the cutoff that hasn't been pruned yet.
//  3. Periodically sweeps the inverse-index rows (sh-i-a-, sh-i-s-) for
//     any embedded blockNum below the cutoff. Those keys are addr-first,
//     so a range delete won't catch them — the sweep is a full prefix
//     scan capped by a batch limit so one pass can't hog Pebble.
//  4. Persists progress in the HistoryConfig sentinel (FirstBlock) so a
//     restart picks up where it left off without rescanning the keyspace.
//
// Archive mode bypasses this package entirely — `cmd/gtron` only
// registers the lifecycle when the resolved mode is "full".
package historyprune

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"

	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/rawdb"
	historypb "github.com/tronprotocol/go-tron/proto/core/historystate"
)

var log = gtronlog.NewModule("core/historyprune")

// Defaults applied when PrunerConfig fields are zero. They are conservative
// enough to keep pruner IO under the chain's apply-block bandwidth while
// still draining a stuck backlog within a few minutes.
const (
	defaultInterval               = time.Minute
	defaultBatchSize              = 5_000
	defaultInversePassEvery       = 100  // every Nth pass, run an inverse-index sweep
	defaultInverseBatchSize       = 10_000
	defaultInverseFreshIfWithin   = time.Hour // also run inverse sweep if it's been this long
)

// ChainSource is the narrow interface the pruner needs from a
// `*core.BlockChain`. Extracted into a contract so unit tests can drive
// the pruner with a stub that lets the test set the solidified block and
// underlying KV store explicitly.
type ChainSource interface {
	// DB returns the disk KV store the pruner mutates. Pruner writes
	// bypass the in-memory applyBlock buffer because every row it touches
	// is strictly below the solidified line, which is already past the
	// buffer flush boundary.
	DB() ethdb.KeyValueStore
	// LatestSolidifiedBlockNum returns the most-recently-solidified block
	// number. The pruner cutoff is (solidified - window); rows for any
	// block below the cutoff are eligible for deletion.
	LatestSolidifiedBlockNum() int64
}

// PrunerConfig parametrises the per-pruner instance. Zero-valued fields
// fall back to package defaults so callers can populate only the knobs
// they care about (typically just Window).
type PrunerConfig struct {
	// Window is the number of recent blocks to retain. Rows for blocks
	// below (solidified - Window) become eligible for deletion. Setting
	// Window = 0 disables pruning entirely — semantically equivalent to
	// running in archive mode but reached via a different code path.
	// cmd/gtron uses Window > 0 to gate registration, so 0 here is a
	// defensive guard that turns the loop into a no-op rather than an
	// accidental "delete everything".
	Window uint64

	// Interval is the period between full prune passes. Default
	// defaultInterval (1 minute) keeps the loop's CPU cost well under
	// 0.1% of one core in steady state.
	Interval time.Duration

	// BatchSize bounds the number of per-block rows deleted in a single
	// pass. The pass deletes whole-block sh-m-/sh-a-/sh-s- ranges per
	// block via DeleteRange, so the unit is "blocks" rather than rows.
	// Default defaultBatchSize.
	BatchSize int

	// InversePassEvery runs an inverse-index sweep every Nth pass.
	// Combined with InverseFreshIfWithin so the sweep also fires on a
	// time bound when N is very large or BatchSize is low. Default
	// defaultInversePassEvery.
	InversePassEvery int

	// InverseBatchSize is the row-level cap for one inverse-index sweep
	// pass. Higher = fewer passes, more IO per pass. Default
	// defaultInverseBatchSize.
	InverseBatchSize int

	// InverseFreshIfWithin forces an inverse-index sweep when the last
	// one was longer ago than this, regardless of pass-counter. Acts as
	// a fail-safe so the index can't go stale during a long idle period.
	// Default defaultInverseFreshIfWithin.
	InverseFreshIfWithin time.Duration
}

// applyDefaults fills in zero fields with package defaults. Returns a
// copy so the caller's struct stays untouched (mirrors the "don't
// surprise the operator" rule from other Lifecycle setups).
func (c PrunerConfig) applyDefaults() PrunerConfig {
	if c.Interval <= 0 {
		c.Interval = defaultInterval
	}
	if c.BatchSize <= 0 {
		c.BatchSize = defaultBatchSize
	}
	if c.InversePassEvery <= 0 {
		c.InversePassEvery = defaultInversePassEvery
	}
	if c.InverseBatchSize <= 0 {
		c.InverseBatchSize = defaultInverseBatchSize
	}
	if c.InverseFreshIfWithin <= 0 {
		c.InverseFreshIfWithin = defaultInverseFreshIfWithin
	}
	return c
}

// Stats is a thread-safe snapshot of pruner progress. Slice 5 exposes
// this via Pruner.Stats; future metrics layers (Prometheus / OTel) can
// translate it without the pruner having a dep on a metrics package.
type Stats struct {
	// LastPrunedBlock is the highest blockNum the pruner has cleared.
	// Equivalent to FirstBlock-1 in the on-disk HistoryConfig (FirstBlock
	// is the lowest block that still has rows; LastPrunedBlock is the
	// last one we have already discarded).
	LastPrunedBlock uint64
	// BlocksPruned is the cumulative count of blocks whose per-block
	// rows were range-deleted by this pruner since it started.
	BlocksPruned uint64
	// InverseRowsPruned is the cumulative count of inverse-index rows
	// removed by sweeps.
	InverseRowsPruned uint64
	// Passes is the count of completed pass iterations. Each call to
	// PrunePass — including no-op passes — increments this.
	Passes uint64
	// LastPassDuration is the wall-clock duration of the most recent
	// successful pass. p99 latency metrics layer over this.
	LastPassDuration time.Duration
	// LastInverseSweepAt is the wall-clock time of the most recent
	// inverse-index sweep, or the zero value if none has happened.
	LastInverseSweepAt time.Time
	// HistoryDiskBytes is the approximate sh-* size on disk, sampled at
	// the end of the most recent pass.
	HistoryDiskBytes uint64
}

// Pruner is the State History Index background sweeper. Construct with
// New, register the returned value as a node.Lifecycle, and the loop
// fires on its own timer until Stop returns.
type Pruner struct {
	chain ChainSource
	cfg   PrunerConfig

	quit chan struct{}
	done chan struct{}
	once sync.Once

	// stats holds atomic counters readable from any goroutine via
	// Pruner.Stats. lastPrunedBlock is the source of truth for "next
	// block to scan"; the pruner rereads HistoryConfig.FirstBlock on
	// Start and writes it back at the end of every successful pass.
	mu                 sync.Mutex
	lastPrunedBlock    uint64
	blocksPruned       atomic.Uint64
	inverseRowsPruned  atomic.Uint64
	passes             atomic.Uint64
	lastPassDuration   atomic.Int64 // nanoseconds
	lastInverseAt      atomic.Int64 // unix nanos
	historyDiskBytes   atomic.Uint64
	passesSinceInverse int
}

// New constructs a Pruner against the given chain source. Returns nil
// when cfg.Window == 0 — callers (cmd/gtron) treat that as "archive
// mode, no pruning needed" and skip registration. The nil-vs-stub
// distinction lets tests pass an explicit zero-window config to assert
// the no-op behavior without needing a fake ChainSource.
func New(chain ChainSource, cfg PrunerConfig) *Pruner {
	cfg = cfg.applyDefaults()
	return &Pruner{
		chain: chain,
		cfg:   cfg,
		quit:  make(chan struct{}),
		done:  make(chan struct{}),
	}
}

// Start implements node.Lifecycle. It loads the persisted last-pruned
// cursor (HistoryConfig.FirstBlock - 1) and launches the prune loop.
// Errors from the initial HistoryConfig read are logged but non-fatal —
// a missing config means "fresh archive, nothing to prune yet".
func (p *Pruner) Start() error {
	if p.cfg.Window == 0 {
		// Defensive: a zero window means the pruner is effectively
		// inert. We still take the Lifecycle slot so Stop() doesn't
		// hang and metrics readers see zero counts.
		log.Info("History pruner registered with window=0 (no-op)")
		close(p.done)
		return nil
	}

	db := p.chain.DB()
	cfg, err := rawdb.ReadHistoryConfig(db)
	switch {
	case errors.Is(err, rawdb.ErrHistoryConfigAbsent):
		// Brand-new chain or fresh archive — start from genesis. We do
		// NOT seed the config here; the writer side (slice 2's
		// AccumulateHistory) is the natural authority for FirstBlock /
		// LastBlock. The pruner only updates FirstBlock once it has
		// removed at least one block's rows.
		p.setLastPrunedBlock(0)
	case err != nil:
		log.Warn("History pruner: failed to read history config (starting from 0)", "err", err)
		p.setLastPrunedBlock(0)
	default:
		// FirstBlock is the lowest block whose rows still exist. The
		// pruner's cursor "lastPrunedBlock" is the last block we
		// removed — exactly FirstBlock - 1 when FirstBlock > 0.
		if cfg.FirstBlock > 0 {
			p.setLastPrunedBlock(cfg.FirstBlock - 1)
		}
	}

	go p.loop()
	log.Info("History pruner started",
		"window", p.cfg.Window,
		"interval", p.cfg.Interval,
		"batch", p.cfg.BatchSize,
		"lastPruned", p.snapshotLastPrunedBlock())
	return nil
}

// Stop implements node.Lifecycle. Signals the loop to exit and waits for
// the in-flight pass (if any) to finish. Returns nil; pruner errors are
// logged but never propagated up — a failed prune pass is recoverable on
// the next interval.
func (p *Pruner) Stop() error {
	p.once.Do(func() { close(p.quit) })
	<-p.done
	log.Info("History pruner stopped",
		"blocksPruned", p.blocksPruned.Load(),
		"inverseRowsPruned", p.inverseRowsPruned.Load())
	return nil
}

// Stats returns a best-effort snapshot of pruner progress. Individual
// fields are read atomically but the snapshot as a whole is NOT
// consistent — a concurrent PrunePass may advance some counters between
// reads (e.g. BlocksPruned and LastPrunedBlock can momentarily disagree).
// Suitable for metrics display; do not assert cross-field invariants on
// the result.
func (p *Pruner) Stats() Stats {
	var inverseAt time.Time
	if t := p.lastInverseAt.Load(); t > 0 {
		inverseAt = time.Unix(0, t)
	}
	return Stats{
		LastPrunedBlock:    p.snapshotLastPrunedBlock(),
		BlocksPruned:       p.blocksPruned.Load(),
		InverseRowsPruned:  p.inverseRowsPruned.Load(),
		Passes:             p.passes.Load(),
		LastPassDuration:   time.Duration(p.lastPassDuration.Load()),
		LastInverseSweepAt: inverseAt,
		HistoryDiskBytes:   p.historyDiskBytes.Load(),
	}
}

func (p *Pruner) loop() {
	defer close(p.done)
	ticker := time.NewTicker(p.cfg.Interval)
	defer ticker.Stop()

	// Run one pass immediately on start so a node that restarts with a
	// massive backlog (operator shut down a full-mode node for a week,
	// then booted it back up) starts draining without waiting an
	// interval. The Interval ticker takes over from there.
	if err := p.PrunePass(); err != nil {
		log.Warn("History pruner: initial pass failed", "err", err)
	}

	for {
		select {
		case <-ticker.C:
			if err := p.PrunePass(); err != nil {
				log.Warn("History pruner: pass failed", "err", err)
			}
		case <-p.quit:
			return
		}
	}
}

// PrunePass runs a single prune iteration synchronously. Exported so
// tests can drive deterministic semantics without spinning up the loop.
// Returns nil on success; per-pass errors leave the persisted cursor and
// counters consistent so the next pass simply retries.
func (p *Pruner) PrunePass() error {
	start := time.Now()
	defer func() {
		p.lastPassDuration.Store(int64(time.Since(start)))
		p.passes.Add(1)
	}()

	solidified := p.chain.LatestSolidifiedBlockNum()
	if solidified <= 0 {
		// Pre-genesis or chain not yet producing — nothing to prune.
		return nil
	}
	if uint64(solidified) <= p.cfg.Window {
		// Chain hasn't accumulated more than `window` blocks yet, so
		// every block of history is still inside the retention window.
		return nil
	}
	cutoff := uint64(solidified) - p.cfg.Window

	lastPruned := p.snapshotLastPrunedBlock()
	if cutoff <= lastPruned+1 {
		// Nothing new below the cutoff since the last pass. Inverse
		// sweep can still run if we're due — fall through.
	} else {
		// Per-block prune: range-delete sh-m-/sh-a-/sh-s- for
		// (lastPruned, cutoff). Cap by BatchSize so a multi-week
		// backlog can't tie up the loop.
		lo := lastPruned + 1
		hi := cutoff - 1
		if uint64(p.cfg.BatchSize) > 0 && hi-lo+1 > uint64(p.cfg.BatchSize) {
			hi = lo + uint64(p.cfg.BatchSize) - 1
		}
		if err := rawdb.PruneHistoryBlockRange(p.chain.DB(), lo, hi); err != nil {
			return err
		}
		blocksThisPass := hi - lo + 1
		p.setLastPrunedBlock(hi)
		p.blocksPruned.Add(blocksThisPass)
		if err := p.persistFirstBlock(hi + 1); err != nil {
			// Persistence failure isn't fatal — the in-memory cursor
			// has advanced, so the next pass simply re-derives the
			// same starting point if we crash before persisting.
			log.Warn("History pruner: failed to persist FirstBlock", "err", err, "firstBlock", hi+1)
		}
	}

	// Inverse-index sweep: run every InversePassEvery passes, or when
	// the last sweep was longer ago than InverseFreshIfWithin.
	p.passesSinceInverse++
	dueByCount := p.passesSinceInverse >= p.cfg.InversePassEvery
	lastInverse := p.lastInverseAt.Load()
	dueByTime := lastInverse > 0 && time.Since(time.Unix(0, lastInverse)) >= p.cfg.InverseFreshIfWithin
	if dueByCount || dueByTime || lastInverse == 0 {
		if err := p.runInverseSweep(cutoff); err != nil {
			return err
		}
		p.passesSinceInverse = 0
		p.lastInverseAt.Store(time.Now().UnixNano())

		// Sample disk size on the same (infrequent) cadence as the
		// inverse sweep — HistoryDiskSize is a full five-prefix iterator
		// scan (~hundreds of K rows on a 27k-block window), so calling it
		// every pass would compete with Pebble compaction. Co-locating it
		// with the inverse sweep bounds it to ~once per InverseFreshIfWithin.
		p.historyDiskBytes.Store(rawdb.HistoryDiskSize(p.chain.DB()))
	}
	return nil
}

// maxInverseDrainRounds bounds how many batched delete rounds a single
// inverse sweep issues per namespace. Without it, a backlog larger than
// InverseBatchSize/2 would linger until the next InversePassEvery tick
// (~100 min default); with it, a sweep drains up to maxInverseDrainRounds ×
// (InverseBatchSize/2) rows before yielding, catching up in O(1) sweeps
// under heavy write load while keeping each underlying delete call's I/O
// rate unchanged.
const maxInverseDrainRounds = 4

func (p *Pruner) runInverseSweep(cutoff uint64) error {
	// Two namespaces, same batch budget — split so a busy addr-inverse
	// can't starve the slot-inverse sweep (and vice versa).
	half := p.cfg.InverseBatchSize / 2
	if half <= 0 {
		half = p.cfg.InverseBatchSize
	}
	var total int
	for _, prune := range []func(uint64, int) (int, bool, error){
		func(c uint64, n int) (int, bool, error) { return rawdb.PruneAddrInverseBelow(p.chain.DB(), c, n) },
		func(c uint64, n int) (int, bool, error) { return rawdb.PruneSlotInverseBelow(p.chain.DB(), c, n) },
	} {
		for round := 0; round < maxInverseDrainRounds; round++ {
			deleted, more, err := prune(cutoff, half)
			total += deleted
			if err != nil {
				return err
			}
			if !more {
				break
			}
		}
	}
	p.inverseRowsPruned.Add(uint64(total))
	return nil
}

// persistFirstBlock updates the on-disk HistoryConfig so a restart picks
// up at the right cursor. Reads-then-writes; missing config seeds a
// fresh sentinel with the prune mode set to "full" (the only mode that
// reaches this code path).
func (p *Pruner) persistFirstBlock(firstBlock uint64) error {
	db := p.chain.DB()
	cfg, err := rawdb.ReadHistoryConfig(db)
	if errors.Is(err, rawdb.ErrHistoryConfigAbsent) {
		cfg = &historypb.HistoryConfig{
			Mode:        uint32(0), // 0 = full
			PruneWindow: p.cfg.Window,
			SchemaVer:   rawdb.HistorySchemaVersion,
		}
	} else if err != nil {
		return err
	}
	cfg.FirstBlock = firstBlock
	return rawdb.WriteHistoryConfig(db, cfg)
}

func (p *Pruner) setLastPrunedBlock(n uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastPrunedBlock = n
}

func (p *Pruner) snapshotLastPrunedBlock() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastPrunedBlock
}
