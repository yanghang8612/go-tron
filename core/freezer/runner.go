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
//     closer to the live head than the configured margin), then cap it at
//     the verified hash-bound `StageFinish` row when that stage exists.
//  3. `freezeFrom = freezer.AncientCount("bodies")` — the freezer's own
//     position is the canonical resume point; all three slice-1 tables
//     advance in lockstep via ModifyAncients, so `bodies` is enough.
//  4. Cap the per-pass range at `freezeFrom + cfg.BatchBlocks`.
//  5. Read each block's raw KV bytes (block proto, tx-infos-per-block,
//     state-root-by-hash-resolved-via-block-hash) and append them inside
//     a single ModifyAncients call. The freezer rolls back atomically on
//     error so a partial pass leaves no orphan ancient rows.
//  6. fsync the ancient (`freezer.Sync()`).
//  7. Delete the now-frozen `b-<num>`, `tib-<num>`, and `bsr-<hash>` rows
//     from Pebble. Direct V2 also publishes the immutable transaction index
//     and deletes its covered `tx-<hash>` duplicates in the same heavy-work
//     lease. `bh-<hash>` and `ti-<txid>` follow their own verified cold-index
//     and receipt-retention lifecycles.
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
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	tcommon "github.com/tronprotocol/go-tron/common"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

var log = gtronlog.NewModule("core/freezer")

// errRunnerStopping is internal control flow, not an operational failure. A
// pass returns it only at a boundary where either no freezer mutation has been
// committed, ModifyAncients has rolled the mutation back, or the ancient rows
// have already crossed the explicit fsync barrier. The loop suppresses warning
// logs for this error and exits.
var errRunnerStopping = errors.New("freezer runner stopping")
var errHeavyMaintenanceDeferred = errors.New("freezer: heavy maintenance deferred for active sync")

// Defaults applied when Config fields are zero. They mirror the spec's
// recommended production values: 30-second cadence, 128-block margin
// (keeps us well below the PBFT solidification line under steady-state
// 27-SR DPoS), and 30 000 blocks per pass (large enough to drain a
// fresh-install backlog in under an hour, small enough that one pass
// can't dominate Pebble's compaction queue).
const (
	defaultInterval                     = 30 * time.Second
	defaultMarginBlocks                 = uint64(128)
	defaultBatchBlocks                  = uint64(30_000)
	defaultV2FrameBlocks                = uint32(64)
	defaultV2SegmentBlocks              = uint64(65_536)
	defaultV2CatchupTimeBudget          = 45 * time.Second
	defaultV2CatchupMaxSegments         = uint64(16)
	defaultTxIndexPrefixBits            = uint32(20)
	defaultCatchupMaintenanceInterval   = 5 * time.Minute
	defaultHeavyMaintenanceStartupDelay = time.Minute
	defaultHeavyMaintenanceErrorBackoff = 30 * time.Minute
	heavyMaintenancePollInterval        = 250 * time.Millisecond
	txIndexDeleteBatchBytes             = 16 << 20
	defaultMetricsNamespace             = "chain/freezer/"
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

	// V2Enabled incrementally promotes complete V1 ranges into seekable Zstd
	// segments. Production defaults to enabled; explicit Config literals stay
	// opt-in unless they set this field.
	V2Enabled bool
	// DirectV2 lets a fresh sync keep an incomplete segment in canonical Pebble
	// and publish each complete segment straight to V2, avoiding the normal V1
	// append-and-rewrite path. It is used only when the store proves V1 has no
	// live suffix; otherwise the runner safely falls back to V1.
	DirectV2 bool

	// V2PromotionAllowed lets the sync coordinator defer compression during an
	// I/O-sensitive phase without disabling the freezer itself.
	V2PromotionAllowed func() bool
	// SyncActive and CatchupMaintenanceInterval turn V2 promotion and immutable
	// transaction-index work into a bounded catch-up duty cycle. Base freezing
	// remains active so Pebble block rows cannot grow without bound.
	SyncActive                 func() bool
	CatchupMaintenanceInterval time.Duration
	// HeavyMaintenanceStartupDelay gives peer discovery enough time to expose
	// an active historical sync before the first optional compression/index job.
	HeavyMaintenanceStartupDelay time.Duration
	// HeavyMaintenanceErrorBackoff prevents a persistent immutable-build error
	// from rebuilding the same large segment on every scheduler tick. Zero
	// keeps immediate retries for explicit test/tool configurations.
	HeavyMaintenanceErrorBackoff time.Duration
	// HeavyWorkGate prevents optional freezer work from overlapping snapshot
	// history/accessor construction in the same process.
	HeavyWorkGate *maintenance.HeavyWorkGate

	V2FrameBlocks   uint32
	V2SegmentBlocks uint64
	// V2CatchupTimeBudget and V2CatchupMaxSegments bound one admitted V2
	// maintenance lease. The runner sizes the batch from the current backlog;
	// the freezer checks the soft time budget only between crash-safe segment
	// publications. This lets fast historical sync convert several small
	// segments per catch-up slot without letting one pass run unbounded.
	V2CatchupTimeBudget  time.Duration
	V2CatchupMaxSegments uint64

	// TransactionIndexEnabled moves V2-covered tx-* rows into immutable
	// fingerprint runs. Publication is completed and verified before hot rows
	// are removed, so readers always retain at least one valid lookup path.
	TransactionIndexEnabled    bool
	TransactionIndexPrefixBits uint32
	// ExternalizeV2ReceiptLogs removes duplicate TransactionInfo.Log payloads
	// from direct Ancient V2 receipts only after the matching immutable event-
	// log range is fully covered. Readers reconstruct logs from that sidecar.
	// This is intentionally opt-in at the runner boundary because legacy/full
	// configurations may not build a cold event-log archive.
	ExternalizeV2ReceiptLogs bool

	// MetricsNamespace is the go-ethereum metrics prefix used for runner
	// gauges. Default "chain/freezer/". Tests may override it to avoid
	// sharing process-global metric names across parallel runners.
	MetricsNamespace string
}

// Default returns the production defaults. Used by cmd/gtron when no
// operator overrides have been supplied.
func Default() Config {
	return Config{
		Enabled:                      true,
		Interval:                     defaultInterval,
		MarginBlocks:                 defaultMarginBlocks,
		BatchBlocks:                  defaultBatchBlocks,
		V2Enabled:                    true,
		DirectV2:                     true,
		V2FrameBlocks:                defaultV2FrameBlocks,
		V2SegmentBlocks:              defaultV2SegmentBlocks,
		V2CatchupTimeBudget:          defaultV2CatchupTimeBudget,
		V2CatchupMaxSegments:         defaultV2CatchupMaxSegments,
		TransactionIndexEnabled:      true,
		TransactionIndexPrefixBits:   defaultTxIndexPrefixBits,
		CatchupMaintenanceInterval:   defaultCatchupMaintenanceInterval,
		HeavyMaintenanceStartupDelay: defaultHeavyMaintenanceStartupDelay,
		HeavyMaintenanceErrorBackoff: defaultHeavyMaintenanceErrorBackoff,
		MetricsNamespace:             defaultMetricsNamespace,
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
	if c.V2FrameBlocks == 0 {
		c.V2FrameBlocks = defaultV2FrameBlocks
	}
	if c.V2SegmentBlocks == 0 {
		c.V2SegmentBlocks = defaultV2SegmentBlocks
	}
	if c.V2CatchupTimeBudget <= 0 {
		c.V2CatchupTimeBudget = defaultV2CatchupTimeBudget
	}
	if c.V2CatchupMaxSegments == 0 {
		c.V2CatchupMaxSegments = defaultV2CatchupMaxSegments
	}
	if c.TransactionIndexPrefixBits == 0 {
		c.TransactionIndexPrefixBits = defaultTxIndexPrefixBits
	}
	if c.MetricsNamespace == "" {
		c.MetricsNamespace = defaultMetricsNamespace
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

	// ReadBlockRawStrict returns the marshalled `corepb.Block` bytes under
	// `b-<num>`. A missing row is treated as a hard error by the freezer pass:
	// if a solidified block disappeared from Pebble, something else is wrong
	// upstream. Storage read errors must be surfaced so the pass rolls back
	// instead of recording ambiguous ancient data.
	ReadBlockRawStrict(number uint64) ([]byte, bool, error)

	// ReadTransactionInfosRawStrict returns the marshalled
	// `corepb.TransactionRet` bytes under `tib-<num>`, or nil if absent.
	// Empty blocks (no transactions) still have a row written by
	// applyBlock through the compact typed writer, so nil only
	// occurs in test fakes; the freezer pass treats nil as "no rows" and
	// appends an empty byte slice to preserve the per-num cardinality of
	// the ancient table.
	ReadTransactionInfosRawStrict(number uint64) ([]byte, bool, error)

	// ReadBlockHashByNumberStrict returns the canonical block hash for the
	// given number. Used by the freezer to verify hash-bound stages and write
	// its own ChainFreezer stage. Storage corruption must return an error so
	// the pass rolls back instead of treating the hash as unavailable.
	ReadBlockHashByNumberStrict(number uint64) (tcommon.Hash, bool, error)

	// ReadBlockStateRootRaw returns the raw state-root bytes under
	// `bsr-<hash>`, or nil if absent. Pre-AccountStateRoot fork blocks
	// don't have this row; the freezer pass writes an empty entry in
	// that case so per-num cardinality matches across all three tables.
	// Corruption and cold-index lookup failures must return an error so
	// the pass rolls back instead of appending an empty state-root row.
	ReadBlockStateRootRaw(hash tcommon.Hash) ([]byte, error)
}

type receiptLogCoverageSource interface {
	ReceiptLogRangeCovered(fromBlock, toBlock uint64) (bool, error)
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

type V2Compactor interface {
	V2Coverage() uint64
	MigrateV2(rawdbfreezer.V2MigrationOptions) (rawdbfreezer.V2MigrationResult, error)
}

type V2DirectAppender interface {
	CanAppendV2Direct(start uint64) bool
	MigrateV2(rawdbfreezer.V2MigrationOptions) (rawdbfreezer.V2MigrationResult, error)
}

// V2SourceTailer exposes the mutable V1 tail independently of the freezer's
// composite logical tail. Once this advances past V2 coverage, online V2
// promotion has no local source for the gap and must stop retrying.
type V2SourceTailer interface {
	V1Tail() uint64
}

// TransactionIndexCompactor is optional so lightweight ancient test stores
// and deployments without the compact index keep working unchanged.
type TransactionIndexCompactor interface {
	AncientDatadir() (string, error)
	TransactionIndexCoverage() uint64
	PublishTransactionIndexRun(rawdbfreezer.TransactionIndexBuildResult) error
	CompactTransactionIndexTail() (bool, error)
}

// Stats is a thread-safe snapshot of runner progress. Operators consume it via
// Runner.Snapshot, and the runner mirrors the same values into metrics gauges.
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
	PebbleSizeAfter     uint64
	V2Coverage          uint64
	V2BlocksCompacted   uint64
	V2BacklogBlocks     uint64
	V2BacklogSegments   uint64
	V2LastBatchSegments uint64
	V2LastBatchDuration time.Duration
	V2BudgetExhausted   uint64
	// TransactionIndexCoverage is the first block not covered by an immutable
	// index run. TransactionIndexPruned is the exclusive hot-row deletion
	// cursor and must never advance beyond coverage.
	TransactionIndexCoverage             uint64
	TransactionIndexPruned               uint64
	TransactionIndexRowsArchived         uint64
	TransactionIndexRowsPruned           uint64
	V2CatchupDeferred                    uint64
	V2ResourceDeferred                   uint64
	V2ErrorBackoffDeferred               uint64
	V2SourcePrunedDeferred               uint64
	V2Errors                             uint64
	TransactionIndexCatchupDeferred      uint64
	TransactionIndexResourceDeferred     uint64
	TransactionIndexErrorBackoffDeferred uint64
	TransactionIndexErrors               uint64
}

type runnerMetrics struct {
	frozenMin               *metrics.Gauge
	frozenMax               *metrics.Gauge
	frozenHas               *metrics.Gauge
	blocksFrozen            *metrics.Gauge
	passesCompleted         *metrics.Gauge
	lastPassAt              *metrics.Gauge
	lastPassDuration        *metrics.Gauge
	pebbleSizeAfter         *metrics.Gauge
	v2Coverage              *metrics.Gauge
	v2Blocks                *metrics.Gauge
	v2BacklogBlocks         *metrics.Gauge
	v2BacklogSegments       *metrics.Gauge
	v2LastBatchSegments     *metrics.Gauge
	v2LastBatchDuration     *metrics.Gauge
	v2BudgetExhausted       *metrics.Gauge
	txIndexCoverage         *metrics.Gauge
	txIndexPruned           *metrics.Gauge
	txIndexArchived         *metrics.Gauge
	txIndexRowsPruned       *metrics.Gauge
	v2CatchupDeferred       *metrics.Gauge
	v2ResourceDeferred      *metrics.Gauge
	v2ErrorBackoffDeferred  *metrics.Gauge
	v2SourcePrunedDeferred  *metrics.Gauge
	v2Errors                *metrics.Gauge
	txIndexCatchupDeferred  *metrics.Gauge
	txIndexResourceDeferred *metrics.Gauge
	txIndexErrorDeferred    *metrics.Gauge
	txIndexErrors           *metrics.Gauge
}

func newRunnerMetrics(namespace string) runnerMetrics {
	namespace = normalizeMetricNamespace(namespace)
	return runnerMetrics{
		frozenMin:       metrics.GetOrRegisterGauge(namespace+"frozen/min", nil),
		frozenMax:       metrics.GetOrRegisterGauge(namespace+"frozen/max", nil),
		frozenHas:       metrics.GetOrRegisterGauge(namespace+"frozen/has", nil),
		blocksFrozen:    metrics.GetOrRegisterGauge(namespace+"blocks", nil),
		passesCompleted: metrics.GetOrRegisterGauge(namespace+"passes", nil),
		lastPassAt:      metrics.GetOrRegisterGauge(namespace+"lastpass/time", nil),
		lastPassDuration: metrics.GetOrRegisterGauge(
			namespace+"lastpass/duration",
			nil,
		),
		pebbleSizeAfter:         metrics.GetOrRegisterGauge(namespace+"pebble/size", nil),
		v2Coverage:              metrics.GetOrRegisterGauge(namespace+"v2/coverage", nil),
		v2Blocks:                metrics.GetOrRegisterGauge(namespace+"v2/blocks", nil),
		v2BacklogBlocks:         metrics.GetOrRegisterGauge(namespace+"v2/backlog/blocks", nil),
		v2BacklogSegments:       metrics.GetOrRegisterGauge(namespace+"v2/backlog/segments", nil),
		v2LastBatchSegments:     metrics.GetOrRegisterGauge(namespace+"v2/batch/segments", nil),
		v2LastBatchDuration:     metrics.GetOrRegisterGauge(namespace+"v2/batch/duration", nil),
		v2BudgetExhausted:       metrics.GetOrRegisterGauge(namespace+"v2/batch/budget_exhausted", nil),
		txIndexCoverage:         metrics.GetOrRegisterGauge(namespace+"txindex/coverage", nil),
		txIndexPruned:           metrics.GetOrRegisterGauge(namespace+"txindex/pruned", nil),
		txIndexArchived:         metrics.GetOrRegisterGauge(namespace+"txindex/rows/archived", nil),
		txIndexRowsPruned:       metrics.GetOrRegisterGauge(namespace+"txindex/rows/pruned", nil),
		v2CatchupDeferred:       metrics.GetOrRegisterGauge(namespace+"v2/deferred/catchup", nil),
		v2ResourceDeferred:      metrics.GetOrRegisterGauge(namespace+"v2/deferred/resource", nil),
		v2ErrorBackoffDeferred:  metrics.GetOrRegisterGauge(namespace+"v2/deferred/error_backoff", nil),
		v2SourcePrunedDeferred:  metrics.GetOrRegisterGauge(namespace+"v2/deferred/source_pruned", nil),
		v2Errors:                metrics.GetOrRegisterGauge(namespace+"v2/errors", nil),
		txIndexCatchupDeferred:  metrics.GetOrRegisterGauge(namespace+"txindex/deferred/catchup", nil),
		txIndexResourceDeferred: metrics.GetOrRegisterGauge(namespace+"txindex/deferred/resource", nil),
		txIndexErrorDeferred:    metrics.GetOrRegisterGauge(namespace+"txindex/deferred/error_backoff", nil),
		txIndexErrors:           metrics.GetOrRegisterGauge(namespace+"txindex/errors", nil),
	}
}

func normalizeMetricNamespace(namespace string) string {
	if namespace == "" {
		namespace = defaultMetricsNamespace
	}
	if !strings.HasSuffix(namespace, "/") {
		namespace += "/"
	}
	return namespace
}

func (m runnerMetrics) update(stats Stats) {
	m.frozenMin.Update(uint64GaugeValue(stats.FrozenMin))
	m.frozenMax.Update(uint64GaugeValue(stats.FrozenMax))
	m.frozenHas.Update(boolGaugeValue(stats.HasFrozen))
	m.blocksFrozen.Update(uint64GaugeValue(stats.BlocksFrozen))
	m.passesCompleted.Update(uint64GaugeValue(stats.PassesCompleted))
	if stats.LastPassAt.IsZero() {
		m.lastPassAt.Update(0)
	} else {
		m.lastPassAt.Update(stats.LastPassAt.Unix())
	}
	m.lastPassDuration.Update(int64(stats.LastPassDuration))
	m.pebbleSizeAfter.Update(uint64GaugeValue(stats.PebbleSizeAfter))
	m.v2Coverage.Update(uint64GaugeValue(stats.V2Coverage))
	m.v2Blocks.Update(uint64GaugeValue(stats.V2BlocksCompacted))
	m.v2BacklogBlocks.Update(uint64GaugeValue(stats.V2BacklogBlocks))
	m.v2BacklogSegments.Update(uint64GaugeValue(stats.V2BacklogSegments))
	m.v2LastBatchSegments.Update(uint64GaugeValue(stats.V2LastBatchSegments))
	m.v2LastBatchDuration.Update(int64(stats.V2LastBatchDuration))
	m.v2BudgetExhausted.Update(uint64GaugeValue(stats.V2BudgetExhausted))
	m.txIndexCoverage.Update(uint64GaugeValue(stats.TransactionIndexCoverage))
	m.txIndexPruned.Update(uint64GaugeValue(stats.TransactionIndexPruned))
	m.txIndexArchived.Update(uint64GaugeValue(stats.TransactionIndexRowsArchived))
	m.txIndexRowsPruned.Update(uint64GaugeValue(stats.TransactionIndexRowsPruned))
	m.v2CatchupDeferred.Update(uint64GaugeValue(stats.V2CatchupDeferred))
	m.v2ResourceDeferred.Update(uint64GaugeValue(stats.V2ResourceDeferred))
	m.v2ErrorBackoffDeferred.Update(uint64GaugeValue(stats.V2ErrorBackoffDeferred))
	m.v2SourcePrunedDeferred.Update(uint64GaugeValue(stats.V2SourcePrunedDeferred))
	m.v2Errors.Update(uint64GaugeValue(stats.V2Errors))
	m.txIndexCatchupDeferred.Update(uint64GaugeValue(stats.TransactionIndexCatchupDeferred))
	m.txIndexResourceDeferred.Update(uint64GaugeValue(stats.TransactionIndexResourceDeferred))
	m.txIndexErrorDeferred.Update(uint64GaugeValue(stats.TransactionIndexErrorBackoffDeferred))
	m.txIndexErrors.Update(uint64GaugeValue(stats.TransactionIndexErrors))
}

func boolGaugeValue(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

func uint64GaugeValue(v uint64) int64 {
	const maxInt64GaugeValue = uint64(1<<63 - 1)
	if v > maxInt64GaugeValue {
		return int64(maxInt64GaugeValue)
	}
	return int64(v)
}

// Runner is the freezer's Lifecycle service. Construct with New, register
// the returned value with the node, and the loop fires on its own timer
// until Stop returns.
type Runner struct {
	chain   ChainSource
	freezer FreezerStore
	cfg     Config
	metrics runnerMetrics

	wake chan struct{}
	quit chan struct{}
	done chan struct{}
	once sync.Once

	hookMu       sync.Mutex
	advanceHooks []func()

	// stats fields are atomics so Snapshot is lock-free against the running
	// goroutine.
	blocksFrozen                  atomic.Uint64
	passesCompleted               atomic.Uint64
	lastPassUnixNano              atomic.Int64
	lastPassDuration              atomic.Int64 // nanoseconds
	pebbleSizeAfter               atomic.Uint64
	v2BlocksCompacted             atomic.Uint64
	v2LastBatchSegments           atomic.Uint64
	v2LastBatchDuration           atomic.Int64
	v2BudgetExhausted             atomic.Uint64
	lastV2Promotion               atomic.Int64
	txIndexRowsArchived           atomic.Uint64
	txIndexRowsPruned             atomic.Uint64
	v2CatchupDeferred             atomic.Uint64
	v2ResourceDeferred            atomic.Uint64
	v2ErrorBackoffDeferred        atomic.Uint64
	v2SourcePrunedDeferred        atomic.Uint64
	v2SourcePrunedWarned          atomic.Bool
	v2Errors                      atomic.Uint64
	txIndexCatchupDeferred        atomic.Uint64
	txIndexResourceDeferred       atomic.Uint64
	txIndexErrorBackoffDeferred   atomic.Uint64
	txIndexErrors                 atomic.Uint64
	lastV2CatchupMaintenance      atomic.Int64
	lastTxIndexCatchupMaintenance atomic.Int64
	lastV2MaintenanceError        atomic.Int64
	lastTxIndexMaintenanceError   atomic.Int64
	startedAt                     time.Time

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
	cfg = cfg.applyDefaults()
	r := &Runner{
		chain:       chain,
		freezer:     fz,
		cfg:         cfg,
		metrics:     newRunnerMetrics(cfg.MetricsNamespace),
		wake:        make(chan struct{}, 1),
		quit:        make(chan struct{}),
		done:        make(chan struct{}),
		pauseCtx:    ctx,
		pauseCancel: cancel,
		startedAt:   time.Now(),
	}
	r.updateMetrics()
	return r
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
		"batch", r.cfg.BatchBlocks,
		"directV2", r.cfg.DirectV2,
		"catchupMaintenanceInterval", r.cfg.CatchupMaintenanceInterval,
		"v2CatchupTimeBudget", r.cfg.V2CatchupTimeBudget,
		"v2CatchupMaxSegments", r.cfg.V2CatchupMaxSegments,
		"receiptLogsExternal", r.cfg.ExternalizeV2ReceiptLogs,
		"heavyMaintenanceStartupDelay", r.cfg.HeavyMaintenanceStartupDelay,
		"heavyMaintenanceErrorBackoff", r.cfg.HeavyMaintenanceErrorBackoff,
		"sharedHeavyWorkGate", r.cfg.HeavyWorkGate != nil)
	return nil
}

// Stop implements node.Lifecycle. It signals the loop to stop at the next
// crash-safe boundary and joins the goroutine before callers close either
// database. Idempotent: safe to call from multiple goroutines / multiple times.
func (r *Runner) Stop() error {
	r.BeginStop()
	<-r.done
	log.Info("Freezer runner stopped",
		"blocksFrozen", r.blocksFrozen.Load(),
		"passes", r.passesCompleted.Load())
	return nil
}

// BeginStop signals cancellation without waiting for the runner goroutine.
// Shutdown coordinators use it before draining other services so the freezer
// can roll back or reach its next durable boundary concurrently. Stop must
// still be called before closing either database.
func (r *Runner) BeginStop() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		close(r.quit)
		r.pauseCancel()
	})
}

func (r *Runner) checkStopping() error {
	if r == nil || r.pauseCtx == nil {
		return nil
	}
	if r.pauseCtx.Err() != nil {
		return errRunnerStopping
	}
	return nil
}

// RequestPass schedules one freezer pass without waiting for it. Requests
// coalesce while a pass is pending, so sync completion can wake the freezer
// without creating an unbounded queue during catch-up.
func (r *Runner) RequestPass() {
	if r == nil {
		return
	}
	select {
	case <-r.quit:
		return
	default:
	}
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// AddChainFreezerAdvanceHook registers a callback that runs after a pass
// advances or repairs the hash-bound ChainFreezer stage. Hooks run without
// runner locks held and must return promptly.
func (r *Runner) AddChainFreezerAdvanceHook(hook func()) {
	if r == nil || hook == nil {
		return
	}
	r.hookMu.Lock()
	r.advanceHooks = append(r.advanceHooks, hook)
	r.hookMu.Unlock()
}

func (r *Runner) notifyChainFreezerAdvance() {
	r.hookMu.Lock()
	hooks := append([]func(){}, r.advanceHooks...)
	r.hookMu.Unlock()
	for _, hook := range hooks {
		hook()
	}
}

// Snapshot returns a thread-safe copy of the runner's current counters.
// Safe to call from any goroutine — every field is read from an atomic.
func (r *Runner) Snapshot() Stats {
	return r.snapshot()
}

func (r *Runner) snapshot() Stats {
	stats := Stats{
		BlocksFrozen:                         r.blocksFrozen.Load(),
		PassesCompleted:                      r.passesCompleted.Load(),
		LastPassDuration:                     time.Duration(r.lastPassDuration.Load()),
		PebbleSizeAfter:                      r.pebbleSizeAfter.Load(),
		V2BlocksCompacted:                    r.v2BlocksCompacted.Load(),
		V2LastBatchSegments:                  r.v2LastBatchSegments.Load(),
		V2LastBatchDuration:                  time.Duration(r.v2LastBatchDuration.Load()),
		V2BudgetExhausted:                    r.v2BudgetExhausted.Load(),
		TransactionIndexRowsArchived:         r.txIndexRowsArchived.Load(),
		TransactionIndexRowsPruned:           r.txIndexRowsPruned.Load(),
		V2CatchupDeferred:                    r.v2CatchupDeferred.Load(),
		V2ResourceDeferred:                   r.v2ResourceDeferred.Load(),
		V2ErrorBackoffDeferred:               r.v2ErrorBackoffDeferred.Load(),
		V2SourcePrunedDeferred:               r.v2SourcePrunedDeferred.Load(),
		V2Errors:                             r.v2Errors.Load(),
		TransactionIndexCatchupDeferred:      r.txIndexCatchupDeferred.Load(),
		TransactionIndexResourceDeferred:     r.txIndexResourceDeferred.Load(),
		TransactionIndexErrorBackoffDeferred: r.txIndexErrorBackoffDeferred.Load(),
		TransactionIndexErrors:               r.txIndexErrors.Load(),
	}
	if t := r.lastPassUnixNano.Load(); t > 0 {
		stats.LastPassAt = time.Unix(0, t)
	}
	if compactor, ok := r.freezer.(V2Compactor); ok {
		stats.V2Coverage = compactor.V2Coverage()
		directAppender, direct := r.freezer.(V2DirectAppender)
		direct = direct && r.cfg.V2Enabled && r.cfg.DirectV2 && directAppender.CanAppendV2Direct(stats.V2Coverage)
		if direct && r.chain != nil {
			solid := r.chain.LatestSolidifiedBlockNum()
			if solid > 0 && uint64(solid) >= r.cfg.MarginBlocks {
				targetExclusive := uint64(solid) - r.cfg.MarginBlocks + 1
				if finish, ok, err := r.verifiedFinishStageBlock(); err == nil && ok && finish+1 < targetExclusive {
					targetExclusive = finish + 1
				}
				target := targetExclusive / r.cfg.V2SegmentBlocks * r.cfg.V2SegmentBlocks
				if target > stats.V2Coverage {
					stats.V2BacklogBlocks = target - stats.V2Coverage
					stats.V2BacklogSegments = stats.V2BacklogBlocks / r.cfg.V2SegmentBlocks
				}
			}
		} else if count, err := r.freezer.AncientCount(rawdbAncientBlocks); err == nil {
			target := count / r.cfg.V2SegmentBlocks * r.cfg.V2SegmentBlocks
			if target > stats.V2Coverage {
				stats.V2BacklogBlocks = target - stats.V2Coverage
				stats.V2BacklogSegments = stats.V2BacklogBlocks / r.cfg.V2SegmentBlocks
			}
		}
	}
	if compactor, ok := r.freezer.(TransactionIndexCompactor); ok {
		stats.TransactionIndexCoverage = compactor.TransactionIndexCoverage()
	}
	if r.chain != nil {
		if progress, ok, err := rawdb.ReadStageProgress(r.chain.DB(), rawdb.StageFreezerTxIndexPrune); err == nil && ok {
			stats.TransactionIndexPruned = progress
		}
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

type heavyMaintenanceKind uint8

const (
	heavyMaintenanceV2 heavyMaintenanceKind = iota + 1
	heavyMaintenanceTxIndex
)

type heavyMaintenanceDeferral uint8

const (
	heavyMaintenanceDeferredCatchup heavyMaintenanceDeferral = iota + 1
	heavyMaintenanceDeferredResource
	heavyMaintenanceDeferredErrorBackoff
)

type heavyMaintenanceLease struct {
	ctx     context.Context
	release func()
}

func (l *heavyMaintenanceLease) Close() {
	if l != nil && l.release != nil {
		l.release()
	}
}

func (r *Runner) beginHeavyMaintenance(kind heavyMaintenanceKind, lastCatchup, lastError *atomic.Int64) (*heavyMaintenanceLease, bool) {
	if r == nil {
		return nil, false
	}
	now := time.Now()
	if r.cfg.HeavyMaintenanceErrorBackoff > 0 {
		if failedAt := lastError.Load(); failedAt > 0 && now.Sub(time.Unix(0, failedAt)) < r.cfg.HeavyMaintenanceErrorBackoff {
			r.recordHeavyMaintenanceDeferred(kind, heavyMaintenanceDeferredErrorBackoff)
			return nil, false
		}
	}
	if r.cfg.HeavyMaintenanceStartupDelay > 0 && now.Sub(r.startedAt) < r.cfg.HeavyMaintenanceStartupDelay {
		r.recordHeavyMaintenanceDeferred(kind, heavyMaintenanceDeferredCatchup)
		return nil, false
	}
	active := r.cfg.SyncActive != nil && r.cfg.SyncActive()
	if active && r.cfg.CatchupMaintenanceInterval > 0 {
		if last := lastCatchup.Load(); last > 0 && now.Sub(time.Unix(0, last)) < r.cfg.CatchupMaintenanceInterval {
			r.recordHeavyMaintenanceDeferred(kind, heavyMaintenanceDeferredCatchup)
			return nil, false
		}
	}
	releaseGate, ok := r.cfg.HeavyWorkGate.TryAcquire()
	if !ok {
		r.recordHeavyMaintenanceDeferred(kind, heavyMaintenanceDeferredResource)
		return nil, false
	}
	// Recheck after admission so a sync transition cannot consume an old
	// catch-up token while this task waited behind another maintenance stage.
	now = time.Now()
	active = r.cfg.SyncActive != nil && r.cfg.SyncActive()
	if active && r.cfg.CatchupMaintenanceInterval > 0 {
		if last := lastCatchup.Load(); last > 0 && now.Sub(time.Unix(0, last)) < r.cfg.CatchupMaintenanceInterval {
			releaseGate()
			r.recordHeavyMaintenanceDeferred(kind, heavyMaintenanceDeferredCatchup)
			return nil, false
		}
		lastCatchup.Store(now.UnixNano())
	}

	ctx := r.pauseCtx
	stopWatch := func() {}
	// A task explicitly admitted from the catch-up budget may finish its one
	// bounded transaction. A task which began while idle is canceled if peer
	// discovery turns sync on underneath it.
	if !active && r.cfg.SyncActive != nil {
		var cancel context.CancelCauseFunc
		ctx, cancel = context.WithCancelCause(r.pauseCtx)
		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			ticker := time.NewTicker(heavyMaintenancePollInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					if r.cfg.SyncActive() {
						cancel(errHeavyMaintenanceDeferred)
						return
					}
				case <-stop:
					return
				case <-ctx.Done():
					return
				}
			}
		}()
		var stopOnce sync.Once
		stopWatch = func() {
			stopOnce.Do(func() { close(stop) })
			<-done
			cancel(nil)
		}
	}
	return &heavyMaintenanceLease{
		ctx: ctx,
		release: func() {
			stopWatch()
			releaseGate()
		},
	}, true
}

func (r *Runner) recordHeavyMaintenanceDeferred(kind heavyMaintenanceKind, reason heavyMaintenanceDeferral) {
	if r == nil {
		return
	}
	switch kind {
	case heavyMaintenanceV2:
		switch reason {
		case heavyMaintenanceDeferredResource:
			r.v2ResourceDeferred.Add(1)
		case heavyMaintenanceDeferredErrorBackoff:
			r.v2ErrorBackoffDeferred.Add(1)
		default:
			r.v2CatchupDeferred.Add(1)
		}
	case heavyMaintenanceTxIndex:
		switch reason {
		case heavyMaintenanceDeferredResource:
			r.txIndexResourceDeferred.Add(1)
		case heavyMaintenanceDeferredErrorBackoff:
			r.txIndexErrorBackoffDeferred.Add(1)
		default:
			r.txIndexCatchupDeferred.Add(1)
		}
	}
	r.updateMetrics()
}

// CompactV2Once promotes a backlog-sized but bounded batch of complete V1
// segments. Admission, error backoff and the shared HeavyWorkGate are owned by
// the runner; the freezer enforces the time budget between crash-safe segment
// publication boundaries.
func (r *Runner) CompactV2Once() (uint64, error) {
	if !r.cfg.Enabled || !r.cfg.V2Enabled {
		return 0, nil
	}
	if r.cfg.V2PromotionAllowed != nil && !r.cfg.V2PromotionAllowed() {
		return 0, nil
	}
	if compactor, ok := r.freezer.(V2Compactor); ok {
		if tailer, ok := r.freezer.(V2SourceTailer); ok {
			coverage, tail := compactor.V2Coverage(), tailer.V1Tail()
			if tail > coverage {
				r.recordV2SourcePruned(coverage, tail)
				return 0, nil
			}
		}
	}
	lease, ok := r.beginHeavyMaintenance(heavyMaintenanceV2, &r.lastV2CatchupMaintenance, &r.lastV2MaintenanceError)
	if !ok {
		return 0, nil
	}
	defer lease.Close()
	compacted, err := r.compactV2OnceContext(lease.ctx)
	if errors.Is(err, context.Canceled) && errors.Is(context.Cause(lease.ctx), errHeavyMaintenanceDeferred) {
		r.recordHeavyMaintenanceDeferred(heavyMaintenanceV2, heavyMaintenanceDeferredCatchup)
		return compacted, nil
	}
	if errors.Is(err, context.Canceled) && r.pauseCtx.Err() != nil {
		// Shutdown is control flow. Any earlier segments in this batch have
		// already crossed their publication/fsync boundaries and are reflected
		// in compacted; the canceled in-flight segment was never published.
		return compacted, err
	}
	if err != nil {
		if errors.Is(err, rawdbfreezer.ErrV2SourcePruned) {
			coverage, tail := uint64(0), uint64(0)
			if compactor, ok := r.freezer.(V2Compactor); ok {
				coverage = compactor.V2Coverage()
			}
			if tailer, ok := r.freezer.(V2SourceTailer); ok {
				tail = tailer.V1Tail()
			}
			r.lastV2MaintenanceError.Store(0)
			r.recordV2SourcePruned(coverage, tail)
			return 0, nil
		}
		r.lastV2MaintenanceError.Store(time.Now().UnixNano())
		r.v2Errors.Add(1)
		r.updateMetrics()
	} else {
		r.lastV2MaintenanceError.Store(0)
		r.updateMetrics()
	}
	return compacted, err
}

func (r *Runner) recordV2SourcePruned(coverage, tail uint64) {
	if r == nil {
		return
	}
	r.v2SourcePrunedDeferred.Add(1)
	if !r.v2SourcePrunedWarned.Swap(true) {
		log.Info("Freezer: online V2 promotion disabled because its V1 source was pruned",
			"v2Coverage", coverage,
			"v1Tail", tail,
			"hint", "cold snapshots remain authoritative for the pruned gap; use an offline rebuild to extend V2")
	}
	r.updateMetrics()
}

func (r *Runner) compactV2OnceContext(ctx context.Context) (uint64, error) {
	compactor, ok := r.freezer.(V2Compactor)
	if !ok {
		return 0, nil
	}
	segmentBlocks := r.cfg.V2SegmentBlocks
	if segmentBlocks == 0 || r.cfg.V2FrameBlocks == 0 || segmentBlocks%uint64(r.cfg.V2FrameBlocks) != 0 {
		return 0, errors.New("freezer: invalid V2 frame/segment configuration")
	}
	head, err := r.freezer.AncientCount(rawdbAncientBlocks)
	if err != nil {
		return 0, err
	}
	coverage := compactor.V2Coverage()
	target := head / segmentBlocks * segmentBlocks
	if target <= coverage {
		r.v2LastBatchSegments.Store(0)
		r.v2LastBatchDuration.Store(0)
		return 0, nil
	}
	backlogBlocks := target - coverage
	backlogSegments := backlogBlocks / segmentBlocks
	maxSegments := backlogSegments
	if configured := r.cfg.V2CatchupMaxSegments; configured > 0 && maxSegments > configured {
		maxSegments = configured
	}
	batchStarted := time.Now()
	migration := rawdbfreezer.V2MigrationOptions{
		Tables:        []string{rawdbAncientBlocks, rawdbAncientTxInfos, rawdbAncientStateRoots},
		SegmentBlocks: segmentBlocks,
		FrameBlocks:   r.cfg.V2FrameBlocks,
		MaxSegments:   maxSegments,
		TimeBudget:    r.cfg.V2CatchupTimeBudget,
		Online:        true,
		Context:       ctx,
		Transform:     rawdb.CompactAncientV2Record,
	}
	if r.cfg.TransactionIndexEnabled {
		migration.TransactionIndexEntries = rawdb.AncientTransactionIndexEntries
		migration.TransactionIndexPrefixBits = r.cfg.TransactionIndexPrefixBits
	}
	result, err := compactor.MigrateV2(migration)
	resultEnd := result.End
	if resultEnd < result.Start {
		resultEnd = result.Start
	}
	compacted := resultEnd - result.Start
	batchElapsed := result.Elapsed
	if batchElapsed <= 0 {
		batchElapsed = time.Since(batchStarted)
	}
	r.v2LastBatchSegments.Store(result.Segments)
	r.v2LastBatchDuration.Store(int64(batchElapsed))
	if result.BudgetExhausted {
		r.v2BudgetExhausted.Add(1)
	}
	if compacted > 0 {
		r.v2BlocksCompacted.Add(compacted)
		if result.TransactionIndexRows > 0 {
			r.txIndexRowsArchived.Add(result.TransactionIndexRows)
		}
		log.Info("Freezer: promoted V1 segments to V2",
			"from", result.Start, "to", result.End,
			"segments", result.Segments,
			"backlogBefore", backlogSegments,
			"backlogAfter", (target-result.End)/segmentBlocks,
			"budget", r.cfg.V2CatchupTimeBudget,
			"budgetExhausted", result.BudgetExhausted,
			"frameBlocks", result.FrameBlocks,
			"transactionIndexRuns", result.TransactionIndexRuns,
			"transactionIndexRows", result.TransactionIndexRows,
			"transactionIndexSpilledRuns", result.TransactionIndexSpilledRuns,
			"elapsed", batchElapsed,
			"physicalBefore", result.PhysicalBytesBefore,
			"physicalAfter", result.PhysicalBytesAfter)
	}
	return compacted, err
}

func (r *Runner) compactV2Scheduled() (uint64, error) {
	if last := r.lastV2Promotion.Load(); last > 0 && time.Since(time.Unix(0, last)) < r.cfg.Interval {
		return 0, nil
	}
	compacted, err := r.CompactV2Once()
	if compacted > 0 {
		r.lastV2Promotion.Store(time.Now().UnixNano())
		r.updateMetrics()
	}
	return compacted, err
}

// MaintainTransactionIndexOnce performs one bounded, crash-resumable action:
// prune one published segment, merge one equal-sized run pair, or publish one
// newly V2-covered segment. Publication always precedes hot-row deletion.
func (r *Runner) MaintainTransactionIndexOnce() (bool, error) {
	if err := r.checkStopping(); err != nil {
		return false, err
	}
	if !r.cfg.Enabled || !r.cfg.V2Enabled || !r.cfg.TransactionIndexEnabled {
		return false, nil
	}
	lease, ok := r.beginHeavyMaintenance(heavyMaintenanceTxIndex, &r.lastTxIndexCatchupMaintenance, &r.lastTxIndexMaintenanceError)
	if !ok {
		return false, nil
	}
	defer lease.Close()
	changed, err := r.maintainTransactionIndexOnceContext(lease.ctx)
	if errors.Is(err, context.Canceled) && errors.Is(context.Cause(lease.ctx), errHeavyMaintenanceDeferred) {
		r.recordHeavyMaintenanceDeferred(heavyMaintenanceTxIndex, heavyMaintenanceDeferredCatchup)
		return false, nil
	}
	if err != nil {
		r.lastTxIndexMaintenanceError.Store(time.Now().UnixNano())
		r.txIndexErrors.Add(1)
		r.updateMetrics()
	} else {
		r.lastTxIndexMaintenanceError.Store(0)
	}
	return changed, err
}

func (r *Runner) maintainTransactionIndexOnceContext(ctx context.Context) (bool, error) {
	v2, ok := r.freezer.(V2Compactor)
	if !ok {
		return false, nil
	}
	index, ok := r.freezer.(TransactionIndexCompactor)
	if !ok {
		return false, nil
	}
	coverage := index.TransactionIndexCoverage()
	v2Coverage := v2.V2Coverage()
	if coverage > v2Coverage {
		return false, fmt.Errorf("freezer: transaction index coverage %d exceeds V2 coverage %d", coverage, v2Coverage)
	}
	pruned, initialized, err := r.transactionIndexPruneProgress(coverage)
	if err != nil || !initialized {
		return false, err
	}
	if pruned > coverage {
		return false, fmt.Errorf("freezer: transaction index prune progress %d exceeds coverage %d", pruned, coverage)
	}
	if pruned < coverage {
		end := pruned + r.cfg.V2SegmentBlocks
		if end > coverage {
			end = coverage
		}
		rows, err := r.pruneHotTransactionIndexRangeContext(ctx, pruned, end)
		if err != nil {
			return false, err
		}
		if err := r.commitTransactionIndexPrune(end, rows); err != nil {
			return false, err
		}
		log.Info("Freezer: pruned archived hot transaction indexes", "from", pruned, "to", end, "rows", rows)
		return true, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if merged, err := index.CompactTransactionIndexTail(); err != nil {
		return false, err
	} else if merged {
		log.Info("Freezer: geometrically merged transaction index tail")
		return true, nil
	}
	if coverage+r.cfg.V2SegmentBlocks > v2Coverage {
		return false, nil
	}
	return r.ensureTransactionIndexCoverageContext(ctx, coverage+r.cfg.V2SegmentBlocks)
}

func (r *Runner) transactionIndexPruneProgress(coverage uint64) (uint64, bool, error) {
	progress, ok, err := rawdb.ReadStageProgress(r.chain.DB(), rawdb.StageFreezerTxIndexPrune)
	if err != nil || ok || coverage == 0 {
		return progress, true, err
	}
	// Fresh direct-V2 databases have no legacy pre-pruned state to discover.
	// Starting from zero is idempotent and avoids an unbounded scan of every
	// tx-* row exactly when the first immutable index run is published.
	return 0, true, nil
}

func (r *Runner) iterateTransactionIndexEntriesContext(ctx context.Context, start, end uint64, yield func(rawdbfreezer.TransactionIndexEntry) error) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if yield == nil {
		return 0, errors.New("freezer: nil transaction-index iterator")
	}
	started := time.Now()
	lastProgress := started
	var rows uint64
	for number := start; number < end; number++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if err := r.checkStopping(); err != nil {
			return 0, err
		}
		body, err := r.freezer.Ancient(rawdbAncientBlocks, number)
		if err != nil {
			return 0, fmt.Errorf("read ancient body %d for transaction index: %w", number, err)
		}
		block, err := coretypes.UnmarshalBlockBorrowed(body)
		if err != nil {
			return 0, fmt.Errorf("decode ancient body %d for transaction index: %w", number, err)
		}
		for ordinal, tx := range block.Transactions() {
			location, err := rawdb.EncodeTransactionLocation(number, ordinal)
			if err != nil {
				return 0, err
			}
			if err := yield(rawdbfreezer.TransactionIndexEntry{Hash: tx.Hash(), Location: location}); err != nil {
				return 0, err
			}
			rows++
		}
		if time.Since(lastProgress) >= 30*time.Second {
			log.Info("Freezer: streaming transaction index", "from", start, "to", end, "block", number, "rows", rows, "elapsed", time.Since(started))
			lastProgress = time.Now()
		}
	}
	return rows, nil
}

func (r *Runner) buildOrRecoverOnlineTransactionIndexRangeContext(ctx context.Context, ancientPath, path string, start, end uint64, prefixBits uint32) (rawdbfreezer.TransactionIndexBuildResult, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return rawdbfreezer.TransactionIndexBuildResult{}, false, err
	}
	if run, err := rawdbfreezer.OpenTransactionIndexRun(path); err == nil {
		defer run.Close()
		if run.StartBlock() != start || run.EndBlock() != end || run.PrefixBits() > prefixBits {
			return rawdbfreezer.TransactionIndexBuildResult{}, false, fmt.Errorf("online transaction index run %q has incompatible metadata", path)
		}
		if err := run.VerifyContext(ctx); err != nil {
			return rawdbfreezer.TransactionIndexBuildResult{}, false, err
		}
		return rawdbfreezer.TransactionIndexBuildResult{Path: path, Rows: run.Rows(), StartBlock: start, EndBlock: end, PrefixBits: run.PrefixBits(), FileBytes: run.Size()}, true, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return rawdbfreezer.TransactionIndexBuildResult{}, false, err
	}
	tempDir := filepath.Join(ancientPath, "tx-index", "etl")
	if _, err := etl.CleanupStaleCollectors(tempDir); err != nil {
		return rawdbfreezer.TransactionIndexBuildResult{}, false, fmt.Errorf("clean transaction-index ETL scratch: %w", err)
	}
	collector, err := etl.NewCollector(etl.Options{TempDir: tempDir, BufferLimit: 32 << 20})
	if err != nil {
		return rawdbfreezer.TransactionIndexBuildResult{}, false, err
	}
	defer collector.Close()
	rows, err := r.iterateTransactionIndexEntriesContext(ctx, start, end, func(entry rawdbfreezer.TransactionIndexEntry) error {
		return collector.PutEncoded(40, 0, func(key, _ []byte) {
			copy(key[:32], entry.Hash[:])
			binary.BigEndian.PutUint64(key[32:], entry.Location)
		})
	})
	if err != nil {
		return rawdbfreezer.TransactionIndexBuildResult{}, false, err
	}
	prefixBits = rawdbfreezer.AdaptiveTransactionIndexPrefixBits(rows, prefixBits)
	result, err := rawdbfreezer.BuildTransactionIndexRun(path, rawdbfreezer.TransactionIndexBuildOptions{
		Context:    ctx,
		PrefixBits: prefixBits,
		StartBlock: start,
		EndBlock:   end,
		Iterate: func(yield func(rawdbfreezer.TransactionIndexEntry) error) error {
			var previous [32]byte
			seen := false
			_, err := collector.Iterate(func(key, _ []byte) error {
				if len(key) != 40 {
					return fmt.Errorf("transaction-index ETL key length %d, want 40", len(key))
				}
				var entry rawdbfreezer.TransactionIndexEntry
				copy(entry.Hash[:], key[:32])
				entry.Location = binary.BigEndian.Uint64(key[32:])
				if seen && previous == entry.Hash {
					return fmt.Errorf("duplicate transaction hash %x in archived range [%d,%d)", entry.Hash, start, end)
				}
				previous = entry.Hash
				seen = true
				return yield(entry)
			})
			return err
		},
	})
	if err == nil && result.Rows != rows {
		return rawdbfreezer.TransactionIndexBuildResult{}, false, fmt.Errorf("transaction-index build wrote %d rows from %d collected entries", result.Rows, rows)
	}
	return result, false, err
}

func (r *Runner) ensureTransactionIndexCoverageContext(ctx context.Context, target uint64) (bool, error) {
	index, ok := r.freezer.(TransactionIndexCompactor)
	if !ok || !r.cfg.TransactionIndexEnabled {
		return false, nil
	}
	coverage := index.TransactionIndexCoverage()
	if coverage >= target {
		return false, nil
	}
	end := coverage + r.cfg.V2SegmentBlocks
	if end > target {
		return false, fmt.Errorf("freezer: transaction-index target %d is not segment-aligned from coverage %d", target, coverage)
	}
	path, err := index.AncientDatadir()
	if err != nil {
		return false, err
	}
	runPath := rawdbfreezer.TransactionIndexRunPath(path, coverage, end)
	result, recovered, err := r.buildOrRecoverOnlineTransactionIndexRangeContext(ctx, path, runPath, coverage, end, r.cfg.TransactionIndexPrefixBits)
	if err != nil {
		return false, err
	}
	if err := index.PublishTransactionIndexRun(result); err != nil {
		return false, err
	}
	r.txIndexRowsArchived.Add(result.Rows)
	r.updateMetrics()
	log.Info("Freezer: published online transaction index",
		"from", coverage, "to", end, "rows", result.Rows,
		"bytes", result.FileBytes, "recovered", recovered)
	return true, nil
}

func (r *Runner) pruneHotTransactionIndexRangeContext(ctx context.Context, start, end uint64) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	batch := r.chain.DB().NewBatchWithSize(txIndexDeleteBatchBytes)
	defer batch.Reset()
	flush := func() error {
		if batch.ValueSize() == 0 {
			return nil
		}
		if err := batch.Write(); err != nil {
			return err
		}
		batch.Reset()
		return nil
	}
	rows, err := r.iterateTransactionIndexEntriesContext(ctx, start, end, func(entry rawdbfreezer.TransactionIndexEntry) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.checkStopping(); err != nil {
			return err
		}
		if err := rawdb.DeleteTransactionIndex(batch, entry.Hash[:]); err != nil {
			return err
		}
		if batch.ValueSize() >= txIndexDeleteBatchBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return rows, flush()
}

func (r *Runner) commitTransactionIndexPrune(end, rows uint64) error {
	if err := rawdb.WriteStageProgress(r.chain.DB(), rawdb.StageFreezerTxIndexPrune, end); err != nil {
		return err
	}
	if syncer, ok := r.chain.DB().(interface{ SyncKeyValue() error }); ok {
		if err := syncer.SyncKeyValue(); err != nil {
			return err
		}
	}
	r.txIndexRowsPruned.Add(rows)
	r.updateMetrics()
	return nil
}

// pruneTransactionIndexDebtContext deletes at most one immutable-covered
// segment of hot tx-* rows. The durable stage advances only after every
// idempotent delete batch has completed, so an interruption simply retries the
// same segment without exposing a lookup gap.
func (r *Runner) pruneTransactionIndexDebtContext(ctx context.Context, maxEnd uint64) (bool, error) {
	index, ok := r.freezer.(TransactionIndexCompactor)
	if !ok || !r.cfg.TransactionIndexEnabled {
		return false, nil
	}
	coverage := index.TransactionIndexCoverage()
	if coverage > maxEnd {
		coverage = maxEnd
	}
	pruned, initialized, err := r.transactionIndexPruneProgress(coverage)
	if err != nil || !initialized {
		return false, err
	}
	if pruned > coverage {
		return false, fmt.Errorf("freezer: transaction index prune progress %d exceeds eligible coverage %d", pruned, coverage)
	}
	if pruned == coverage {
		return false, nil
	}
	end := pruned + r.cfg.V2SegmentBlocks
	if end > coverage {
		end = coverage
	}
	rows, err := r.pruneHotTransactionIndexRangeContext(ctx, pruned, end)
	if err != nil {
		return false, err
	}
	if err := r.commitTransactionIndexPrune(end, rows); err != nil {
		return false, err
	}
	log.Info("Freezer: pruned direct-V2 hot transaction indexes", "from", pruned, "to", end, "rows", rows)
	return true, nil
}

// serviceDirectTransactionIndexDebt gives immutable-covered tx-* cleanup
// priority over publishing another direct V2 segment. Unlike optional
// maintenance, this debt is part of the publication pipeline and therefore is
// not subject to startup/catch-up throttles which could starve it forever.
func (r *Runner) serviceDirectTransactionIndexDebt(maxEnd uint64) (bool, error) {
	if !r.cfg.V2Enabled || !r.cfg.TransactionIndexEnabled {
		return false, nil
	}
	index, ok := r.freezer.(TransactionIndexCompactor)
	if !ok {
		return false, nil
	}
	coverage := index.TransactionIndexCoverage()
	if coverage > maxEnd {
		return false, fmt.Errorf("freezer: transaction index coverage %d exceeds direct V2 coverage %d", coverage, maxEnd)
	}
	pruned, initialized, err := r.transactionIndexPruneProgress(coverage)
	if err != nil || !initialized {
		return false, err
	}
	if pruned > coverage {
		return false, fmt.Errorf("freezer: transaction index prune progress %d exceeds direct coverage %d", pruned, coverage)
	}
	if pruned == coverage && coverage == maxEnd {
		return false, nil
	}
	if r.cfg.HeavyMaintenanceErrorBackoff > 0 {
		if failedAt := r.lastTxIndexMaintenanceError.Load(); failedAt > 0 && time.Since(time.Unix(0, failedAt)) < r.cfg.HeavyMaintenanceErrorBackoff {
			r.recordHeavyMaintenanceDeferred(heavyMaintenanceTxIndex, heavyMaintenanceDeferredErrorBackoff)
			return true, nil
		}
	}
	release, ok := r.cfg.HeavyWorkGate.TryAcquire()
	if !ok {
		r.recordHeavyMaintenanceDeferred(heavyMaintenanceTxIndex, heavyMaintenanceDeferredResource)
		return true, nil
	}
	defer release()
	changed := false
	if pruned < coverage {
		changed, err = r.pruneTransactionIndexDebtContext(r.pauseCtx, coverage)
	} else {
		target := coverage + r.cfg.V2SegmentBlocks
		if target > maxEnd {
			target = maxEnd
		}
		changed, err = r.ensureTransactionIndexCoverageContext(r.pauseCtx, target)
		if err == nil && changed {
			_, err = r.pruneTransactionIndexDebtContext(r.pauseCtx, target)
		}
	}
	if err != nil {
		r.lastTxIndexMaintenanceError.Store(time.Now().UnixNano())
		r.txIndexErrors.Add(1)
		r.updateMetrics()
		return true, err
	}
	r.lastTxIndexMaintenanceError.Store(0)
	return changed, nil
}

func (r *Runner) updateMetrics() {
	r.metrics.update(r.snapshot())
}

// OnePass runs a single freezing pass synchronously and returns the
// number of blocks moved into ancient. Exported so tests can drive the
// pass deterministically without spinning up the loop.
//
// Returns nil error on success, even on no-op passes (e.g. chain hasn't
// produced enough blocks above the margin yet). Per-pass errors leave
// the freezer in a consistent state thanks to ModifyAncients' atomic
// rollback; the next pass simply retries.
func (r *Runner) OnePass() (frozen uint64, err error) {
	if err := r.checkStopping(); err != nil {
		return 0, err
	}
	start := time.Now()
	stageAdvanced := false
	defer func() {
		if errors.Is(err, errRunnerStopping) {
			return
		}
		r.lastPassUnixNano.Store(start.UnixNano())
		r.lastPassDuration.Store(int64(time.Since(start)))
		r.passesCompleted.Add(1)
		r.updateMetrics()
		if err == nil && stageAdvanced {
			r.notifyChainFreezerAdvance()
		}
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
	finishStage, hasFinishStage, err := r.verifiedFinishStageBlock()
	if err != nil {
		return 0, err
	}
	if hasFinishStage && finishStage < freezeTo {
		freezeTo = finishStage
	}

	// Resume from the freezer's own canonical position. Reading
	// AncientCount on every pass means we never need to persist a
	// separate cursor — the freezer table itself is the source of truth.
	freezeFromN, err := r.freezer.AncientCount(rawdbAncientBlocks)
	if err != nil {
		return 0, err
	}
	if hasFinishStage && freezeFromN > 0 && freezeFromN-1 > finishStage {
		return 0, fmt.Errorf("freezer: ancient head %d exceeds verified finish stage %d", freezeFromN-1, finishStage)
	}
	// Crash reconciliation. A crash that landed
	// between Phase 2 (ancient Sync) and Phase 3 (Pebble DeleteRange) of a
	// prior pass leaves blocks [x, freezeFromN) durably in ancient but
	// with their hot `b-`/`tib-` rows still in Pebble. No later pass would
	// ever revisit them — passes only delete the range they just froze
	// ([freezeFromN, cap)) — so the frozen-but-undeleted rows would leak
	// disk space forever. Detect the condition cheaply on every pass by probing
	// both highest frozen hot rows and sweep [0, freezeFromN) when either is
	// present. Checking every pass also repairs a Phase-3 delete failure from a
	// later segment without requiring a process restart. The
	// expensive DeleteRange+Compact only runs when a crash actually left
	// rows behind; a clean pass pays two point reads.
	if freezeFromN > 0 {
		leftoverHi := freezeFromN - 1
		leftoverBlock, leftoverTxInfos, err := rawdb.HasHotFrozenBlockRows(r.chain.DB(), leftoverHi)
		if err != nil {
			return 0, err
		}
		if leftoverBlock || leftoverTxInfos {
			if err := rawdb.DeleteFrozenBlockRange(r.chain.DB(), 0, leftoverHi); err != nil {
				return 0, err
			}
			advanced, err := r.writeChainFreezerStage(leftoverHi)
			if err != nil {
				return 0, err
			}
			stageAdvanced = stageAdvanced || advanced
			if err := r.checkStopping(); err != nil {
				return 0, err
			}
			r.compactFrozenHotRanges(0, leftoverHi, "crash-leftover")
			log.Info("Freezer: swept crash-leftover hot rows", "upTo", leftoverHi)
		}
	}
	if freezeFromN > 0 {
		advanced, err := r.writeChainFreezerStage(freezeFromN - 1)
		if err != nil {
			return 0, err
		}
		stageAdvanced = stageAdvanced || advanced
	}
	if err := r.checkStopping(); err != nil {
		return 0, err
	}

	// Progress the hash-keyed cleanup cursor on every pass. Direct V2 may leave
	// an incomplete segment hot for hours; tying this cleanup only to successful
	// segment publication would otherwise strand crash leftovers during that
	// entire interval.
	if freezeFromN > 0 {
		if err := r.pruneFrozenStateRoots(freezeFromN - 1); err != nil {
			return 0, err
		}
	}
	directAppender, directLayout := r.freezer.(V2DirectAppender)
	directLayout = directLayout && directAppender.CanAppendV2Direct(freezeFromN)
	if directLayout {
		handled, err := r.serviceDirectTransactionIndexDebt(freezeFromN)
		if err != nil {
			return 0, err
		}
		if handled {
			return 0, nil
		}
	}
	if freezeTo < freezeFromN {
		return 0, nil
	}
	// The freezer pass works in half-open [freezeFromN, capExclusive). A
	// fresh/direct V2 store leaves an incomplete segment in canonical Pebble
	// and publishes exactly one complete segment, avoiding any V1 copy. Legacy
	// stores retain the bounded V1 path until their suffix is fully reclaimed.
	capExclusive := freezeTo + 1
	// Once an immutable-only direct layout has advanced, V1 append cannot resume
	// at its logical head. The kill switch therefore pauses future publication;
	// it must not turn into a noisy, permanently failing V1 fallback.
	if directLayout && freezeFromN > 0 && (!r.cfg.V2Enabled || !r.cfg.DirectV2) {
		return 0, nil
	}
	directV2 := directLayout && r.cfg.V2Enabled && r.cfg.DirectV2
	if directV2 && r.cfg.V2PromotionAllowed != nil && !r.cfg.V2PromotionAllowed() {
		return 0, nil
	}
	if directV2 && freezeFromN%r.cfg.V2SegmentBlocks == 0 {
		if capExclusive-freezeFromN < r.cfg.V2SegmentBlocks {
			return 0, nil
		}
		capExclusive = freezeFromN + r.cfg.V2SegmentBlocks
		if r.cfg.ExternalizeV2ReceiptLogs {
			coverage, ok := r.chain.(receiptLogCoverageSource)
			if !ok {
				return 0, errors.New("freezer: receipt-log externalization requires an event-log coverage source")
			}
			coverageFrom := freezeFromN
			if coverageFrom == 0 {
				// Genesis has no transaction logs and event-log manifests begin at
				// block one. Do not make the first V2 segment wait for impossible
				// block-zero event coverage.
				coverageFrom = 1
			}
			if coverageFrom < capExclusive {
				covered, err := coverage.ReceiptLogRangeCovered(coverageFrom, capExclusive-1)
				if err != nil {
					return 0, fmt.Errorf("freezer: verify event-log coverage for receipt range [%d,%d): %w", freezeFromN, capExclusive, err)
				}
				if !covered {
					return 0, nil
				}
			}
		}
		release, ok := r.cfg.HeavyWorkGate.TryAcquire()
		if !ok {
			r.recordHeavyMaintenanceDeferred(heavyMaintenanceV2, heavyMaintenanceDeferredResource)
			return 0, nil
		}
		defer release()
	} else if r.cfg.BatchBlocks > 0 && capExclusive-freezeFromN > r.cfg.BatchBlocks {
		capExclusive = freezeFromN + r.cfg.BatchBlocks
	}
	if capExclusive <= freezeFromN {
		return 0, nil
	}
	var blockHashes []tcommon.Hash

	// Phase 1: publish one complete segment directly when possible. The V1
	// fallback remains for upgraded stores with an existing mutable suffix.
	if directV2 {
		var err error
		blockHashes, err = r.appendDirectV2Segment(directAppender, freezeFromN, capExclusive, r.cfg.ExternalizeV2ReceiptLogs)
		if err != nil {
			return 0, err
		}
	} else if _, err := r.freezer.ModifyAncients(func(op rawdb.AncientWriteOp) error {
		blockHashes = make([]tcommon.Hash, 0, capExclusive-freezeFromN)
		for n := freezeFromN; n < capExclusive; n++ {
			// Returning from the callback with an error makes ModifyAncients
			// truncate every table back to its pre-pass head. This is the only
			// safe way to interrupt phase 1 without exposing partial ancient
			// table cardinalities.
			if err := r.checkStopping(); err != nil {
				return err
			}
			blockRaw, ok, err := r.chain.ReadBlockRawStrict(n)
			if err != nil {
				return fmt.Errorf("freezer: read block %d: %w", n, err)
			}
			if !ok || len(blockRaw) == 0 {
				return errMissingBlock(n)
			}
			block, err := decodeFreezerBlockRaw(n, blockRaw)
			if err != nil {
				return err
			}
			if err := op.AppendRaw(rawdbAncientBlocks, n, blockRaw); err != nil {
				return err
			}
			txInfosRaw, _, err := r.chain.ReadTransactionInfosRawStrict(n)
			if err != nil {
				return fmt.Errorf("freezer: read tx infos for block %d: %w", n, err)
			}
			if err := validateFreezerTransactionInfosRaw(n, block, txInfosRaw); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdbAncientTxInfos, n, txInfosRaw); err != nil {
				return err
			}
			// State-root row is hash-keyed; resolve via the block proto.
			// Pre-AccountStateRoot fork blocks have no row, in which case
			// ReadBlockStateRootRaw returns nil — append nil so the
			// ancient table's per-num cardinality stays aligned with
			// `bodies` / `tx_infos`. Empty entries decode back to the
			// zero hash via the slice-2 read path, which matches the
			// pre-freezer Pebble miss → zero-hash behavior.
			hash := block.Hash()
			stateRoot, err := r.chain.ReadBlockStateRootRaw(hash)
			if err != nil {
				return fmt.Errorf("freezer: read state root for block %d hash %x: %w", n, hash.Bytes(), err)
			}
			if err := validateFreezerStateRootRaw(n, stateRoot); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdbAncientStateRoots, n, stateRoot); err != nil {
				return err
			}
			blockHashes = append(blockHashes, hash)
		}
		return nil
	}); err != nil {
		return 0, err
	}
	if directV2 {
		// A crash can publish the V2 manifest after building, but before
		// publishing, its fused transaction-index run. Recover or rebuild that
		// exact run before hot tx-* deletion and before another V2 segment may
		// advance past the gap.
		if _, err := r.ensureTransactionIndexCoverageContext(r.pauseCtx, capExclusive); err != nil {
			r.lastTxIndexMaintenanceError.Store(time.Now().UnixNano())
			r.txIndexErrors.Add(1)
			r.updateMetrics()
			return 0, err
		}
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
	// Once ModifyAncients has committed we must not honor cancellation until
	// after Sync: Pebble deletion is allowed to lag ancient durability, never
	// lead it. If shutdown lands here, the next process detects the durable
	// ancient head plus duplicate hot rows and reconciles them before freezing
	// new blocks.
	if err := r.checkStopping(); err != nil {
		return 0, err
	}

	// Phase 3: delete the now-frozen hot rows from Pebble. `b-<num>` and
	// `tib-<num>` leave through range tombstones while `bsr-<hash>` leaves in
	// the same batch: state_roots is already durable in ancient, and hot
	// bh-<hash> rows retain the ancient fallback path until cold chain-index
	// coverage permits pruning those lookup rows too.
	frozenHi := capExclusive - 1
	if err := rawdb.DeleteFrozenBlockRangeWithStateRoots(r.chain.DB(), freezeFromN, frozenHi, blockHashes); err != nil {
		return 0, err
	}
	advanced, err := r.writeChainFreezerStage(frozenHi)
	if err != nil {
		return 0, err
	}
	stageAdvanced = stageAdvanced || advanced
	if directV2 {
		if err := r.fastForwardDirectStateRootPruneStage(freezeFromN, frozenHi); err != nil {
			return 0, err
		}
		// The fused immutable tx index was published under the same V2 commit.
		// Remove its hot duplicate before releasing the heavy-work lease so the
		// optional maintenance scheduler and its cooldown cannot starve cleanup.
		if _, err := r.pruneTransactionIndexDebtContext(r.pauseCtx, capExclusive); err != nil {
			r.lastTxIndexMaintenanceError.Store(time.Now().UnixNano())
			r.txIndexErrors.Add(1)
			r.updateMetrics()
			return 0, err
		}
		r.lastTxIndexMaintenanceError.Store(0)
	}
	// Compaction only reclaims space; it is not part of the data-durability
	// transition. Skip starting it during shutdown. A compaction already inside
	// Pebble v1 cannot be interrupted, but this check and the loop's stop
	// priority ensure shutdown never starts another one.
	if err := r.checkStopping(); err != nil {
		return 0, err
	}

	// Phase 4: compact the freed range. Pebble turns DeleteRange into
	// range tombstones, which are O(1) on the write path but only reclaim
	// space when their containing SSTables get compacted. Explicit
	// Compact triggers a synchronous compaction so the operator sees the
	// datadir shrink without waiting for background compaction to roll
	// through (which can take hours on a healthy LSM).
	r.compactFrozenHotRanges(freezeFromN, frozenHi, "freeze")
	if err := r.pruneFrozenStateRoots(frozenHi); err != nil {
		return 0, err
	}
	if err := r.checkStopping(); err != nil {
		return 0, err
	}

	// Phase 5: update stats. PebbleSizeAfter is sampled by an iterator
	// pass on the still-hot `b-` prefix — cheap because after a successful
	// freeze the prefix only holds the post-margin window.
	frozen = capExclusive - freezeFromN
	pebbleSize, err := r.pebbleBlockNamespaceSize()
	if err != nil {
		return 0, err
	}
	r.blocksFrozen.Add(frozen)
	r.pebbleSizeAfter.Store(pebbleSize)
	return frozen, nil
}

func (r *Runner) appendDirectV2Segment(appender V2DirectAppender, start, end uint64, externalizeReceiptLogs bool) ([]tcommon.Hash, error) {
	if appender == nil || end <= start {
		return nil, errors.New("freezer: invalid direct V2 segment")
	}
	hashes := make([]tcommon.Hash, end-start)
	readSource := func(kind string, number uint64) ([]byte, error) {
		if number < start || number >= end {
			return nil, fmt.Errorf("freezer: direct V2 source %s[%d] outside [%d,%d)", kind, number, start, end)
		}
		if err := r.checkStopping(); err != nil {
			return nil, err
		}
		index := number - start
		switch kind {
		case rawdbAncientBlocks:
			blockRaw, ok, err := r.chain.ReadBlockRawStrict(number)
			if err != nil {
				return nil, fmt.Errorf("freezer: read block %d: %w", number, err)
			}
			if !ok || len(blockRaw) == 0 {
				return nil, errMissingBlock(number)
			}
			block, err := decodeFreezerBlockRaw(number, blockRaw)
			if err != nil {
				return nil, err
			}
			hashes[index] = block.Hash()
			return blockRaw, nil
		case rawdbAncientTxInfos:
			blockRaw, ok, err := r.chain.ReadBlockRawStrict(number)
			if err != nil {
				return nil, fmt.Errorf("freezer: read block %d for tx infos: %w", number, err)
			}
			if !ok || len(blockRaw) == 0 {
				return nil, errMissingBlock(number)
			}
			block, err := decodeFreezerBlockRaw(number, blockRaw)
			if err != nil {
				return nil, err
			}
			raw, _, err := r.chain.ReadTransactionInfosRawStrict(number)
			if err != nil {
				return nil, fmt.Errorf("freezer: read tx infos for block %d: %w", number, err)
			}
			if err := validateFreezerTransactionInfosRaw(number, block, raw); err != nil {
				return nil, err
			}
			return raw, nil
		case rawdbAncientStateRoots:
			hash := hashes[index]
			if hash == (tcommon.Hash{}) {
				blockRaw, ok, err := r.chain.ReadBlockRawStrict(number)
				if err != nil {
					return nil, fmt.Errorf("freezer: read block %d for state root: %w", number, err)
				}
				if !ok || len(blockRaw) == 0 {
					return nil, errMissingBlock(number)
				}
				block, err := decodeFreezerBlockRaw(number, blockRaw)
				if err != nil {
					return nil, err
				}
				hash = block.Hash()
				hashes[index] = hash
			}
			raw, err := r.chain.ReadBlockStateRootRaw(hash)
			if err != nil {
				return nil, fmt.Errorf("freezer: read state root for block %d hash %x: %w", number, hash.Bytes(), err)
			}
			if err := validateFreezerStateRootRaw(number, raw); err != nil {
				return nil, err
			}
			return raw, nil
		default:
			return nil, fmt.Errorf("freezer: unknown direct V2 table %s", kind)
		}
	}
	transform := rawdb.CompactAncientV2Record
	if externalizeReceiptLogs {
		transform = rawdb.CompactAncientV2RecordWithExternalLogs
	}
	options := rawdbfreezer.V2MigrationOptions{
		Tables:                     []string{rawdbAncientBlocks, rawdbAncientTxInfos, rawdbAncientStateRoots},
		SegmentBlocks:              end - start,
		FrameBlocks:                r.cfg.V2FrameBlocks,
		MaxSegments:                1,
		Online:                     true,
		Context:                    r.pauseCtx,
		Source:                     readSource,
		SourceHead:                 end,
		Transform:                  transform,
		TransactionIndexPrefixBits: r.cfg.TransactionIndexPrefixBits,
	}
	if r.cfg.TransactionIndexEnabled {
		options.TransactionIndexEntries = rawdb.AncientTransactionIndexEntries
	}
	result, err := appender.MigrateV2(options)
	if err != nil {
		return nil, err
	}
	if result.Start != start || result.End != end || result.Segments != 1 {
		return nil, fmt.Errorf("freezer: direct V2 published range [%d,%d) segments=%d, want [%d,%d) segments=1", result.Start, result.End, result.Segments, start, end)
	}
	// Same-process recovery of a manifest that was published before live
	// installation deliberately skips the source scan. Reconstruct any missing
	// hashes before deleting hot state-root rows; otherwise the V2 data is safe
	// but bsr-* rows from the recovered segment would leak in Pebble.
	for number := start; number < end; number++ {
		index := number - start
		if hashes[index] != (tcommon.Hash{}) {
			continue
		}
		blockRaw, ok, err := r.chain.ReadBlockRawStrict(number)
		if err != nil {
			return nil, fmt.Errorf("freezer: recover block hash %d: %w", number, err)
		}
		if !ok || len(blockRaw) == 0 {
			return nil, errMissingBlock(number)
		}
		block, err := decodeFreezerBlockRaw(number, blockRaw)
		if err != nil {
			return nil, err
		}
		hashes[index] = block.Hash()
	}
	if result.TransactionIndexRows > 0 {
		r.txIndexRowsArchived.Add(result.TransactionIndexRows)
	}
	r.v2BlocksCompacted.Add(end - start)
	r.v2LastBatchSegments.Store(1)
	r.v2LastBatchDuration.Store(int64(result.Elapsed))
	r.updateMetrics()
	return hashes, nil
}

func decodeFreezerBlockRaw(blockNum uint64, raw []byte) (*coretypes.Block, error) {
	block, err := coretypes.UnmarshalBlock(raw)
	if err != nil {
		return nil, fmt.Errorf("freezer: decode block %d: %w", blockNum, err)
	}
	if block.Number() != blockNum {
		return nil, fmt.Errorf("freezer: block row %d contains block number %d", blockNum, block.Number())
	}
	return block, nil
}

func validateFreezerTransactionInfosRaw(blockNum uint64, block *coretypes.Block, raw []byte) error {
	if len(raw) == 0 {
		if blockNum == 0 {
			var txs []*coretypes.Transaction
			if block != nil {
				txs = block.Transactions()
			}
			return rawdb.ValidateTransactionInfosForBlock(blockNum, txs, nil, "chain freezer append")
		}
		if block != nil && len(block.Transactions()) != 0 {
			return fmt.Errorf("freezer: missing transaction info coverage for block %d with %d transactions", blockNum, len(block.Transactions()))
		}
		return nil
	}
	var ret corepb.TransactionRet
	if err := proto.Unmarshal(raw, &ret); err != nil {
		return fmt.Errorf("freezer: decode tx infos for block %d: %w", blockNum, err)
	}
	if ret.BlockNumber != 0 && ret.BlockNumber != int64(blockNum) {
		return fmt.Errorf("freezer: tx infos row %d contains block number %d", blockNum, ret.BlockNumber)
	}
	return rawdb.ValidateTransactionInfosForBlock(blockNum, block.Transactions(), ret.Transactioninfo, "chain freezer append")
}

func validateFreezerStateRootRaw(blockNum uint64, raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	if len(raw) != tcommon.HashLength {
		return fmt.Errorf("freezer: state root for block %d has length %d, want %d", blockNum, len(raw), tcommon.HashLength)
	}
	return nil
}

func (r *Runner) compactFrozenHotRanges(lo, hi uint64, reason string) {
	if r == nil || r.chain == nil || r.chain.DB() == nil || lo > hi {
		return
	}
	ranges := []struct {
		name         string
		start, limit []byte
	}{
		{name: rawdbAncientBlocks},
		{name: rawdbAncientTxInfos},
	}
	ranges[0].start, ranges[0].limit = rawdb.BlockRangeBounds(lo, hi)
	ranges[1].start, ranges[1].limit = rawdb.TxInfoBlockRangeBounds(lo, hi)
	for index, keyRange := range ranges {
		if index > 0 && r.checkStopping() != nil {
			return
		}
		if err := r.chain.DB().Compact(keyRange.start, keyRange.limit); err != nil {
			// Compaction only reclaims physical space. Logical deletion already
			// committed atomically, so a failure is safe to retry in the background.
			log.Warn("Freezer: compact failed (rows still deleted)",
				"reason", reason, "table", keyRange.name,
				"from", lo, "to", hi, "err", err)
		}
	}
}

// fastForwardDirectStateRootPruneStage records the full direct segment in one
// step when Phase 3 has just atomically deleted every bsr-* row in it. The
// cursor advances only across a proven contiguous boundary; gaps left by older
// binaries continue through the bounded pruneFrozenStateRoots fallback.
func (r *Runner) fastForwardDirectStateRootPruneStage(start, end uint64) error {
	if r == nil || r.chain == nil || r.chain.DB() == nil || end < start {
		return nil
	}
	current, ok, err := rawdb.ReadStageProgressRow(r.chain.DB(), rawdb.StageChainFreezerStateRootPrune)
	if err != nil {
		return err
	}
	contiguous := !ok && start == 0
	if ok && current.HasBlockHash && current.BlockNum < ^uint64(0) && current.BlockNum+1 == start {
		contiguous = true
	}
	if !contiguous {
		return nil
	}
	_, err = r.writeChainFreezerStateRootPruneStage(end)
	return err
}

func (r *Runner) verifiedFinishStageBlock() (uint64, bool, error) {
	if r == nil || r.chain == nil || r.chain.DB() == nil {
		return 0, false, nil
	}
	block, ok, err := rawdb.ReadVerifiedStageProgressBlockWithHashLookup(
		r.chain.DB(),
		rawdb.StageFinish,
		r.chain.ReadBlockHashByNumberStrict,
	)
	if err != nil {
		return 0, ok, fmt.Errorf("freezer: %w", err)
	}
	return block, ok, nil
}

func (r *Runner) writeChainFreezerStage(blockNum uint64) (bool, error) {
	return r.writeHashBoundStage(rawdb.StageChainFreezer, blockNum)
}

func (r *Runner) writeChainFreezerStateRootPruneStage(blockNum uint64) (bool, error) {
	return r.writeHashBoundStage(rawdb.StageChainFreezerStateRootPrune, blockNum)
}

func (r *Runner) writeHashBoundStage(stage rawdb.StageID, blockNum uint64) (bool, error) {
	db := r.chain.DB()
	blockHash, ok, err := r.chain.ReadBlockHashByNumberStrict(blockNum)
	if err != nil {
		return false, fmt.Errorf("freezer: read canonical hash for %s stage %d: %w", stage, blockNum, err)
	}
	if !ok || blockHash == (tcommon.Hash{}) {
		return false, fmt.Errorf("freezer: cannot resolve block hash for %s stage %d", stage, blockNum)
	}
	current, ok, err := rawdb.ReadStageProgressRow(db, stage)
	if err != nil {
		return false, err
	}
	if ok && current.BlockNum > blockNum {
		return false, fmt.Errorf("freezer: %s stage %d is ahead of local ancient head %d", stage, current.BlockNum, blockNum)
	}
	if ok && current.BlockNum == blockNum && current.HasBlockHash {
		if current.BlockHash != blockHash {
			return false, fmt.Errorf("freezer: %s stage %d hash %x does not match canonical hash %x", stage, blockNum, current.BlockHash, blockHash)
		}
		return false, nil
	}
	if err := rawdb.WriteStageProgressWithHash(db, stage, blockNum, blockHash); err != nil {
		return false, err
	}
	return true, nil
}

// pruneFrozenStateRoots incrementally removes bsr- rows left by freezer
// versions that kept hash-keyed state roots hot. The same Config.BatchBlocks
// budget bounds migration work so an upgrade cannot monopolize one freezer
// pass. Ancient state-root presence is checked before every delete; the
// hash-bound progress row makes retries and restarts idempotent.
func (r *Runner) pruneFrozenStateRoots(upTo uint64) error {
	if r == nil || r.chain == nil || r.chain.DB() == nil || r.freezer == nil {
		return nil
	}
	current, hasCurrent, err := rawdb.ReadStageProgressRow(r.chain.DB(), rawdb.StageChainFreezerStateRootPrune)
	if err != nil {
		return err
	}
	start := uint64(0)
	if hasCurrent {
		if current.BlockNum > upTo {
			return fmt.Errorf("freezer: %s stage %d is ahead of local ancient head %d", rawdb.StageChainFreezerStateRootPrune, current.BlockNum, upTo)
		}
		if current.HasBlockHash {
			canonicalHash, ok, err := r.chain.ReadBlockHashByNumberStrict(current.BlockNum)
			if err != nil {
				return fmt.Errorf("freezer: read canonical hash for %s stage %d: %w", rawdb.StageChainFreezerStateRootPrune, current.BlockNum, err)
			}
			if !ok || canonicalHash == (tcommon.Hash{}) {
				return fmt.Errorf("freezer: cannot resolve block hash for %s stage %d", rawdb.StageChainFreezerStateRootPrune, current.BlockNum)
			}
			if current.BlockHash != canonicalHash {
				return fmt.Errorf("freezer: %s stage %d hash %x does not match canonical hash %x", rawdb.StageChainFreezerStateRootPrune, current.BlockNum, current.BlockHash, canonicalHash)
			}
			if current.BlockNum == upTo {
				return nil
			}
			start = current.BlockNum + 1
		} else {
			// Reprocess the legacy unbound boundary so it is upgraded to a
			// hash-bound row before later blocks are skipped.
			start = current.BlockNum
		}
	}
	if start > upTo {
		return nil
	}
	end := upTo
	if limit := r.cfg.BatchBlocks; limit > 0 && end-start+1 > limit {
		end = start + limit - 1
	}
	hashes := make([]tcommon.Hash, 0, end-start+1)
	for number := start; ; number++ {
		// No mutation occurs until the complete hash slice is validated, so an
		// interrupted migration can simply restart from its persisted stage.
		if err := r.checkStopping(); err != nil {
			return err
		}
		hasRoot, err := r.freezer.HasAncient(rawdb.AncientStateRootsTable, number)
		if err != nil {
			return fmt.Errorf("freezer: check ancient state root %d: %w", number, err)
		}
		if !hasRoot {
			return fmt.Errorf("freezer: ancient state root %d is missing below ChainFreezer stage", number)
		}
		blockHash, ok, err := r.chain.ReadBlockHashByNumberStrict(number)
		if err != nil {
			return fmt.Errorf("freezer: read canonical hash for frozen block %d while pruning state roots: %w", number, err)
		}
		if !ok || blockHash == (tcommon.Hash{}) {
			return fmt.Errorf("freezer: cannot resolve canonical hash for frozen block %d while pruning state roots", number)
		}
		hashes = append(hashes, blockHash)
		if number == end {
			break
		}
	}
	if err := rawdb.DeleteFrozenBlockStateRoots(r.chain.DB(), hashes); err != nil {
		return fmt.Errorf("freezer: delete frozen state roots [%d,%d]: %w", start, end, err)
	}
	_, err = r.writeChainFreezerStateRootPruneStage(end)
	return err
}

// loop is the goroutine. Fires once on Start so a fresh-install backlog
// begins draining without waiting an interval, then ticks on
// cfg.Interval until quit is signalled.
func (r *Runner) loop() {
	defer close(r.done)

	if frozen, err := r.OnePass(); err != nil {
		if errors.Is(err, errRunnerStopping) {
			return
		}
		log.Warn("Freezer: initial pass failed", "err", err)
	} else if frozen > 0 {
		log.Info("Freezer: initial pass frozen", "blocks", frozen)
	}
	if compacted, err := r.compactV2Scheduled(); err != nil && !errors.Is(err, context.Canceled) {
		log.Warn("Freezer: initial V2 compaction failed", "err", err)
	} else if compacted > 0 {
		log.Info("Freezer: initial V2 compaction complete", "blocks", compacted)
	}
	if changed, err := r.MaintainTransactionIndexOnce(); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, errRunnerStopping) {
		log.Warn("Freezer: initial transaction-index maintenance failed", "err", err)
	} else if changed {
		log.Info("Freezer: initial transaction-index maintenance complete")
	}

	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()
	for {
		// Give shutdown priority over an already-buffered ticker or requested
		// pass. The second check inside OnePass closes the remaining select race.
		select {
		case <-r.quit:
			return
		default:
		}
		select {
		case <-r.quit:
			return
		case <-ticker.C:
			if frozen, err := r.OnePass(); err != nil {
				if errors.Is(err, errRunnerStopping) {
					return
				}
				log.Warn("Freezer: pass failed", "err", err)
			} else if frozen > 0 {
				log.Info("Freezer: pass frozen", "blocks", frozen)
			}
			if compacted, err := r.compactV2Scheduled(); err != nil && !errors.Is(err, context.Canceled) {
				log.Warn("Freezer: V2 compaction failed", "err", err)
			} else if compacted > 0 {
				log.Info("Freezer: V2 compaction complete", "blocks", compacted)
			}
			if changed, err := r.MaintainTransactionIndexOnce(); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, errRunnerStopping) {
				log.Warn("Freezer: transaction-index maintenance failed", "err", err)
			} else if changed {
				log.Info("Freezer: transaction-index maintenance complete")
			}
		case <-r.wake:
			if frozen, err := r.OnePass(); err != nil {
				if errors.Is(err, errRunnerStopping) {
					return
				}
				log.Warn("Freezer: requested pass failed", "err", err)
			} else if frozen > 0 {
				log.Info("Freezer: requested pass frozen", "blocks", frozen)
			}
			if compacted, err := r.compactV2Scheduled(); err != nil && !errors.Is(err, context.Canceled) {
				log.Warn("Freezer: requested V2 compaction failed", "err", err)
			} else if compacted > 0 {
				log.Info("Freezer: requested V2 compaction complete", "blocks", compacted)
			}
			if changed, err := r.MaintainTransactionIndexOnce(); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, errRunnerStopping) {
				log.Warn("Freezer: requested transaction-index maintenance failed", "err", err)
			} else if changed {
				log.Info("Freezer: requested transaction-index maintenance complete")
			}
		}
	}
}

// pebbleBlockNamespaceSize iterates `b-` rows and returns the cumulative
// key+value bytes. Approximate — Pebble's on-disk footprint after
// compression and block-overhead deduction is smaller — but accurate
// enough as an unbounded-growth detector. Called once at the end of each
// pass; cost is O(remaining-block-rows), which is bounded by
// MarginBlocks + BatchBlocks under steady state.
func (r *Runner) pebbleBlockNamespaceSize() (uint64, error) {
	it := r.chain.DB().NewIterator(blockNamespacePrefix, nil)
	defer it.Release()
	var size uint64
	for it.Next() {
		if err := r.checkStopping(); err != nil {
			return 0, err
		}
		size += uint64(len(it.Key()) + len(it.Value()))
	}
	return size, it.Error()
}

// blockNamespacePrefix is the `b-` prefix mirrored from
// core/rawdb/schema.go. Duplicated as a package-private constant so the
// freezer package doesn't have to reach into rawdb's private symbol set.
// rawdb's `blockPrefix` is package-private and the slice-1 schema is
// stable enough that mirroring is safer than exposing it. A future slice
// that changes the prefix must update both places.
var blockNamespacePrefix = []byte("b-")

// rawdbAncient* aliases rawdb's per-table ancient name constants so the
// runner, chain accessors, and snapshot installer stay on one table layout.
const (
	rawdbAncientBlocks     = rawdb.AncientBlocksTable
	rawdbAncientTxInfos    = rawdb.AncientTxInfosTable
	rawdbAncientStateRoots = rawdb.AncientStateRootsTable
)

// FreezerTableSet returns the table-name/config map the runner expects
// when opening the freezer. Used by cmd/gtron in the NewFreezer call so
// the slice 3 wiring doesn't have to coin its own list — it must stay
// synced with the runner's table-name constants above.
//
// Compression is Snappy for `bodies` (proto blobs compress well) and
// `tx_infos`; raw bytes for `state_roots` because 32-byte payloads
// already sit below Snappy's per-row overhead. All three tables are marked
// prunable so minimal-mode retention can advance the ancient virtual tail
// consistently across chain bodies, tx infos, and state roots.
func FreezerTableSet() map[string]rawdbfreezer.TableConfig {
	return map[string]rawdbfreezer.TableConfig{
		rawdbAncientBlocks:     {NoSnappy: false, Prunable: true},
		rawdbAncientTxInfos:    {NoSnappy: false, Prunable: true},
		rawdbAncientStateRoots: {NoSnappy: true, Prunable: true},
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
