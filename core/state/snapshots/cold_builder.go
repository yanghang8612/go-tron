package snapshots

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/common"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

var coldSnapshotLog = gtronlog.NewModule("core/state/snapshots")

const (
	defaultColdSnapshotInterval    = time.Minute
	defaultColdSnapshotBatchBlocks = uint64(5_000)
	// Match Erigon's current default aggregation step. TRON still ends a cold
	// step on a complete block boundary and retains BatchBlocks as a second cap.
	defaultColdSnapshotBatchTxNums = uint64(390_625)
	defaultColdSnapshotMetrics     = "state/snapshot/cold/"
	coldSnapshotCatchupRateReset   = 15 * time.Minute
)

var ErrCommitmentBranchRotationNotSolidified = errors.New("snapshots: commitment branch rotation boundary is not solidified")

// DefaultLatestBuildBlocks is the default latest-snapshot build cadence
// (~33h of TRON blocks). Coarse because latest builds full-scan every latest
// keyspace; CommitmentBranch shares this cadence too.
const DefaultLatestBuildBlocks = defaultColdSnapshotBatchBlocks * 8 // 40_000

// ChainSource is the narrow chain surface needed by the cold snapshot builder.
type ChainSource interface {
	DB() AggregatorDB
	LatestSolidifiedBlockNum() int64
}

type canonicalHashSource interface {
	CanonicalBlockHash(blockNum uint64) (common.Hash, bool)
}

type canonicalHashLookupSource interface {
	CanonicalBlockHashStrict(blockNum uint64) (common.Hash, bool, error)
}

type syncRemainingSource interface {
	SyncRemainingBlocks() (uint64, bool)
}

type commitmentBranchRotator interface {
	BeginCommitmentBranchRotation() (rawdb.CommitmentBranchRotation, bool, error)
	CompleteCommitmentBranchRotation(rawdb.CommitmentBranchRotation, *Manager) error
}

// Config controls the cold history snapshot builder lifecycle.
type Config struct {
	Dir            string
	Enabled        bool
	HistoryDataset SegmentDataset
	Interval       time.Duration
	HistoryWindow  uint64
	BatchBlocks    uint64
	// BatchTxNums bounds one base history step by transaction-number span.
	// The selected range always includes a complete final block, so a single
	// dense block may exceed the target. BatchBlocks independently caps sparse
	// ranges whose txNum frontier advances slowly.
	BatchTxNums            uint64
	CompactMaxSteps        uint64
	RetainObsoleteSegments bool
	// CatchupBuildMinInterval rate-limits history collation while sync is
	// active. One bounded batch may start per interval unless adaptive catch-up
	// below is enabled and its lag threshold is exceeded. Once sync is inactive
	// the backlog drains immediately; zero preserves the unthrottled behavior
	// used by offline tooling and tests.
	CatchupBuildMinInterval time.Duration
	// CatchupUnthrottledLagBlocks bypasses CatchupBuildMinInterval while sync is
	// active and more than this many complete hot-history blocks are ready to be
	// published. Each build remains bounded by BatchBlocks/BatchTxNums, and the
	// ordered SnapshotLifecycle still runs freezer/prune maintenance after every
	// published batch. Zero preserves the fixed-interval behavior.
	CatchupUnthrottledLagBlocks uint64
	// CatchupHeavyWorkCooldown shortens the recovery window installed after an
	// accelerated history build. It does not bypass a lease or cooldown already
	// in force. Zero keeps the HeavyWorkGate default, which preserves the normal
	// recovery window near the tip and for every other maintenance subsystem.
	CatchupHeavyWorkCooldown time.Duration
	// HeavyWorkGate prevents history/accessor construction from overlapping
	// optional freezer compression and index maintenance in the same process.
	// Losing non-blocking admission defers the pass to its normal cadence.
	HeavyWorkGate *maintenance.HeavyWorkGate
	// LatestBuildBlocks is the minimum number of solidified blocks that must
	// elapse between production latest-snapshot builds. 0 disables the latest
	// build pass entirely. Latest builds are full-keyspace scans, so all latest
	// datasets share this single coarse cadence rather than rebuilding every tick.
	LatestBuildBlocks uint64
	// DeferLatestBuildWhileSyncing prevents the full-keyspace latest snapshot
	// scan from competing with an active historical sync session. The regular
	// lifecycle/sync-complete wake builds it once the importer is idle.
	DeferLatestBuildWhileSyncing bool
	// DeferHistoryBuildWhileSyncing prevents history compression, companion
	// index construction, and derived sidecar work from competing with a node
	// that is farther behind than HistoryWindow. The ordered lifecycle drains
	// the bounded backlog immediately once sync enters the recent hot window.
	DeferHistoryBuildWhileSyncing bool
	// BuildSectionBlooms builds full-section cold section-bloom sidecars once
	// the state-history cutoff has fully covered the source block section.
	BuildSectionBlooms bool
	// BuildBalanceTraces builds cold balance-trace sidecars for the same block
	// range as a newly published state-history segment, but only when every
	// canonical block in that range already has a matching hot BlockBalanceTrace.
	BuildBalanceTraces bool
	// BuildEventLogs builds registered cold event-log sidecars for the same
	// block range as each newly published state-history segment.
	BuildEventLogs bool
	// EventLogVersion selects the main event-log writer. Zero/2 retains the
	// legacy V2 writer; 3 writes dictionary/framed V3 segments directly.
	EventLogVersion uint32
	// ColdChainVerificationCache carries the exact locally built event-log and
	// index pair into the chain-freezer lifecycle's semantic-proof cache. Only
	// outputs active in the newly published manifest are recorded; a restart
	// still authenticates their complete SHA-256 identities.
	ColdChainVerificationCache *ChainFreezerVerificationCache
	// ETL configures sorted scratch ingestion for derived cold sidecar builds
	// launched by this lifecycle pass. Zero values preserve collector defaults.
	ETL RestoreETLOptions
	// CatalogSigningKey optionally lets SnapshotLifecycle sign the final active
	// production manifest after a lifecycle pass. CatalogChain identifies the
	// chain served by that catalog.
	CatalogSigningKey ed25519.PrivateKey
	CatalogChain      *ChainIdentity
	// MetricsNamespace prefixes the cold-build production gauges. Tests may
	// override it so process-global metric registrations remain isolated.
	MetricsNamespace string
}

// PassResult describes a single cold snapshot builder pass.
type PassResult struct {
	Built                bool
	HistoryDeferred      bool
	HistoryRateLimited   bool
	HistoryAccelerated   bool
	HistoryGateDeferred  bool
	HistoryRetryAfter    time.Duration
	LatestBuilt          bool
	LatestDeferred       bool
	Compaction           HistoryCompactionResult
	FromTxNum            uint64
	ToTxNum              uint64
	FromBlock            uint64
	ToBlock              uint64
	CutoffBlock          uint64
	EligibleCutoffBlock  uint64
	PublishedBlock       uint64
	SolidifiedBlock      uint64
	PreviousVisibleTx    uint64
	Segment              SegmentRef
	Segments             []SegmentRef
	SectionBloomBuilt    bool
	BalanceTraceBuilt    bool
	EventLogBuilt        bool
	CatalogPublished     bool
	Manifest             *Manifest
	HistoryDuration      time.Duration
	BalanceTraceDuration time.Duration
	EventLogDuration     time.Duration
	SectionBloomDuration time.Duration
	PublishDuration      time.Duration
	BuildDuration        time.Duration
	CompactionDuration   time.Duration
	LatestDuration       time.Duration
}

// NeedsCatchup reports whether a successful bounded history build published
// less than the verified cutoff that was ready when the pass began. Callers
// may schedule another pass immediately instead of waiting for the normal
// maintenance interval. A pass that made no progress never requests another
// run, which prevents malformed or temporarily incomplete hot ranges from
// spinning.
func (r PassResult) NeedsCatchup() bool {
	return r.Built && r.PublishedBlock < r.EligibleCutoffBlock
}

// Stats is a thread-safe snapshot of lifecycle progress.
type Stats struct {
	PassesCompleted         uint64
	PassErrors              uint64
	SegmentsBuilt           uint64
	SegmentsCompacted       uint64
	CompactionMerges        uint64
	CompactionCatchupDefers uint64
	BytesBuilt              uint64
	LastSolidified          uint64
	LastCutoffBlock         uint64
	LastEligibleCutoffBlock uint64
	LastPublishedBlock      uint64
	LastLagBlocks           uint64
	LastVisibleTxEnd        uint64
	LastFromTxNum           uint64
	LastToTxNum             uint64
	LastPassDuration        time.Duration
	// LastBuildDuration is the duration of the most recent successful history
	// build, not the eligibility-check latency of a later deferred pass.
	LastBuildDuration        time.Duration
	LastCompactionDuration   time.Duration
	LastCompactionMerges     uint64
	LastLatestDuration       time.Duration
	LatestDeferredSync       uint64
	HistoryDeferredSync      uint64
	HistoryRateLimitedSync   uint64
	HistoryAcceleratedBuilds uint64
	HistoryGateDeferred      uint64
	LastLatestBuildBlock     uint64
}

type coldRunnerMetrics struct {
	passes                   *metrics.Gauge
	errors                   *metrics.Gauge
	segmentsBuilt            *metrics.Gauge
	segmentsCompacted        *metrics.Gauge
	compactionMerges         *metrics.Gauge
	compactionCatchupDefers  *metrics.Gauge
	bytesBuilt               *metrics.Gauge
	lastSolidified           *metrics.Gauge
	lastEligibleCutoff       *metrics.Gauge
	lastSelectedCutoff       *metrics.Gauge
	lastPublishedBlock       *metrics.Gauge
	lagBlocks                *metrics.Gauge
	lastVisibleTxEnd         *metrics.Gauge
	lastFromTxNum            *metrics.Gauge
	lastToTxNum              *metrics.Gauge
	lastPassDuration         *metrics.Gauge
	lastBuildDuration        *metrics.Gauge
	lastCompactionDuration   *metrics.Gauge
	lastCompactionMerges     *metrics.Gauge
	lastLatestDuration       *metrics.Gauge
	latestDeferredSync       *metrics.Gauge
	historyDeferredSync      *metrics.Gauge
	historyRateLimitedSync   *metrics.Gauge
	historyAcceleratedBuilds *metrics.Gauge
	historyGateDeferred      *metrics.Gauge
	lastLatestBuildBlock     *metrics.Gauge
}

func newColdRunnerMetrics(namespace string) coldRunnerMetrics {
	namespace = normalizeColdSnapshotMetricNamespace(namespace)
	return coldRunnerMetrics{
		passes:                   metrics.GetOrRegisterGauge(namespace+"passes", nil),
		errors:                   metrics.GetOrRegisterGauge(namespace+"errors", nil),
		segmentsBuilt:            metrics.GetOrRegisterGauge(namespace+"segments/built", nil),
		segmentsCompacted:        metrics.GetOrRegisterGauge(namespace+"segments/compacted", nil),
		compactionMerges:         metrics.GetOrRegisterGauge(namespace+"compaction/merges", nil),
		compactionCatchupDefers:  metrics.GetOrRegisterGauge(namespace+"compaction/deferred/catchup", nil),
		bytesBuilt:               metrics.GetOrRegisterGauge(namespace+"bytes/built", nil),
		lastSolidified:           metrics.GetOrRegisterGauge(namespace+"last/solidified_block", nil),
		lastEligibleCutoff:       metrics.GetOrRegisterGauge(namespace+"last/eligible_cutoff_block", nil),
		lastSelectedCutoff:       metrics.GetOrRegisterGauge(namespace+"last/selected_cutoff_block", nil),
		lastPublishedBlock:       metrics.GetOrRegisterGauge(namespace+"last/published_block", nil),
		lagBlocks:                metrics.GetOrRegisterGauge(namespace+"lag/blocks", nil),
		lastVisibleTxEnd:         metrics.GetOrRegisterGauge(namespace+"last/visible_tx_end", nil),
		lastFromTxNum:            metrics.GetOrRegisterGauge(namespace+"last/from_tx", nil),
		lastToTxNum:              metrics.GetOrRegisterGauge(namespace+"last/to_tx", nil),
		lastPassDuration:         metrics.GetOrRegisterGauge(namespace+"lastpass/duration", nil),
		lastBuildDuration:        metrics.GetOrRegisterGauge(namespace+"lastpass/build/duration", nil),
		lastCompactionDuration:   metrics.GetOrRegisterGauge(namespace+"lastpass/compaction/duration", nil),
		lastCompactionMerges:     metrics.GetOrRegisterGauge(namespace+"lastpass/compaction/merges", nil),
		lastLatestDuration:       metrics.GetOrRegisterGauge(namespace+"lastpass/latest/duration", nil),
		latestDeferredSync:       metrics.GetOrRegisterGauge(namespace+"latest/deferred/sync", nil),
		historyDeferredSync:      metrics.GetOrRegisterGauge(namespace+"history/deferred/sync", nil),
		historyRateLimitedSync:   metrics.GetOrRegisterGauge(namespace+"history/deferred/rate_limit", nil),
		historyAcceleratedBuilds: metrics.GetOrRegisterGauge(namespace+"history/accelerated/builds", nil),
		historyGateDeferred:      metrics.GetOrRegisterGauge(namespace+"history/deferred/resource", nil),
		lastLatestBuildBlock:     metrics.GetOrRegisterGauge(namespace+"last/latest_build_block", nil),
	}
}

func normalizeColdSnapshotMetricNamespace(namespace string) string {
	if namespace == "" {
		namespace = defaultColdSnapshotMetrics
	}
	if !strings.HasSuffix(namespace, "/") {
		namespace += "/"
	}
	return namespace
}

func (m coldRunnerMetrics) update(stats Stats) {
	m.passes.Update(coldSnapshotUintGauge(stats.PassesCompleted))
	m.errors.Update(coldSnapshotUintGauge(stats.PassErrors))
	m.segmentsBuilt.Update(coldSnapshotUintGauge(stats.SegmentsBuilt))
	m.segmentsCompacted.Update(coldSnapshotUintGauge(stats.SegmentsCompacted))
	m.compactionMerges.Update(coldSnapshotUintGauge(stats.CompactionMerges))
	m.compactionCatchupDefers.Update(coldSnapshotUintGauge(stats.CompactionCatchupDefers))
	m.bytesBuilt.Update(coldSnapshotUintGauge(stats.BytesBuilt))
	m.lastSolidified.Update(coldSnapshotUintGauge(stats.LastSolidified))
	m.lastEligibleCutoff.Update(coldSnapshotUintGauge(stats.LastEligibleCutoffBlock))
	m.lastSelectedCutoff.Update(coldSnapshotUintGauge(stats.LastCutoffBlock))
	m.lastPublishedBlock.Update(coldSnapshotUintGauge(stats.LastPublishedBlock))
	m.lagBlocks.Update(coldSnapshotUintGauge(stats.LastLagBlocks))
	m.lastVisibleTxEnd.Update(coldSnapshotUintGauge(stats.LastVisibleTxEnd))
	m.lastFromTxNum.Update(coldSnapshotUintGauge(stats.LastFromTxNum))
	m.lastToTxNum.Update(coldSnapshotUintGauge(stats.LastToTxNum))
	m.lastPassDuration.Update(int64(stats.LastPassDuration))
	m.lastBuildDuration.Update(int64(stats.LastBuildDuration))
	m.lastCompactionDuration.Update(int64(stats.LastCompactionDuration))
	m.lastCompactionMerges.Update(coldSnapshotUintGauge(stats.LastCompactionMerges))
	m.lastLatestDuration.Update(int64(stats.LastLatestDuration))
	m.latestDeferredSync.Update(coldSnapshotUintGauge(stats.LatestDeferredSync))
	m.historyDeferredSync.Update(coldSnapshotUintGauge(stats.HistoryDeferredSync))
	m.historyRateLimitedSync.Update(coldSnapshotUintGauge(stats.HistoryRateLimitedSync))
	m.historyAcceleratedBuilds.Update(coldSnapshotUintGauge(stats.HistoryAcceleratedBuilds))
	m.historyGateDeferred.Update(coldSnapshotUintGauge(stats.HistoryGateDeferred))
	m.lastLatestBuildBlock.Update(coldSnapshotUintGauge(stats.LastLatestBuildBlock))
}

func coldSnapshotUintGauge(value uint64) int64 {
	const maxInt64GaugeValue = uint64(1<<63 - 1)
	if value > maxInt64GaugeValue {
		return int64(maxInt64GaugeValue)
	}
	return int64(value)
}

// Runner builds registered history snapshot segments in the background.
type Runner struct {
	chain   ChainSource
	cfg     Config
	metrics coldRunnerMetrics

	quit chan struct{}
	done chan struct{}

	startOnce   sync.Once
	stopOnce    sync.Once
	prepareOnce sync.Once
	passMu      sync.Mutex
	startErr    error
	prepareErr  error

	passesCompleted          atomic.Uint64
	passErrors               atomic.Uint64
	segmentsBuilt            atomic.Uint64
	segmentsCompacted        atomic.Uint64
	compactionMerges         atomic.Uint64
	compactionCatchupDefers  atomic.Uint64
	bytesBuilt               atomic.Uint64
	lastSolidified           atomic.Uint64
	lastCutoffBlock          atomic.Uint64
	lastEligibleCutoff       atomic.Uint64
	lastPublishedBlock       atomic.Uint64
	lastLagBlocks            atomic.Uint64
	lastVisibleTxEnd         atomic.Uint64
	lastFromTxNum            atomic.Uint64
	lastToTxNum              atomic.Uint64
	lastPassDuration         atomic.Int64
	lastBuildDuration        atomic.Int64
	lastCompactionDuration   atomic.Int64
	lastCompactionMerges     atomic.Uint64
	lastLatestDuration       atomic.Int64
	lastLatestBuildBlock     atomic.Uint64
	latestDeferredSync       atomic.Uint64
	historyDeferredSync      atomic.Uint64
	historyRateLimitedSync   atomic.Uint64
	historyAcceleratedBuilds atomic.Uint64
	historyGateDeferred      atomic.Uint64
	lastHistoryBuildAt       atomic.Int64
	// catchupRate is updated only while passMu is held and smooths the noisy
	// one-pass lag delta used for operator ETA logs.
	catchupRate float64
}

func NewRunner(chain ChainSource, cfg Config) *Runner {
	cfg = cfg.applyDefaults()
	runner := &Runner{
		chain:   chain,
		cfg:     cfg,
		metrics: newColdRunnerMetrics(cfg.MetricsNamespace),
		quit:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	runner.updateMetrics()
	return runner
}

func (c Config) applyDefaults() Config {
	if c.HistoryDataset == "" {
		c.HistoryDataset = SegmentDatasetStateDomainChange
	}
	if c.Interval <= 0 {
		c.Interval = defaultColdSnapshotInterval
	}
	if c.BatchBlocks == 0 {
		c.BatchBlocks = defaultColdSnapshotBatchBlocks
	}
	if c.BatchTxNums == 0 {
		c.BatchTxNums = defaultColdSnapshotBatchTxNums
	}
	if c.CompactMaxSteps == 0 {
		c.CompactMaxSteps = defaultCompactionMaxSteps
	}
	if c.MetricsNamespace == "" {
		c.MetricsNamespace = defaultColdSnapshotMetrics
	}
	if c.CatalogSigningKey != nil {
		c.CatalogSigningKey = append(ed25519.PrivateKey(nil), c.CatalogSigningKey...)
	}
	if c.CatalogChain != nil {
		identity := *c.CatalogChain
		c.CatalogChain = &identity
	}
	return c
}

func (c Config) validate() error {
	if !c.Enabled {
		return nil
	}
	if c.Dir == "" {
		return errors.New("snapshots: cold builder directory is empty")
	}
	if c.Interval <= 0 {
		return fmt.Errorf("snapshots: invalid cold builder interval %s", c.Interval)
	}
	if c.BatchBlocks == 0 {
		return errors.New("snapshots: cold builder batch blocks is zero")
	}
	if c.CatchupHeavyWorkCooldown < 0 {
		return fmt.Errorf("snapshots: invalid catch-up heavy-work cooldown %s", c.CatchupHeavyWorkCooldown)
	}
	if c.HistoryDataset == "" {
		c.HistoryDataset = SegmentDatasetStateDomainChange
	}
	cfg, ok := DefaultDomainRegistry().Dataset(c.HistoryDataset)
	if !ok || !cfg.HasHistory {
		return fmt.Errorf("snapshots: unknown cold builder history dataset %q", c.HistoryDataset)
	}
	if cfg.BuildHistory == nil {
		return fmt.Errorf("snapshots: history domain %s has no builder", c.HistoryDataset)
	}
	if c.CatalogChain != nil {
		identity := *c.CatalogChain
		normalizeChainIdentity(&identity)
		if err := validateChainIdentity(&identity); err != nil {
			return fmt.Errorf("snapshots: invalid catalog chain identity: %w", err)
		}
	}
	if len(c.CatalogSigningKey) != 0 {
		if len(c.CatalogSigningKey) != ed25519.PrivateKeySize {
			return fmt.Errorf("snapshots: catalog signing key length %d, want %d", len(c.CatalogSigningKey), ed25519.PrivateKeySize)
		}
		if c.CatalogChain == nil {
			return errors.New("snapshots: catalog signing requires a chain identity")
		}
	}
	return nil
}

// Start implements node.Lifecycle.
func (r *Runner) Start() error {
	if r == nil {
		return nil
	}
	r.startOnce.Do(func() {
		if r.quit == nil {
			r.quit = make(chan struct{})
		}
		if r.done == nil {
			r.done = make(chan struct{})
		}
		if err := r.Prepare(); err != nil {
			close(r.done)
			r.startErr = err
			return
		}
		if !r.cfg.Enabled {
			close(r.done)
			return
		}
		go r.loop()
		coldSnapshotLog.Info("History cold snapshot builder started",
			"dir", r.cfg.Dir,
			"dataset", r.cfg.HistoryDataset,
			"interval", r.cfg.Interval,
			"historyWindow", r.cfg.HistoryWindow,
			"batchBlocks", r.cfg.BatchBlocks,
			"batchTxNums", r.cfg.BatchTxNums,
			"catchupBuildMinInterval", r.cfg.CatchupBuildMinInterval,
			"catchupHeavyWorkCooldown", r.cfg.CatchupHeavyWorkCooldown,
			"sharedHeavyWorkGate", r.cfg.HeavyWorkGate != nil,
			"compactMaxSteps", r.cfg.CompactMaxSteps,
			"sectionBloomBuild", r.cfg.BuildSectionBlooms,
			"balanceTraceBuild", r.cfg.BuildBalanceTraces,
			"eventLogBuild", r.cfg.BuildEventLogs)
	})
	return r.startErr
}

// Prepare validates the runner and seeds its latest-build cadence watermark.
// It is shared by the standalone Runner lifecycle and SnapshotLifecycle, which
// owns the production loop and therefore must not call Runner.Start.
func (r *Runner) Prepare() error {
	if r == nil {
		return nil
	}
	r.prepareOnce.Do(func() {
		if err := r.cfg.validate(); err != nil {
			r.prepareErr = err
			return
		}
		if !r.cfg.Enabled {
			return
		}
		if r.chain == nil || r.chain.DB() == nil {
			r.prepareErr = errors.New("snapshots: nil cold builder chain or database")
			return
		}
		// Seed from the hash-bound persisted stage when present; a fresh node
		// uses its current solidified watermark so the first lifecycle tick does
		// not launch a redundant full-keyspace latest snapshot scan.
		r.prepareErr = r.seedLatestBuildBlock()
	})
	return r.prepareErr
}

// Stop implements node.Lifecycle.
func (r *Runner) Stop() error {
	if r == nil {
		return nil
	}
	r.stopOnce.Do(func() {
		if r.quit != nil {
			close(r.quit)
		}
	})
	if r.done != nil {
		<-r.done
	}
	coldSnapshotLog.Info("History cold snapshot builder stopped",
		"dataset", r.cfg.HistoryDataset,
		"passes", r.passesCompleted.Load(),
		"segments", r.segmentsBuilt.Load())
	return nil
}

// Snapshot returns a thread-safe copy of runner progress.
func (r *Runner) Snapshot() Stats {
	if r == nil {
		return Stats{}
	}
	return Stats{
		PassesCompleted:          r.passesCompleted.Load(),
		PassErrors:               r.passErrors.Load(),
		SegmentsBuilt:            r.segmentsBuilt.Load(),
		SegmentsCompacted:        r.segmentsCompacted.Load(),
		CompactionMerges:         r.compactionMerges.Load(),
		CompactionCatchupDefers:  r.compactionCatchupDefers.Load(),
		BytesBuilt:               r.bytesBuilt.Load(),
		LastSolidified:           r.lastSolidified.Load(),
		LastCutoffBlock:          r.lastCutoffBlock.Load(),
		LastEligibleCutoffBlock:  r.lastEligibleCutoff.Load(),
		LastPublishedBlock:       r.lastPublishedBlock.Load(),
		LastLagBlocks:            r.lastLagBlocks.Load(),
		LastVisibleTxEnd:         r.lastVisibleTxEnd.Load(),
		LastFromTxNum:            r.lastFromTxNum.Load(),
		LastToTxNum:              r.lastToTxNum.Load(),
		LastPassDuration:         time.Duration(r.lastPassDuration.Load()),
		LastBuildDuration:        time.Duration(r.lastBuildDuration.Load()),
		LastCompactionDuration:   time.Duration(r.lastCompactionDuration.Load()),
		LastCompactionMerges:     r.lastCompactionMerges.Load(),
		LastLatestDuration:       time.Duration(r.lastLatestDuration.Load()),
		LatestDeferredSync:       r.latestDeferredSync.Load(),
		HistoryDeferredSync:      r.historyDeferredSync.Load(),
		HistoryRateLimitedSync:   r.historyRateLimitedSync.Load(),
		HistoryAcceleratedBuilds: r.historyAcceleratedBuilds.Load(),
		HistoryGateDeferred:      r.historyGateDeferred.Load(),
		LastLatestBuildBlock:     r.lastLatestBuildBlock.Load(),
	}
}

// OnePass builds at most one registered history segment, compacts history with
// catch-up-aware geometric scheduling, then runs one latest-snapshot build pass
// if the cadence interval has elapsed.
func (r *Runner) OnePass() (PassResult, error) {
	return r.OnePassContext(context.Background())
}

func (r *Runner) OnePassContext(ctx context.Context) (PassResult, error) {
	if r == nil {
		return PassResult{}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return PassResult{}, err
	}
	r.passMu.Lock()
	defer r.passMu.Unlock()

	start := time.Now()
	phaseStart := start
	result, err := r.onePass()
	result.BuildDuration = coldSnapshotPhaseDuration(phaseStart)
	if err == nil {
		err = ctx.Err()
	}
	if err == nil && !result.HistoryDeferred {
		phaseStart = time.Now()
		result.Compaction, err = r.compactHistory(result.NeedsCatchup())
		result.CompactionDuration = coldSnapshotPhaseDuration(phaseStart)
		if err == nil {
			err = ctx.Err()
		}
	}
	if err == nil {
		phaseStart = time.Now()
		built, deferred, perr := r.latestPassWithStatusContext(ctx)
		result.LatestDuration = coldSnapshotPhaseDuration(phaseStart)
		result.LatestDeferred = deferred
		if perr != nil {
			err = perr
		} else {
			result.LatestBuilt = built
		}
	}
	r.recordPass(result, start, err)
	return result, err
}

func coldSnapshotPhaseDuration(start time.Time) time.Duration {
	elapsed := time.Since(start)
	if elapsed <= 0 {
		return time.Nanosecond
	}
	return elapsed
}

// PreflightCatalog verifies an existing manifest identity before a lifecycle
// pass can prune hot data. New manifests are bound and signed after the pass,
// once every manifest progress update is complete.
func (r *Runner) PreflightCatalog() error {
	if r == nil || len(r.cfg.CatalogSigningKey) == 0 {
		return nil
	}
	if r.cfg.CatalogChain == nil {
		return errors.New("snapshots: catalog signing requires a chain identity")
	}
	if err := r.cfg.validate(); err != nil {
		return err
	}
	if _, err := LoadProductionManifest(r.cfg.Dir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if _, err := EnsureProductionManifestChainIdentity(r.cfg.Dir, *r.cfg.CatalogChain); err != nil {
		return err
	}
	// Upgrade a valid legacy catalog before build/merge can mutate its root
	// manifest or retire referenced segments. The immutable copy becomes the
	// first downloader lease and closes the one-pass migration race.
	catalog, err := LoadSnapshotCatalog(r.cfg.Dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if catalog.ManifestPath == ManifestFile {
		_, err = PublishSignedSnapshotCatalog(r.cfg.Dir, r.cfg.CatalogSigningKey)
		return err
	}
	return nil
}

// PublishCatalogIfManifestChanged signs the final manifest view after a
// lifecycle pass. It avoids rehashing every cold segment when the existing
// catalog already authenticates the same manifest for this signer and chain.
func (r *Runner) PublishCatalogIfManifestChanged() (bool, error) {
	if r == nil || len(r.cfg.CatalogSigningKey) == 0 {
		return false, nil
	}
	if r.cfg.CatalogChain == nil {
		return false, errors.New("snapshots: catalog signing requires a chain identity")
	}
	if err := r.cfg.validate(); err != nil {
		return false, err
	}
	if _, err := LoadProductionManifest(r.cfg.Dir); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if _, err := EnsureProductionManifestChainIdentity(r.cfg.Dir, *r.cfg.CatalogChain); err != nil {
		return false, err
	}
	checksum, err := checksumFile(filepath.Join(r.cfg.Dir, ManifestFile))
	if err != nil {
		return false, err
	}
	if catalog, err := LoadSnapshotCatalog(r.cfg.Dir); err == nil &&
		strings.EqualFold(catalog.ManifestChecksum, checksum) &&
		catalog.ValidateChainIdentity(*r.cfg.CatalogChain) == nil &&
		catalog.VerifySignature([]ed25519.PublicKey{r.cfg.CatalogSigningKey.Public().(ed25519.PublicKey)}) == nil {
		return false, nil
	}
	if _, err := PublishSignedSnapshotCatalog(r.cfg.Dir, r.cfg.CatalogSigningKey); err != nil {
		return false, err
	}
	return true, nil
}

func (r *Runner) onePass() (PassResult, error) {
	if !r.cfg.Enabled {
		return PassResult{}, nil
	}
	if err := r.cfg.validate(); err != nil {
		return PassResult{}, err
	}
	if r.chain == nil {
		return PassResult{}, errors.New("snapshots: nil cold builder chain")
	}
	db := r.chain.DB()
	if db == nil {
		return PassResult{}, errors.New("snapshots: nil cold builder database")
	}
	historyCfg, ok := DefaultDomainRegistry().Dataset(r.cfg.HistoryDataset)
	if !ok || historyCfg.BuildHistory == nil {
		return PassResult{}, fmt.Errorf("snapshots: history domain %s is not registered", r.cfg.HistoryDataset)
	}

	solidified := r.chain.LatestSolidifiedBlockNum()
	if solidified <= 0 || uint64(solidified) < r.cfg.HistoryWindow {
		return PassResult{}, nil
	}
	cutoffBlock := uint64(solidified) - r.cfg.HistoryWindow
	finishStage, hasFinishStage, err := r.verifiedFinishStageBlock(db)
	if err != nil {
		return PassResult{}, err
	}
	if hasFinishStage && finishStage < cutoffBlock {
		cutoffBlock = finishStage
	}
	result := PassResult{
		SolidifiedBlock:     uint64(solidified),
		CutoffBlock:         cutoffBlock,
		EligibleCutoffBlock: cutoffBlock,
	}
	if r.cfg.DeferHistoryBuildWhileSyncing {
		if source, ok := r.chain.(syncRemainingSource); ok {
			if remaining, active := source.SyncRemainingBlocks(); active && remaining > r.cfg.HistoryWindow {
				result.HistoryDeferred = true
				return result, nil
			}
		}
	}

	cutoffRange, ok, err := historyCfg.HotHistoryTxRangeForBlock(db, cutoffBlock)
	if err != nil {
		return PassResult{}, err
	}
	if !ok {
		return result, nil
	}
	if cutoffRange.EndTxNum < cutoffRange.BeginTxNum {
		return PassResult{}, fmt.Errorf("snapshots: state tx range for block %d is inverted", cutoffBlock)
	}

	productionManifest, err := loadOptionalProductionManifest(r.cfg.Dir)
	if err != nil {
		return PassResult{}, err
	}
	visibleEnd, err := coldSnapshotVisibleTxEndFromManifest(productionManifest, r.cfg.HistoryDataset)
	if err != nil {
		return PassResult{}, err
	}
	result.PreviousVisibleTx = visibleEnd
	if visibleEnd == ^uint64(0) {
		return result, nil
	}
	searchFromBlock := uint64(0)
	if visibleEnd > 0 {
		previousBuildBlock, hasPreviousBuild, err := r.verifiedSnapshotBuildStageBlock(db)
		if err != nil {
			return PassResult{}, err
		}
		if hasPreviousBuild {
			if previousBuildBlock == ^uint64(0) {
				return result, nil
			}
			searchFromBlock = previousBuildBlock + 1
			result.PublishedBlock = previousBuildBlock
		}
	}
	fromTxNum := visibleEnd + 1
	if fromTxNum > cutoffRange.EndTxNum {
		return result, nil
	}

	toTxNum := cutoffRange.EndTxNum
	startBlock, ok, err := firstHotHistoryTxRangeBlockAtOrAfterTx(historyCfg, db, fromTxNum, searchFromBlock, cutoffBlock)
	if err != nil {
		return PassResult{}, err
	}
	if !ok {
		return result, nil
	}
	readyBlocks := cutoffBlock - startBlock + 1
	result.HistoryAccelerated = r.historyBuildAccelerated(readyBlocks)
	if r.historyBuildRateLimited(time.Now(), result.HistoryAccelerated) {
		result.HistoryDeferred = true
		result.HistoryRateLimited = true
		return result, nil
	}
	if r.cfg.BatchBlocks > 0 {
		batchCutoffBlock := startBlock + r.cfg.BatchBlocks - 1
		if batchCutoffBlock < startBlock || batchCutoffBlock > cutoffBlock {
			batchCutoffBlock = cutoffBlock
		}
		if batchCutoffBlock != cutoffBlock {
			cutoffBlock = batchCutoffBlock
			result.CutoffBlock = cutoffBlock
			cutoffRange, ok, err = historyCfg.HotHistoryTxRangeForBlock(db, cutoffBlock)
			if err != nil {
				return PassResult{}, err
			}
			if !ok {
				return result, nil
			}
			if cutoffRange.EndTxNum < cutoffRange.BeginTxNum {
				return PassResult{}, fmt.Errorf("snapshots: state tx range for block %d is inverted", cutoffBlock)
			}
			toTxNum = cutoffRange.EndTxNum
		}
	}
	if r.cfg.BatchTxNums > 0 && toTxNum >= fromTxNum && toTxNum-fromTxNum >= r.cfg.BatchTxNums {
		targetTxNum := fromTxNum + r.cfg.BatchTxNums - 1
		if targetTxNum < fromTxNum {
			targetTxNum = ^uint64(0)
		}
		txCutoffBlock, found, err := firstHotHistoryTxRangeBlockAtOrAfterTx(historyCfg, db, targetTxNum, startBlock, cutoffBlock)
		if err != nil {
			return PassResult{}, err
		}
		if found && txCutoffBlock < cutoffBlock {
			cutoffBlock = txCutoffBlock
			result.CutoffBlock = cutoffBlock
			cutoffRange, ok, err = historyCfg.HotHistoryTxRangeForBlock(db, cutoffBlock)
			if err != nil {
				return PassResult{}, err
			}
			if !ok {
				return result, nil
			}
			if cutoffRange.EndTxNum < cutoffRange.BeginTxNum {
				return PassResult{}, fmt.Errorf("snapshots: state tx range for block %d is inverted", cutoffBlock)
			}
			toTxNum = cutoffRange.EndTxNum
		}
	}
	if fromTxNum > toTxNum {
		return result, nil
	}
	var releaseHeavyWork func()
	var admitted bool
	if result.HistoryAccelerated && r.cfg.CatchupHeavyWorkCooldown > 0 {
		releaseHeavyWork, admitted = r.cfg.HeavyWorkGate.TryAcquireWithCooldown(r.cfg.CatchupHeavyWorkCooldown)
	} else {
		releaseHeavyWork, admitted = r.cfg.HeavyWorkGate.TryAcquire()
	}
	if !admitted {
		result.HistoryDeferred = true
		result.HistoryGateDeferred = true
		result.HistoryRetryAfter = r.cfg.HeavyWorkGate.CooldownRemaining()
		return result, nil
	}
	defer releaseHeavyWork()

	var snapshotBuildHash common.Hash
	if _, ok := db.(ethdb.KeyValueWriter); ok {
		snapshotBuildHash, err = r.snapshotBuildStageBoundaryHash(db, rawdb.StageSnapshotBuild, cutoffBlock)
		if err != nil {
			return PassResult{}, err
		}
	}

	var refs []SegmentRef
	historyBuildStarted := time.Now()
	buildBlocks := cutoffBlock - startBlock + 1
	buildTxs := toTxNum - fromTxNum + 1
	backlogBlocks := uint64(0)
	if result.EligibleCutoffBlock > cutoffBlock {
		backlogBlocks = result.EligibleCutoffBlock - cutoffBlock
	}
	coldSnapshotLog.Info("History cold snapshot build started",
		"dataset", r.cfg.HistoryDataset,
		"fromTx", fromTxNum,
		"toTx", toTxNum,
		"txs", buildTxs,
		"fromBlock", startBlock,
		"toBlock", cutoffBlock,
		"blocks", buildBlocks,
		"eligibleCutoffBlock", result.EligibleCutoffBlock,
		"backlogBlocks", backlogBlocks,
		"accelerated", result.HistoryAccelerated)
	buildProgress := startColdSnapshotBuildProgress(r.cfg.HistoryDataset, fromTxNum, toTxNum, startBlock, cutoffBlock, result.EligibleCutoffBlock, 0)
	defer buildProgress.Stop()
	if historyCfg.BuildHistoryBlockRange != nil {
		refs, err = historyCfg.BuildHistoryBlockRange(db, r.cfg.Dir, fromTxNum, toTxNum, startBlock, cutoffBlock, historyCfg.HistoryPath(fromTxNum, toTxNum))
	} else {
		refs, err = historyCfg.BuildHistory(db, r.cfg.Dir, fromTxNum, toTxNum, historyCfg.HistoryPath(fromTxNum, toTxNum))
	}
	if err != nil {
		coldSnapshotLog.Warn("History cold snapshot build failed",
			"dataset", r.cfg.HistoryDataset,
			"fromTx", fromTxNum,
			"toTx", toTxNum,
			"fromBlock", startBlock,
			"toBlock", cutoffBlock,
			"elapsed", time.Since(historyBuildStarted).Round(time.Millisecond),
			"err", err)
		return PassResult{}, err
	}
	if len(refs) == 0 {
		return result, nil
	}
	result.HistoryDuration = time.Since(historyBuildStarted)
	historyRefs := len(refs)
	historyBytes := segmentRefsSize(refs)
	coldSnapshotLog.Debug("History cold snapshot history files built",
		"dataset", r.cfg.HistoryDataset,
		"fromTx", fromTxNum,
		"toTx", toTxNum,
		"fromBlock", startBlock,
		"toBlock", cutoffBlock,
		"refs", historyRefs,
		"bytes", historyBytes,
		"elapsed", result.HistoryDuration.Round(time.Millisecond))
	buildProgress.SetPhase("prepare-derived")
	aggregator := NewAggregator(r.cfg.Dir)
	var chainDB *rawdb.ChainDB
	if r.cfg.BuildBalanceTraces || r.cfg.BuildEventLogs {
		chainDB, err = r.derivedIndexChainDB()
		if err != nil {
			return PassResult{}, err
		}
	}
	balanceTraceBuilt := false
	if r.cfg.BuildBalanceTraces {
		buildProgress.SetPhase("balance-trace")
		balanceTraceStarted := time.Now()
		traceRefs, err := r.balanceTracePass(chainDB, db, startBlock, cutoffBlock)
		if err != nil {
			return PassResult{}, err
		}
		if len(traceRefs) > 0 {
			refs = append(refs, traceRefs...)
			balanceTraceBuilt = true
		}
		result.BalanceTraceDuration = coldSnapshotPhaseDuration(balanceTraceStarted)
	}
	eventLogBuilt := false
	var eventLogRef, eventLogIndexRef SegmentRef
	if r.cfg.BuildEventLogs {
		buildProgress.SetPhase("event-log")
		eventLogStarted := time.Now()
		eventRefs, err := buildEventLogPairFromChain(chainDB, r.cfg.Dir, startBlock, cutoffBlock, EventLogBuildOptions{Version: r.cfg.EventLogVersion, ETL: r.cfg.ETL})
		if err != nil {
			return PassResult{}, err
		}
		eventLogRef, eventLogIndexRef, err = eventLogBuildCompanions(eventRefs)
		if err != nil {
			return PassResult{}, err
		}
		refs = append(refs, eventRefs...)
		// Keep the lookup sidecar aligned with this immutable event segment.
		// Existing adjacent indexes remain active in the manifest; rebuilding a
		// chain-wide index on every catch-up step makes historical sync quadratic.
		eventLogBuilt = true
		result.EventLogDuration = coldSnapshotPhaseDuration(eventLogStarted)
	}
	sectionBloomBuilt := false
	if r.cfg.BuildSectionBlooms {
		buildProgress.SetPhase("section-bloom")
		sectionBloomStarted := time.Now()
		sectionRefs, err := r.sectionBloomPassWithManifest(db, cutoffBlock, productionManifest)
		if err != nil {
			return PassResult{}, err
		}
		if len(sectionRefs) > 0 {
			refs = append(refs, sectionRefs...)
			sectionBloomBuilt = true
		}
		result.SectionBloomDuration = coldSnapshotPhaseDuration(sectionBloomStarted)
	}
	buildProgress.SetPhase("publish")
	publishStarted := time.Now()
	manifest, err := aggregator.integrateWithManifest(fromTxNum, toTxNum, refs, productionManifest)
	if err != nil {
		return PassResult{}, err
	}
	if eventLogBuilt {
		eventRefs := []SegmentRef{eventLogRef, eventLogIndexRef}
		if err := requireBuiltSegmentsActive(manifest, eventRefs); err != nil {
			return PassResult{}, fmt.Errorf("snapshots: authenticate cold-builder event-log range [%d,%d]: %w", startBlock, cutoffBlock, err)
		}
		if err := r.cfg.ColdChainVerificationCache.recordTrustedEventLogs(r.cfg.Dir, eventLogIndexRef, []SegmentRef{eventLogRef}); err != nil {
			return PassResult{}, fmt.Errorf("snapshots: record trusted cold-builder event-log range [%d,%d]: %w", startBlock, cutoffBlock, err)
		}
	}
	result.Built = true
	result.FromTxNum = fromTxNum
	result.ToTxNum = toTxNum
	result.FromBlock = startBlock
	result.ToBlock = cutoffBlock
	result.PublishedBlock = cutoffBlock
	result.Segment = refs[0]
	result.Segments = append([]SegmentRef(nil), refs...)
	result.SectionBloomBuilt = sectionBloomBuilt
	result.BalanceTraceBuilt = balanceTraceBuilt
	result.EventLogBuilt = eventLogBuilt
	result.Manifest = manifest
	if writer, ok := db.(ethdb.KeyValueWriter); ok {
		stageProgress := newRawDBStageProgressStore(writer)
		if err := writeSnapshotBuildStage(writer, rawdb.StageSnapshotBuild, cutoffBlock, snapshotBuildHash); err != nil {
			return PassResult{}, err
		}
		if eventLogBuilt {
			if err := writeEventLogBuildStage(chainDB, manifest); err != nil {
				return PassResult{}, err
			}
		}
		if err := writeManifestProgressStages(stageProgress, manifest.Progress); err != nil {
			return PassResult{}, err
		}
	}
	result.PublishDuration = coldSnapshotPhaseDuration(publishStarted)
	buildProgress.Stop()
	logColdSnapshotPublished(r, result, historyBuildStarted, historyRefs, historyBytes)
	return result, nil
}

func logColdSnapshotPublished(r *Runner, result PassResult, started time.Time, historyRefs int, historyBytes uint64) {
	if r == nil || !result.Built {
		return
	}
	elapsed := time.Since(started)
	blocks := result.ToBlock - result.FromBlock + 1
	txs := result.ToTxNum - result.FromTxNum + 1
	backlogBlocks := uint64(0)
	if result.EligibleCutoffBlock > result.PublishedBlock {
		backlogBlocks = result.EligibleCutoffBlock - result.PublishedBlock
	}
	totalBytes := segmentRefsSize(result.Segments)
	coldSnapshotLog.Debug("History cold snapshot publication details",
		"dataset", r.cfg.HistoryDataset,
		"fromTx", result.FromTxNum,
		"toTx", result.ToTxNum,
		"historyRefs", historyRefs,
		"totalRefs", len(result.Segments),
		"historyBytes", historyBytes,
		"totalBytes", totalBytes,
		"balanceTraceElapsed", result.BalanceTraceDuration.Round(time.Millisecond),
		"sectionBloomElapsed", result.SectionBloomDuration.Round(time.Millisecond))
	ctx := []any{
		"dataset", r.cfg.HistoryDataset,
		"fromBlock", result.FromBlock,
		"toBlock", result.ToBlock,
		"blocks", blocks,
		"txs", txs,
		"totalBytes", totalBytes,
		"historyElapsed", result.HistoryDuration.Round(time.Millisecond),
		"eventLogElapsed", result.EventLogDuration.Round(time.Millisecond),
		"publishElapsed", result.PublishDuration.Round(time.Millisecond),
		"elapsed", elapsed.Round(time.Millisecond),
		"blocksPerSec", coldSnapshotRate(blocks, elapsed),
		"txsPerSec", coldSnapshotRate(txs, elapsed),
		"publishedBlock", result.PublishedBlock,
		"eligibleCutoffBlock", result.EligibleCutoffBlock,
		"backlogBlocks", backlogBlocks,
		"accelerated", result.HistoryAccelerated,
	}
	previousLag := r.lastLagBlocks.Load()
	previousAt := r.lastHistoryBuildAt.Load()
	if previousAt > 0 {
		window := time.Since(time.Unix(0, previousAt))
		if window >= coldSnapshotCatchupRateReset {
			// Do not let a pre-pause EWMA leak into the next short build when
			// the first post-pause pass merely holds or increases the backlog.
			r.catchupRate = 0
		}
		if previousLag > backlogBlocks {
			instantRate := coldSnapshotRawRate(previousLag-backlogBlocks, window)
			if instantRate > 0 {
				r.catchupRate = smoothColdSnapshotCatchupRate(r.catchupRate, instantRate, window)
				ctx = append(ctx, "netCatchupBlocksPerSec", coldSnapshotDisplayRate(r.catchupRate))
				if eta, ok := coldSnapshotETA(backlogBlocks, r.catchupRate); ok {
					ctx = append(ctx, "eta", eta)
				}
			}
		}
	}
	coldSnapshotLog.Info("History cold snapshot published", ctx...)
}

func coldSnapshotRate(items uint64, elapsed time.Duration) float64 {
	return coldSnapshotDisplayRate(coldSnapshotRawRate(items, elapsed))
}

func coldSnapshotRawRate(items uint64, elapsed time.Duration) float64 {
	if items == 0 || elapsed <= 0 {
		return 0
	}
	return float64(items) / elapsed.Seconds()
}

func coldSnapshotDisplayRate(rate float64) float64 {
	if rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
		return 0
	}
	precision := 100.0
	if rate < 0.01 {
		precision = 10_000
	}
	return math.Round(rate*precision) / precision
}

func smoothColdSnapshotCatchupRate(previous, instant float64, window time.Duration) float64 {
	if instant <= 0 {
		return previous
	}
	if previous <= 0 || window >= coldSnapshotCatchupRateReset {
		return instant
	}
	return previous*0.8 + instant*0.2
}

func coldSnapshotETA(items uint64, rate float64) (time.Duration, bool) {
	if items == 0 || rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
		return 0, false
	}
	seconds := float64(items) / rate
	maxSeconds := float64(math.MaxInt64) / float64(time.Second)
	if math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds > maxSeconds {
		return 0, false
	}
	return time.Duration(seconds * float64(time.Second)).Round(time.Second), true
}

func segmentRefsSize(refs []SegmentRef) uint64 {
	var total uint64
	for _, ref := range refs {
		if ^uint64(0)-total < ref.Size {
			return ^uint64(0)
		}
		total += ref.Size
	}
	return total
}

func (r *Runner) historyBuildAccelerated(readyBlocks uint64) bool {
	if r == nil || r.cfg.CatchupUnthrottledLagBlocks == 0 || readyBlocks <= r.cfg.CatchupUnthrottledLagBlocks {
		return false
	}
	source, ok := r.chain.(syncRemainingSource)
	if !ok {
		return false
	}
	_, active := source.SyncRemainingBlocks()
	return active
}

func (r *Runner) historyBuildRateLimited(now time.Time, accelerated bool) bool {
	if r == nil || r.cfg.CatchupBuildMinInterval <= 0 {
		return false
	}
	source, ok := r.chain.(syncRemainingSource)
	if !ok {
		return false
	}
	if _, active := source.SyncRemainingBlocks(); !active {
		return false
	}
	if accelerated {
		return false
	}
	last := r.lastHistoryBuildAt.Load()
	if last <= 0 || now.Sub(time.Unix(0, last)) >= r.cfg.CatchupBuildMinInterval {
		return false
	}
	return true
}

func writeSnapshotBuildStage(writer ethdb.KeyValueWriter, stage rawdb.StageID, block uint64, hash common.Hash) error {
	if writer == nil {
		return errNilStageProgressStore
	}
	if hash == (common.Hash{}) {
		return fmt.Errorf("snapshots: missing canonical hash for %s stage block %d", stage, block)
	}
	return rawdb.WriteStageProgressWithHash(writer, stage, block, hash)
}

func (r *Runner) snapshotBuildStageBoundaryHash(db AggregatorDB, stage rawdb.StageID, block uint64) (common.Hash, error) {
	hash, ok, err := r.snapshotBuildStageCanonicalHash(db, stage, block)
	if err != nil {
		return common.Hash{}, err
	}
	if !ok {
		return common.Hash{}, fmt.Errorf("snapshots: missing canonical hash for %s stage block %d", stage, block)
	}
	return hash, nil
}

func (r *Runner) snapshotBuildStageCanonicalHash(db AggregatorDB, stage rawdb.StageID, block uint64) (common.Hash, bool, error) {
	if db == nil {
		return common.Hash{}, false, fmt.Errorf("snapshots: %s stage block %d requires readable database", stage, block)
	}
	hash, ok, err := r.canonicalHashLookup(db)(block)
	if err != nil {
		return common.Hash{}, false, fmt.Errorf("snapshots: read %s stage block %d canonical hash: %w", stage, block, err)
	}
	if ok && hash != (common.Hash{}) {
		return hash, true, nil
	}
	return common.Hash{}, false, nil
}

func (r *Runner) verifiedFinishStageBlock(db AggregatorDB) (uint64, bool, error) {
	block, ok, err := rawdb.ReadVerifiedStageProgressBlockWithHashLookup(db, rawdb.StageFinish, r.canonicalHashLookup(db))
	if err != nil {
		return 0, ok, fmt.Errorf("snapshots: %w", err)
	}
	return block, ok, nil
}

func (r *Runner) verifiedSnapshotBuildStageBlock(db AggregatorDB) (uint64, bool, error) {
	block, ok, err := rawdb.ReadVerifiedStageProgressBlockWithHashLookup(db, rawdb.StageSnapshotBuild, r.canonicalHashLookup(db))
	if err != nil {
		return 0, ok, fmt.Errorf("snapshots: %w", err)
	}
	return block, ok, nil
}

func (r *Runner) canonicalHashLookup(db AggregatorDB) func(uint64) (common.Hash, bool, error) {
	return func(blockNum uint64) (common.Hash, bool, error) {
		if r != nil && r.chain != nil {
			if source, ok := r.chain.(canonicalHashLookupSource); ok {
				return source.CanonicalBlockHashStrict(blockNum)
			}
			if source, ok := r.chain.(canonicalHashSource); ok {
				hash, ok := source.CanonicalBlockHash(blockNum)
				return hash, ok, nil
			}
		}
		return rawdb.ReadBlockHashByNumberStrict(db, blockNum)
	}
}

func (r *Runner) balanceTracePass(chain *rawdb.ChainDB, db AggregatorDB, fromBlock, toBlock uint64) ([]SegmentRef, error) {
	if r == nil || !r.cfg.BuildBalanceTraces {
		return nil, nil
	}
	if chain == nil {
		return nil, errors.New("snapshots: nil balance trace chain database")
	}
	if db == nil {
		return nil, errors.New("snapshots: nil balance trace build database")
	}
	coverage, err := rawdb.AuditBlockBalanceTraceCoverage(chain, db, fromBlock, toBlock, 1)
	if err != nil {
		return nil, err
	}
	if coverage.MismatchedBlockBalanceTrace > 0 {
		detail := ""
		if len(coverage.Issues) > 0 {
			detail = ": " + coverage.Issues[0].Detail
		}
		return nil, fmt.Errorf("snapshots: balance trace coverage mismatch over [%d,%d]%s", fromBlock, toBlock, detail)
	}
	if coverage.MissingBlockBalanceTrace > 0 || coverage.MissingAccountTrace > 0 {
		return nil, nil
	}
	ref, err := BuildBalanceTraceSegmentFromDBWithOptions(db, r.cfg.Dir, BalanceTraceSegmentPath(fromBlock, toBlock), fromBlock, toBlock, r.cfg.ETL)
	if err != nil {
		return nil, err
	}
	return []SegmentRef{ref}, nil
}

func (r *Runner) sectionBloomPass(db AggregatorDB, cutoffBlock uint64) ([]SegmentRef, error) {
	manifest, err := loadOptionalProductionManifest(r.cfg.Dir)
	if err != nil {
		return nil, err
	}
	return r.sectionBloomPassWithManifest(db, cutoffBlock, manifest)
}

func (r *Runner) sectionBloomPassWithManifest(db AggregatorDB, cutoffBlock uint64, manifest *Manifest) ([]SegmentRef, error) {
	if r == nil || !r.cfg.BuildSectionBlooms {
		return nil, nil
	}
	if db == nil {
		return nil, errors.New("snapshots: nil section bloom build database")
	}
	if cutoffBlock < rawdb.SectionBloomBlockPerSection-1 {
		return nil, nil
	}
	maxSection := cutoffBlock / rawdb.SectionBloomBlockPerSection
	covered, err := sectionBloomFullSectionCoverage(manifest, maxSection)
	if err != nil {
		return nil, err
	}
	for section := uint64(0); section <= maxSection; section++ {
		fromBlock := section * rawdb.SectionBloomBlockPerSection
		toBlock := sectionBloomSectionEndBlock(section)
		if toBlock > cutoffBlock {
			break
		}
		if covered[section] {
			continue
		}
		ref, err := BuildSectionBloomSegmentFromDBWithOptions(db, r.cfg.Dir, SectionBloomSegmentPath(fromBlock, toBlock), fromBlock, toBlock, r.cfg.ETL)
		if err != nil {
			return nil, err
		}
		return []SegmentRef{ref}, nil
	}
	return nil, nil
}

// sectionBloomFullSectionCoverage reduces the manifest to one compact coverage
// vector in O(manifest segments + chain sections). The old hot path called
// sectionBloomRefs for every section, rescanning and sorting the complete
// manifest thousands of times on mainnet.
func sectionBloomFullSectionCoverage(manifest *Manifest, maxSection uint64) ([]bool, error) {
	maxInt := uint64(^uint(0) >> 1)
	if maxSection > maxInt-2 {
		return nil, fmt.Errorf("snapshots: section bloom coverage %d exceeds addressable memory", maxSection)
	}
	delta := make([]int64, int(maxSection)+2)
	blocksPerSection := uint64(rawdb.SectionBloomBlockPerSection)
	if manifest != nil {
		for _, ref := range manifest.Segments {
			if ref.Kind != SegmentSectionBloom || ref.normalizedDataset() != SegmentDatasetSectionBloom {
				continue
			}
			first := ref.FromTxNum / blocksPerSection
			if ref.FromTxNum%blocksPerSection != 0 {
				first++
			}
			last := ref.ToTxNum / blocksPerSection
			if ref.ToTxNum%blocksPerSection != blocksPerSection-1 {
				if last == 0 {
					continue
				}
				last--
			}
			if first > last || first > maxSection {
				continue
			}
			if last > maxSection {
				last = maxSection
			}
			delta[int(first)]++
			delta[int(last)+1]--
		}
	}
	covered := make([]bool, int(maxSection)+1)
	var active int64
	for section := range covered {
		active += delta[section]
		covered[section] = active > 0
	}
	return covered, nil
}

func sectionBloomManifestCoversFullSection(manifest *Manifest, section uint64) bool {
	if manifest == nil {
		return false
	}
	fromBlock := section * rawdb.SectionBloomBlockPerSection
	toBlock := sectionBloomSectionEndBlock(section)
	for _, ref := range sectionBloomRefs(manifest) {
		if ref.FromTxNum <= fromBlock && ref.ToTxNum >= toBlock {
			return true
		}
	}
	return false
}

type eventLogChainSource interface {
	EventLogDB() *rawdb.ChainDB
}

func (r *Runner) derivedIndexChainDB() (*rawdb.ChainDB, error) {
	if r == nil || r.chain == nil {
		return nil, errors.New("snapshots: nil derived index chain source")
	}
	if source, ok := r.chain.(eventLogChainSource); ok {
		if db := source.EventLogDB(); db != nil {
			return db, nil
		}
	}
	if db, ok := r.chain.DB().(*rawdb.ChainDB); ok && db != nil {
		return db, nil
	}
	return nil, errors.New("snapshots: derived index build requires rawdb.ChainDB")
}

func (r *Runner) recordPass(result PassResult, start time.Time, passErr error) {
	r.passesCompleted.Add(1)
	if passErr != nil {
		r.passErrors.Add(1)
	}
	if result.Built {
		r.segmentsBuilt.Add(1)
		r.lastHistoryBuildAt.Store(time.Now().UnixNano())
		if result.HistoryAccelerated {
			r.historyAcceleratedBuilds.Add(1)
		}
	}
	if result.Compaction.Merged {
		r.segmentsCompacted.Add(uint64(result.Compaction.SegmentsMerged))
		r.compactionMerges.Add(uint64(result.Compaction.MergePasses))
	}
	if result.NeedsCatchup() && !result.Compaction.Merged {
		r.compactionCatchupDefers.Add(1)
	}
	if result.LatestDeferred {
		r.latestDeferredSync.Add(1)
	}
	if result.HistoryRateLimited {
		r.historyRateLimitedSync.Add(1)
	} else if result.HistoryGateDeferred {
		r.historyGateDeferred.Add(1)
	} else if result.HistoryDeferred {
		r.historyDeferredSync.Add(1)
	}
	builtRefs := append(append([]SegmentRef(nil), result.Segments...), result.Compaction.Segments...)
	builtBytes := segmentRefsSize(builtRefs)
	if builtBytes > 0 {
		r.bytesBuilt.Add(builtBytes)
	}
	publishedBlock := r.lastPublishedBlock.Load()
	if result.PublishedBlock > 0 {
		publishedBlock = result.PublishedBlock
	}
	if result.Built {
		publishedBlock = result.ToBlock
	}
	lagBlocks := uint64(0)
	if result.EligibleCutoffBlock > publishedBlock {
		lagBlocks = result.EligibleCutoffBlock - publishedBlock
	}
	visibleTxEnd := r.lastVisibleTxEnd.Load()
	if result.PreviousVisibleTx > 0 {
		visibleTxEnd = result.PreviousVisibleTx
	}
	if result.Built {
		visibleTxEnd = result.ToTxNum
	}
	if result.SolidifiedBlock > 0 {
		r.lastSolidified.Store(result.SolidifiedBlock)
		r.lastCutoffBlock.Store(result.CutoffBlock)
		r.lastEligibleCutoff.Store(result.EligibleCutoffBlock)
		r.lastPublishedBlock.Store(publishedBlock)
		r.lastLagBlocks.Store(lagBlocks)
		r.lastVisibleTxEnd.Store(visibleTxEnd)
	}
	if result.Built {
		r.lastFromTxNum.Store(result.FromTxNum)
		r.lastToTxNum.Store(result.ToTxNum)
	}
	r.lastPassDuration.Store(int64(time.Since(start)))
	// A rate/resource-deferred pass only measured a cheap eligibility check.
	// Preserve the duration of the last successful history build instead of
	// overwriting it with that no-op latency, which made the production metric
	// look three orders of magnitude faster than the actual cold build.
	if result.Built {
		r.lastBuildDuration.Store(int64(result.BuildDuration))
	}
	r.lastCompactionDuration.Store(int64(result.CompactionDuration))
	r.lastCompactionMerges.Store(uint64(result.Compaction.MergePasses))
	r.lastLatestDuration.Store(int64(result.LatestDuration))
	r.updateMetrics()
}

func (r *Runner) updateMetrics() {
	if r == nil {
		return
	}
	r.metrics.update(r.Snapshot())
}

func (r *Runner) compactHistory(catchingUp bool) (HistoryCompactionResult, error) {
	if r == nil || !r.cfg.Enabled {
		return HistoryCompactionResult{}, nil
	}
	cfg := CompactionConfig{
		MaxSteps:       r.cfg.CompactMaxSteps,
		DeleteObsolete: !r.cfg.RetainObsoleteSegments,
	}
	drain := true
	if catchingUp {
		// Erigon builds every ready base step before its merge loop. Preserve our
		// smaller build/publish/prune boundary, but defer intermediate 2/4/... step
		// rewrites until a full frozen span is available.
		cfg.MinSteps = r.cfg.CompactMaxSteps
		drain = false
	}

	var total HistoryCompactionResult
	for {
		result, err := CompactHistoryDomain(r.cfg.Dir, r.cfg.HistoryDataset, cfg)
		if err != nil {
			return total, err
		}
		if !result.Merged {
			return total, nil
		}
		mergeHistoryCompactionResult(&total, result)
		if !drain {
			return total, nil
		}
	}
}

func mergeHistoryCompactionResult(total *HistoryCompactionResult, next HistoryCompactionResult) {
	if total == nil || !next.Merged {
		return
	}
	if !total.Merged {
		*total = next
		total.Segments = append([]SegmentRef(nil), next.Segments...)
		return
	}
	total.MergePasses += next.MergePasses
	total.FromTxNum = min(total.FromTxNum, next.FromTxNum)
	total.ToTxNum = max(total.ToTxNum, next.ToTxNum)
	if ^uint64(0)-total.AggregationSteps < next.AggregationSteps {
		total.AggregationSteps = ^uint64(0)
	} else {
		total.AggregationSteps += next.AggregationSteps
	}
	total.SegmentsMerged += next.SegmentsMerged
	total.Segments = append(total.Segments, next.Segments...)
}

func (r *Runner) seedLatestBuildBlock() error {
	if r == nil || !r.cfg.Enabled || r.cfg.LatestBuildBlocks == 0 {
		return nil
	}
	block, ok, hadStage, err := r.latestBuildStageSeed()
	if err != nil {
		return err
	}
	if ok {
		r.lastLatestBuildBlock.Store(block)
		return nil
	}
	if hadStage {
		return nil
	}
	block, _, ok, err = r.latestBuildWatermark()
	if err != nil {
		return err
	}
	if ok {
		r.lastLatestBuildBlock.Store(block)
	}
	return nil
}

func (r *Runner) latestBuildStageSeed() (block uint64, ok bool, hadStage bool, err error) {
	if r == nil || r.chain == nil || r.chain.DB() == nil {
		return 0, false, false, errors.New("snapshots: nil cold builder chain or database")
	}
	db := r.chain.DB()
	row, exists, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotLatestBuild)
	if err != nil || !exists {
		return 0, false, exists, err
	}
	if !row.HasBlockHash {
		return 0, false, true, nil
	}
	hash, hasHash, err := r.snapshotBuildStageCanonicalHash(db, rawdb.StageSnapshotLatestBuild, row.BlockNum)
	if err != nil {
		return 0, false, true, err
	}
	if !hasHash || hash != row.BlockHash {
		return 0, false, true, nil
	}
	return row.BlockNum, true, true, nil
}

func (r *Runner) latestBuildWatermark() (block uint64, txNum uint64, ok bool, err error) {
	solidified := r.chain.LatestSolidifiedBlockNum()
	if solidified <= 0 {
		return 0, 0, false, nil
	}
	db := r.chain.DB()
	block = uint64(solidified)
	finishStage, hasFinishStage, err := r.verifiedFinishStageBlock(db)
	if err != nil {
		return 0, 0, false, err
	}
	if hasFinishStage && finishStage < block {
		block = finishStage
	}
	tx, err := StateDomainHistoryTxNumAtBlockEnd(db, block)
	if err != nil {
		return 0, 0, false, err
	}
	return block, tx, tx > 0, nil
}

func (r *Runner) latestPass() (bool, error) {
	built, _, err := r.latestPassWithStatusContext(context.Background())
	return built, err
}

func (r *Runner) latestPassWithStatus() (built bool, deferred bool, err error) {
	return r.latestPassWithStatusContext(context.Background())
}

func (r *Runner) latestPassWithStatusContext(ctx context.Context) (built bool, deferred bool, err error) {
	if r == nil || !r.cfg.Enabled || r.cfg.LatestBuildBlocks == 0 {
		return false, false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return false, false, err
	}
	db := r.chain.DB()
	if db == nil {
		return false, false, errors.New("snapshots: nil cold builder database")
	}
	block, txNum, ok, err := r.latestBuildWatermark()
	if err != nil || !ok {
		return false, false, err
	}
	prevBlock := r.lastLatestBuildBlock.Load()
	if prevBlock != 0 && block < prevBlock+r.cfg.LatestBuildBlocks {
		return false, false, nil // not enough blocks elapsed
	}
	if r.cfg.DeferLatestBuildWhileSyncing {
		if source, ok := r.chain.(syncRemainingSource); ok {
			if _, active := source.SyncRemainingBlocks(); active {
				return false, true, nil
			}
		}
	}
	var (
		rotation rawdb.CommitmentBranchRotation
		rotating bool
		rotator  commitmentBranchRotator
	)
	if source, ok := r.chain.(commitmentBranchRotator); ok {
		rotator = source
		rotation, rotating, err = rotator.BeginCommitmentBranchRotation()
		if err != nil {
			return false, false, err
		}
		if rotating {
			block = rotation.BlockNum
			txNum = rotation.SnapshotTxNum
		}
	}
	var latestBuildHash common.Hash
	if rotating {
		latestBuildHash = rotation.BlockHash
	} else if _, ok := db.(ethdb.KeyValueWriter); ok {
		latestBuildHash, err = r.snapshotBuildStageBoundaryHash(db, rawdb.StageSnapshotLatestBuild, block)
		if err != nil {
			return false, false, err
		}
	}
	res, err := NewAggregator(r.cfg.Dir).BuildLatestContext(ctx, db, AggregatorBuildOptions{FromTxNum: 1, ToTxNum: txNum})
	if err != nil {
		return false, false, err
	}
	if rotating {
		if res == nil || res.Manifest == nil {
			return false, false, errors.New("snapshots: commitment branch rotation built no manifest")
		}
		mgr, openErr := OpenPinnedManager(r.cfg.Dir, res.Manifest)
		if openErr != nil {
			return false, false, openErr
		}
		if err := rotator.CompleteCommitmentBranchRotation(rotation, mgr); err != nil {
			if errors.Is(err, ErrCommitmentBranchRotationNotSolidified) {
				return res != nil && len(res.Segments) > 0, true, nil
			}
			return false, false, err
		}
	}
	r.lastLatestBuildBlock.Store(block)
	if writer, ok := db.(ethdb.KeyValueWriter); ok {
		if err := writeSnapshotBuildStage(writer, rawdb.StageSnapshotLatestBuild, block, latestBuildHash); err != nil {
			return false, false, err
		}
	}
	return res != nil && len(res.Segments) > 0, false, nil
}

func (r *Runner) loop() {
	defer close(r.done)
	catchup := make(chan struct{}, 1)
	scheduleCatchup := func(result PassResult, err error) {
		if err != nil || !result.NeedsCatchup() {
			return
		}
		select {
		case catchup <- struct{}{}:
		default:
		}
	}

	result, err := r.OnePass()
	if err != nil {
		coldSnapshotLog.Warn("History cold snapshot initial pass failed", "dataset", r.cfg.HistoryDataset, "err", err)
	} else if result.Built {
		coldSnapshotLog.Debug("History cold snapshot initial pass completed",
			"dataset", r.cfg.HistoryDataset,
			"fromTx", result.FromTxNum,
			"toTx", result.ToTxNum,
			"fromBlock", result.FromBlock,
			"toBlock", result.ToBlock,
			"cutoffBlock", result.CutoffBlock,
			"compactionMergePasses", result.Compaction.MergePasses,
			"compactionSteps", result.Compaction.AggregationSteps,
			"compactionSegments", result.Compaction.SegmentsMerged,
			"sectionBloomBuilt", result.SectionBloomBuilt,
			"balanceTraceBuilt", result.BalanceTraceBuilt,
			"eventLogBuilt", result.EventLogBuilt)
	} else if result.Compaction.Merged {
		coldSnapshotLog.Debug("History cold snapshot initial pass compacted",
			"dataset", result.Compaction.Dataset,
			"fromTx", result.Compaction.FromTxNum,
			"toTx", result.Compaction.ToTxNum,
			"mergePasses", result.Compaction.MergePasses,
			"aggregationSteps", result.Compaction.AggregationSteps,
			"segments", result.Compaction.SegmentsMerged)
	} else if result.LatestBuilt {
		coldSnapshotLog.Info("Latest cold snapshot pass built", "dataset", "all-latest", "toBlock", r.lastLatestBuildBlock.Load())
	}
	scheduleCatchup(result, err)

	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()
	for {
		// Prefer shutdown over an already queued catch-up request after a long
		// build. This keeps Stop bounded to the pass that was in flight when it
		// was called.
		select {
		case <-r.quit:
			return
		default:
		}
		select {
		case <-ticker.C:
		case <-catchup:
		case <-r.quit:
			return
		}
		result, err := r.OnePass()
		if err != nil {
			coldSnapshotLog.Warn("History cold snapshot pass failed", "dataset", r.cfg.HistoryDataset, "err", err)
		} else if result.Built {
			coldSnapshotLog.Debug("History cold snapshot pass completed",
				"dataset", r.cfg.HistoryDataset,
				"fromTx", result.FromTxNum,
				"toTx", result.ToTxNum,
				"fromBlock", result.FromBlock,
				"toBlock", result.ToBlock,
				"cutoffBlock", result.CutoffBlock,
				"compactionMergePasses", result.Compaction.MergePasses,
				"compactionSteps", result.Compaction.AggregationSteps,
				"compactionSegments", result.Compaction.SegmentsMerged,
				"sectionBloomBuilt", result.SectionBloomBuilt,
				"balanceTraceBuilt", result.BalanceTraceBuilt,
				"eventLogBuilt", result.EventLogBuilt)
		} else if result.Compaction.Merged {
			coldSnapshotLog.Debug("History cold snapshot pass compacted",
				"dataset", result.Compaction.Dataset,
				"fromTx", result.Compaction.FromTxNum,
				"toTx", result.Compaction.ToTxNum,
				"mergePasses", result.Compaction.MergePasses,
				"aggregationSteps", result.Compaction.AggregationSteps,
				"segments", result.Compaction.SegmentsMerged)
		} else if result.LatestBuilt {
			coldSnapshotLog.Info("Latest cold snapshot pass built", "dataset", "all-latest", "toBlock", r.lastLatestBuildBlock.Load())
		}
		scheduleCatchup(result, err)
	}
}

func coldSnapshotVisibleTxEnd(dir string, dataset SegmentDataset) (uint64, error) {
	manifest, err := loadOptionalProductionManifest(dir)
	if err != nil {
		return 0, err
	}
	return coldSnapshotVisibleTxEndFromManifest(manifest, dataset)
}

func loadOptionalProductionManifest(dir string) (*Manifest, error) {
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return manifest, nil
}

func coldSnapshotVisibleTxEndFromManifest(manifest *Manifest, dataset SegmentDataset) (uint64, error) {
	if manifest == nil {
		return 0, nil
	}
	visibleEnd := ContiguousHistoryVisibleTxEnd(manifest, dataset, 1)
	if manifest.Progress != nil && manifest.Progress.HistoryBuildTxNum > visibleEnd {
		return 0, fmt.Errorf("snapshots: history progress %d exceeds visible %s coverage %d", manifest.Progress.HistoryBuildTxNum, dataset, visibleEnd)
	}
	return visibleEnd, nil
}

func firstHotHistoryTxRangeBlockAtOrAfterTx(cfg DomainCfg, db AggregatorDB, txNum, fromBlock, cutoffBlock uint64) (uint64, bool, error) {
	if cfg.IterateHotHistoryTxRanges == nil {
		return 0, false, fmt.Errorf("snapshots: %s missing hot history tx-range iterator", cfg.Dataset)
	}
	if fromBlock > cutoffBlock {
		return 0, false, nil
	}
	var block uint64
	var found bool
	visit := func(row *rawdb.StateTxRange) (bool, error) {
		if row.EndTxNum < row.BeginTxNum {
			return false, fmt.Errorf("snapshots: state tx range for block %d is inverted", row.BlockNum)
		}
		if row.BlockNum < fromBlock {
			return true, nil
		}
		if row.BlockNum > cutoffBlock {
			return false, nil
		}
		if row.EndTxNum < txNum {
			return true, nil
		}
		block = row.BlockNum
		found = true
		return false, nil
	}
	var err error
	if cfg.IterateHotHistoryTxRangeBlocks != nil {
		err = cfg.IterateHotHistoryTxRangeBlocks(db, fromBlock, cutoffBlock, visit)
	} else {
		err = cfg.IterateHotHistoryTxRanges(db, visit)
	}
	if err != nil {
		return 0, false, err
	}
	return block, found, nil
}

func stateDomainChangeHistorySegmentPath(fromTxNum, toTxNum uint64) string {
	if cfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange); ok {
		return cfg.HistoryPath(fromTxNum, toTxNum)
	}
	return fmt.Sprintf("history/state-domain-change-%d-%d.seg", fromTxNum, toTxNum)
}
