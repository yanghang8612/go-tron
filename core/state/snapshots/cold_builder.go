package snapshots

import (
	"crypto/ed25519"
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
	"github.com/tronprotocol/go-tron/common"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

var coldSnapshotLog = gtronlog.NewModule("core/state/snapshots")

const (
	defaultColdSnapshotInterval    = time.Minute
	defaultColdSnapshotBatchBlocks = uint64(5_000)
	defaultColdSnapshotMetrics     = "state/snapshot/cold/"
)

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

// Config controls the cold history snapshot builder lifecycle.
type Config struct {
	Dir                    string
	Enabled                bool
	HistoryDataset         SegmentDataset
	Interval               time.Duration
	HistoryWindow          uint64
	BatchBlocks            uint64
	CompactMinSegments     int
	CompactMaxTxSpan       uint64
	RetainObsoleteSegments bool
	// LatestBuildBlocks is the minimum number of solidified blocks that must
	// elapse between production latest-snapshot builds. 0 disables the latest
	// build pass entirely. Latest builds are full-keyspace scans, so all latest
	// datasets share this single coarse cadence rather than rebuilding every tick.
	LatestBuildBlocks uint64
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
	Built               bool
	LatestBuilt         bool
	Compaction          HistoryCompactionResult
	FromTxNum           uint64
	ToTxNum             uint64
	FromBlock           uint64
	ToBlock             uint64
	CutoffBlock         uint64
	EligibleCutoffBlock uint64
	PublishedBlock      uint64
	SolidifiedBlock     uint64
	PreviousVisibleTx   uint64
	Segment             SegmentRef
	Segments            []SegmentRef
	SectionBloomBuilt   bool
	BalanceTraceBuilt   bool
	EventLogBuilt       bool
	CatalogPublished    bool
	Manifest            *Manifest
	BuildDuration       time.Duration
	CompactionDuration  time.Duration
	LatestDuration      time.Duration
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
	LastBuildDuration       time.Duration
	LastCompactionDuration  time.Duration
	LastLatestDuration      time.Duration
}

type coldRunnerMetrics struct {
	passes                 *metrics.Gauge
	errors                 *metrics.Gauge
	segmentsBuilt          *metrics.Gauge
	segmentsCompacted      *metrics.Gauge
	bytesBuilt             *metrics.Gauge
	lastSolidified         *metrics.Gauge
	lastEligibleCutoff     *metrics.Gauge
	lastSelectedCutoff     *metrics.Gauge
	lastPublishedBlock     *metrics.Gauge
	lagBlocks              *metrics.Gauge
	lastVisibleTxEnd       *metrics.Gauge
	lastFromTxNum          *metrics.Gauge
	lastToTxNum            *metrics.Gauge
	lastPassDuration       *metrics.Gauge
	lastBuildDuration      *metrics.Gauge
	lastCompactionDuration *metrics.Gauge
	lastLatestDuration     *metrics.Gauge
}

func newColdRunnerMetrics(namespace string) coldRunnerMetrics {
	namespace = normalizeColdSnapshotMetricNamespace(namespace)
	return coldRunnerMetrics{
		passes:                 metrics.GetOrRegisterGauge(namespace+"passes", nil),
		errors:                 metrics.GetOrRegisterGauge(namespace+"errors", nil),
		segmentsBuilt:          metrics.GetOrRegisterGauge(namespace+"segments/built", nil),
		segmentsCompacted:      metrics.GetOrRegisterGauge(namespace+"segments/compacted", nil),
		bytesBuilt:             metrics.GetOrRegisterGauge(namespace+"bytes/built", nil),
		lastSolidified:         metrics.GetOrRegisterGauge(namespace+"last/solidified_block", nil),
		lastEligibleCutoff:     metrics.GetOrRegisterGauge(namespace+"last/eligible_cutoff_block", nil),
		lastSelectedCutoff:     metrics.GetOrRegisterGauge(namespace+"last/selected_cutoff_block", nil),
		lastPublishedBlock:     metrics.GetOrRegisterGauge(namespace+"last/published_block", nil),
		lagBlocks:              metrics.GetOrRegisterGauge(namespace+"lag/blocks", nil),
		lastVisibleTxEnd:       metrics.GetOrRegisterGauge(namespace+"last/visible_tx_end", nil),
		lastFromTxNum:          metrics.GetOrRegisterGauge(namespace+"last/from_tx", nil),
		lastToTxNum:            metrics.GetOrRegisterGauge(namespace+"last/to_tx", nil),
		lastPassDuration:       metrics.GetOrRegisterGauge(namespace+"lastpass/duration", nil),
		lastBuildDuration:      metrics.GetOrRegisterGauge(namespace+"lastpass/build/duration", nil),
		lastCompactionDuration: metrics.GetOrRegisterGauge(namespace+"lastpass/compaction/duration", nil),
		lastLatestDuration:     metrics.GetOrRegisterGauge(namespace+"lastpass/latest/duration", nil),
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
	m.lastLatestDuration.Update(int64(stats.LastLatestDuration))
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

	startOnce sync.Once
	stopOnce  sync.Once
	passMu    sync.Mutex
	startErr  error

	passesCompleted        atomic.Uint64
	passErrors             atomic.Uint64
	segmentsBuilt          atomic.Uint64
	segmentsCompacted      atomic.Uint64
	bytesBuilt             atomic.Uint64
	lastSolidified         atomic.Uint64
	lastCutoffBlock        atomic.Uint64
	lastEligibleCutoff     atomic.Uint64
	lastPublishedBlock     atomic.Uint64
	lastLagBlocks          atomic.Uint64
	lastVisibleTxEnd       atomic.Uint64
	lastFromTxNum          atomic.Uint64
	lastToTxNum            atomic.Uint64
	lastPassDuration       atomic.Int64
	lastBuildDuration      atomic.Int64
	lastCompactionDuration atomic.Int64
	lastLatestDuration     atomic.Int64
	lastLatestBuildBlock   atomic.Uint64
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
	if c.CompactMinSegments == 0 {
		c.CompactMinSegments = 8
	}
	if c.CompactMaxTxSpan == 0 {
		c.CompactMaxTxSpan = c.BatchBlocks * uint64(c.CompactMinSegments)
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
		if err := r.cfg.validate(); err != nil {
			close(r.done)
			r.startErr = err
			return
		}
		if !r.cfg.Enabled {
			close(r.done)
			return
		}
		if r.chain == nil || r.chain.DB() == nil {
			close(r.done)
			r.startErr = errors.New("snapshots: nil cold builder chain or database")
			return
		}
		// Seed lastLatestBuildBlock from the persisted stage first (survives
		// restarts); fall back to the current solidified block for fresh nodes
		// that have never run a latest build (self-heals after the first build).
		if err := r.seedLatestBuildBlock(); err != nil {
			close(r.done)
			r.startErr = err
			return
		}
		go r.loop()
		coldSnapshotLog.Info("History cold snapshot builder started",
			"dir", r.cfg.Dir,
			"dataset", r.cfg.HistoryDataset,
			"interval", r.cfg.Interval,
			"historyWindow", r.cfg.HistoryWindow,
			"batchBlocks", r.cfg.BatchBlocks,
			"sectionBloomBuild", r.cfg.BuildSectionBlooms,
			"balanceTraceBuild", r.cfg.BuildBalanceTraces,
			"eventLogBuild", r.cfg.BuildEventLogs)
	})
	return r.startErr
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
		PassesCompleted:         r.passesCompleted.Load(),
		PassErrors:              r.passErrors.Load(),
		SegmentsBuilt:           r.segmentsBuilt.Load(),
		SegmentsCompacted:       r.segmentsCompacted.Load(),
		BytesBuilt:              r.bytesBuilt.Load(),
		LastSolidified:          r.lastSolidified.Load(),
		LastCutoffBlock:         r.lastCutoffBlock.Load(),
		LastEligibleCutoffBlock: r.lastEligibleCutoff.Load(),
		LastPublishedBlock:      r.lastPublishedBlock.Load(),
		LastLagBlocks:           r.lastLagBlocks.Load(),
		LastVisibleTxEnd:        r.lastVisibleTxEnd.Load(),
		LastFromTxNum:           r.lastFromTxNum.Load(),
		LastToTxNum:             r.lastToTxNum.Load(),
		LastPassDuration:        time.Duration(r.lastPassDuration.Load()),
		LastBuildDuration:       time.Duration(r.lastBuildDuration.Load()),
		LastCompactionDuration:  time.Duration(r.lastCompactionDuration.Load()),
		LastLatestDuration:      time.Duration(r.lastLatestDuration.Load()),
	}
}

// OnePass builds at most one registered history segment, then compacts history,
// then runs one latest-snapshot build pass if the cadence interval has elapsed.
func (r *Runner) OnePass() (PassResult, error) {
	if r == nil {
		return PassResult{}, nil
	}
	r.passMu.Lock()
	defer r.passMu.Unlock()

	start := time.Now()
	phaseStart := start
	result, err := r.onePass()
	result.BuildDuration = time.Since(phaseStart)
	if err == nil {
		phaseStart = time.Now()
		result.Compaction, err = r.compactHistory()
		result.CompactionDuration = time.Since(phaseStart)
	}
	if err == nil {
		phaseStart = time.Now()
		built, perr := r.latestPass()
		result.LatestDuration = time.Since(phaseStart)
		if perr != nil {
			err = perr
		} else {
			result.LatestBuilt = built
		}
	}
	r.recordPass(result, start, err)
	return result, err
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
	_, err := EnsureProductionManifestChainIdentity(r.cfg.Dir, *r.cfg.CatalogChain)
	return err
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

	visibleEnd, err := coldSnapshotVisibleTxEnd(r.cfg.Dir, r.cfg.HistoryDataset)
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
	if fromTxNum > toTxNum {
		return result, nil
	}

	var snapshotBuildHash common.Hash
	if _, ok := db.(ethdb.KeyValueWriter); ok {
		snapshotBuildHash, err = r.snapshotBuildStageBoundaryHash(db, rawdb.StageSnapshotBuild, cutoffBlock)
		if err != nil {
			return PassResult{}, err
		}
	}

	var refs []SegmentRef
	if historyCfg.BuildHistoryBlockRange != nil {
		refs, err = historyCfg.BuildHistoryBlockRange(db, r.cfg.Dir, fromTxNum, toTxNum, startBlock, cutoffBlock, historyCfg.HistoryPath(fromTxNum, toTxNum))
	} else {
		refs, err = historyCfg.BuildHistory(db, r.cfg.Dir, fromTxNum, toTxNum, historyCfg.HistoryPath(fromTxNum, toTxNum))
	}
	if err != nil {
		return PassResult{}, err
	}
	if len(refs) == 0 {
		return result, nil
	}
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
		traceRefs, err := r.balanceTracePass(chainDB, db, startBlock, cutoffBlock)
		if err != nil {
			return PassResult{}, err
		}
		if len(traceRefs) > 0 {
			refs = append(refs, traceRefs...)
			balanceTraceBuilt = true
		}
	}
	eventLogBuilt := false
	if r.cfg.BuildEventLogs {
		ref, err := BuildEventLogSegmentFromChainWithOptions(chainDB, r.cfg.Dir, EventLogSegmentPath(startBlock, cutoffBlock), startBlock, cutoffBlock, r.cfg.ETL)
		if err != nil {
			return PassResult{}, err
		}
		refs = append(refs, ref)
		eventRefs, err := aggregator.eventLogRefsAfterIntegrating([]SegmentRef{ref})
		if err != nil {
			return PassResult{}, err
		}
		indexRef, err := BuildEventLogIndexSegmentFromEventLogSegmentsWithOptions(r.cfg.Dir, eventRefs, EventLogIndexSegmentPath(eventRefs[0].FromTxNum, eventRefs[len(eventRefs)-1].ToTxNum), r.cfg.ETL)
		if err != nil {
			return PassResult{}, err
		}
		refs = append(refs, indexRef)
		eventLogBuilt = true
	}
	sectionBloomBuilt := false
	if r.cfg.BuildSectionBlooms {
		sectionRefs, err := r.sectionBloomPass(db, cutoffBlock)
		if err != nil {
			return PassResult{}, err
		}
		if len(sectionRefs) > 0 {
			refs = append(refs, sectionRefs...)
			sectionBloomBuilt = true
		}
	}
	manifest, err := aggregator.Integrate(fromTxNum, toTxNum, refs)
	if err != nil {
		return PassResult{}, err
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
	return result, nil
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
	if r == nil || !r.cfg.BuildSectionBlooms {
		return nil, nil
	}
	if db == nil {
		return nil, errors.New("snapshots: nil section bloom build database")
	}
	if cutoffBlock < rawdb.SectionBloomBlockPerSection-1 {
		return nil, nil
	}
	manifest, err := LoadProductionManifest(r.cfg.Dir)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	maxSection := cutoffBlock / rawdb.SectionBloomBlockPerSection
	for section := uint64(0); section <= maxSection; section++ {
		fromBlock := section * rawdb.SectionBloomBlockPerSection
		toBlock := sectionBloomSectionEndBlock(section)
		if toBlock > cutoffBlock {
			break
		}
		if sectionBloomManifestCoversFullSection(manifest, section) {
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
	}
	if result.Compaction.Merged {
		r.segmentsCompacted.Add(uint64(result.Compaction.SegmentsMerged))
	}
	var builtBytes uint64
	for _, ref := range append(append([]SegmentRef(nil), result.Segments...), result.Compaction.Segments...) {
		if ^uint64(0)-builtBytes < ref.Size {
			builtBytes = ^uint64(0)
			break
		}
		builtBytes += ref.Size
	}
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
	r.lastBuildDuration.Store(int64(result.BuildDuration))
	r.lastCompactionDuration.Store(int64(result.CompactionDuration))
	r.lastLatestDuration.Store(int64(result.LatestDuration))
	r.updateMetrics()
}

func (r *Runner) updateMetrics() {
	if r == nil {
		return
	}
	r.metrics.update(r.Snapshot())
}

func (r *Runner) compactHistory() (HistoryCompactionResult, error) {
	if r == nil || !r.cfg.Enabled {
		return HistoryCompactionResult{}, nil
	}
	return CompactHistoryDomain(r.cfg.Dir, r.cfg.HistoryDataset, CompactionConfig{
		MinSegments:    r.cfg.CompactMinSegments,
		MaxTxSpan:      r.cfg.CompactMaxTxSpan,
		DeleteObsolete: !r.cfg.RetainObsoleteSegments,
	})
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
	if r == nil || !r.cfg.Enabled || r.cfg.LatestBuildBlocks == 0 {
		return false, nil
	}
	db := r.chain.DB()
	if db == nil {
		return false, errors.New("snapshots: nil cold builder database")
	}
	block, txNum, ok, err := r.latestBuildWatermark()
	if err != nil || !ok {
		return false, err
	}
	prevBlock := r.lastLatestBuildBlock.Load()
	if prevBlock != 0 && block < prevBlock+r.cfg.LatestBuildBlocks {
		return false, nil // not enough blocks elapsed
	}
	var latestBuildHash common.Hash
	if _, ok := db.(ethdb.KeyValueWriter); ok {
		latestBuildHash, err = r.snapshotBuildStageBoundaryHash(db, rawdb.StageSnapshotLatestBuild, block)
		if err != nil {
			return false, err
		}
	}
	res, err := NewAggregator(r.cfg.Dir).BuildLatest(db, AggregatorBuildOptions{FromTxNum: 1, ToTxNum: txNum})
	if err != nil {
		return false, err
	}
	r.lastLatestBuildBlock.Store(block)
	if writer, ok := db.(ethdb.KeyValueWriter); ok {
		if err := writeSnapshotBuildStage(writer, rawdb.StageSnapshotLatestBuild, block, latestBuildHash); err != nil {
			return false, err
		}
	}
	return res != nil && len(res.Segments) > 0, nil
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
		coldSnapshotLog.Info("History cold snapshot initial pass built",
			"dataset", r.cfg.HistoryDataset,
			"fromTx", result.FromTxNum,
			"toTx", result.ToTxNum,
			"fromBlock", result.FromBlock,
			"toBlock", result.ToBlock,
			"cutoffBlock", result.CutoffBlock,
			"sectionBloomBuilt", result.SectionBloomBuilt,
			"balanceTraceBuilt", result.BalanceTraceBuilt,
			"eventLogBuilt", result.EventLogBuilt)
	} else if result.Compaction.Merged {
		coldSnapshotLog.Info("History cold snapshot initial pass compacted",
			"dataset", result.Compaction.Dataset,
			"fromTx", result.Compaction.FromTxNum,
			"toTx", result.Compaction.ToTxNum,
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
			coldSnapshotLog.Info("History cold snapshot pass built",
				"dataset", r.cfg.HistoryDataset,
				"fromTx", result.FromTxNum,
				"toTx", result.ToTxNum,
				"fromBlock", result.FromBlock,
				"toBlock", result.ToBlock,
				"cutoffBlock", result.CutoffBlock,
				"sectionBloomBuilt", result.SectionBloomBuilt,
				"balanceTraceBuilt", result.BalanceTraceBuilt,
				"eventLogBuilt", result.EventLogBuilt)
		} else if result.Compaction.Merged {
			coldSnapshotLog.Info("History cold snapshot pass compacted",
				"dataset", result.Compaction.Dataset,
				"fromTx", result.Compaction.FromTxNum,
				"toTx", result.Compaction.ToTxNum,
				"segments", result.Compaction.SegmentsMerged)
		} else if result.LatestBuilt {
			coldSnapshotLog.Info("Latest cold snapshot pass built", "dataset", "all-latest", "toBlock", r.lastLatestBuildBlock.Load())
		}
		scheduleCatchup(result, err)
	}
}

func coldSnapshotVisibleTxEnd(dir string, dataset SegmentDataset) (uint64, error) {
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
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
