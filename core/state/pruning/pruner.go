package pruning

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/common"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

var log = gtronlog.NewModule("core/state/pruning")

const (
	defaultInterval = time.Minute
	// Keep catch-up pruning busy for most of the one-minute lifecycle interval.
	// Delete writes are independently capped at 32 MiB by Worker, so increasing
	// the block selection window raises throughput without recreating an
	// unbounded Pebble batch. At the live tip the iterator simply stops after the
	// handful of newly eligible blocks.
	defaultBatch                  = 25_000
	defaultPrunerMetricsNamespace = "state/prune/"
)

type ChainSource interface {
	DB() ethdb.KeyValueStore
	LatestSolidifiedBlockNum() int64
}

type syncRemainingSource interface {
	SyncRemainingBlocks() (uint64, bool)
}

type canonicalHashSource interface {
	CanonicalBlockHash(blockNum uint64) (common.Hash, bool)
}

type canonicalHashLookupSource interface {
	CanonicalBlockHashStrict(blockNum uint64) (common.Hash, bool, error)
}

type chainDBSource interface {
	ChainDB() *rawdb.ChainDB
}

type PrunerConfig struct {
	Policy Policy

	Interval  time.Duration
	BatchSize int
	// SnapshotDir points at the snapshot manifest directory used by snap mode
	// to decide when hot StateDomainChanges are safely covered by snapshots.
	SnapshotDir string

	// MaxSyncLag defers pruning while the node is still catching up and the
	// sync service can report more than this many blocks remaining. A zero
	// value disables the catch-up gate.
	MaxSyncLag uint64
	// MetricsNamespace prefixes production prune gauges. Tests may override it
	// to isolate process-global metric registrations.
	MetricsNamespace string
}

type PrunerStats struct {
	Passes                       uint64
	Errors                       uint64
	SkippedCatchup               uint64
	DeletedTxRanges              uint64
	DeletedDomainChangeBlocks    uint64
	DeletedCommitmentCheckpoints uint64
	DeletedStateCodeRows         uint64
	LastSolidifiedBlock          uint64
	LastPassDuration             time.Duration
}

type Pruner struct {
	chain   ChainSource
	cfg     PrunerConfig
	metrics prunerMetrics

	quit chan struct{}
	done chan struct{}
	once sync.Once

	passes                       atomic.Uint64
	errors                       atomic.Uint64
	deletedTxRanges              atomic.Uint64
	deletedDomainChangeBlocks    atomic.Uint64
	deletedCommitmentCheckpoints atomic.Uint64
	deletedStateCodeRows         atomic.Uint64
	skippedCatchup               atomic.Uint64
	lastSolidifiedBlock          atomic.Uint64
	lastPassDuration             atomic.Int64
}

type prunerMetrics struct {
	passes                       *metrics.Gauge
	errors                       *metrics.Gauge
	skippedCatchup               *metrics.Gauge
	deletedTxRanges              *metrics.Gauge
	deletedDomainChangeBlocks    *metrics.Gauge
	deletedCommitmentCheckpoints *metrics.Gauge
	deletedStateCodeRows         *metrics.Gauge
	lastSolidifiedBlock          *metrics.Gauge
	lastPassDuration             *metrics.Gauge
}

func newPrunerMetrics(namespace string) prunerMetrics {
	namespace = normalizePrunerMetricNamespace(namespace)
	return prunerMetrics{
		passes:                       metrics.GetOrRegisterGauge(namespace+"passes", nil),
		errors:                       metrics.GetOrRegisterGauge(namespace+"errors", nil),
		skippedCatchup:               metrics.GetOrRegisterGauge(namespace+"skipped/catchup", nil),
		deletedTxRanges:              metrics.GetOrRegisterGauge(namespace+"deleted/tx_ranges", nil),
		deletedDomainChangeBlocks:    metrics.GetOrRegisterGauge(namespace+"deleted/domain_change_blocks", nil),
		deletedCommitmentCheckpoints: metrics.GetOrRegisterGauge(namespace+"deleted/commitment_checkpoints", nil),
		deletedStateCodeRows:         metrics.GetOrRegisterGauge(namespace+"deleted/state_code_rows", nil),
		lastSolidifiedBlock:          metrics.GetOrRegisterGauge(namespace+"last/solidified_block", nil),
		lastPassDuration:             metrics.GetOrRegisterGauge(namespace+"lastpass/duration", nil),
	}
}

func normalizePrunerMetricNamespace(namespace string) string {
	if namespace == "" {
		namespace = defaultPrunerMetricsNamespace
	}
	if namespace[len(namespace)-1] != '/' {
		namespace += "/"
	}
	return namespace
}

func (m prunerMetrics) update(stats PrunerStats) {
	m.passes.Update(prunerUintGauge(stats.Passes))
	m.errors.Update(prunerUintGauge(stats.Errors))
	m.skippedCatchup.Update(prunerUintGauge(stats.SkippedCatchup))
	m.deletedTxRanges.Update(prunerUintGauge(stats.DeletedTxRanges))
	m.deletedDomainChangeBlocks.Update(prunerUintGauge(stats.DeletedDomainChangeBlocks))
	m.deletedCommitmentCheckpoints.Update(prunerUintGauge(stats.DeletedCommitmentCheckpoints))
	m.deletedStateCodeRows.Update(prunerUintGauge(stats.DeletedStateCodeRows))
	m.lastSolidifiedBlock.Update(prunerUintGauge(stats.LastSolidifiedBlock))
	m.lastPassDuration.Update(int64(stats.LastPassDuration))
}

func prunerUintGauge(value uint64) int64 {
	const maxInt64GaugeValue = uint64(1<<63 - 1)
	if value > maxInt64GaugeValue {
		return int64(maxInt64GaugeValue)
	}
	return int64(value)
}

func NewPruner(chain ChainSource, cfg PrunerConfig) *Pruner {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultInterval
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatch
	}
	if cfg.MetricsNamespace == "" {
		cfg.MetricsNamespace = defaultPrunerMetricsNamespace
	}
	pruner := &Pruner{
		chain:   chain,
		cfg:     cfg,
		metrics: newPrunerMetrics(cfg.MetricsNamespace),
		quit:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	pruner.updateMetrics()
	return pruner
}

func (p *Pruner) Start() error {
	if p == nil {
		return nil
	}
	if p.chain == nil || p.chain.DB() == nil {
		close(p.done)
		return nil
	}
	if err := p.cfg.Policy.Validate(); err != nil {
		close(p.done)
		return err
	}
	if p.cfg.Policy.Mode == ModeArchive {
		close(p.done)
		log.Info("Domain state pruner disabled", "mode", ModeArchive)
		return nil
	}
	go p.loop()
	log.Info("Domain state pruner started",
		"mode", p.cfg.Policy.Mode,
		"historyWindow", p.cfg.Policy.HistoryWindow,
		"reorgWindow", p.cfg.Policy.ReorgWindow,
		"interval", p.cfg.Interval,
		"batch", p.cfg.BatchSize,
		"snapshotDir", p.cfg.SnapshotDir,
		"maxSyncLag", p.cfg.MaxSyncLag)
	return nil
}

func (p *Pruner) Stop() error {
	if p == nil {
		return nil
	}
	p.once.Do(func() { close(p.quit) })
	<-p.done
	log.Info("Domain state pruner stopped",
		"passes", p.passes.Load(),
		"skippedCatchup", p.skippedCatchup.Load(),
		"txRanges", p.deletedTxRanges.Load(),
		"changeBlocks", p.deletedDomainChangeBlocks.Load(),
		"commitments", p.deletedCommitmentCheckpoints.Load(),
		"codeRows", p.deletedStateCodeRows.Load())
	return nil
}

func (p *Pruner) Stats() PrunerStats {
	if p == nil {
		return PrunerStats{}
	}
	return PrunerStats{
		Passes:                       p.passes.Load(),
		Errors:                       p.errors.Load(),
		DeletedTxRanges:              p.deletedTxRanges.Load(),
		DeletedDomainChangeBlocks:    p.deletedDomainChangeBlocks.Load(),
		DeletedCommitmentCheckpoints: p.deletedCommitmentCheckpoints.Load(),
		DeletedStateCodeRows:         p.deletedStateCodeRows.Load(),
		SkippedCatchup:               p.skippedCatchup.Load(),
		LastSolidifiedBlock:          p.lastSolidifiedBlock.Load(),
		LastPassDuration:             time.Duration(p.lastPassDuration.Load()),
	}
}

func (p *Pruner) updateMetrics() {
	if p == nil {
		return
	}
	p.metrics.update(p.Stats())
}

func (p *Pruner) loop() {
	defer close(p.done)
	ticker := time.NewTicker(p.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if _, err := p.PrunePass(); err != nil {
				log.Warn("Domain state prune pass failed", "err", err)
			}
		case <-p.quit:
			return
		}
	}
}

func (p *Pruner) PrunePass() (stats Stats, err error) {
	start := time.Now()
	defer func() {
		p.lastPassDuration.Store(time.Since(start).Nanoseconds())
		if err != nil {
			p.errors.Add(1)
		}
		p.updateMetrics()
	}()
	solidified := p.chain.LatestSolidifiedBlockNum()
	if solidified < 0 {
		solidified = 0
	}
	if p.shouldSkipForCatchup() {
		p.skippedCatchup.Add(1)
		p.lastSolidifiedBlock.Store(uint64(solidified))
		return Stats{}, nil
	}
	pruneHead := uint64(solidified)
	pruneHead, pruneHeadHash, pruneHeadHasHash, err := p.pruneHeadWithVerifiedBoundary(pruneHead)
	if err != nil {
		return Stats{}, err
	}
	stats, err = Worker{
		DB:               p.chain.DB(),
		Policy:           p.cfg.Policy,
		MaxBlocks:        p.cfg.BatchSize,
		SnapshotDir:      p.cfg.SnapshotDir,
		PruneHeadHash:    pruneHeadHash,
		PruneHeadHasHash: pruneHeadHasHash,
	}.PruneTo(pruneHead)
	if err != nil {
		return Stats{}, err
	}
	p.passes.Add(1)
	p.deletedTxRanges.Add(uint64(stats.DeletedTxRanges))
	p.deletedDomainChangeBlocks.Add(uint64(stats.DeletedDomainChangeBlocks))
	p.deletedCommitmentCheckpoints.Add(uint64(stats.DeletedCommitmentCheckpoints))
	p.deletedStateCodeRows.Add(uint64(stats.DeletedStateCodeRows))
	p.lastSolidifiedBlock.Store(uint64(solidified))
	if stats.DeletedTxRanges != 0 || stats.DeletedDomainChangeBlocks != 0 || stats.DeletedCommitmentCheckpoints != 0 || stats.DeletedStateCodeRows != 0 {
		log.Info("Domain state prune pass completed",
			"pruneHead", pruneHead,
			"solidified", solidified,
			"txRanges", stats.DeletedTxRanges,
			"changeBlocks", stats.DeletedDomainChangeBlocks,
			"commitments", stats.DeletedCommitmentCheckpoints,
			"codeRows", stats.DeletedStateCodeRows,
			"elapsed", time.Since(start))
	}
	return stats, nil
}

func (p *Pruner) pruneHeadWithVerifiedBoundary(pruneHead uint64) (uint64, common.Hash, bool, error) {
	row, ok, err := newRawDBStageProgressReader(p.chain.DB()).Read(rawdb.StageFinish)
	if err != nil || !ok {
		hash, hashOK, hashErr := p.canonicalBlockHash(pruneHead)
		if hashErr != nil {
			return 0, common.Hash{}, false, fmt.Errorf("pruning: prune head %d canonical hash lookup: %w", pruneHead, hashErr)
		}
		return pruneHead, hash, hashOK, err
	}
	if !row.HasBlockHash {
		return 0, common.Hash{}, false, fmt.Errorf("pruning: finish stage %d is not hash-bound", row.BlockNum)
	}
	hash, ok, err := p.canonicalBlockHash(row.BlockNum)
	if err != nil {
		return 0, common.Hash{}, false, fmt.Errorf("pruning: finish stage %d canonical hash lookup: %w", row.BlockNum, err)
	}
	if !ok {
		return 0, common.Hash{}, false, fmt.Errorf("pruning: finish stage %d has hash %x but canonical block is unavailable", row.BlockNum, row.BlockHash)
	}
	if hash != row.BlockHash {
		return 0, common.Hash{}, false, fmt.Errorf("pruning: finish stage %d hash %x does not match canonical hash %x", row.BlockNum, row.BlockHash, hash)
	}
	if row.BlockNum < pruneHead {
		return row.BlockNum, row.BlockHash, true, nil
	}
	if row.BlockNum == pruneHead {
		return pruneHead, row.BlockHash, true, nil
	}
	hash, ok, err = p.canonicalBlockHash(pruneHead)
	if err != nil {
		return 0, common.Hash{}, false, fmt.Errorf("pruning: prune head %d canonical hash lookup: %w", pruneHead, err)
	}
	if ok {
		return pruneHead, hash, true, nil
	}
	return pruneHead, common.Hash{}, false, nil
}

func (p *Pruner) canonicalBlockHash(blockNum uint64) (common.Hash, bool, error) {
	return canonicalBlockHashLookupFromChainSource(p.chain, blockNum)
}

func canonicalBlockHashFromChainSource(chain ChainSource, blockNum uint64) (common.Hash, bool) {
	hash, ok, _ := canonicalBlockHashLookupFromChainSource(chain, blockNum)
	return hash, ok
}

func canonicalBlockHashLookupFromChainSource(chain ChainSource, blockNum uint64) (common.Hash, bool, error) {
	if chain == nil {
		return common.Hash{}, false, nil
	}
	if source, ok := chain.(canonicalHashLookupSource); ok {
		return source.CanonicalBlockHashStrict(blockNum)
	}
	if source, ok := chain.(canonicalHashSource); ok {
		hash, ok := source.CanonicalBlockHash(blockNum)
		return hash, ok, nil
	}
	if source, ok := chain.(chainDBSource); ok {
		if db := source.ChainDB(); db != nil {
			return rawdb.ReadBlockHashByNumberStrict(db, blockNum)
		}
	}
	hash, ok, err := rawdb.ReadBlockHashByNumberStrict(chain.DB(), blockNum)
	if err != nil || !ok || hash == (common.Hash{}) {
		return hash, ok, err
	}
	return hash, true, nil
}

func (p *Pruner) shouldSkipForCatchup() bool {
	if p.cfg.MaxSyncLag == 0 {
		return false
	}
	source, ok := p.chain.(syncRemainingSource)
	if !ok {
		return false
	}
	remaining, ok := source.SyncRemainingBlocks()
	if !ok {
		return false
	}
	return remaining > p.cfg.MaxSyncLag
}
