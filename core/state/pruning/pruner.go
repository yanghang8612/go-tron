package pruning

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/common"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

var log = gtronlog.NewModule("core/state/pruning")

const (
	defaultInterval = time.Minute
	// Sync can become active after the lifecycle's initial pass has already
	// crossed its one-shot catch-up check. Poll while the read/verification
	// phase is running so startup peer discovery cannot leave a multi-minute
	// semantic audit competing with the importer.
	pruneCatchupPollInterval = 250 * time.Millisecond
	// Keep catch-up pruning busy for most of the one-minute lifecycle interval.
	// Delete writes are independently capped at 32 MiB by Worker, so increasing
	// the block selection window raises throughput without recreating an
	// unbounded Pebble batch. At the live tip the iterator simply stops after the
	// handful of newly eligible blocks.
	defaultBatch                   = 25_000
	defaultPrunerMetricsNamespace  = "state/prune/"
	maxRetiredHistoryVerifyWorkers = 8
	coldPruneProgressLogInterval   = time.Minute
)

var errPruneDeferredForCatchup = errors.New("pruning: deferred for sync catch-up")

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
	// DeferStateCodePruneWhileSyncing keeps immutable hot bytecode available
	// during historical sync instead of repeatedly scanning every CodeDomain
	// reference. Hot-history pruning still runs to bound the large changeset
	// backlog, and the sync-complete lifecycle wake resumes code reclamation.
	DeferStateCodePruneWhileSyncing bool
	// MetricsNamespace prefixes production prune gauges. Tests may override it
	// to isolate process-global metric registrations.
	MetricsNamespace string
}

type PrunerStats struct {
	Passes                             uint64
	Errors                             uint64
	SkippedCatchup                     uint64
	CanceledCatchup                    uint64
	VerificationMemoryHits             uint64
	VerificationPersistentHits         uint64
	VerificationFull                   uint64
	VerificationTrusted                uint64
	VerificationCacheEntries           uint64
	VerificationActiveSegments         uint64
	VerificationActiveBytes            uint64
	VerificationChecksumInFlight       uint64
	VerificationChecksumStarted        uint64
	VerificationChecksumCompleted      uint64
	VerificationChecksumFailed         uint64
	VerificationChecksumCanceled       uint64
	VerificationChecksumBytesInFlight  uint64
	VerificationChecksumBytesStarted   uint64
	VerificationChecksumBytesCompleted uint64
	RetiredVerificationMemoryHits      uint64
	RetiredVerificationPersistentHits  uint64
	RetiredVerificationFull            uint64
	RetiredVerificationCanceled        uint64
	DeletedTxRanges                    uint64
	DeletedDomainChangeBlocks          uint64
	DeletedCommitmentCheckpoints       uint64
	DeletedStateCodeRows               uint64
	DeferredStateCodePrune             uint64
	LastSolidifiedBlock                uint64
	LastDomainChangeStartBlock         uint64
	LastDomainChangePrunedThrough      uint64
	LastDomainChangePrunedThroughTx    uint64
	LastPassDuration                   time.Duration
}

type Pruner struct {
	chain                     ChainSource
	cfg                       PrunerConfig
	metrics                   prunerMetrics
	coverageVerificationCache *snapshotCoverageVerificationCache

	quit chan struct{}
	done chan struct{}
	once sync.Once

	passes                            atomic.Uint64
	errors                            atomic.Uint64
	deletedTxRanges                   atomic.Uint64
	deletedDomainChangeBlocks         atomic.Uint64
	deletedCommitmentCheckpoints      atomic.Uint64
	deletedStateCodeRows              atomic.Uint64
	deferredStateCodePrune            atomic.Uint64
	skippedCatchup                    atomic.Uint64
	canceledCatchup                   atomic.Uint64
	retiredVerificationMemoryHits     atomic.Uint64
	retiredVerificationPersistentHits atomic.Uint64
	retiredVerificationFull           atomic.Uint64
	retiredVerificationCanceled       atomic.Uint64
	lastSolidifiedBlock               atomic.Uint64
	lastDomainChangeStartBlock        atomic.Uint64
	lastDomainChangePrunedThrough     atomic.Uint64
	lastDomainChangePrunedThroughTx   atomic.Uint64
	lastPassDuration                  atomic.Int64
	lastColdPruneProgressLogAt        atomic.Int64
	coldPruneProgressSuppressed       atomic.Uint64
}

type prunerMetrics struct {
	passes                             *metrics.Gauge
	errors                             *metrics.Gauge
	skippedCatchup                     *metrics.Gauge
	canceledCatchup                    *metrics.Gauge
	verificationMemoryHits             *metrics.Gauge
	verificationPersistentHits         *metrics.Gauge
	verificationFull                   *metrics.Gauge
	verificationTrusted                *metrics.Gauge
	verificationCacheEntries           *metrics.Gauge
	verificationActiveSegments         *metrics.Gauge
	verificationActiveBytes            *metrics.Gauge
	verificationChecksumInFlight       *metrics.Gauge
	verificationChecksumStarted        *metrics.Gauge
	verificationChecksumCompleted      *metrics.Gauge
	verificationChecksumFailed         *metrics.Gauge
	verificationChecksumCanceled       *metrics.Gauge
	verificationChecksumBytesInFlight  *metrics.Gauge
	verificationChecksumBytesStarted   *metrics.Gauge
	verificationChecksumBytesCompleted *metrics.Gauge
	retiredVerificationMemoryHits      *metrics.Gauge
	retiredVerificationPersistentHits  *metrics.Gauge
	retiredVerificationFull            *metrics.Gauge
	retiredVerificationCanceled        *metrics.Gauge
	deletedTxRanges                    *metrics.Gauge
	deletedDomainChangeBlocks          *metrics.Gauge
	deletedCommitmentCheckpoints       *metrics.Gauge
	deletedStateCodeRows               *metrics.Gauge
	deferredStateCodePrune             *metrics.Gauge
	lastSolidifiedBlock                *metrics.Gauge
	lastDomainChangeStartBlock         *metrics.Gauge
	lastDomainChangePrunedThrough      *metrics.Gauge
	lastDomainChangePrunedThroughTx    *metrics.Gauge
	lastPassDuration                   *metrics.Gauge
}

func newPrunerMetrics(namespace string) prunerMetrics {
	namespace = normalizePrunerMetricNamespace(namespace)
	return prunerMetrics{
		passes:                             metrics.GetOrRegisterGauge(namespace+"passes", nil),
		errors:                             metrics.GetOrRegisterGauge(namespace+"errors", nil),
		skippedCatchup:                     metrics.GetOrRegisterGauge(namespace+"skipped/catchup", nil),
		canceledCatchup:                    metrics.GetOrRegisterGauge(namespace+"verification/canceled/catchup", nil),
		verificationMemoryHits:             metrics.GetOrRegisterGauge(namespace+"verification/memory_hits", nil),
		verificationPersistentHits:         metrics.GetOrRegisterGauge(namespace+"verification/persisted_hits", nil),
		verificationFull:                   metrics.GetOrRegisterGauge(namespace+"verification/full", nil),
		verificationTrusted:                metrics.GetOrRegisterGauge(namespace+"verification/trusted", nil),
		verificationCacheEntries:           metrics.GetOrRegisterGauge(namespace+"verification/cache_entries", nil),
		verificationActiveSegments:         metrics.GetOrRegisterGauge(namespace+"verification/active_segments", nil),
		verificationActiveBytes:            metrics.GetOrRegisterGauge(namespace+"verification/active_bytes", nil),
		verificationChecksumInFlight:       metrics.GetOrRegisterGauge(namespace+"verification/checksum/inflight", nil),
		verificationChecksumStarted:        metrics.GetOrRegisterGauge(namespace+"verification/checksum/started", nil),
		verificationChecksumCompleted:      metrics.GetOrRegisterGauge(namespace+"verification/checksum/completed", nil),
		verificationChecksumFailed:         metrics.GetOrRegisterGauge(namespace+"verification/checksum/failed", nil),
		verificationChecksumCanceled:       metrics.GetOrRegisterGauge(namespace+"verification/checksum/canceled", nil),
		verificationChecksumBytesInFlight:  metrics.GetOrRegisterGauge(namespace+"verification/checksum/bytes/inflight", nil),
		verificationChecksumBytesStarted:   metrics.GetOrRegisterGauge(namespace+"verification/checksum/bytes/started", nil),
		verificationChecksumBytesCompleted: metrics.GetOrRegisterGauge(namespace+"verification/checksum/bytes/completed", nil),
		retiredVerificationMemoryHits:      metrics.GetOrRegisterGauge(namespace+"retired/verification/memory_hits", nil),
		retiredVerificationPersistentHits:  metrics.GetOrRegisterGauge(namespace+"retired/verification/persisted_hits", nil),
		retiredVerificationFull:            metrics.GetOrRegisterGauge(namespace+"retired/verification/full", nil),
		retiredVerificationCanceled:        metrics.GetOrRegisterGauge(namespace+"retired/verification/canceled/catchup", nil),
		deletedTxRanges:                    metrics.GetOrRegisterGauge(namespace+"deleted/tx_ranges", nil),
		deletedDomainChangeBlocks:          metrics.GetOrRegisterGauge(namespace+"deleted/domain_change_blocks", nil),
		deletedCommitmentCheckpoints:       metrics.GetOrRegisterGauge(namespace+"deleted/commitment_checkpoints", nil),
		deletedStateCodeRows:               metrics.GetOrRegisterGauge(namespace+"deleted/state_code_rows", nil),
		deferredStateCodePrune:             metrics.GetOrRegisterGauge(namespace+"state_code/deferred/catchup", nil),
		lastSolidifiedBlock:                metrics.GetOrRegisterGauge(namespace+"last/solidified_block", nil),
		lastDomainChangeStartBlock:         metrics.GetOrRegisterGauge(namespace+"last/domain_change/start_block", nil),
		lastDomainChangePrunedThrough:      metrics.GetOrRegisterGauge(namespace+"last/domain_change/pruned_through_block", nil),
		lastDomainChangePrunedThroughTx:    metrics.GetOrRegisterGauge(namespace+"last/domain_change/pruned_through_tx", nil),
		lastPassDuration:                   metrics.GetOrRegisterGauge(namespace+"lastpass/duration", nil),
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
	m.canceledCatchup.Update(prunerUintGauge(stats.CanceledCatchup))
	m.retiredVerificationMemoryHits.Update(prunerUintGauge(stats.RetiredVerificationMemoryHits))
	m.retiredVerificationPersistentHits.Update(prunerUintGauge(stats.RetiredVerificationPersistentHits))
	m.retiredVerificationFull.Update(prunerUintGauge(stats.RetiredVerificationFull))
	m.retiredVerificationCanceled.Update(prunerUintGauge(stats.RetiredVerificationCanceled))
	m.deletedTxRanges.Update(prunerUintGauge(stats.DeletedTxRanges))
	m.deletedDomainChangeBlocks.Update(prunerUintGauge(stats.DeletedDomainChangeBlocks))
	m.deletedCommitmentCheckpoints.Update(prunerUintGauge(stats.DeletedCommitmentCheckpoints))
	m.deletedStateCodeRows.Update(prunerUintGauge(stats.DeletedStateCodeRows))
	m.deferredStateCodePrune.Update(prunerUintGauge(stats.DeferredStateCodePrune))
	m.lastSolidifiedBlock.Update(prunerUintGauge(stats.LastSolidifiedBlock))
	m.lastDomainChangeStartBlock.Update(prunerUintGauge(stats.LastDomainChangeStartBlock))
	m.lastDomainChangePrunedThrough.Update(prunerUintGauge(stats.LastDomainChangePrunedThrough))
	m.lastDomainChangePrunedThroughTx.Update(prunerUintGauge(stats.LastDomainChangePrunedThroughTx))
	m.lastPassDuration.Update(int64(stats.LastPassDuration))
}

// updateVerification is called directly by the verification cache at segment
// boundaries, so an in-flight startup checksum is observable before the first
// prune pass completes.
func (m prunerMetrics) updateVerification(stats snapshotCoverageVerificationCacheStats) {
	m.verificationMemoryHits.Update(prunerUintGauge(stats.MemoryHits))
	m.verificationPersistentHits.Update(prunerUintGauge(stats.PersistentHits))
	m.verificationFull.Update(prunerUintGauge(stats.FullVerified))
	m.verificationTrusted.Update(prunerUintGauge(stats.TrustedRecorded))
	m.verificationCacheEntries.Update(prunerUintGauge(stats.Entries))
	m.verificationActiveSegments.Update(prunerUintGauge(stats.ActiveEntries))
	m.verificationActiveBytes.Update(prunerUintGauge(stats.ActiveBytes))
	m.verificationChecksumInFlight.Update(prunerUintGauge(stats.ChecksumInFlight))
	m.verificationChecksumStarted.Update(prunerUintGauge(stats.ChecksumStarted))
	m.verificationChecksumCompleted.Update(prunerUintGauge(stats.ChecksumCompleted))
	m.verificationChecksumFailed.Update(prunerUintGauge(stats.ChecksumFailed))
	m.verificationChecksumCanceled.Update(prunerUintGauge(stats.ChecksumCanceled))
	m.verificationChecksumBytesInFlight.Update(prunerUintGauge(stats.ChecksumBytesInFlight))
	m.verificationChecksumBytesStarted.Update(prunerUintGauge(stats.ChecksumBytesStarted))
	m.verificationChecksumBytesCompleted.Update(prunerUintGauge(stats.ChecksumBytesCompleted))
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
		chain:                     chain,
		cfg:                       cfg,
		metrics:                   newPrunerMetrics(cfg.MetricsNamespace),
		coverageVerificationCache: newSnapshotCoverageVerificationCache(cfg.SnapshotDir),
		quit:                      make(chan struct{}),
		done:                      make(chan struct{}),
	}
	pruner.coverageVerificationCache.setStatsObserver(pruner.metrics.updateVerification)
	if err := pruner.coverageVerificationCache.LoadError(); err != nil {
		log.Warn("Ignoring invalid domain snapshot verification cache", "err", err)
	}
	pruner.updateMetrics()
	return pruner
}

// PruneRetiredSnapshotFilesContext removes retired snapshot files through the
// same content-addressed active-history verification cache used by the online
// pruning lifecycle. Persistent hits are always re-authenticated by hashing
// the active history, inverted-index, and accessor files before deletion; a
// missing or invalid cache safely falls back to exhaustive semantic
// verification.
//
// This helper is intended for offline maintenance commands. It keeps those
// commands from repeating a record-by-record audit that the node has already
// completed and persisted, while preserving the destructive deletion gate.
func PruneRetiredSnapshotFilesContext(ctx context.Context, dir string) (*snapshots.PruneRetiredSegmentFilesResult, error) {
	pruner := NewPruner(nil, PrunerConfig{SnapshotDir: dir})
	return snapshots.PruneRetiredSegmentFilesContextWithVerifier(ctx, dir, pruner.verifyActiveSnapshotManifest)
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
	if p.cfg.Policy.Mode == ModeArchive && p.cfg.Policy.HistoryWindow == 0 {
		close(p.done)
		log.Info("Domain state pruner disabled", "mode", ModeArchive, "reason", "cold history policy not configured")
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
		"maxSyncLag", p.cfg.MaxSyncLag,
		"deferStateCodePruneWhileSyncing", p.cfg.DeferStateCodePruneWhileSyncing)
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
		"codeRows", p.deletedStateCodeRows.Load(),
		"deferredStateCodePrune", p.deferredStateCodePrune.Load())
	return nil
}

func (p *Pruner) Stats() PrunerStats {
	if p == nil {
		return PrunerStats{}
	}
	verification := p.coverageVerificationCache.Stats()
	return PrunerStats{
		Passes:                             p.passes.Load(),
		Errors:                             p.errors.Load(),
		DeletedTxRanges:                    p.deletedTxRanges.Load(),
		DeletedDomainChangeBlocks:          p.deletedDomainChangeBlocks.Load(),
		DeletedCommitmentCheckpoints:       p.deletedCommitmentCheckpoints.Load(),
		DeletedStateCodeRows:               p.deletedStateCodeRows.Load(),
		DeferredStateCodePrune:             p.deferredStateCodePrune.Load(),
		SkippedCatchup:                     p.skippedCatchup.Load(),
		CanceledCatchup:                    p.canceledCatchup.Load(),
		VerificationMemoryHits:             verification.MemoryHits,
		VerificationPersistentHits:         verification.PersistentHits,
		VerificationFull:                   verification.FullVerified,
		VerificationTrusted:                verification.TrustedRecorded,
		VerificationCacheEntries:           verification.Entries,
		VerificationActiveSegments:         verification.ActiveEntries,
		VerificationActiveBytes:            verification.ActiveBytes,
		VerificationChecksumInFlight:       verification.ChecksumInFlight,
		VerificationChecksumStarted:        verification.ChecksumStarted,
		VerificationChecksumCompleted:      verification.ChecksumCompleted,
		VerificationChecksumFailed:         verification.ChecksumFailed,
		VerificationChecksumCanceled:       verification.ChecksumCanceled,
		VerificationChecksumBytesInFlight:  verification.ChecksumBytesInFlight,
		VerificationChecksumBytesStarted:   verification.ChecksumBytesStarted,
		VerificationChecksumBytesCompleted: verification.ChecksumBytesCompleted,
		RetiredVerificationMemoryHits:      p.retiredVerificationMemoryHits.Load(),
		RetiredVerificationPersistentHits:  p.retiredVerificationPersistentHits.Load(),
		RetiredVerificationFull:            p.retiredVerificationFull.Load(),
		RetiredVerificationCanceled:        p.retiredVerificationCanceled.Load(),
		LastSolidifiedBlock:                p.lastSolidifiedBlock.Load(),
		LastDomainChangeStartBlock:         p.lastDomainChangeStartBlock.Load(),
		LastDomainChangePrunedThrough:      p.lastDomainChangePrunedThrough.Load(),
		LastDomainChangePrunedThroughTx:    p.lastDomainChangePrunedThroughTx.Load(),
		LastPassDuration:                   time.Duration(p.lastPassDuration.Load()),
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
	return p.PrunePassContext(context.Background())
}

// RecordTrustedSnapshotSegments carries the local builder/compactor's narrow
// trust boundary into the immediately following hot-prune gate. Only exact
// state-domain history refs still active in the final production manifest are
// recorded. A restart re-hashes all three immutable objects before reusing the
// durable record; external or changed refs still require the full audit.
func (p *Pruner) RecordTrustedSnapshotSegments(refs []snapshots.SegmentRef) error {
	if p == nil || p.coverageVerificationCache == nil || p.cfg.SnapshotDir == "" || len(refs) == 0 {
		return nil
	}
	manifest, err := snapshots.LoadProductionManifest(p.cfg.SnapshotDir)
	if err != nil {
		return err
	}
	active := make(map[snapshots.SegmentRef]struct{}, len(manifest.Segments))
	for _, ref := range manifest.Segments {
		active[ref] = struct{}{}
	}
	for _, ref := range refs {
		if ref.NormalizedDataset() != snapshots.SegmentDatasetStateDomainChange || ref.Kind != snapshots.SegmentHistory {
			continue
		}
		if _, ok := active[ref]; !ok {
			continue
		}
		key, err := snapshotHistoryVerificationKeyFor(p.cfg.SnapshotDir, manifest, ref)
		if err != nil {
			return fmt.Errorf("pruning: record trusted state-domain history %q: %w", ref.Path, err)
		}
		if err := p.coverageVerificationCache.addTrusted(key); err != nil {
			return fmt.Errorf("pruning: persist trusted state-domain history %q: %w", ref.Path, err)
		}
	}
	return nil
}

// verifyActiveSnapshotManifest is the retired-file deletion gate. When every
// retired candidate belongs to state history, only the active state-history
// triples can be affected by their removal; avoid re-reading unrelated event,
// bloom, latest and chain-freezer families. Mixed retirements retain the full
// manifest gate. A destructive gate always re-hashes a same-process memory hit
// immediately before allowing old fallback files to be removed.
func (p *Pruner) verifyActiveSnapshotManifest(ctx context.Context, dir string, manifest *snapshots.Manifest) error {
	if p == nil || p.coverageVerificationCache == nil {
		_, err := snapshots.VerifyLoadedManifestFiles(dir, manifest, snapshots.VerifyManifestOptions{Context: ctx})
		return err
	}
	active := make(map[snapshotHistoryVerificationKey]struct{})
	var activeMu sync.Mutex
	verifyHistory := func(ctx context.Context, dir string, manifest *snapshots.Manifest, ref snapshots.SegmentRef) error {
		key, route, err := p.coverageVerificationCache.verifyHistory(ctx, dir, manifest, ref, true, "retired active-view gate")
		if err != nil {
			return err
		}
		activeMu.Lock()
		active[key] = struct{}{}
		activeMu.Unlock()
		switch route {
		case snapshotHistoryVerificationMemory:
			p.retiredVerificationMemoryHits.Add(1)
		case snapshotHistoryVerificationPersistent:
			p.retiredVerificationPersistentHits.Add(1)
		case snapshotHistoryVerificationFull:
			p.retiredVerificationFull.Add(1)
		}
		return nil
	}
	var err error
	if manifestRetiresOnlyStateHistory(manifest) {
		refs := make([]snapshots.SegmentRef, 0, len(manifest.Segments)/3)
		for _, ref := range manifest.Segments {
			if ref.Dataset != snapshots.SegmentDatasetStateDomainChange || ref.Kind != snapshots.SegmentHistory {
				continue
			}
			refs = append(refs, ref)
		}
		workers := min(runtime.GOMAXPROCS(0), maxRetiredHistoryVerifyWorkers, len(refs))
		if workers > 1 {
			log.Info("Verifying active domain snapshot histories in parallel", "operation", "retired active-view gate", "segments", len(refs), "workers", workers)
		}
		err = verifySnapshotHistoryRefsConcurrently(ctx, refs, workers, func(ctx context.Context, ref snapshots.SegmentRef) error {
			return verifyHistory(ctx, dir, manifest, ref)
		})
	} else {
		_, err = snapshots.VerifyLoadedManifestFiles(dir, manifest, snapshots.VerifyManifestOptions{
			Context:                    ctx,
			StateDomainHistoryVerifier: verifyHistory,
		})
	}
	if err == nil {
		err = p.coverageVerificationCache.retain(active)
	}
	p.updateMetrics()
	return err
}

func verifySnapshotHistoryRefsConcurrently(ctx context.Context, refs []snapshots.SegmentRef, workers int, verify func(context.Context, snapshots.SegmentRef) error) error {
	if len(refs) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if verify == nil {
		return errors.New("pruning: nil state-history verifier")
	}
	workers = min(max(workers, 1), len(refs))
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan snapshots.SegmentRef)
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-workCtx.Done():
					return
				case ref, ok := <-jobs:
					if !ok {
						return
					}
					if err := verify(workCtx, ref); err != nil {
						errOnce.Do(func() {
							firstErr = err
							cancel()
						})
						return
					}
				}
			}
		}()
	}

send:
	for _, ref := range refs {
		select {
		case <-workCtx.Done():
			break send
		case jobs <- ref:
		}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

func manifestRetiresOnlyStateHistory(manifest *snapshots.Manifest) bool {
	if manifest == nil || len(manifest.Retired) == 0 {
		return false
	}
	for _, ref := range manifest.Retired {
		if ref.Dataset != snapshots.SegmentDatasetStateDomainChange {
			return false
		}
		switch ref.Kind {
		case snapshots.SegmentHistory, snapshots.SegmentAccessor, snapshots.SegmentInverted:
		default:
			return false
		}
	}
	return true
}

func (p *Pruner) recordRetiredVerificationCanceled() {
	if p == nil {
		return
	}
	p.retiredVerificationCanceled.Add(1)
	p.updateMetrics()
}

// PrunePassContext runs one prune pass that can be interrupted while it is
// still in a read/plan phase. Lifecycle shutdown uses this path so a historical
// CodeDomain reference scan cannot hold process exit open for minutes.
func (p *Pruner) PrunePassContext(ctx context.Context) (stats Stats, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	start := time.Now()
	defer func() {
		p.lastPassDuration.Store(time.Since(start).Nanoseconds())
		if err != nil && !(errors.Is(err, context.Canceled) && ctx.Err() != nil) {
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
	workCtx, stopCatchupWatch := p.contextWithCatchupCancellation(ctx)
	defer stopCatchupWatch()
	pruneHead := uint64(solidified)
	pruneHead, pruneHeadHash, pruneHeadHasHash, err := p.pruneHeadWithVerifiedBoundary(pruneHead)
	if err != nil {
		return Stats{}, err
	}
	stats, err = Worker{
		DB:          p.chain.DB(),
		Policy:      p.cfg.Policy,
		MaxBlocks:   p.cfg.BatchSize,
		SnapshotDir: p.cfg.SnapshotDir,
		ShouldDeferStateCodePrune: func() bool {
			return p.cfg.DeferStateCodePruneWhileSyncing && p.syncActive()
		},
		PruneHeadHash:               pruneHeadHash,
		PruneHeadHasHash:            pruneHeadHasHash,
		coverageVerificationCache:   p.coverageVerificationCache,
		coverageVerificationContext: workCtx,
		coverageVerificationDone:    stopCatchupWatch,
	}.PruneToContext(ctx, pruneHead)
	stopCatchupWatch()
	if err != nil && errors.Is(err, context.Canceled) && errors.Is(context.Cause(workCtx), errPruneDeferredForCatchup) {
		p.skippedCatchup.Add(1)
		p.canceledCatchup.Add(1)
		p.lastSolidifiedBlock.Store(uint64(solidified))
		return Stats{}, nil
	}
	if err != nil {
		return Stats{}, err
	}
	p.passes.Add(1)
	p.deletedTxRanges.Add(uint64(stats.DeletedTxRanges))
	p.deletedDomainChangeBlocks.Add(uint64(stats.DeletedDomainChangeBlocks))
	p.deletedCommitmentCheckpoints.Add(uint64(stats.DeletedCommitmentCheckpoints))
	p.deletedStateCodeRows.Add(uint64(stats.DeletedStateCodeRows))
	if stats.StateCodePruneDeferred {
		p.deferredStateCodePrune.Add(1)
	}
	p.lastSolidifiedBlock.Store(uint64(solidified))
	if stats.DeletedDomainChangeBlocks > 0 {
		p.lastDomainChangeStartBlock.Store(stats.DomainChangeStartBlock)
		p.lastDomainChangePrunedThrough.Store(stats.DomainChangePrunedThrough)
		p.lastDomainChangePrunedThroughTx.Store(stats.DomainChangePrunedThroughTx)
	}
	if stats.DeletedTxRanges != 0 || stats.DeletedDomainChangeBlocks != 0 || stats.DeletedCommitmentCheckpoints != 0 || stats.DeletedStateCodeRows != 0 {
		elapsed := time.Since(start)
		ctx := prunePassLogContext(pruneHead, solidified, p.cfg.Policy, stats, elapsed)
		if info, suppressed := p.prunePassLogDecision(pruneHead, stats, time.Now()); info {
			if suppressed > 0 {
				ctx = append(ctx, "suppressedPasses", suppressed)
			}
			log.Info("Domain state prune pass completed", ctx...)
		} else {
			log.Debug("Domain state prune pass completed", ctx...)
		}
	}
	return stats, nil
}

func (p *Pruner) prunePassLogDecision(pruneHead uint64, stats Stats, now time.Time) (info bool, suppressed uint64) {
	if p == nil {
		return true, 0
	}
	policy := p.cfg.Policy
	coldHistoryMode := policy.Mode == ModeSnap || (policy.Mode == ModeArchive && policy.HistoryWindow > 0)
	hasUncommonChanges := stats.DeletedTxRanges > 0 || stats.DeletedCommitmentCheckpoints > 0 || stats.DeletedStateCodeRows > 0
	caughtUp := pruneHead >= policy.HistoryWindow && stats.DomainChangePrunedThrough >= pruneHead-policy.HistoryWindow
	if !coldHistoryMode || hasUncommonChanges || caughtUp {
		p.lastColdPruneProgressLogAt.Store(now.UnixNano())
		return true, p.coldPruneProgressSuppressed.Swap(0)
	}

	// Cold snapshot catch-up can make a short prune commit every few seconds.
	// Preserve every committed cursor at Debug and sample Info once per minute;
	// uncommon side effects and the caught-up boundary are always Info.
	p.coldPruneProgressSuppressed.Add(1)
	last := p.lastColdPruneProgressLogAt.Load()
	if last != 0 && now.Sub(time.Unix(0, last)) < coldPruneProgressLogInterval {
		return false, 0
	}
	if !p.lastColdPruneProgressLogAt.CompareAndSwap(last, now.UnixNano()) {
		return false, 0
	}
	count := p.coldPruneProgressSuppressed.Swap(0)
	if count > 0 {
		count--
	}
	return true, count
}

func prunePassLogContext(pruneHead uint64, solidified int64, policy Policy, stats Stats, elapsed time.Duration) []any {
	ctx := []any{
		"headBlock", pruneHead,
		"solidifiedBlock", solidified,
		"historyChanged", stats.DeletedDomainChangeBlocks > 0,
		"txRanges", stats.DeletedTxRanges,
		"changeBlocks", stats.DeletedDomainChangeBlocks,
		"commitments", stats.DeletedCommitmentCheckpoints,
		"codeRows", stats.DeletedStateCodeRows,
		"stateCodePruneDeferred", stats.StateCodePruneDeferred,
		"elapsed", elapsed.Round(time.Millisecond),
	}
	if pruneHead >= policy.HistoryWindow {
		ctx = append(ctx, "targetPruneThrough", pruneHead-policy.HistoryWindow)
	}
	if stats.DeletedDomainChangeBlocks > 0 {
		ctx = append(ctx,
			"startBlock", stats.DomainChangeStartBlock,
			"prunedThroughBlock", stats.DomainChangePrunedThrough,
			"prunedThroughTx", stats.DomainChangePrunedThroughTx,
			"historyBlocksPerSec", pruneRate(stats.DeletedDomainChangeBlocks, elapsed))
	}
	return ctx
}

func pruneRate(items int, elapsed time.Duration) float64 {
	if items <= 0 || elapsed <= 0 {
		return 0
	}
	rate := float64(items) / elapsed.Seconds()
	return float64(int64(rate*100+0.5)) / 100
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

func (p *Pruner) syncActive() bool {
	if p == nil {
		return false
	}
	source, ok := p.chain.(syncRemainingSource)
	if !ok {
		return false
	}
	_, active := source.SyncRemainingBlocks()
	return active
}

type pruneCatchupCancellation struct {
	mu      sync.Mutex
	stopped bool
	stop    chan struct{}
	done    chan struct{}
	cancel  context.CancelCauseFunc
}

func (p *Pruner) contextWithCatchupCancellation(parent context.Context) (context.Context, func()) {
	if p == nil || p.cfg.MaxSyncLag == 0 {
		if parent == nil {
			parent = context.Background()
		}
		return parent, func() {}
	}
	if _, ok := p.chain.(syncRemainingSource); !ok {
		if parent == nil {
			parent = context.Background()
		}
		return parent, func() {}
	}
	return contextWithPruneCancellation(parent, p.shouldSkipForCatchup, errPruneDeferredForCatchup)
}

func (p *Pruner) contextWithSyncActiveCancellation(parent context.Context, cause error) (context.Context, func()) {
	if p == nil {
		if parent == nil {
			parent = context.Background()
		}
		return parent, func() {}
	}
	if _, ok := p.chain.(syncRemainingSource); !ok {
		if parent == nil {
			parent = context.Background()
		}
		return parent, func() {}
	}
	return contextWithPruneCancellation(parent, p.syncActive, cause)
}

func contextWithPruneCancellation(parent context.Context, shouldCancel func() bool, cause error) (context.Context, func()) {
	if parent == nil {
		parent = context.Background()
	}
	if shouldCancel == nil {
		return parent, func() {}
	}
	ctx, cancel := context.WithCancelCause(parent)
	watch := &pruneCatchupCancellation{
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		cancel: cancel,
	}
	go func() {
		defer close(watch.done)
		ticker := time.NewTicker(pruneCatchupPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if !shouldCancel() {
					continue
				}
				watch.mu.Lock()
				if !watch.stopped {
					watch.cancel(cause)
				}
				watch.mu.Unlock()
				return
			case <-watch.stop:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return ctx, func() {
		watch.mu.Lock()
		if !watch.stopped {
			watch.stopped = true
			close(watch.stop)
		}
		watch.mu.Unlock()
		<-watch.done
		cancel(nil)
	}
}
