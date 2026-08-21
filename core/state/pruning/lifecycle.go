package pruning

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/tronprotocol/go-tron/common"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

var lifecycleLog = gtronlog.NewModule("core/state/lifecycle")

var errRetiredPruneDeferredForCatchup = errors.New("pruning: retired-file verification deferred for sync catch-up")
var errStateChangeIndexPruneDeferredForCatchup = errors.New("pruning: state-change index sweep deferred for sync catch-up")

// SnapshotLifecycleConfig wires the Erigon-style cold/hot lifecycle together:
// build and publish cold history files, compact old history files, then prune
// hot data covered by the visible snapshot view.
type SnapshotLifecycleConfig struct {
	Snapshot          snapshots.Config
	Pruner            PrunerConfig
	ChainFreezerBuild ChainFreezerBuildFunc
	ChainLookupPrune  ChainLookupPruneFunc
	SectionBloomPrune SectionBloomPruneFunc
	BalanceTracePrune BalanceTracePruneFunc
	// StateChangeIndexPrune performs the infrequent sequential reclamation of
	// immutable posting frames after cold history and hot pruning have caught
	// up. It is never admitted while historical sync is active.
	StateChangeIndexPrune StateChangeIndexPruneFunc
	RetiredPrune          RetiredPruneFunc
	// DeferRetiredPruneWhileSyncing postpones the full active-manifest
	// verification and retired-file deletion gate while historical sync is
	// active. The sync-complete lifecycle wake runs it once the importer is idle.
	DeferRetiredPruneWhileSyncing bool
	Interval                      time.Duration
}

type ChainLookupPruneFunc func() (*snapshots.PruneHotChainLookupResult, error)
type SectionBloomPruneFunc func() (*snapshots.PruneHotSectionBloomResult, error)
type BalanceTracePruneFunc func() (*snapshots.PruneHotBalanceTraceResult, error)
type StateChangeIndexPruneFunc func(context.Context) (*rawdb.StateChangePostingPruneResult, error)
type RetiredPruneFunc func(context.Context, snapshots.ActiveManifestVerifier) (*snapshots.PruneRetiredSegmentFilesResult, error)
type ChainFreezerBuildFunc func() (snapshots.ChainFreezerSnapshotPassResult, error)

// SnapshotLifecyclePass is the result of one ordered lifecycle pass.
type SnapshotLifecyclePass struct {
	Snapshot                 snapshots.PassResult
	ChainFreezerBuild        snapshots.ChainFreezerSnapshotPassResult
	Prune                    Stats
	ChainLookupPrune         *snapshots.PruneHotChainLookupResult
	SectionBloomPrune        *snapshots.PruneHotSectionBloomResult
	BalanceTracePrune        *snapshots.PruneHotBalanceTraceResult
	StateChangeIndexPrune    *rawdb.StateChangePostingPruneResult
	StateChangeIndexDeferred bool
	RetiredPrune             *snapshots.PruneRetiredSegmentFilesResult
	RetiredDeferred          bool
}

// SnapshotLifecycle owns the state snapshot builder/compactor and hot pruner
// under one node.Lifecycle, so their progress advances in one ordered pass
// instead of via independent background loops.
type SnapshotLifecycle struct {
	builder               *snapshots.Runner
	pruner                *Pruner
	chainFreezerBuild     ChainFreezerBuildFunc
	chainLookupPrune      ChainLookupPruneFunc
	sectionBloomPrune     SectionBloomPruneFunc
	balanceTracePrune     BalanceTracePruneFunc
	stateChangeIndexPrune StateChangeIndexPruneFunc
	retiredPrune          RetiredPruneFunc
	deferRetiredPrune     bool

	interval time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
	wake     chan struct{}
	quit     chan struct{}
	done     chan struct{}
	once     sync.Once

	hookMu    sync.Mutex
	passHooks []func()

	failureLogMu sync.Mutex
	failureLog   snapshotLifecycleFailureLogState
}

func NewSnapshotLifecycle(chain ChainSource, cfg SnapshotLifecycleConfig) *SnapshotLifecycle {
	ctx, cancel := context.WithCancel(context.Background())
	interval := cfg.Interval
	if interval <= 0 {
		interval = cfg.Pruner.Interval
	}
	if interval <= 0 {
		interval = cfg.Snapshot.Interval
	}
	if interval <= 0 {
		interval = defaultInterval
	}

	var builder *snapshots.Runner
	if cfg.Snapshot.Enabled {
		builder = snapshots.NewRunner(snapshotChainSource{chain: chain}, cfg.Snapshot)
	}
	return &SnapshotLifecycle{
		builder:               builder,
		pruner:                NewPruner(chain, cfg.Pruner),
		chainFreezerBuild:     cfg.ChainFreezerBuild,
		chainLookupPrune:      cfg.ChainLookupPrune,
		sectionBloomPrune:     cfg.SectionBloomPrune,
		balanceTracePrune:     cfg.BalanceTracePrune,
		stateChangeIndexPrune: cfg.StateChangeIndexPrune,
		retiredPrune:          cfg.RetiredPrune,
		deferRetiredPrune:     cfg.DeferRetiredPruneWhileSyncing,
		interval:              interval,
		ctx:                   ctx,
		cancel:                cancel,
		wake:                  make(chan struct{}, 1),
		quit:                  make(chan struct{}),
		done:                  make(chan struct{}),
	}
}

func (l *SnapshotLifecycle) Start() error {
	if l == nil {
		return nil
	}
	if l.pruner == nil || l.pruner.chain == nil || l.pruner.chain.DB() == nil {
		close(l.done)
		return nil
	}
	if err := l.pruner.cfg.Policy.Validate(); err != nil {
		close(l.done)
		return err
	}
	if l.builder != nil {
		if err := l.builder.Prepare(); err != nil {
			close(l.done)
			return err
		}
	}
	go l.loop()
	lifecycleLog.Info("Domain state snapshot/prune lifecycle started",
		"snapshotEnabled", l.builder != nil,
		"chainFreezerBuild", l.chainFreezerBuild != nil,
		"chainLookupPrune", l.chainLookupPrune != nil,
		"sectionBloomPrune", l.sectionBloomPrune != nil,
		"balanceTracePrune", l.balanceTracePrune != nil,
		"stateChangeIndexPrune", l.stateChangeIndexPrune != nil,
		"retiredPrune", l.retiredPrune != nil,
		"deferRetiredPruneWhileSyncing", l.deferRetiredPrune,
		"mode", l.pruner.cfg.Policy.Mode,
		"interval", l.interval,
		"snapshotDir", l.pruner.cfg.SnapshotDir)
	return nil
}

func (l *SnapshotLifecycle) Stop() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() {
		l.cancel()
		close(l.quit)
	})
	<-l.done
	lifecycleLog.Info("Domain state snapshot/prune lifecycle stopped")
	return nil
}

// RequestPass schedules one lifecycle pass without waiting for it. Requests
// coalesce while a pass is pending, which makes this suitable for sync and
// freezer completion notifications.
func (l *SnapshotLifecycle) RequestPass() {
	if l == nil {
		return
	}
	select {
	case <-l.quit:
		return
	default:
	}
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// AddPassCompleteHook registers a callback after a full successful lifecycle
// pass. Hooks run without lifecycle locks held and must return promptly; they
// are intended to wake dependent maintenance stages.
func (l *SnapshotLifecycle) AddPassCompleteHook(hook func()) {
	if l == nil || hook == nil {
		return
	}
	l.hookMu.Lock()
	l.passHooks = append(l.passHooks, hook)
	l.hookMu.Unlock()
}

func (l *SnapshotLifecycle) notifyPassComplete() {
	l.hookMu.Lock()
	hooks := append([]func(){}, l.passHooks...)
	l.hookMu.Unlock()
	for _, hook := range hooks {
		hook()
	}
}

func (l *SnapshotLifecycle) OnePass() (SnapshotLifecyclePass, error) {
	if l == nil {
		return SnapshotLifecyclePass{}, nil
	}
	started := time.Now()
	var out SnapshotLifecyclePass
	var stopErr error
	if l.builder != nil {
		if err := l.builder.PreflightCatalog(); err != nil {
			return out, err
		}
		result, err := l.builder.OnePassContext(l.ctx)
		if err != nil {
			return out, err
		}
		out.Snapshot = result
		trusted := append(append([]snapshots.SegmentRef(nil), result.Segments...), result.Compaction.Segments...)
		if err := l.pruner.RecordTrustedSnapshotSegments(trusted); err != nil {
			return out, err
		}
		// A resource-deferred history pass means another heavy stage owns the
		// shared admission window. A rate-limited pass, however, deliberately
		// leaves capacity for chain-freezer snapshots and ordered pruning, whose
		// own lazy gate prevents overlap and starts a recovery cooldown.
		if result.HistoryGateDeferred {
			return out, nil
		}
	}
	if l.chainFreezerBuild != nil {
		result, err := l.chainFreezerBuild()
		if err != nil {
			return out, err
		}
		out.ChainFreezerBuild = result
	}
	if l.pruner != nil {
		stats, err := l.pruner.PrunePassContext(l.ctx)
		if err != nil {
			return out, err
		}
		out.Prune = stats
	}
	// Packed posting frames are immutable and intentionally remain after the
	// per-block hot changeset prune. Sweep them only after the cold builder has
	// reached its verified cutoff and network catch-up is idle. If sync becomes
	// active during the scan, cancel promptly; partial frame deletions are
	// idempotent and the durable manifest cursor advances only on full success.
	if l.stateChangeIndexPrune != nil && l.builder != nil &&
		!out.Snapshot.HistoryDeferred && !out.Snapshot.HistoryGateDeferred &&
		out.Snapshot.EligibleCutoffBlock > 0 &&
		out.Snapshot.PublishedBlock >= out.Snapshot.EligibleCutoffBlock {
		out.StateChangeIndexDeferred = l.pruner != nil && l.pruner.syncActive()
		if !out.StateChangeIndexDeferred {
			pruneCtx := l.ctx
			stopPruneWatch := func() {}
			if l.pruner != nil {
				pruneCtx, stopPruneWatch = l.pruner.contextWithSyncActiveCancellation(l.ctx, errStateChangeIndexPruneDeferredForCatchup)
			}
			result, err := l.stateChangeIndexPrune(pruneCtx)
			stopPruneWatch()
			if err != nil {
				if errors.Is(err, context.Canceled) && errors.Is(context.Cause(pruneCtx), errStateChangeIndexPruneDeferredForCatchup) {
					out.StateChangeIndexDeferred = true
				} else if !errors.Is(err, context.Canceled) || l.ctx.Err() == nil {
					return out, err
				} else {
					stopErr = err
				}
			} else {
				out.StateChangeIndexPrune = result
			}
		}
	}
	if l.chainLookupPrune != nil {
		result, err := l.chainLookupPrune()
		if err != nil {
			return out, err
		}
		out.ChainLookupPrune = result
	}
	if l.sectionBloomPrune != nil {
		result, err := l.sectionBloomPrune()
		if err != nil {
			return out, err
		}
		out.SectionBloomPrune = result
	}
	if l.balanceTracePrune != nil {
		result, err := l.balanceTracePrune()
		if err != nil {
			return out, err
		}
		out.BalanceTracePrune = result
	}
	if l.retiredPrune != nil && l.deferRetiredPrune && l.pruner != nil {
		out.RetiredDeferred = l.pruner.syncActive()
	}
	if l.retiredPrune != nil && !out.RetiredDeferred {
		retiredCtx := l.ctx
		stopRetiredWatch := func() {}
		if l.deferRetiredPrune && l.pruner != nil {
			retiredCtx, stopRetiredWatch = l.pruner.contextWithSyncActiveCancellation(l.ctx, errRetiredPruneDeferredForCatchup)
		}
		var verifyActive snapshots.ActiveManifestVerifier
		if l.pruner != nil {
			verifyActive = l.pruner.verifyActiveSnapshotManifest
		}
		result, err := l.retiredPrune(retiredCtx, verifyActive)
		stopRetiredWatch()
		if err != nil {
			if errors.Is(err, context.Canceled) && errors.Is(context.Cause(retiredCtx), errRetiredPruneDeferredForCatchup) {
				out.RetiredDeferred = true
				l.pruner.recordRetiredVerificationCanceled()
			} else if !errors.Is(err, context.Canceled) || l.ctx.Err() == nil {
				return out, err
			} else {
				// Retired-file inspection is read-only until its final deletion
				// loop, so it is safe to cancel during shutdown. Still publish any
				// manifest changes made by the ordered build/prune stages above.
				stopErr = err
			}
		} else {
			out.RetiredPrune = result
		}
	}
	if l.builder != nil {
		published, err := l.builder.PublishCatalogIfManifestChanged()
		if err != nil {
			return out, err
		}
		out.Snapshot.CatalogPublished = published
		if published {
			lifecycleLog.Info("Signed snapshot catalog published")
		}
	}
	if stopErr != nil {
		return out, stopErr
	}
	latestBlock := uint64(0)
	if l.builder != nil {
		latestBlock = l.builder.Snapshot().LastLatestBuildBlock
	}
	if ctx, changed := snapshotLifecyclePassLogContext(out, latestBlock, time.Since(started)); changed {
		lifecycleLog.Info("Domain state lifecycle maintenance completed", ctx...)
	}
	l.notifyPassComplete()
	return out, nil
}

func snapshotLifecyclePassLogContext(pass SnapshotLifecyclePass, latestBlock uint64, elapsed time.Duration) ([]any, bool) {
	ctx := []any{"elapsed", elapsed.Round(time.Millisecond)}
	changed := false
	if pass.Snapshot.LatestBuilt {
		changed = true
		ctx = append(ctx, "latestSnapshotBuilt", true, "latestSnapshotBlock", latestBlock, "latestSnapshotElapsed", pass.Snapshot.LatestDuration.Round(time.Millisecond))
	}
	if pass.ChainFreezerBuild.Built || pass.ChainFreezerBuild.EventLogBuilt {
		changed = true
		ctx = append(ctx, "chainFreezerSnapshotBuilt", pass.ChainFreezerBuild.Built, "chainFreezerEventLogBuilt", pass.ChainFreezerBuild.EventLogBuilt)
		if pass.ChainFreezerBuild.Built {
			ctx = append(ctx,
				"chainFreezerFromBlock", pass.ChainFreezerBuild.FromBlock,
				"chainFreezerToBlock", pass.ChainFreezerBuild.ToBlock)
		}
		if pass.ChainFreezerBuild.EventLogBuilt {
			ctx = append(ctx,
				"chainFreezerEventLogFromBlock", pass.ChainFreezerBuild.EventLogFromBlock,
				"chainFreezerEventLogToBlock", pass.ChainFreezerBuild.EventLogToBlock)
		}
	}
	if result := pass.ChainLookupPrune; result != nil && result.HasRange {
		changed = true
		ctx = append(ctx,
			"chainLookupFromBlock", result.FromBlock,
			"chainLookupToBlock", result.ToBlock,
			"chainLookupBlockIndexesDeleted", result.BlockIndexesDeleted,
			"chainLookupStateRootsDeleted", result.StateRootsDeleted,
			"chainLookupTxIndexesDeleted", result.TxIndexesDeleted,
			"chainLookupTxInfosDeleted", result.TxInfosDeleted)
	}
	if result := pass.SectionBloomPrune; result != nil && result.HasRange {
		changed = true
		ctx = append(ctx,
			"sectionBloomFromSection", result.FromSection,
			"sectionBloomToSection", result.ToSection,
			"sectionBloomRowsDeleted", result.RowsDeleted)
	}
	if result := pass.BalanceTracePrune; result != nil && result.HasRange {
		changed = true
		ctx = append(ctx,
			"balanceTraceFromBlock", result.FromBlock,
			"balanceTraceToBlock", result.ToBlock,
			"balanceTraceBlocksDeleted", result.BlockTracesDeleted,
			"balanceTraceAccountsDeleted", result.AccountTracesDeleted)
	}
	if result := pass.StateChangeIndexPrune; result != nil && (result.PostingRowsDeleted > 0 || result.DirectoryRowsDeleted > 0) {
		changed = true
		ctx = append(ctx,
			"stateChangePostingRowsDeleted", result.PostingRowsDeleted,
			"stateChangeDirectoryRowsDeleted", result.DirectoryRowsDeleted)
	}
	if result := pass.RetiredPrune; result != nil && (result.FilesDeleted > 0 || result.FilesMissing > 0) {
		changed = true
		ctx = append(ctx,
			"retiredSnapshotFilesDeleted", result.FilesDeleted,
			"retiredSnapshotFilesMissing", result.FilesMissing,
			"retiredSnapshotBytesDeleted", result.BytesDeleted)
	}
	return ctx, changed
}

func (l *SnapshotLifecycle) loop() {
	defer close(l.done)
	var retryTimer *time.Timer
	var retry <-chan time.Time
	cancelRetry := func() {
		if retryTimer != nil && !retryTimer.Stop() {
			select {
			case <-retryTimer.C:
			default:
			}
		}
		retry = nil
	}
	scheduleRetry := func(after time.Duration) {
		if after <= 0 {
			return
		}
		if retryTimer == nil {
			retryTimer = time.NewTimer(after)
		} else {
			if !retryTimer.Stop() {
				select {
				case <-retryTimer.C:
				default:
				}
			}
			retryTimer.Reset(after)
		}
		retry = retryTimer.C
	}
	defer func() {
		if retryTimer != nil {
			retryTimer.Stop()
		}
	}()
	runPass := func(reason string) {
		cancelRetry()
		result, err := l.OnePass()
		if err != nil {
			if errors.Is(err, context.Canceled) && l.ctx.Err() != nil {
				return
			}
			l.logPassFailure(reason, err, time.Now())
			return
		}
		l.logPassRecovery(time.Now())
		// Erigon's background aggregator drains every ready immutable step in
		// one run. Preserve go-tron's smaller build/publish/prune transaction
		// boundary, but coalesce an immediate next pass while verified lag
		// remains instead of sleeping for a full maintenance interval.
		if result.Snapshot.NeedsCatchup() {
			l.RequestPass()
		} else if result.Snapshot.HistoryRetryAfter > 0 {
			scheduleRetry(result.Snapshot.HistoryRetryAfter)
		}
	}
	runPass("initial")
	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()
	for {
		// A pass can take longer than the shutdown request. Do not let its
		// coalesced follow-up win over Stop once the in-flight ordered pass has
		// completed.
		select {
		case <-l.quit:
			return
		default:
		}
		select {
		case <-ticker.C:
			runPass("interval")
		case <-l.wake:
			runPass("requested")
		case <-retry:
			retry = nil
			runPass("resource-retry")
		case <-l.quit:
			return
		}
	}
}

type snapshotChainSource struct {
	chain ChainSource
}

func (s snapshotChainSource) DB() snapshots.AggregatorDB {
	if s.chain == nil {
		return nil
	}
	return s.chain.DB()
}

type eventLogDBSource interface {
	EventLogDB() *rawdb.ChainDB
}

func (s snapshotChainSource) EventLogDB() *rawdb.ChainDB {
	if s.chain == nil {
		return nil
	}
	if source, ok := s.chain.(eventLogDBSource); ok {
		return source.EventLogDB()
	}
	if db, ok := s.chain.DB().(*rawdb.ChainDB); ok {
		return db
	}
	return nil
}

func (s snapshotChainSource) LatestSolidifiedBlockNum() int64 {
	if s.chain == nil {
		return 0
	}
	return s.chain.LatestSolidifiedBlockNum()
}

func (s snapshotChainSource) SyncRemainingBlocks() (uint64, bool) {
	if s.chain == nil {
		return 0, false
	}
	source, ok := s.chain.(syncRemainingSource)
	if !ok {
		return 0, false
	}
	return source.SyncRemainingBlocks()
}

func (s snapshotChainSource) BeginCommitmentBranchRotation() (rawdb.CommitmentBranchRotation, bool, error) {
	if s.chain == nil {
		return rawdb.CommitmentBranchRotation{}, false, nil
	}
	source, ok := s.chain.(interface {
		BeginCommitmentBranchRotation() (rawdb.CommitmentBranchRotation, bool, error)
	})
	if !ok {
		return rawdb.CommitmentBranchRotation{}, false, nil
	}
	return source.BeginCommitmentBranchRotation()
}

func (s snapshotChainSource) CompleteCommitmentBranchRotation(rotation rawdb.CommitmentBranchRotation, mgr *snapshots.Manager) error {
	if s.chain == nil {
		return nil
	}
	source, ok := s.chain.(interface {
		CompleteCommitmentBranchRotation(rawdb.CommitmentBranchRotation, *snapshots.Manager) error
	})
	if !ok {
		return nil
	}
	return source.CompleteCommitmentBranchRotation(rotation, mgr)
}

func (s snapshotChainSource) CanonicalBlockHash(blockNum uint64) (common.Hash, bool) {
	return canonicalBlockHashFromChainSource(s.chain, blockNum)
}

func (s snapshotChainSource) CanonicalBlockHashStrict(blockNum uint64) (common.Hash, bool, error) {
	return canonicalBlockHashLookupFromChainSource(s.chain, blockNum)
}

var _ snapshots.ChainSource = snapshotChainSource{}
