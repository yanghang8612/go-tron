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
	RetiredPrune      RetiredPruneFunc
	// DeferRetiredPruneWhileSyncing postpones the full active-manifest
	// verification and retired-file deletion gate while historical sync is
	// active. The sync-complete lifecycle wake runs it once the importer is idle.
	DeferRetiredPruneWhileSyncing bool
	Interval                      time.Duration
}

type ChainLookupPruneFunc func() (*snapshots.PruneHotChainLookupResult, error)
type SectionBloomPruneFunc func() (*snapshots.PruneHotSectionBloomResult, error)
type BalanceTracePruneFunc func() (*snapshots.PruneHotBalanceTraceResult, error)
type RetiredPruneFunc func(context.Context) (*snapshots.PruneRetiredSegmentFilesResult, error)
type ChainFreezerBuildFunc func() (snapshots.ChainFreezerSnapshotPassResult, error)

// SnapshotLifecyclePass is the result of one ordered lifecycle pass.
type SnapshotLifecyclePass struct {
	Snapshot          snapshots.PassResult
	ChainFreezerBuild snapshots.ChainFreezerSnapshotPassResult
	Prune             Stats
	ChainLookupPrune  *snapshots.PruneHotChainLookupResult
	SectionBloomPrune *snapshots.PruneHotSectionBloomResult
	BalanceTracePrune *snapshots.PruneHotBalanceTraceResult
	RetiredPrune      *snapshots.PruneRetiredSegmentFilesResult
	RetiredDeferred   bool
}

// SnapshotLifecycle owns the state snapshot builder/compactor and hot pruner
// under one node.Lifecycle, so their progress advances in one ordered pass
// instead of via independent background loops.
type SnapshotLifecycle struct {
	builder           *snapshots.Runner
	pruner            *Pruner
	chainFreezerBuild ChainFreezerBuildFunc
	chainLookupPrune  ChainLookupPruneFunc
	sectionBloomPrune SectionBloomPruneFunc
	balanceTracePrune BalanceTracePruneFunc
	retiredPrune      RetiredPruneFunc
	deferRetiredPrune bool

	interval time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
	wake     chan struct{}
	quit     chan struct{}
	done     chan struct{}
	once     sync.Once

	hookMu    sync.Mutex
	passHooks []func()
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
		builder:           builder,
		pruner:            NewPruner(chain, cfg.Pruner),
		chainFreezerBuild: cfg.ChainFreezerBuild,
		chainLookupPrune:  cfg.ChainLookupPrune,
		sectionBloomPrune: cfg.SectionBloomPrune,
		balanceTracePrune: cfg.BalanceTracePrune,
		retiredPrune:      cfg.RetiredPrune,
		deferRetiredPrune: cfg.DeferRetiredPruneWhileSyncing,
		interval:          interval,
		ctx:               ctx,
		cancel:            cancel,
		wake:              make(chan struct{}, 1),
		quit:              make(chan struct{}),
		done:              make(chan struct{}),
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
	var out SnapshotLifecyclePass
	var stopErr error
	if l.builder != nil {
		if err := l.builder.PreflightCatalog(); err != nil {
			return out, err
		}
		result, err := l.builder.OnePass()
		if err != nil {
			return out, err
		}
		out.Snapshot = result
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
		if source, ok := l.pruner.chain.(syncRemainingSource); ok {
			_, out.RetiredDeferred = source.SyncRemainingBlocks()
		}
	}
	if l.retiredPrune != nil && !out.RetiredDeferred {
		result, err := l.retiredPrune(l.ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) || l.ctx.Err() == nil {
				return out, err
			}
			// Retired-file inspection is read-only until its final deletion
			// loop, so it is safe to cancel during shutdown. Still publish any
			// manifest changes made by the ordered build/prune stages above.
			stopErr = err
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
	l.notifyPassComplete()
	return out, nil
}

func (l *SnapshotLifecycle) loop() {
	defer close(l.done)
	runPass := func(reason string) {
		result, err := l.OnePass()
		if err != nil {
			if errors.Is(err, context.Canceled) && l.ctx.Err() != nil {
				return
			}
			lifecycleLog.Warn("Domain state snapshot/prune pass failed", "reason", reason, "err", err)
			return
		}
		// Erigon's background aggregator drains every ready immutable step in
		// one run. Preserve go-tron's smaller build/publish/prune transaction
		// boundary, but coalesce an immediate next pass while verified lag
		// remains instead of sleeping for a full maintenance interval.
		if result.Snapshot.NeedsCatchup() {
			l.RequestPass()
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
