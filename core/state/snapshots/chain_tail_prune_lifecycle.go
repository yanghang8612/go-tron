package snapshots

import (
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

const (
	defaultChainFreezerTailPruneInterval = time.Minute

	// ChainFreezerTailMinRetainBlocks is the minimum local freezer retention
	// needed by the TVM BLOCKHASH opcode, which can read any ancestor inside
	// the previous 256 blocks. Minimal-mode tail pruning must leave that
	// window in the local freezer, because execution does not consult cold
	// snapshot segments.
	ChainFreezerTailMinRetainBlocks = uint64(256)
)

type ChainFreezerTailPruneLifecycleConfig struct {
	RetainBlocks  uint64
	Interval      time.Duration
	HeadBlock     func() uint64
	HeavyWorkGate *maintenance.HeavyWorkGate
}

type ChainFreezerTailPruneLifecycle struct {
	db      ethdb.KeyValueReader
	freezer ChainFreezerTailPruner
	cold    rawdb.AncientReader
	cfg     ChainFreezerTailPruneLifecycleConfig

	wake chan struct{}
	quit chan struct{}
	done chan struct{}
	once sync.Once
}

func NewChainFreezerTailPruneLifecycle(db ethdb.KeyValueReader, freezer ChainFreezerTailPruner, cold rawdb.AncientReader, cfg ChainFreezerTailPruneLifecycleConfig) *ChainFreezerTailPruneLifecycle {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultChainFreezerTailPruneInterval
	}
	cfg.RetainBlocks = EffectiveChainFreezerTailRetainBlocks(cfg.RetainBlocks)
	return &ChainFreezerTailPruneLifecycle{
		db:      db,
		freezer: freezer,
		cold:    cold,
		cfg:     cfg,
		wake:    make(chan struct{}, 1),
		quit:    make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// EffectiveChainFreezerTailRetainBlocks applies the BLOCKHASH safety floor to
// a configured minimal-mode chain-freezer tail retention window.
func EffectiveChainFreezerTailRetainBlocks(retainBlocks uint64) uint64 {
	if retainBlocks == 0 || retainBlocks >= ChainFreezerTailMinRetainBlocks {
		return retainBlocks
	}
	return ChainFreezerTailMinRetainBlocks
}

func (l *ChainFreezerTailPruneLifecycle) Start() error {
	if l == nil {
		return nil
	}
	if l.db == nil || l.freezer == nil || l.cold == nil || l.cfg.RetainBlocks == 0 || l.cfg.HeadBlock == nil {
		close(l.done)
		return nil
	}
	go l.loop()
	coldSnapshotLog.Info("Chain freezer tail prune lifecycle started",
		"retainBlocks", l.cfg.RetainBlocks,
		"interval", l.cfg.Interval)
	return nil
}

func (l *ChainFreezerTailPruneLifecycle) Stop() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() { close(l.quit) })
	<-l.done
	coldSnapshotLog.Info("Chain freezer tail prune lifecycle stopped")
	return nil
}

// RequestPass schedules one tail-prune pass without waiting for it. Requests
// coalesce while a pass is pending so upstream snapshot completion can wake
// this dependent stage safely.
func (l *ChainFreezerTailPruneLifecycle) RequestPass() {
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

func (l *ChainFreezerTailPruneLifecycle) OnePass() (*ChainFreezerTailPruneApplyResult, error) {
	if l == nil || l.db == nil || l.freezer == nil || l.cold == nil || l.cfg.RetainBlocks == 0 || l.cfg.HeadBlock == nil {
		return nil, nil
	}
	currentTail, err := chainFreezerTailForPruning(l.freezer)
	if err != nil {
		return nil, err
	}
	ancientHead, err := l.freezer.AncientCount(rawdb.AncientBlocksTable)
	if err != nil {
		return nil, err
	}
	plan, err := PlanChainFreezerTailPruneFromDB(l.db, currentTail, ancientHead, l.cfg.HeadBlock(), l.cfg.RetainBlocks)
	if err != nil {
		return nil, err
	}
	// Stage repair is metadata-only. Acquire the process-wide heavy-work lease
	// only when this pass can actually advance and reclaim the mutable V1 tail.
	if !plan.CanPrune {
		return ApplyChainFreezerTailPruneFromDB(l.db, l.freezer, l.cold, l.cfg.HeadBlock(), l.cfg.RetainBlocks)
	}
	release, ok := l.cfg.HeavyWorkGate.TryAcquire()
	if !ok {
		return &ChainFreezerTailPruneApplyResult{
			Plan:             plan,
			ResourceDeferred: true,
			OldTail:          currentTail,
			NewTail:          currentTail,
		}, nil
	}
	defer release()
	return ApplyChainFreezerTailPruneFromDB(l.db, l.freezer, l.cold, l.cfg.HeadBlock(), l.cfg.RetainBlocks)
}

func (l *ChainFreezerTailPruneLifecycle) loop() {
	defer close(l.done)
	result, err := l.OnePass()
	l.logPass("initial", result, err)
	ticker := time.NewTicker(l.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			result, err := l.OnePass()
			l.logPass("periodic", result, err)
		case <-l.wake:
			result, err := l.OnePass()
			l.logPass("requested", result, err)
		case <-l.quit:
			return
		}
	}
}

func (l *ChainFreezerTailPruneLifecycle) logPass(label string, result *ChainFreezerTailPruneApplyResult, err error) {
	if err != nil {
		coldSnapshotLog.Warn("Chain freezer tail prune pass failed", "kind", label, "err", err)
		return
	}
	if result == nil || !result.Applied {
		if result != nil && result.StageRepaired {
			coldSnapshotLog.Info("Chain freezer tail prune stage repaired",
				"kind", label,
				"tail", result.NewTail,
				"prunedThroughBlock", result.NewTail-1,
				"coverageTail", result.Plan.CoverageTail,
				"retentionTail", result.Plan.RetentionTail)
		}
		return
	}
	coldSnapshotLog.Info("Chain freezer tail prune pass completed",
		"kind", label,
		"oldTail", result.OldTail,
		"newTail", result.NewTail,
		"prunedThroughBlock", result.NewTail-1,
		"prunedTailFiles", result.PrunedTailFiles,
		"coverageTail", result.Plan.CoverageTail,
		"retentionTail", result.Plan.RetentionTail)
}
