package snapshots

import (
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

const defaultChainFreezerTailPruneInterval = time.Minute

type ChainFreezerTailPruneLifecycleConfig struct {
	RetainBlocks uint64
	Interval     time.Duration
	HeadBlock    func() uint64
}

type ChainFreezerTailPruneLifecycle struct {
	db      ethdb.KeyValueReader
	freezer ChainFreezerTailPruner
	cold    rawdb.AncientReader
	cfg     ChainFreezerTailPruneLifecycleConfig

	quit chan struct{}
	done chan struct{}
	once sync.Once
}

func NewChainFreezerTailPruneLifecycle(db ethdb.KeyValueReader, freezer ChainFreezerTailPruner, cold rawdb.AncientReader, cfg ChainFreezerTailPruneLifecycleConfig) *ChainFreezerTailPruneLifecycle {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultChainFreezerTailPruneInterval
	}
	return &ChainFreezerTailPruneLifecycle{
		db:      db,
		freezer: freezer,
		cold:    cold,
		cfg:     cfg,
		quit:    make(chan struct{}),
		done:    make(chan struct{}),
	}
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

func (l *ChainFreezerTailPruneLifecycle) OnePass() (*ChainFreezerTailPruneApplyResult, error) {
	if l == nil || l.db == nil || l.freezer == nil || l.cold == nil || l.cfg.RetainBlocks == 0 || l.cfg.HeadBlock == nil {
		return nil, nil
	}
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
