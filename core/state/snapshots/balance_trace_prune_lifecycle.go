package snapshots

import (
	"os"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
)

const defaultBalanceTracePruneInterval = time.Minute

type BalanceTracePruneLifecycleConfig struct {
	Dir      string
	Interval time.Duration
}

// BalanceTracePruneLifecycle periodically prunes hot account/balance trace
// rows after verified local balance-trace snapshot coverage is visible.
type BalanceTracePruneLifecycle struct {
	db  ethdb.KeyValueStore
	cfg BalanceTracePruneLifecycleConfig

	quit chan struct{}
	done chan struct{}
	once sync.Once
}

func NewBalanceTracePruneLifecycle(db ethdb.KeyValueStore, cfg BalanceTracePruneLifecycleConfig) *BalanceTracePruneLifecycle {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultBalanceTracePruneInterval
	}
	return &BalanceTracePruneLifecycle{
		db:   db,
		cfg:  cfg,
		quit: make(chan struct{}),
		done: make(chan struct{}),
	}
}

func (l *BalanceTracePruneLifecycle) Start() error {
	if l == nil {
		return nil
	}
	if l.db == nil || l.cfg.Dir == "" {
		close(l.done)
		return nil
	}
	go l.loop()
	coldSnapshotLog.Info("Balance trace prune lifecycle started", "dir", l.cfg.Dir, "interval", l.cfg.Interval)
	return nil
}

func (l *BalanceTracePruneLifecycle) Stop() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() { close(l.quit) })
	<-l.done
	coldSnapshotLog.Info("Balance trace prune lifecycle stopped")
	return nil
}

func (l *BalanceTracePruneLifecycle) OnePass() (*PruneHotBalanceTraceResult, error) {
	if l == nil || l.db == nil || l.cfg.Dir == "" {
		return nil, nil
	}
	manifest, err := LoadProductionManifest(l.cfg.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return PruneHotBalanceTracesWithProgress(l.db, l.cfg.Dir, manifest)
}

func (l *BalanceTracePruneLifecycle) loop() {
	defer close(l.done)
	if result, err := l.OnePass(); err != nil {
		coldSnapshotLog.Warn("Balance trace prune initial pass failed", "err", err)
	} else if result != nil && result.HasRange {
		coldSnapshotLog.Info("Balance trace prune initial pass completed",
			"fromBlock", result.FromBlock,
			"toBlock", result.ToBlock,
			"coldTraceSegments", result.ColdTraceSegments,
			"blockTraces", result.BlockTracesDeleted,
			"accountTraces", result.AccountTracesDeleted)
	}

	ticker := time.NewTicker(l.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			result, err := l.OnePass()
			if err != nil {
				coldSnapshotLog.Warn("Balance trace prune pass failed", "err", err)
			} else if result != nil && result.HasRange {
				coldSnapshotLog.Info("Balance trace prune pass completed",
					"fromBlock", result.FromBlock,
					"toBlock", result.ToBlock,
					"coldTraceSegments", result.ColdTraceSegments,
					"blockTraces", result.BlockTracesDeleted,
					"accountTraces", result.AccountTracesDeleted)
			}
		case <-l.quit:
			return
		}
	}
}
