package snapshots

import (
	"os"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
)

const defaultChainLookupPruneInterval = time.Minute

type ChainLookupPruneLifecycleConfig struct {
	Dir      string
	Interval time.Duration
}

// ChainLookupPruneLifecycle periodically prunes hot hash-keyed block/tx lookup
// rows after verified local chain-freezer + chain-index coverage is visible.
type ChainLookupPruneLifecycle struct {
	db  ethdb.KeyValueStore
	cfg ChainLookupPruneLifecycleConfig

	quit chan struct{}
	done chan struct{}
	once sync.Once
}

func NewChainLookupPruneLifecycle(db ethdb.KeyValueStore, cfg ChainLookupPruneLifecycleConfig) *ChainLookupPruneLifecycle {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultChainLookupPruneInterval
	}
	return &ChainLookupPruneLifecycle{
		db:   db,
		cfg:  cfg,
		quit: make(chan struct{}),
		done: make(chan struct{}),
	}
}

func (l *ChainLookupPruneLifecycle) Start() error {
	if l == nil {
		return nil
	}
	if l.db == nil || l.cfg.Dir == "" {
		close(l.done)
		return nil
	}
	go l.loop()
	coldSnapshotLog.Info("Chain lookup prune lifecycle started", "dir", l.cfg.Dir, "interval", l.cfg.Interval)
	return nil
}

func (l *ChainLookupPruneLifecycle) Stop() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() { close(l.quit) })
	<-l.done
	coldSnapshotLog.Info("Chain lookup prune lifecycle stopped")
	return nil
}

func (l *ChainLookupPruneLifecycle) OnePass() (*PruneHotChainLookupResult, error) {
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
	return PruneHotChainLookupsWithProgress(l.db, l.cfg.Dir, manifest)
}

func (l *ChainLookupPruneLifecycle) loop() {
	defer close(l.done)
	if result, err := l.OnePass(); err != nil {
		coldSnapshotLog.Warn("Chain lookup prune initial pass failed", "err", err)
	} else if result != nil && result.HasRange {
		coldSnapshotLog.Info("Chain lookup prune initial pass completed",
			"fromBlock", result.FromBlock,
			"toBlock", result.ToBlock,
			"coldIndexSegments", result.ColdIndexSegments,
			"blockIndexes", result.BlockIndexesDeleted,
			"txIndexes", result.TxIndexesDeleted)
	}

	ticker := time.NewTicker(l.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			result, err := l.OnePass()
			if err != nil {
				coldSnapshotLog.Warn("Chain lookup prune pass failed", "err", err)
			} else if result != nil && result.HasRange {
				coldSnapshotLog.Info("Chain lookup prune pass completed",
					"fromBlock", result.FromBlock,
					"toBlock", result.ToBlock,
					"coldIndexSegments", result.ColdIndexSegments,
					"blockIndexes", result.BlockIndexesDeleted,
					"txIndexes", result.TxIndexesDeleted)
			}
		case <-l.quit:
			return
		}
	}
}
