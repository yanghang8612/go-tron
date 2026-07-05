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

// ChainLookupPruneLifecycleDB is the database boundary required by the
// standalone chain lookup pruner: hot KV writes plus readable ancient coverage.
type ChainLookupPruneLifecycleDB interface {
	ethdb.KeyValueStore
	AncientCount(kind string) (uint64, error)
}

// ChainLookupPruneLifecycle periodically prunes hot hash-keyed block/tx lookup
// rows after verified readable chain-freezer + chain-index coverage is visible.
type ChainLookupPruneLifecycle struct {
	db  ChainLookupPruneLifecycleDB
	cfg ChainLookupPruneLifecycleConfig

	quit chan struct{}
	done chan struct{}
	once sync.Once
}

func NewChainLookupPruneLifecycle(db ChainLookupPruneLifecycleDB, cfg ChainLookupPruneLifecycleConfig) *ChainLookupPruneLifecycle {
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

func shouldLogChainLookupPruneResult(result *PruneHotChainLookupResult) bool {
	return result != nil && (result.HasRange || result.MissingIndexSegments > 0)
}

func (l *ChainLookupPruneLifecycle) loop() {
	defer close(l.done)
	if result, err := l.OnePass(); err != nil {
		coldSnapshotLog.Warn("Chain lookup prune initial pass failed", "err", err)
	} else if shouldLogChainLookupPruneResult(result) {
		coldSnapshotLog.Info("Chain lookup prune initial pass completed",
			"fromBlock", result.FromBlock,
			"toBlock", result.ToBlock,
			"coldIndexSegments", result.ColdIndexSegments,
			"missingIndexSegments", result.MissingIndexSegments,
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
			} else if shouldLogChainLookupPruneResult(result) {
				coldSnapshotLog.Info("Chain lookup prune pass completed",
					"fromBlock", result.FromBlock,
					"toBlock", result.ToBlock,
					"coldIndexSegments", result.ColdIndexSegments,
					"missingIndexSegments", result.MissingIndexSegments,
					"blockIndexes", result.BlockIndexesDeleted,
					"txIndexes", result.TxIndexesDeleted)
			}
		case <-l.quit:
			return
		}
	}
}
