package pruning

import (
	"sync"
	"time"

	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

var lifecycleLog = gtronlog.NewModule("core/state/lifecycle")

// SnapshotLifecycleConfig wires the Erigon-style cold/hot lifecycle together:
// build and publish cold history files, compact old history files, then prune
// hot data covered by the visible snapshot view.
type SnapshotLifecycleConfig struct {
	Snapshot snapshots.Config
	Pruner   PrunerConfig
	Interval time.Duration
}

// SnapshotLifecyclePass is the result of one ordered lifecycle pass.
type SnapshotLifecyclePass struct {
	Snapshot snapshots.PassResult
	Prune    Stats
}

// SnapshotLifecycle owns the state snapshot builder/compactor and hot pruner
// under one node.Lifecycle, so their progress advances in one ordered pass
// instead of via independent background loops.
type SnapshotLifecycle struct {
	builder *snapshots.Runner
	pruner  *Pruner

	interval time.Duration
	quit     chan struct{}
	done     chan struct{}
	once     sync.Once
}

func NewSnapshotLifecycle(chain ChainSource, cfg SnapshotLifecycleConfig) *SnapshotLifecycle {
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
		builder:  builder,
		pruner:   NewPruner(chain, cfg.Pruner),
		interval: interval,
		quit:     make(chan struct{}),
		done:     make(chan struct{}),
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
	go l.loop()
	lifecycleLog.Info("Domain state snapshot/prune lifecycle started",
		"snapshotEnabled", l.builder != nil,
		"mode", l.pruner.cfg.Policy.Mode,
		"interval", l.interval,
		"snapshotDir", l.pruner.cfg.SnapshotDir)
	return nil
}

func (l *SnapshotLifecycle) Stop() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() { close(l.quit) })
	<-l.done
	lifecycleLog.Info("Domain state snapshot/prune lifecycle stopped")
	return nil
}

func (l *SnapshotLifecycle) OnePass() (SnapshotLifecyclePass, error) {
	if l == nil {
		return SnapshotLifecyclePass{}, nil
	}
	var out SnapshotLifecyclePass
	if l.builder != nil {
		result, err := l.builder.OnePass()
		if err != nil {
			return out, err
		}
		out.Snapshot = result
	}
	if l.pruner != nil {
		stats, err := l.pruner.PrunePass()
		if err != nil {
			return out, err
		}
		out.Prune = stats
	}
	return out, nil
}

func (l *SnapshotLifecycle) loop() {
	defer close(l.done)
	if _, err := l.OnePass(); err != nil {
		lifecycleLog.Warn("Domain state snapshot/prune initial pass failed", "err", err)
	}
	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if _, err := l.OnePass(); err != nil {
				lifecycleLog.Warn("Domain state snapshot/prune pass failed", "err", err)
			}
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

func (s snapshotChainSource) LatestSolidifiedBlockNum() int64 {
	if s.chain == nil {
		return 0
	}
	return s.chain.LatestSolidifiedBlockNum()
}

var _ snapshots.ChainSource = snapshotChainSource{}
