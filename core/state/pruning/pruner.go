package pruning

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

var log = gtronlog.NewModule("core/state/pruning")

const (
	defaultInterval = time.Minute
	defaultBatch    = 5_000
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
}

type PrunerStats struct {
	Passes                       uint64
	SkippedCatchup               uint64
	DeletedTxRanges              uint64
	DeletedDomainChangeBlocks    uint64
	DeletedCommitmentCheckpoints uint64
	DeletedStateCodeRows         uint64
	LastSolidifiedBlock          uint64
	LastPassDuration             time.Duration
}

type Pruner struct {
	chain ChainSource
	cfg   PrunerConfig

	quit chan struct{}
	done chan struct{}
	once sync.Once

	passes                       atomic.Uint64
	deletedTxRanges              atomic.Uint64
	deletedDomainChangeBlocks    atomic.Uint64
	deletedCommitmentCheckpoints atomic.Uint64
	deletedStateCodeRows         atomic.Uint64
	skippedCatchup               atomic.Uint64
	lastSolidifiedBlock          atomic.Uint64
	lastPassDuration             atomic.Int64
}

func NewPruner(chain ChainSource, cfg PrunerConfig) *Pruner {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultInterval
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatch
	}
	return &Pruner{
		chain: chain,
		cfg:   cfg,
		quit:  make(chan struct{}),
		done:  make(chan struct{}),
	}
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
		DeletedTxRanges:              p.deletedTxRanges.Load(),
		DeletedDomainChangeBlocks:    p.deletedDomainChangeBlocks.Load(),
		DeletedCommitmentCheckpoints: p.deletedCommitmentCheckpoints.Load(),
		DeletedStateCodeRows:         p.deletedStateCodeRows.Load(),
		SkippedCatchup:               p.skippedCatchup.Load(),
		LastSolidifiedBlock:          p.lastSolidifiedBlock.Load(),
		LastPassDuration:             time.Duration(p.lastPassDuration.Load()),
	}
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

func (p *Pruner) PrunePass() (Stats, error) {
	start := time.Now()
	solidified := p.chain.LatestSolidifiedBlockNum()
	if solidified < 0 {
		solidified = 0
	}
	if p.shouldSkipForCatchup() {
		p.skippedCatchup.Add(1)
		p.lastSolidifiedBlock.Store(uint64(solidified))
		p.lastPassDuration.Store(time.Since(start).Nanoseconds())
		return Stats{}, nil
	}
	pruneHead := uint64(solidified)
	var err error
	pruneHead, err = p.capPruneHeadAtVerifiedFinishStage(pruneHead)
	if err != nil {
		return Stats{}, err
	}
	stats, err := Worker{
		DB:          p.chain.DB(),
		Policy:      p.cfg.Policy,
		MaxBlocks:   p.cfg.BatchSize,
		SnapshotDir: p.cfg.SnapshotDir,
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
	p.lastPassDuration.Store(time.Since(start).Nanoseconds())
	return stats, nil
}

func (p *Pruner) capPruneHeadAtVerifiedFinishStage(pruneHead uint64) (uint64, error) {
	row, ok, err := newRawDBStageProgressReader(p.chain.DB()).Read(rawdb.StageFinish)
	if err != nil || !ok {
		return pruneHead, err
	}
	if row.HasBlockHash {
		hash, ok := p.canonicalBlockHash(row.BlockNum)
		if !ok {
			return 0, fmt.Errorf("pruning: finish stage %d has hash %x but canonical block is unavailable", row.BlockNum, row.BlockHash)
		}
		if hash != row.BlockHash {
			return 0, fmt.Errorf("pruning: finish stage %d hash %x does not match canonical hash %x", row.BlockNum, row.BlockHash, hash)
		}
	}
	if row.BlockNum < pruneHead {
		return row.BlockNum, nil
	}
	return pruneHead, nil
}

func (p *Pruner) canonicalBlockHash(blockNum uint64) (common.Hash, bool) {
	if source, ok := p.chain.(canonicalHashSource); ok {
		return source.CanonicalBlockHash(blockNum)
	}
	block := rawdb.ReadBlockKV(p.chain.DB(), blockNum)
	if block == nil {
		return common.Hash{}, false
	}
	return block.Hash(), true
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
