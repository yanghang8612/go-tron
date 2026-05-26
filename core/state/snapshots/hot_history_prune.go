package snapshots

import (
	"errors"
	"fmt"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

type HotHistoryPruneDecision struct {
	DeleteTxRange      bool
	DeleteHistoryBlock bool
}

type HotHistoryPruneOptions struct {
	MaxBlocks int
	Decide    func(*rawdb.StateTxRange) (HotHistoryPruneDecision, error)
}

type HotHistoryPruneStats struct {
	DeletedTxRanges          int
	DeletedHistoryBlocks     int
	MaxDeletedHistoryBlockTx uint64
}

// PruneHotHistory owns the hot history metadata lifecycle for a registered
// history domain: tx-range discovery, hot block deletion, and tx-range deletion.
func (cfg DomainCfg) PruneHotHistory(db rawdb.StateKVLatestStore, opts HotHistoryPruneOptions) (HotHistoryPruneStats, error) {
	if db == nil {
		return HotHistoryPruneStats{}, errors.New("snapshots: nil hot history database")
	}
	if !cfg.HasHistory {
		return HotHistoryPruneStats{}, fmt.Errorf("snapshots: %s is not a history domain", cfg.Dataset)
	}
	if cfg.IterateHotHistoryTxRanges == nil {
		return HotHistoryPruneStats{}, fmt.Errorf("snapshots: %s missing hot history tx-range iterator", cfg.Dataset)
	}
	if cfg.DeleteHotHistoryBlock == nil {
		return HotHistoryPruneStats{}, fmt.Errorf("snapshots: %s missing hot history block deleter", cfg.Dataset)
	}
	if cfg.DeleteHotHistoryTxRange == nil {
		return HotHistoryPruneStats{}, fmt.Errorf("snapshots: %s missing hot history tx-range deleter", cfg.Dataset)
	}
	if opts.Decide == nil {
		return HotHistoryPruneStats{}, errors.New("snapshots: missing hot history prune decision callback")
	}

	var blocks []hotHistoryPruneBlock
	if err := cfg.IterateHotHistoryTxRanges(db, func(row *rawdb.StateTxRange) (bool, error) {
		if row == nil {
			return false, errors.New("snapshots: nil hot history tx range")
		}
		if row.EndTxNum < row.BeginTxNum {
			return false, fmt.Errorf("snapshots: hot history tx range for block %d is inverted", row.BlockNum)
		}
		rowCopy := *row
		decision, err := opts.Decide(&rowCopy)
		if err != nil {
			return false, err
		}
		if !decision.DeleteHistoryBlock && !decision.DeleteTxRange {
			return true, nil
		}
		blocks = append(blocks, hotHistoryPruneBlock{
			blockNum:           row.BlockNum,
			endTxNum:           row.EndTxNum,
			deleteHistoryBlock: decision.DeleteHistoryBlock,
			deleteTxRange:      decision.DeleteTxRange,
		})
		if opts.MaxBlocks > 0 && len(blocks) >= opts.MaxBlocks {
			return false, nil
		}
		return true, nil
	}); err != nil {
		return HotHistoryPruneStats{}, err
	}

	var stats HotHistoryPruneStats
	for _, block := range blocks {
		if block.deleteHistoryBlock {
			if err := cfg.DeleteHotHistoryBlock(db, block.blockNum); err != nil {
				return HotHistoryPruneStats{}, err
			}
			stats.DeletedHistoryBlocks++
			stats.MaxDeletedHistoryBlockTx = max(stats.MaxDeletedHistoryBlockTx, block.endTxNum)
		}
		if block.deleteTxRange {
			if err := cfg.DeleteHotHistoryTxRange(db, block.blockNum); err != nil {
				return HotHistoryPruneStats{}, err
			}
			stats.DeletedTxRanges++
		}
	}
	return stats, nil
}

type hotHistoryPruneBlock struct {
	blockNum           uint64
	endTxNum           uint64
	deleteHistoryBlock bool
	deleteTxRange      bool
}
