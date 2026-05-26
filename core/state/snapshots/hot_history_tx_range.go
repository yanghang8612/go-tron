package snapshots

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type HotHistoryTxRangeReader func(db ethdb.KeyValueReader, blockNum uint64) (*rawdb.StateTxRange, bool, error)

type HotHistoryTxRangeChangeIterator func(db ethdb.Iteratee, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error

func StateDomainHistoryTxRangeForBlock(db ethdb.KeyValueReader, blockNum uint64) (*rawdb.StateTxRange, bool, error) {
	cfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	if !ok {
		return nil, false, errors.New("snapshots: missing state-domain history config")
	}
	return cfg.HotHistoryTxRangeForBlock(db, blockNum)
}

func StateDomainHistoryTxNumAtBlockEnd(db ethdb.KeyValueReader, blockNum uint64) (uint64, error) {
	cfg, ok := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	if !ok {
		return 0, errors.New("snapshots: missing state-domain history config")
	}
	return cfg.HotHistoryTxNumAtBlockEnd(db, blockNum)
}

func (cfg DomainCfg) HotHistoryTxRangeForBlock(db ethdb.KeyValueReader, blockNum uint64) (*rawdb.StateTxRange, bool, error) {
	if db == nil {
		return nil, false, errors.New("snapshots: nil hot history database")
	}
	if !cfg.HasHistory {
		return nil, false, fmt.Errorf("snapshots: %s is not a history domain", cfg.Dataset)
	}
	if cfg.ReadHotHistoryTxRange == nil {
		return nil, false, fmt.Errorf("snapshots: %s missing hot history tx-range reader", cfg.Dataset)
	}
	row, ok, err := cfg.ReadHotHistoryTxRange(db, blockNum)
	if err != nil || !ok {
		return nil, ok, err
	}
	if row.EndTxNum < row.BeginTxNum {
		return nil, false, fmt.Errorf("snapshots: hot history tx range for block %d is inverted", row.BlockNum)
	}
	rowCopy := *row
	return &rowCopy, true, nil
}

func (cfg DomainCfg) HotHistoryTxNumAtBlockEnd(db ethdb.KeyValueReader, blockNum uint64) (uint64, error) {
	row, ok, err := cfg.HotHistoryTxRangeForBlock(db, blockNum)
	if err != nil {
		return 0, err
	}
	if !ok {
		return blockNum, nil
	}
	return row.EndTxNum, nil
}

func (cfg DomainCfg) IterateHotHistoryChangesByTxRange(db ethdb.Iteratee, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	if db == nil {
		return errors.New("snapshots: nil hot history database")
	}
	if !cfg.HasHistory {
		return fmt.Errorf("snapshots: %s is not a history domain", cfg.Dataset)
	}
	if cfg.IterateHotHistoryTxRangeChanges == nil {
		return fmt.Errorf("snapshots: %s missing hot history tx-range change iterator", cfg.Dataset)
	}
	return cfg.IterateHotHistoryTxRangeChanges(db, fromTxNum, toTxNum, fn)
}
