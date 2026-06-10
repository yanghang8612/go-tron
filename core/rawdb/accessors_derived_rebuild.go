package rawdb

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

type RebuildTransactionDerivedIndexesResult struct {
	FromBlock               uint64
	ToBlock                 uint64
	BlocksScanned           uint64
	TransactionsIndexed     uint64
	BlocksWithTxInfo        uint64
	TransactionInfosIndexed uint64
	ETL                     etl.Stats
}

// RebuildTransactionDerivedIndexesFromBlocks rebuilds transaction lookup/info
// rows from retained canonical blocks plus existing per-block TransactionRet
// rows. It is intended for offline repair/backfill paths, not the per-block
// consensus execution hot path.
func RebuildTransactionDerivedIndexesFromBlocks(chain *ChainDB, writer ethdb.KeyValueWriter, fromBlock, toBlock uint64, opts etl.Options) (*RebuildTransactionDerivedIndexesResult, error) {
	if chain == nil {
		return nil, errors.New("rawdb: nil chain db")
	}
	if writer == nil {
		return nil, errors.New("rawdb: nil derived index writer")
	}
	if toBlock < fromBlock {
		return nil, fmt.Errorf("rawdb: inverted transaction derived index rebuild range [%d,%d]", fromBlock, toBlock)
	}
	collector, err := NewDerivedIndexCollector(opts)
	if err != nil {
		return nil, err
	}
	defer collector.Close()

	result := &RebuildTransactionDerivedIndexesResult{
		FromBlock: fromBlock,
		ToBlock:   toBlock,
	}
	for blockNum := fromBlock; ; blockNum++ {
		block := ReadBlock(chain, blockNum)
		if block == nil {
			return nil, fmt.Errorf("rawdb: missing block %d during transaction derived index rebuild", blockNum)
		}
		result.BlocksScanned++
		for _, tx := range block.Transactions() {
			if tx == nil {
				continue
			}
			txHash := tx.Hash()
			if err := collector.PutTransactionIndex(txHash[:], blockNum); err != nil {
				return nil, err
			}
			result.TransactionsIndexed++
		}
		infos := ReadTransactionInfosByBlock(chain, blockNum)
		if len(infos) != 0 {
			if err := collector.PutTransactionInfosByBlock(blockNum, infos); err != nil {
				return nil, err
			}
			result.BlocksWithTxInfo++
			for _, info := range infos {
				if info == nil || len(info.Id) == 0 {
					continue
				}
				if err := collector.PutTransactionInfo(info.Id, info); err != nil {
					return nil, err
				}
				result.TransactionInfosIndexed++
			}
		}
		if blockNum == toBlock {
			break
		}
	}
	stats, err := collector.Load(writer)
	if err != nil {
		return nil, err
	}
	result.ETL = stats
	return result, nil
}
