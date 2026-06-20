package snapshots

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func readHashBoundPruneResumeBlock(db ethdb.KeyValueReader, stage rawdb.StageID) (uint64, bool, error) {
	row, ok, err := rawdb.ReadStageProgressRow(db, stage)
	if err != nil || !ok {
		return 0, ok, err
	}
	if !row.HasBlockHash {
		return 0, false, nil
	}
	return row.BlockNum, true, nil
}
