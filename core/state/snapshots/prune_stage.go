package snapshots

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type hashBoundPruneResumeBoundary struct {
	block   uint64
	hash    common.Hash
	hasHash bool
}

func readHashBoundPruneResumeBlock(db ethdb.KeyValueReader, stage rawdb.StageID) (uint64, bool, error) {
	boundary, err := readHashBoundPruneResumeBoundary(db, stage)
	if err != nil || !boundary.hasHash {
		return 0, false, err
	}
	return boundary.block, true, nil
}

func readHashBoundPruneResumeBoundary(db ethdb.KeyValueReader, stage rawdb.StageID) (hashBoundPruneResumeBoundary, error) {
	row, ok, err := rawdb.ReadStageProgressRow(db, stage)
	if err != nil || !ok {
		return hashBoundPruneResumeBoundary{}, err
	}
	if !row.HasBlockHash {
		return hashBoundPruneResumeBoundary{}, nil
	}
	return hashBoundPruneResumeBoundary{
		block:   row.BlockNum,
		hash:    row.BlockHash,
		hasHash: true,
	}, nil
}
