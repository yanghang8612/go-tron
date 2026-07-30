package snapshots

import (
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func snapshotStageBoundaryHash(db ethdb.KeyValueReader, stage rawdb.StageID, blockNum uint64) (common.Hash, bool, error) {
	hash, ok, err := rawdb.ReadBlockHashByNumberStrict(db, blockNum)
	if err != nil {
		return common.Hash{}, false, fmt.Errorf("snapshots: read %s stage block %d canonical hash: %w", stage, blockNum, err)
	}
	if ok && hash != (common.Hash{}) {
		return hash, true, nil
	}
	row, ok, err := rawdb.ReadStateTxRange(db, blockNum)
	if err != nil {
		return common.Hash{}, false, fmt.Errorf("snapshots: read %s stage block %d tx range hash: %w", stage, blockNum, err)
	}
	if ok && row.BlockHash != (common.Hash{}) {
		return row.BlockHash, true, nil
	}
	return common.Hash{}, false, nil
}

func requireSnapshotStageBoundaryHash(db ethdb.KeyValueReader, stage rawdb.StageID, blockNum uint64) (common.Hash, error) {
	hash, ok, err := snapshotStageBoundaryHash(db, stage, blockNum)
	if err != nil {
		return common.Hash{}, err
	}
	if !ok {
		return common.Hash{}, fmt.Errorf("snapshots: missing canonical hash for %s stage block %d", stage, blockNum)
	}
	return hash, nil
}
