package snapshots

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type PruneHotChainLookupResult struct {
	HasRange            bool
	FromBlock           uint64
	ToBlock             uint64
	BlockIndexesDeleted uint64
	StateRootsDeleted   uint64
	TxIndexesDeleted    uint64
	TxInfosDeleted      uint64
	ColdIndexSegments   uint64
	ToBlockHash         common.Hash
	HasToBlockHash      bool
}

type chainLookupPruneBounds struct {
	lastPrunedBlock    uint64
	hasLastPrunedBlock bool
	maxBlock           uint64
	hasMaxBlock        bool
}

// PruneHotChainLookups deletes hash-keyed historical lookup rows that are
// covered by verified chain-freezer + chain-index segment pairs in manifest.
// It does not delete b-<num>/tib-<num>; the freezer runner already owns those
// numbered rows via DeleteFrozenBlockRange.
func PruneHotChainLookups(db ethdb.KeyValueWriter, dir string, manifest *Manifest) (*PruneHotChainLookupResult, error) {
	return pruneHotChainLookups(db, dir, manifest, chainLookupPruneBounds{})
}

// PruneHotChainLookupsWithProgress deletes covered hot lookup rows after the
// persisted StageSnapshotChainLookupPrune boundary and advances that boundary
// when new freezer rows are processed. It also caps work at StageChainFreezer:
// hash-keyed lookup rows are removed only after the local ancient store is
// known to cover the matching num-keyed block/tx-info rows.
func PruneHotChainLookupsWithProgress(db ethdb.KeyValueStore, dir string, manifest *Manifest) (*PruneHotChainLookupResult, error) {
	if db == nil {
		return nil, errors.New("snapshots: nil chain lookup prune database")
	}
	localFreezerBlock, hasLocalFreezerBlock, err := rawdb.ReadStageProgress(db, rawdb.StageChainFreezer)
	if err != nil {
		return nil, err
	}
	if !hasLocalFreezerBlock {
		return new(PruneHotChainLookupResult), nil
	}
	lastPrunedBlock, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotChainLookupPrune)
	if err != nil {
		return nil, err
	}
	result, err := pruneHotChainLookups(db, dir, manifest, chainLookupPruneBounds{
		lastPrunedBlock:    lastPrunedBlock,
		hasLastPrunedBlock: ok,
		maxBlock:           localFreezerBlock,
		hasMaxBlock:        true,
	})
	if err != nil {
		return nil, err
	}
	if result.HasRange {
		if err := writeSnapshotChainLookupPruneStage(db, result.ToBlock, result.ToBlockHash, result.HasToBlockHash); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func pruneHotChainLookups(db ethdb.KeyValueWriter, dir string, manifest *Manifest, bounds chainLookupPruneBounds) (*PruneHotChainLookupResult, error) {
	if db == nil {
		return nil, errors.New("snapshots: nil chain lookup prune writer")
	}
	if manifest == nil {
		return nil, errors.New("snapshots: nil manifest")
	}
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	result := new(PruneHotChainLookupResult)
	for _, freezerRef := range manifest.Segments {
		if freezerRef.Kind != SegmentChainFreezer {
			continue
		}
		if bounds.hasLastPrunedBlock && freezerRef.ToTxNum <= bounds.lastPrunedBlock {
			continue
		}
		if bounds.hasMaxBlock && freezerRef.FromTxNum > bounds.maxBlock {
			continue
		}
		indexRef, ok := chainIndexRefForFreezer(manifest, freezerRef)
		if !ok {
			continue
		}
		if err := VerifyChainIndexSegmentAgainstChainFreezer(dir, indexRef, freezerRef); err != nil {
			return nil, err
		}
		pruned, err := pruneHotChainLookupsForFreezerSegment(db, dir, freezerRef, result, bounds)
		if err != nil {
			return nil, err
		}
		if pruned {
			result.ColdIndexSegments++
		}
	}
	return result, nil
}

func pruneHotChainLookupsForFreezerSegment(db ethdb.KeyValueWriter, dir string, ref SegmentRef, result *PruneHotChainLookupResult, bounds chainLookupPruneBounds) (bool, error) {
	var pruned bool
	err := iterateChainFreezerSegmentRows(dir, ref, func(row chainFreezerRow) error {
		if bounds.hasLastPrunedBlock && row.blockNum <= bounds.lastPrunedBlock {
			return nil
		}
		if bounds.hasMaxBlock && row.blockNum > bounds.maxBlock {
			return nil
		}
		verified, err := validateChainFreezerRowPayload(row, "chain lookup prune")
		if err != nil {
			return err
		}
		block := verified.block
		if !result.HasRange {
			result.HasRange = true
			result.FromBlock = row.blockNum
		}
		if !result.HasToBlockHash || row.blockNum >= result.ToBlock {
			result.ToBlock = row.blockNum
			result.ToBlockHash = block.Hash()
			result.HasToBlockHash = true
		}
		blockHash := block.Hash()
		if err := rawdb.DeleteBlockNumber(db, blockHash); err != nil {
			return err
		}
		result.BlockIndexesDeleted++
		rawdb.DeleteBlockStateRoot(db, blockHash)
		result.StateRootsDeleted++
		for _, tx := range block.Transactions() {
			txHash := tx.Hash()
			if err := rawdb.DeleteTransactionIndex(db, txHash[:]); err != nil {
				return err
			}
			result.TxIndexesDeleted++
			if err := rawdb.DeleteTransactionInfo(db, txHash[:]); err != nil {
				return err
			}
			result.TxInfosDeleted++
		}
		pruned = true
		return nil
	})
	return pruned, err
}

func writeSnapshotChainLookupPruneStage(db ethdb.KeyValueStore, blockNum uint64, blockHash common.Hash, hasBlockHash bool) error {
	if !hasBlockHash {
		return fmt.Errorf("snapshots: missing hash for %s stage block %d", rawdb.StageSnapshotChainLookupPrune, blockNum)
	}
	current, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotChainLookupPrune)
	if err != nil {
		return err
	}
	if ok && current.BlockNum > blockNum {
		return nil
	}
	if ok && current.BlockNum == blockNum && current.HasBlockHash {
		if current.BlockHash != blockHash {
			return fmt.Errorf("snapshots: %s stage %d hash %x does not match pruned block hash %x", rawdb.StageSnapshotChainLookupPrune, blockNum, current.BlockHash, blockHash)
		}
		return nil
	}
	return rawdb.WriteStageProgressWithHash(db, rawdb.StageSnapshotChainLookupPrune, blockNum, blockHash)
}
