package rawdb

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
)

const resetScanBatch = 10000

// ResetMutableState removes every replay-derived mutable state namespace while
// preserving immutable chain data (blocks and block-hash indexes). It is used
// by historical sync restart: after the reset, genesis side stores are seeded
// again and canonical blocks are replayed up to the requested height.
func ResetMutableState(db ethdb.KeyValueStore) error {
	if db == nil {
		return errors.New("reset mutable state: nil database")
	}
	for _, prefix := range resetMutablePrefixes {
		if err := deletePrefix(db, prefix); err != nil {
			return fmt.Errorf("delete prefix %q: %w", prefix, err)
		}
	}
	for _, key := range resetMutableSingletons {
		if err := db.Delete(key); err != nil {
			return fmt.Errorf("delete key %q: %w", key, err)
		}
	}
	return nil
}

var resetMutablePrefixes = [][]byte{
	blockStateRootPrefix,
	txPrefix,
	txInfoPrefix,
	txInfoBlockPrefix,
	accountPrefix,
	witnessPrefix,
	witnessLatestBlockPrefix,
	codePrefix,
	contractPrefix,
	storagePrefix,
	dynPropPrefix,
	delegationPrefix,
	delegationIndexPrefix,
	brokeragePrefix,
	nullifierPrefix,
	noteCommitmentPrefix,
	zkProofPrefix,
	incrMerkleTreePrefix,
	merkleTreeIndexPrefix,
	forkStatsPrefix,
	delegRewardPrefix,
	abiPrefix,
	contractStatePrefix,
	accountAssetPrefix,
	accountTracePrefix,
	stateKVLatestPrefix,
	stateCodePrefix,
	stateTxRangePrefix,
	stateChangeSetPrefix,
	stateChangeInversePrefix,
	stateCommitmentPrefix,
	stateKVGenerationPrefix,
	sectionBloomPrefix,
	treeBlockIndexPrefix,
	pbftSignDataPrefix,
	balanceTracePrefix,
	rewardViPrefix,
	taposPrefix,
	drAccIdxPrefix,
	checkPointV2Prefix,
	shMetaPrefix,
	shAccountPrefix,
	shSlotPrefix,
	shAddrInversePrefix,
	shSlotInversePrefix,
	shConfigKey,
	shBackfillCursorKey,
}

var resetMutableSingletons = [][]byte{
	headBlockKey,
	headSolidBlockKey,
	totalTransactionCountKey,
	genesisStateRootKey,
	witnessScheduleKey,
	shuffledWitnessesKey,
	previousShuffledWitnessesKey,
	genesisWitnessesKey,
	noteCommitmentCountKey,
	latestPbftBlockNumKey,
}

func deletePrefix(db ethdb.KeyValueStore, prefix []byte) error {
	if deleter, ok := db.(ethdb.KeyValueRangeDeleter); ok {
		if err := deleter.DeleteRange(prefix, prefixUpperBound(prefix)); err == nil {
			return nil
		} else if !errors.Is(err, ethdb.ErrTooManyKeys) {
			return err
		}
	}
	return deletePrefixByScan(db, prefix)
}

func deletePrefixByScan(db ethdb.KeyValueStore, prefix []byte) error {
	for {
		it := db.NewIterator(prefix, nil)
		keys := make([][]byte, 0, resetScanBatch)
		for it.Next() {
			keys = append(keys, append([]byte(nil), it.Key()...))
			if len(keys) >= resetScanBatch {
				break
			}
		}
		err := it.Error()
		it.Release()
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		batch := db.NewBatch()
		for _, key := range keys {
			if err := batch.Delete(key); err != nil {
				batch.Reset()
				return err
			}
		}
		if err := batch.Write(); err != nil {
			batch.Reset()
			return err
		}
		batch.Reset()
		if len(keys) < resetScanBatch {
			return nil
		}
	}
}

func prefixUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	limit := append([]byte(nil), prefix...)
	for i := len(limit) - 1; i >= 0; i-- {
		if limit[i] != 0xff {
			limit[i]++
			return limit[:i+1]
		}
	}
	return nil
}
