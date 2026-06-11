package rawdb

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/types"
)

func WriteSyncStagedBlock(db ethdb.KeyValueWriter, block *types.Block) error {
	if db == nil {
		return errors.New("rawdb: nil sync staged block writer")
	}
	if block == nil {
		return errors.New("rawdb: nil sync staged block")
	}
	data, err := block.Marshal()
	if err != nil {
		return err
	}
	return db.Put(syncStagedBlockKey(block.Number()), data)
}

func ReadSyncStagedBlock(db ethdb.KeyValueReader, number uint64) (*types.Block, bool, error) {
	if db == nil {
		return nil, false, nil
	}
	data, err := db.Get(syncStagedBlockKey(number))
	if err != nil {
		return nil, false, nil
	}
	block, err := types.UnmarshalBlock(data)
	if err != nil {
		return nil, true, err
	}
	if block.Number() != number {
		return nil, true, fmt.Errorf("rawdb: sync staged block key %d contains block %d", number, block.Number())
	}
	return block, true, nil
}

func DeleteSyncStagedBlock(db ethdb.KeyValueWriter, number uint64) error {
	if db == nil {
		return nil
	}
	return db.Delete(syncStagedBlockKey(number))
}

func DeleteSyncStagedBlocksThrough(db ethdb.KeyValueStore, blockNum uint64) (int, error) {
	if db == nil {
		return 0, nil
	}
	it := db.NewIterator(syncStagedBlockPrefix, nil)
	var keys [][]byte
	for it.Next() {
		key := append([]byte{}, it.Key()...)
		if len(key) != len(syncStagedBlockPrefix)+8 {
			continue
		}
		num := binary.BigEndian.Uint64(key[len(syncStagedBlockPrefix):])
		if num > blockNum {
			break
		}
		keys = append(keys, key)
	}
	if err := it.Error(); err != nil {
		it.Release()
		return 0, err
	}
	it.Release()
	for _, key := range keys {
		if err := db.Delete(key); err != nil {
			return 0, err
		}
	}
	return len(keys), nil
}

func DeleteSyncStagedBlocksFrom(db ethdb.KeyValueStore, blockNum uint64) (int, error) {
	if db == nil {
		return 0, nil
	}
	var start [8]byte
	binary.BigEndian.PutUint64(start[:], blockNum)
	it := db.NewIterator(syncStagedBlockPrefix, start[:])
	var keys [][]byte
	for it.Next() {
		key := append([]byte{}, it.Key()...)
		if len(key) != len(syncStagedBlockPrefix)+8 {
			continue
		}
		keys = append(keys, key)
	}
	if err := it.Error(); err != nil {
		it.Release()
		return 0, err
	}
	it.Release()
	for _, key := range keys {
		if err := db.Delete(key); err != nil {
			return 0, err
		}
	}
	return len(keys), nil
}

func DeleteAllSyncStagedBlocks(db ethdb.KeyValueStore) (int, error) {
	if db == nil {
		return 0, nil
	}
	it := db.NewIterator(syncStagedBlockPrefix, nil)
	var keys [][]byte
	for it.Next() {
		keys = append(keys, append([]byte{}, it.Key()...))
	}
	if err := it.Error(); err != nil {
		it.Release()
		return 0, err
	}
	it.Release()
	for _, key := range keys {
		if err := db.Delete(key); err != nil {
			return 0, err
		}
	}
	return len(keys), nil
}
