package rawdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

type SyncStagedBlockRow struct {
	Number uint64
	Hash   common.Hash
	Raw    []byte
}

func WriteSyncStagedBlock(db ethdb.KeyValueWriter, block *types.Block) error {
	return WriteSyncStagedBlockRaw(db, block, nil)
}

// WriteSyncStagedBlockRaw stores a sync-staged block body. When raw is
// supplied it is the exact block payload received from the wire; preserving it
// lets sync restore the downloader body stage without a decode/remarshal cycle.
func WriteSyncStagedBlockRaw(db ethdb.KeyValueWriter, block *types.Block, raw []byte) error {
	if db == nil {
		return errors.New("rawdb: nil sync staged block writer")
	}
	if block == nil {
		return errors.New("rawdb: nil sync staged block")
	}
	data := raw
	if len(data) == 0 {
		var err error
		data, err = block.Marshal()
		if err != nil {
			return err
		}
	}
	return db.Put(syncStagedBlockKey(block.Number()), append([]byte(nil), data...))
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

func ReadSyncStagedBlockRaw(db ethdb.KeyValueReader, number uint64) (SyncStagedBlockRow, bool, error) {
	if db == nil {
		return SyncStagedBlockRow{}, false, nil
	}
	data, err := db.Get(syncStagedBlockKey(number))
	if err != nil {
		return SyncStagedBlockRow{}, false, nil
	}
	return decodeSyncStagedBlockRow(number, data)
}

func IterateSyncStagedBlocksFrom(db ethdb.Iteratee, start uint64, fn func(SyncStagedBlockRow) (bool, error)) error {
	if db == nil || fn == nil {
		return nil
	}
	var seek [8]byte
	binary.BigEndian.PutUint64(seek[:], start)
	it := db.NewIterator(syncStagedBlockPrefix, seek[:])
	defer it.Release()
	for it.Next() {
		number, ok := parseSyncStagedBlockKey(it.Key())
		if !ok {
			continue
		}
		row, _, err := decodeSyncStagedBlockRow(number, it.Value())
		if err != nil {
			return err
		}
		cont, err := fn(row)
		if err != nil || !cont {
			return err
		}
	}
	return it.Error()
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

func decodeSyncStagedBlockRow(number uint64, data []byte) (SyncStagedBlockRow, bool, error) {
	block, err := types.UnmarshalBlock(data)
	if err != nil {
		return SyncStagedBlockRow{}, true, err
	}
	if block.Number() != number {
		return SyncStagedBlockRow{}, true, fmt.Errorf("rawdb: sync staged block key %d contains block %d", number, block.Number())
	}
	return SyncStagedBlockRow{
		Number: number,
		Hash:   block.Hash(),
		Raw:    append([]byte(nil), data...),
	}, true, nil
}

func parseSyncStagedBlockKey(key []byte) (uint64, bool) {
	if len(key) != len(syncStagedBlockPrefix)+8 || !bytes.HasPrefix(key, syncStagedBlockPrefix) {
		return 0, false
	}
	return binary.BigEndian.Uint64(key[len(syncStagedBlockPrefix):]), true
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
