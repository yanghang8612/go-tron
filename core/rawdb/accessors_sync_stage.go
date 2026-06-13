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

type SyncStagedTailPruneResult struct {
	Deleted          int
	HadProgress      bool
	PreviousProgress StageProgress
	DeletedProgress  bool
	RewoundProgress  bool
	RewindBlock      uint64
	RewindHash       common.Hash
}

type SyncStagedBlockWriteResult struct {
	Number              uint64
	Hash                common.Hash
	Staged              bool
	HadPreviousProgress bool
	PreviousProgress    StageProgress
	ProgressWritten     bool
	ProgressSkipped     bool
	StageError          error
	ProgressReadError   error
	ProgressWriteError  error
}

type SyncStagedResetResult struct {
	DeletedBodies            int
	StagedDeleteError        error
	BodiesProgressError      error
	BodiesReadyProgressError error
}

type SyncStagedBlockDelete struct {
	Number uint64
	Hash   common.Hash
}

type SyncStagedBlockDeleteError struct {
	Number uint64
	Hash   common.Hash
	Err    error
}

type SyncStagedBlockDeleteResult struct {
	Deleted int
	Errors  []SyncStagedBlockDeleteError
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
	data, err := encodeSyncStagedBlockRaw(block, raw)
	if err != nil {
		return err
	}
	return db.Put(syncStagedBlockKey(block.Number()), data)
}

func encodeSyncStagedBlockRaw(block *types.Block, raw []byte) ([]byte, error) {
	if block == nil {
		return nil, errors.New("rawdb: nil sync staged block")
	}
	data := raw
	if len(data) == 0 {
		var err error
		data, err = block.Marshal()
		if err != nil {
			return nil, err
		}
	}
	return append([]byte(nil), data...), nil
}

// WriteSyncStagedBlockRawAndProgress persists a downloader body row and
// advances the hash-bound SyncBodies watermark unless that watermark already
// points to a higher block. It intentionally does not refresh SyncBodiesReady:
// the ready frontier depends on the caller's current canonical head and target.
func WriteSyncStagedBlockRawAndProgress(db ethdb.KeyValueStore, block *types.Block, raw []byte) SyncStagedBlockWriteResult {
	var result SyncStagedBlockWriteResult
	if block != nil {
		result.Number = block.Number()
		result.Hash = block.Hash()
	}
	if db == nil {
		result.StageError = errors.New("rawdb: nil sync staged block writer")
		return result
	}
	data, err := encodeSyncStagedBlockRaw(block, raw)
	if err != nil {
		result.StageError = err
		return result
	}
	row, ok, err := ReadStageProgressRow(db, StageSyncBodies)
	if err != nil {
		if err := db.Put(syncStagedBlockKey(result.Number), data); err != nil {
			result.StageError = err
			return result
		}
		result.Staged = true
		result.ProgressReadError = err
		return result
	}
	if ok {
		result.HadPreviousProgress = true
		result.PreviousProgress = row
		if row.BlockNum > result.Number {
			if err := db.Put(syncStagedBlockKey(result.Number), data); err != nil {
				result.StageError = err
				return result
			}
			result.Staged = true
			result.ProgressSkipped = true
			return result
		}
	}
	batch := db.NewBatchWithSize(len(data) + 8 + common.HashLength)
	defer batch.Reset()
	if err := batch.Put(syncStagedBlockKey(result.Number), data); err != nil {
		result.StageError = err
		return result
	}
	if err := batch.Put(stageProgressKey(StageSyncBodies), encodeStageProgress(result.Number, result.Hash, true)); err != nil {
		result.ProgressWriteError = err
		return result
	}
	if err := batch.Write(); err != nil {
		result.StageError = err
		result.ProgressWriteError = err
		return result
	}
	result.Staged = true
	result.ProgressWritten = true
	return result
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

func DeleteSyncStagedBlockBatch(db ethdb.KeyValueWriter, blocks []SyncStagedBlockDelete) SyncStagedBlockDeleteResult {
	var result SyncStagedBlockDeleteResult
	if db == nil || len(blocks) == 0 {
		return result
	}
	for _, block := range blocks {
		if err := db.Delete(syncStagedBlockKey(block.Number)); err != nil {
			result.Errors = append(result.Errors, SyncStagedBlockDeleteError{
				Number: block.Number,
				Hash:   block.Hash,
				Err:    err,
			})
			continue
		}
		result.Deleted++
	}
	return result
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

// PruneSyncStagedBlocksFrom removes a stale downloader body tail and keeps the
// SyncBodies watermark consistent with the newest contiguous staged body kept
// by the caller. SyncBodiesReady is deliberately not recomputed here: it
// depends on the current canonical head and target head, so the sync service
// refreshes it after this storage-level prune.
func PruneSyncStagedBlocksFrom(db ethdb.KeyValueStore, blockNum uint64, lastRestoredNum uint64, lastRestoredHash common.Hash, haveLastRestored bool) (SyncStagedTailPruneResult, error) {
	var result SyncStagedTailPruneResult
	deleted, err := DeleteSyncStagedBlocksFrom(db, blockNum)
	if err != nil {
		return result, err
	}
	result.Deleted = deleted
	row, ok, err := ReadStageProgressRow(db, StageSyncBodies)
	if err != nil {
		return result, err
	}
	if !ok || row.BlockNum < blockNum {
		return result, nil
	}
	result.HadProgress = true
	result.PreviousProgress = row
	if !haveLastRestored {
		if err := DeleteStageProgress(db, StageSyncBodies); err != nil {
			return result, err
		}
		result.DeletedProgress = true
		return result, nil
	}
	if err := WriteStageProgressWithHash(db, StageSyncBodies, lastRestoredNum, lastRestoredHash); err != nil {
		return result, err
	}
	result.RewoundProgress = true
	result.RewindBlock = lastRestoredNum
	result.RewindHash = lastRestoredHash
	return result, nil
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

// ResetSyncStagedBodies clears the downloader body staging table and its body
// progress rows. It attempts every cleanup step and reports per-step errors so
// callers can log without leaving later cleanup undone.
func ResetSyncStagedBodies(db ethdb.KeyValueStore) SyncStagedResetResult {
	var result SyncStagedResetResult
	deleted, err := DeleteAllSyncStagedBlocks(db)
	result.DeletedBodies = deleted
	result.StagedDeleteError = err
	result.BodiesProgressError = DeleteStageProgress(db, StageSyncBodies)
	result.BodiesReadyProgressError = DeleteStageProgress(db, StageSyncBodiesReady)
	return result
}
