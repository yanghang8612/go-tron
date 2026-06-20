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

type SyncImportProgressWriteResult struct {
	Deleted       int
	DeleteErrors  []SyncStagedBlockDeleteError
	ProgressRows  int
	ProgressError error
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
	if err := validateSyncStagedBlockRawForProgress(block, raw); err != nil {
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
		if row.HasBlockHash && row.BlockNum > result.Number {
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

func validateSyncStagedBlockRawForProgress(block *types.Block, raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	wireBlock, err := types.UnmarshalBlock(raw)
	if err != nil {
		return fmt.Errorf("rawdb: sync staged raw for block %d decode: %w", block.Number(), err)
	}
	if wireBlock.Number() != block.Number() {
		return fmt.Errorf("rawdb: sync staged raw key %d contains block %d", block.Number(), wireBlock.Number())
	}
	wireHash := wireBlock.Hash()
	blockHash := block.Hash()
	if wireHash != blockHash {
		return fmt.Errorf("rawdb: sync staged raw block %d hash %x, want %x", block.Number(), wireHash, blockHash)
	}
	return nil
}

func ReadSyncStagedBlock(db ethdb.KeyValueReader, number uint64) (*types.Block, bool, error) {
	if db == nil {
		return nil, false, nil
	}
	key := syncStagedBlockKey(number)
	exists, err := db.Has(key)
	if err != nil {
		return nil, false, err
	}
	if !exists {
		return nil, false, nil
	}
	data, err := db.Get(key)
	if err != nil {
		return nil, false, err
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
	key := syncStagedBlockKey(number)
	exists, err := db.Has(key)
	if err != nil {
		return SyncStagedBlockRow{}, false, err
	}
	if !exists {
		return SyncStagedBlockRow{}, false, nil
	}
	data, err := db.Get(key)
	if err != nil {
		return SyncStagedBlockRow{}, false, err
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
	if batcher, ok := db.(ethdb.Batcher); ok {
		batch := batcher.NewBatchWithSize(len(blocks) * 8)
		defer batch.Reset()
		enqueued := make([]SyncStagedBlockDelete, 0, len(blocks))
		for _, block := range blocks {
			if err := batch.Delete(syncStagedBlockKey(block.Number)); err != nil {
				result.Errors = append(result.Errors, SyncStagedBlockDeleteError{
					Number: block.Number,
					Hash:   block.Hash,
					Err:    err,
				})
				continue
			}
			enqueued = append(enqueued, block)
		}
		if len(enqueued) == 0 {
			return result
		}
		if err := batch.Write(); err != nil {
			for _, block := range enqueued {
				result.Errors = append(result.Errors, SyncStagedBlockDeleteError{
					Number: block.Number,
					Hash:   block.Hash,
					Err:    err,
				})
			}
			return result
		}
		result.Deleted = len(enqueued)
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

// WriteSyncImportProgressBatch commits the storage side effects for an applied
// sync import prefix: delete imported staged body rows and persist hash-bound
// sync pipeline progress rows. Backends with batch support flush all writes in
// one batch.
func WriteSyncImportProgressBatch(db interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
}, deletes []SyncStagedBlockDelete, progress []StageProgress) SyncImportProgressWriteResult {
	var result SyncImportProgressWriteResult
	if db == nil || (len(deletes) == 0 && len(progress) == 0) {
		return result
	}
	if err := validateSyncImportProgressRows(progress); err != nil {
		result.ProgressError = err
		return result
	}
	if result.DeleteErrors = validateSyncImportDeleteRows(db, deletes); len(result.DeleteErrors) > 0 {
		return result
	}
	if err := validateSyncImportProgressAgainstDeletes(deletes, progress); err != nil {
		result.ProgressError = err
		return result
	}
	if batcher, ok := db.(ethdb.Batcher); ok {
		batch := batcher.NewBatchWithSize((len(deletes) + len(progress)) * 8)
		defer batch.Reset()
		enqueuedDeletes := make([]SyncStagedBlockDelete, 0, len(deletes))
		for _, block := range deletes {
			if err := batch.Delete(syncStagedBlockKey(block.Number)); err != nil {
				result.DeleteErrors = append(result.DeleteErrors, SyncStagedBlockDeleteError{
					Number: block.Number,
					Hash:   block.Hash,
					Err:    err,
				})
				continue
			}
			enqueuedDeletes = append(enqueuedDeletes, block)
		}
		for _, row := range progress {
			if err := batch.Put(stageProgressKey(row.Stage), encodeStageProgress(row.BlockNum, row.BlockHash, row.HasBlockHash)); err != nil {
				result.ProgressError = fmt.Errorf("rawdb: write stage progress %s at %d: %w", row.Stage, row.BlockNum, err)
				return result
			}
		}
		if len(enqueuedDeletes) == 0 && len(progress) == 0 {
			return result
		}
		if err := batch.Write(); err != nil {
			for _, block := range enqueuedDeletes {
				result.DeleteErrors = append(result.DeleteErrors, SyncStagedBlockDeleteError{
					Number: block.Number,
					Hash:   block.Hash,
					Err:    err,
				})
			}
			if len(progress) > 0 {
				result.ProgressError = fmt.Errorf("rawdb: write sync import progress batch: %w", err)
			}
			return result
		}
		result.Deleted = len(enqueuedDeletes)
		result.ProgressRows = len(progress)
		return result
	}
	if err := WriteStageProgressRows(db, progress); err != nil {
		result.ProgressError = err
		return result
	}
	result.ProgressRows = len(progress)
	deleteResult := DeleteSyncStagedBlockBatch(db, deletes)
	result.Deleted = deleteResult.Deleted
	result.DeleteErrors = deleteResult.Errors
	return result
}

func validateSyncImportDeleteRows(db ethdb.KeyValueReader, deletes []SyncStagedBlockDelete) []SyncStagedBlockDeleteError {
	if len(deletes) == 0 {
		return nil
	}
	errs := make([]SyncStagedBlockDeleteError, 0)
	for i, block := range deletes {
		if i > 0 && block.Number != deletes[i-1].Number+1 {
			errs = append(errs, SyncStagedBlockDeleteError{
				Number: block.Number,
				Hash:   block.Hash,
				Err:    fmt.Errorf("rawdb: sync staged delete row %d block %d does not continue from block %d", i, block.Number, deletes[i-1].Number),
			})
			continue
		}
		row, ok, err := ReadSyncStagedBlockRaw(db, block.Number)
		if err != nil {
			errs = append(errs, SyncStagedBlockDeleteError{
				Number: block.Number,
				Hash:   block.Hash,
				Err:    fmt.Errorf("rawdb: validate sync staged block delete %d: %w", block.Number, err),
			})
			continue
		}
		if !ok {
			errs = append(errs, SyncStagedBlockDeleteError{
				Number: block.Number,
				Hash:   block.Hash,
				Err:    fmt.Errorf("rawdb: sync staged block %d missing for imported delete", block.Number),
			})
			continue
		}
		if row.Hash != block.Hash {
			errs = append(errs, SyncStagedBlockDeleteError{
				Number: block.Number,
				Hash:   block.Hash,
				Err:    fmt.Errorf("rawdb: sync staged block %d hash %x, want %x", block.Number, row.Hash, block.Hash),
			})
		}
	}
	return errs
}

func validateSyncImportProgressAgainstDeletes(deletes []SyncStagedBlockDelete, rows []StageProgress) error {
	if len(deletes) == 0 || len(rows) == 0 {
		return nil
	}
	first := deletes[0]
	last := deletes[len(deletes)-1]
	for i, row := range rows {
		if row.BlockNum < first.Number || row.BlockNum > last.Number {
			return fmt.Errorf("rawdb: sync import progress %s at row %d block %d is outside staged delete prefix [%d,%d]", row.Stage, i, row.BlockNum, first.Number, last.Number)
		}
		deleteRow := deletes[row.BlockNum-first.Number]
		if deleteRow.Number != row.BlockNum {
			return fmt.Errorf("rawdb: sync import progress %s at row %d block %d is not covered by staged delete prefix", row.Stage, i, row.BlockNum)
		}
		if deleteRow.Hash != row.BlockHash {
			return fmt.Errorf("rawdb: sync import progress %s at row %d block %d hash %x, want staged delete hash %x", row.Stage, i, row.BlockNum, row.BlockHash, deleteRow.Hash)
		}
	}
	return nil
}

func validateSyncImportProgressRows(rows []StageProgress) error {
	for i, row := range rows {
		if row.Stage == "" {
			return fmt.Errorf("rawdb: empty stage id at row %d", i)
		}
		if !isSyncImportProgressStage(row.Stage) {
			return fmt.Errorf("rawdb: unexpected sync import progress stage %s at row %d", row.Stage, i)
		}
		if i >= len(syncImportProgressStageOrder) {
			return fmt.Errorf("rawdb: sync import progress row %d stage %s exceeds pipeline length", i, row.Stage)
		}
		if want := syncImportProgressStageOrder[i]; row.Stage != want {
			return fmt.Errorf("rawdb: sync import progress row %d stage %s, want %s", i, row.Stage, want)
		}
		if !row.HasBlockHash {
			return fmt.Errorf("rawdb: sync import progress %s at row %d block %d is not hash-bound", row.Stage, i, row.BlockNum)
		}
		if i > 0 {
			prev := rows[i-1]
			if row.BlockNum > prev.BlockNum {
				return fmt.Errorf("rawdb: sync import progress %s at row %d block %d is ahead of upstream %s block %d", row.Stage, i, row.BlockNum, prev.Stage, prev.BlockNum)
			}
			if row.BlockNum == prev.BlockNum && row.BlockHash != prev.BlockHash {
				return fmt.Errorf("rawdb: sync import progress %s at row %d block %d hash %x, want upstream hash %x", row.Stage, i, row.BlockNum, row.BlockHash, prev.BlockHash)
			}
		}
	}
	return nil
}

var syncImportProgressStageOrder = []StageID{
	StageSyncImport,
	StageSyncExecution,
	StageSyncCommitment,
	StageSyncFinish,
}

func isSyncImportProgressStage(stage StageID) bool {
	switch stage {
	case StageSyncImport, StageSyncExecution, StageSyncCommitment, StageSyncFinish:
		return true
	default:
		return false
	}
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
	if err := deleteKeyBatch(db, keys); err != nil {
		return 0, err
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
	keys, err := collectSyncStagedBlockKeysFrom(db, blockNum)
	if err != nil {
		return 0, err
	}
	if err := deleteKeyBatch(db, keys); err != nil {
		return 0, err
	}
	return len(keys), nil
}

func collectSyncStagedBlockKeysFrom(db ethdb.Iteratee, blockNum uint64) ([][]byte, error) {
	if db == nil {
		return nil, nil
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
		return nil, err
	}
	it.Release()
	return keys, nil
}

// PruneSyncStagedBlocksFrom removes a stale downloader body tail and keeps the
// SyncBodies watermark consistent with the newest contiguous staged body kept
// by the caller. SyncBodiesReady is deliberately not recomputed here: it
// depends on the current canonical head and target head, so the sync service
// refreshes it after this storage-level prune.
func PruneSyncStagedBlocksFrom(db ethdb.KeyValueStore, blockNum uint64, lastRestoredNum uint64, lastRestoredHash common.Hash, haveLastRestored bool) (SyncStagedTailPruneResult, error) {
	var result SyncStagedTailPruneResult
	if db == nil {
		return result, nil
	}
	keys, err := collectSyncStagedBlockKeysFrom(db, blockNum)
	if err != nil {
		return result, err
	}
	row, ok, err := ReadStageProgressRow(db, StageSyncBodies)
	if err != nil {
		return result, err
	}
	var (
		deleteProgress bool
		rewindProgress bool
	)
	if ok && row.BlockNum >= blockNum {
		result.HadProgress = true
		result.PreviousProgress = row
		if haveLastRestored {
			rewindProgress = true
		} else {
			deleteProgress = true
		}
	}
	if len(keys) == 0 && !deleteProgress && !rewindProgress {
		return result, nil
	}
	batch := db.NewBatchWithSize(len(keys)*8 + 8 + common.HashLength)
	defer batch.Reset()
	for _, key := range keys {
		if err := batch.Delete(key); err != nil {
			return result, err
		}
	}
	if deleteProgress {
		if err := batch.Delete(stageProgressKey(StageSyncBodies)); err != nil {
			return result, err
		}
	}
	if rewindProgress {
		if err := batch.Put(stageProgressKey(StageSyncBodies), encodeStageProgress(lastRestoredNum, lastRestoredHash, true)); err != nil {
			return result, err
		}
	}
	if err := batch.Write(); err != nil {
		return result, err
	}
	result.Deleted = len(keys)
	if deleteProgress {
		result.DeletedProgress = true
	}
	if rewindProgress {
		result.RewoundProgress = true
		result.RewindBlock = lastRestoredNum
		result.RewindHash = lastRestoredHash
	}
	return result, nil
}

func DeleteAllSyncStagedBlocks(db ethdb.KeyValueStore) (int, error) {
	if db == nil {
		return 0, nil
	}
	keys, err := collectAllSyncStagedBlockKeys(db)
	if err != nil {
		return 0, err
	}
	if err := deleteKeyBatch(db, keys); err != nil {
		return 0, err
	}
	return len(keys), nil
}

func collectAllSyncStagedBlockKeys(db ethdb.Iteratee) ([][]byte, error) {
	if db == nil {
		return nil, nil
	}
	it := db.NewIterator(syncStagedBlockPrefix, nil)
	var keys [][]byte
	for it.Next() {
		keys = append(keys, append([]byte{}, it.Key()...))
	}
	if err := it.Error(); err != nil {
		it.Release()
		return nil, err
	}
	it.Release()
	return keys, nil
}

func deleteKeyBatch(db ethdb.KeyValueWriter, keys [][]byte) error {
	if db == nil || len(keys) == 0 {
		return nil
	}
	batcher, ok := db.(ethdb.Batcher)
	if !ok {
		for _, key := range keys {
			if err := db.Delete(key); err != nil {
				return err
			}
		}
		return nil
	}
	batch := batcher.NewBatchWithSize(len(keys) * 8)
	defer batch.Reset()
	for _, key := range keys {
		if err := batch.Delete(key); err != nil {
			return err
		}
	}
	return batch.Write()
}

// ResetSyncStagedBodies clears the downloader body staging table and its body
// progress rows. It attempts every cleanup step and reports per-step errors so
// callers can log without leaving later cleanup undone.
func ResetSyncStagedBodies(db ethdb.KeyValueStore) SyncStagedResetResult {
	var result SyncStagedResetResult
	if db == nil {
		return result
	}
	keys, err := collectAllSyncStagedBlockKeys(db)
	if err != nil {
		result.StagedDeleteError = err
		result.BodiesProgressError = DeleteStageProgress(db, StageSyncBodies)
		result.BodiesReadyProgressError = DeleteStageProgress(db, StageSyncBodiesReady)
		return result
	}
	batch := db.NewBatchWithSize(len(keys)*8 + 2*8)
	defer batch.Reset()
	for _, key := range keys {
		if err := batch.Delete(key); err != nil && result.StagedDeleteError == nil {
			result.StagedDeleteError = err
		}
	}
	if err := batch.Delete(stageProgressKey(StageSyncBodies)); err != nil {
		result.BodiesProgressError = err
	}
	if err := batch.Delete(stageProgressKey(StageSyncBodiesReady)); err != nil {
		result.BodiesReadyProgressError = err
	}
	if err := batch.Write(); err != nil {
		if len(keys) > 0 && result.StagedDeleteError == nil {
			result.StagedDeleteError = err
		}
		if result.BodiesProgressError == nil {
			result.BodiesProgressError = err
		}
		if result.BodiesReadyProgressError == nil {
			result.BodiesReadyProgressError = err
		}
		return result
	}
	if result.StagedDeleteError == nil {
		result.DeletedBodies = len(keys)
	}
	return result
}
