package rawdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

type SyncStagedBlockRow struct {
	Number uint64
	Hash   common.Hash
	Raw    []byte
}

type syncStagedMetadataViewContext struct {
	key       []byte
	number    uint64
	row       SyncStagedBlockRow
	consumeFn func([]byte) error
}

var syncStagedMetadataViewContextPool = sync.Pool{New: func() any {
	ctx := &syncStagedMetadataViewContext{key: make([]byte, len(syncStagedBlockPrefix)+8)}
	ctx.consumeFn = ctx.consume
	return ctx
}}

func (c *syncStagedMetadataViewContext) prepare(number uint64) {
	copy(c.key, syncStagedBlockPrefix)
	binary.BigEndian.PutUint64(c.key[len(syncStagedBlockPrefix):], number)
	c.number = number
	c.row = SyncStagedBlockRow{}
}

func (c *syncStagedMetadataViewContext) consume(data []byte) error {
	row, err := decodeSyncStagedBlockMetadata(c.number, data)
	if err == nil {
		c.row = row
	}
	return err
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
	existing, existingOK, err := ReadSyncStagedBlockMetadata(db, result.Number)
	if err != nil {
		result.StageError = fmt.Errorf("rawdb: read existing sync staged block %d: %w", result.Number, err)
		return result
	}
	if existingOK && existing.Hash != result.Hash {
		result.StageError = fmt.Errorf("rawdb: sync staged block %d hash %x conflicts staged block hash %x", result.Number, existing.Hash, result.Hash)
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
		if row.HasBlockHash && row.BlockNum == result.Number && row.BlockHash != result.Hash {
			result.StageError = fmt.Errorf("rawdb: sync bodies progress block %d hash %x conflicts staged block hash %x", result.Number, row.BlockHash, result.Hash)
			return result
		}
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

func validateSyncStagedBlockRawForProgress(block *types.Block, raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	wireHash, err := types.BlockHashFromRaw(raw)
	if err != nil {
		return fmt.Errorf("rawdb: sync staged raw for block %d header: %w", block.Number(), err)
	}
	wireNumber := binary.BigEndian.Uint64(wireHash[:8])
	if wireNumber != block.Number() {
		return fmt.Errorf("rawdb: sync staged raw key %d contains block %d", block.Number(), wireNumber)
	}
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
	data, exists, err := readPresentValue(db, syncStagedBlockKey(number), fmt.Sprintf("sync staged block %d", number))
	if err != nil || !exists {
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
	data, exists, err := readPresentValue(db, syncStagedBlockKey(number), fmt.Sprintf("sync staged block %d", number))
	if err != nil || !exists {
		return SyncStagedBlockRow{}, false, err
	}
	return decodeSyncStagedBlockRow(number, data)
}

// ReadSyncStagedBlockMetadata reads only the number and canonical hash of one
// staged block. Pebble exposes its value through a callback-scoped view, so the
// sync ready-frontier scan can skip both a full transaction-tree unmarshal and
// a defensive copy of the raw block. Generic readers retain the ordinary
// presence-coupled fallback.
func ReadSyncStagedBlockMetadata(db ethdb.KeyValueReader, number uint64) (SyncStagedBlockRow, bool, error) {
	if db == nil {
		return SyncStagedBlockRow{}, false, nil
	}
	if viewer, ok := db.(interface {
		View([]byte, func([]byte) error) error
	}); ok {
		ctx := syncStagedMetadataViewContextPool.Get().(*syncStagedMetadataViewContext)
		ctx.prepare(number)
		err := viewer.View(ctx.key, ctx.consumeFn)
		row := ctx.row
		ctx.row = SyncStagedBlockRow{}
		syncStagedMetadataViewContextPool.Put(ctx)
		if err == nil {
			return row, true, nil
		}
		if classifier, ok := db.(interface{ IsKeyNotFound(error) bool }); ok && classifier.IsKeyNotFound(err) {
			return SyncStagedBlockRow{}, false, nil
		}
		return SyncStagedBlockRow{}, false, fmt.Errorf("rawdb: read sync staged block metadata %d: %w", number, err)
	}
	key := syncStagedBlockKey(number)
	data, exists, err := readPresentValue(db, key, fmt.Sprintf("sync staged block metadata %d", number))
	if err != nil || !exists {
		return SyncStagedBlockRow{}, false, err
	}
	row, err := decodeSyncStagedBlockMetadata(number, data)
	return row, true, err
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
// sync pipeline progress rows. Imported body deletes require at least one
// progress row so the staged-body proof is not discarded before a durable
// SyncImport boundary exists; cleanup-only callers should use the explicit
// staged-body cleanup helpers instead.
func WriteSyncImportProgressBatch(db interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
}, deletes []SyncStagedBlockDelete, progress []StageProgress) SyncImportProgressWriteResult {
	return writeSyncImportProgressBatch(db, deletes, progress, nil, false)
}

// WriteSyncImportProgressAndReadyBatch also updates the downstream
// SyncBodiesReady frontier in the same batch as the imported-body delete and
// import-stage progress rows. A nil ready row deletes the frontier; otherwise
// it must be a hash-bound SyncBodiesReady row backed by a staged body after
// the imported delete prefix. This API requires a batch-capable writer; callers
// compute the frontier before the batch
// only when the canonical head is already at the imported boundary.
func WriteSyncImportProgressAndReadyBatch(db interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
}, deletes []SyncStagedBlockDelete, progress []StageProgress, ready *StageProgress) SyncImportProgressWriteResult {
	return writeSyncImportProgressBatch(db, deletes, progress, ready, true)
}

func writeSyncImportProgressBatch(db interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
}, deletes []SyncStagedBlockDelete, progress []StageProgress, ready *StageProgress, updateReady bool) SyncImportProgressWriteResult {
	var result SyncImportProgressWriteResult
	if db == nil || (len(deletes) == 0 && len(progress) == 0) {
		return result
	}
	if err := validateSyncImportProgressRows(progress); err != nil {
		result.ProgressError = err
		return result
	}
	if err := validateSyncImportProgressMonotonic(db, progress); err != nil {
		result.ProgressError = err
		return result
	}
	if err := validateSyncImportMergedProgressOrder(db, progress); err != nil {
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
	if updateReady {
		if err := validateSyncImportReadyProgress(db, deletes, ready); err != nil {
			result.ProgressError = err
			return result
		}
	}
	if batcher, ok := db.(ethdb.Batcher); ok {
		batch := batcher.NewBatchWithSize((len(deletes) + len(progress) + 1) * 8)
		defer batch.Reset()
		enqueuedDeletes := make([]SyncStagedBlockDelete, 0, len(deletes))
		for _, block := range deletes {
			if err := batch.Delete(syncStagedBlockKey(block.Number)); err != nil {
				result.DeleteErrors = append(result.DeleteErrors, SyncStagedBlockDeleteError{
					Number: block.Number,
					Hash:   block.Hash,
					Err:    err,
				})
				return result
			}
			enqueuedDeletes = append(enqueuedDeletes, block)
		}
		for _, row := range progress {
			if err := batch.Put(stageProgressKey(row.Stage), encodeStageProgress(row.BlockNum, row.BlockHash, row.HasBlockHash)); err != nil {
				result.ProgressError = fmt.Errorf("rawdb: write stage progress %s at %d: %w", row.Stage, row.BlockNum, err)
				return result
			}
		}
		if updateReady {
			if ready == nil {
				if err := batch.Delete(stageProgressKey(StageSyncBodiesReady)); err != nil {
					result.ProgressError = fmt.Errorf("rawdb: delete sync bodies ready progress: %w", err)
					return result
				}
			} else if err := batch.Put(stageProgressKey(StageSyncBodiesReady), encodeStageProgress(ready.BlockNum, ready.BlockHash, true)); err != nil {
				result.ProgressError = fmt.Errorf("rawdb: write sync bodies ready progress at %d: %w", ready.BlockNum, err)
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
	if updateReady {
		result.ProgressError = errors.New("rawdb: sync import progress and ready update requires batch writer")
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

func validateSyncImportReadyProgress(db ethdb.KeyValueReader, deletes []SyncStagedBlockDelete, ready *StageProgress) error {
	if ready == nil {
		return nil
	}
	if ready.Stage != StageSyncBodiesReady {
		return fmt.Errorf("rawdb: ready progress stage %s, want %s", ready.Stage, StageSyncBodiesReady)
	}
	if !ready.HasBlockHash {
		return fmt.Errorf("rawdb: sync bodies ready progress at block %d is not hash-bound", ready.BlockNum)
	}
	if len(deletes) > 0 && ready.BlockNum <= deletes[len(deletes)-1].Number {
		return fmt.Errorf("rawdb: sync bodies ready progress at block %d must follow imported delete prefix ending at block %d", ready.BlockNum, deletes[len(deletes)-1].Number)
	}
	row, ok, err := ReadSyncStagedBlockMetadata(db, ready.BlockNum)
	if err != nil {
		return fmt.Errorf("rawdb: read sync bodies ready staged block %d: %w", ready.BlockNum, err)
	}
	if !ok {
		return fmt.Errorf("rawdb: sync bodies ready staged block %d missing", ready.BlockNum)
	}
	if row.Hash != ready.BlockHash {
		return fmt.Errorf("rawdb: sync bodies ready staged block %d hash %x, want %x", ready.BlockNum, row.Hash, ready.BlockHash)
	}
	return nil
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
		row, ok, err := ReadSyncStagedBlockMetadata(db, block.Number)
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
	if len(rows) == 0 && len(deletes) == 0 {
		return nil
	}
	if len(rows) == 0 {
		return fmt.Errorf("rawdb: sync staged deletes have no import progress proof")
	}
	if len(deletes) == 0 {
		return fmt.Errorf("rawdb: sync import progress has no staged delete prefix")
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

func validateSyncImportMergedProgressOrder(db ethdb.KeyValueReader, rows []StageProgress) error {
	if len(rows) == 0 {
		return nil
	}
	if db == nil {
		return errors.New("rawdb: nil sync import progress reader")
	}
	merged := make(map[StageID]StageProgress, len(syncImportProgressStageOrder))
	for _, stage := range syncImportProgressStageOrder {
		row, ok, err := ReadStageProgressRow(db, stage)
		if err != nil {
			return fmt.Errorf("rawdb: read existing sync import progress %s: %w", stage, err)
		}
		if ok {
			merged[stage] = row
		}
	}
	for _, row := range rows {
		merged[row.Stage] = row
	}
	for i := 1; i < len(syncImportProgressStageOrder); i++ {
		upstreamStage := syncImportProgressStageOrder[i-1]
		downstreamStage := syncImportProgressStageOrder[i]
		downstream, downstreamOK := merged[downstreamStage]
		if !downstreamOK {
			continue
		}
		if !downstream.HasBlockHash {
			return fmt.Errorf("rawdb: sync import progress %s at block %d is not hash-bound", downstreamStage, downstream.BlockNum)
		}
		upstream, upstreamOK := merged[upstreamStage]
		if !upstreamOK {
			return fmt.Errorf("rawdb: sync import progress %s at block %d requires upstream %s", downstreamStage, downstream.BlockNum, upstreamStage)
		}
		if !upstream.HasBlockHash {
			return fmt.Errorf("rawdb: sync import progress %s at block %d is not hash-bound", upstreamStage, upstream.BlockNum)
		}
		if downstream.BlockNum > upstream.BlockNum {
			return fmt.Errorf("rawdb: sync import progress %s block %d is ahead of upstream %s block %d", downstreamStage, downstream.BlockNum, upstreamStage, upstream.BlockNum)
		}
		if downstream.BlockNum == upstream.BlockNum && downstream.HasBlockHash && upstream.HasBlockHash && downstream.BlockHash != upstream.BlockHash {
			return fmt.Errorf("rawdb: sync import progress %s block %d hash %x does not match upstream %s hash %x", downstreamStage, downstream.BlockNum, downstream.BlockHash, upstreamStage, upstream.BlockHash)
		}
	}
	return nil
}

func validateSyncImportProgressMonotonic(db ethdb.KeyValueReader, rows []StageProgress) error {
	if len(rows) == 0 {
		return nil
	}
	if db == nil {
		return errors.New("rawdb: nil sync import progress reader")
	}
	for i, row := range rows {
		existing, ok, err := ReadStageProgressRow(db, row.Stage)
		if err != nil {
			return fmt.Errorf("rawdb: read existing sync import progress %s: %w", row.Stage, err)
		}
		if !ok {
			continue
		}
		if existing.BlockNum > row.BlockNum {
			return fmt.Errorf("rawdb: sync import progress %s at row %d block %d would regress existing block %d", row.Stage, i, row.BlockNum, existing.BlockNum)
		}
		if existing.BlockNum == row.BlockNum && existing.HasBlockHash && row.HasBlockHash && existing.BlockHash != row.BlockHash {
			return fmt.Errorf("rawdb: sync import progress %s at row %d block %d hash %x would replace existing hash %x", row.Stage, i, row.BlockNum, row.BlockHash, existing.BlockHash)
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
	return deleteSyncStagedBlockRange(db, syncStagedBlockPrefix, syncStagedBlockRangeEnd(blockNum), false)
}

func decodeSyncStagedBlockRow(number uint64, data []byte) (SyncStagedBlockRow, bool, error) {
	row, err := decodeSyncStagedBlockMetadata(number, data)
	if err != nil {
		return SyncStagedBlockRow{}, true, err
	}
	row.Raw = append([]byte(nil), data...)
	return row, true, nil
}

func decodeSyncStagedBlockMetadata(number uint64, data []byte) (SyncStagedBlockRow, error) {
	hash, err := types.BlockHashFromRaw(data)
	if err != nil {
		return SyncStagedBlockRow{}, err
	}
	wireNumber := binary.BigEndian.Uint64(hash[:8])
	if wireNumber != number {
		return SyncStagedBlockRow{}, fmt.Errorf("rawdb: sync staged block key %d contains block %d", number, wireNumber)
	}
	return SyncStagedBlockRow{Number: number, Hash: hash}, nil
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
	return deleteSyncStagedBlockRange(db, syncStagedBlockKey(blockNum), prefixUpperBound(syncStagedBlockPrefix), false)
}

// syncStagedBlockRangeEnd returns the exclusive key bound for staged rows up
// to and including blockNum. The MaxUint64 case must extend to the end of the
// namespace because blockNum+1 would wrap before the prefix.
func syncStagedBlockRangeEnd(blockNum uint64) []byte {
	if blockNum == ^uint64(0) {
		return prefixUpperBound(syncStagedBlockPrefix)
	}
	return syncStagedBlockKey(blockNum + 1)
}

// syncStagedBlockRangeDeletePlan retains the normal range-tombstone path
// without giving malformed keys a different cleanup meaning. Production rows
// are always fixed-width block-number keys, so Pebble receives one range
// tombstone instead of an in-memory key slice and one point tombstone per row.
// A corrupt namespace falls back to point deletes for its valid rows, matching
// the former behavior that left malformed keys untouched.
type syncStagedBlockRangeDeletePlan struct {
	Deleted     int
	Start       []byte
	End         []byte
	RangeDelete bool
	Keys        [][]byte
}

func planSyncStagedBlockRangeDelete(db ethdb.Iteratee, start, end []byte, deleteMalformed bool) (syncStagedBlockRangeDeletePlan, error) {
	plan := syncStagedBlockRangeDeletePlan{
		Start:       start,
		End:         end,
		RangeDelete: true,
	}
	if db == nil {
		return plan, nil
	}
	if !bytes.HasPrefix(start, syncStagedBlockPrefix) {
		return plan, fmt.Errorf("rawdb: invalid sync staged block range start %x", start)
	}
	it := db.NewIterator(syncStagedBlockPrefix, start[len(syncStagedBlockPrefix):])
	for it.Next() {
		key := it.Key()
		if end != nil && bytes.Compare(key, end) >= 0 {
			break
		}
		if deleteMalformed {
			plan.Deleted++
			continue
		}
		if _, ok := parseSyncStagedBlockKey(key); !ok {
			plan.RangeDelete = false
			continue
		}
		plan.Deleted++
	}
	err := it.Error()
	it.Release()
	if err != nil {
		return syncStagedBlockRangeDeletePlan{}, err
	}
	if plan.RangeDelete || plan.Deleted == 0 {
		return plan, nil
	}
	// Preserve the old malformed-key behavior on damaged data. This path is not
	// used by normal staged sync rows and may allocate one key per valid row.
	it = db.NewIterator(syncStagedBlockPrefix, start[len(syncStagedBlockPrefix):])
	defer it.Release()
	plan.Keys = make([][]byte, 0, plan.Deleted)
	for it.Next() {
		key := it.Key()
		if end != nil && bytes.Compare(key, end) >= 0 {
			break
		}
		if _, ok := parseSyncStagedBlockKey(key); !ok {
			continue
		}
		plan.Keys = append(plan.Keys, append([]byte(nil), key...))
	}
	if err := it.Error(); err != nil {
		return syncStagedBlockRangeDeletePlan{}, err
	}
	return plan, nil
}

func (p syncStagedBlockRangeDeletePlan) appendTo(batch ethdb.Batch) error {
	if p.Deleted == 0 {
		return nil
	}
	if p.RangeDelete {
		return batch.DeleteRange(p.Start, p.End)
	}
	for _, key := range p.Keys {
		if err := batch.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

func deleteSyncStagedBlockRange(db ethdb.KeyValueStore, start, end []byte, deleteMalformed bool) (int, error) {
	plan, err := planSyncStagedBlockRangeDelete(db, start, end, deleteMalformed)
	if err != nil || plan.Deleted == 0 {
		return plan.Deleted, err
	}
	batch := db.NewBatchWithSize(len(start) + len(end))
	defer batch.Reset()
	if err := plan.appendTo(batch); err != nil {
		return 0, err
	}
	if err := batch.Write(); err != nil {
		return 0, err
	}
	return plan.Deleted, nil
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
	plan, err := planSyncStagedBlockRangeDelete(db, syncStagedBlockKey(blockNum), prefixUpperBound(syncStagedBlockPrefix), false)
	if err != nil {
		return result, err
	}
	if plan.Deleted == 0 && !deleteProgress && !rewindProgress {
		return result, nil
	}
	batch := db.NewBatchWithSize(len(plan.Start) + len(plan.End) + 8 + common.HashLength)
	defer batch.Reset()
	if err := plan.appendTo(batch); err != nil {
		return result, err
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
	result.Deleted = plan.Deleted
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
	return deleteSyncStagedBlockRange(db, syncStagedBlockPrefix, prefixUpperBound(syncStagedBlockPrefix), true)
}

// ResetSyncStagedBodies clears the downloader body staging table and its body
// progress rows. It attempts every cleanup step and reports per-step errors so
// callers can log without leaving later cleanup undone.
func ResetSyncStagedBodies(db ethdb.KeyValueStore) SyncStagedResetResult {
	var result SyncStagedResetResult
	if db == nil {
		return result
	}
	plan, err := planSyncStagedBlockRangeDelete(db, syncStagedBlockPrefix, prefixUpperBound(syncStagedBlockPrefix), true)
	if err != nil {
		result.StagedDeleteError = err
		result.BodiesProgressError = DeleteStageProgress(db, StageSyncBodies)
		result.BodiesReadyProgressError = DeleteStageProgress(db, StageSyncBodiesReady)
		return result
	}
	batch := db.NewBatchWithSize(len(plan.Start) + len(plan.End) + 2*8)
	defer batch.Reset()
	if err := plan.appendTo(batch); err != nil {
		result.StagedDeleteError = err
	}
	if err := batch.Delete(stageProgressKey(StageSyncBodies)); err != nil {
		result.BodiesProgressError = err
	}
	if err := batch.Delete(stageProgressKey(StageSyncBodiesReady)); err != nil {
		result.BodiesReadyProgressError = err
	}
	if err := batch.Write(); err != nil {
		if plan.Deleted > 0 && result.StagedDeleteError == nil {
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
		result.DeletedBodies = plan.Deleted
	}
	return result
}
