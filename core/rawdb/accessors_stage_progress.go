package rawdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

type StageID string

const (
	StageHeaders       StageID = "Headers"
	StageBodies        StageID = "Bodies"
	StageExecution     StageID = "Execution"
	StageCommitment    StageID = "Commitment"
	StageFinish        StageID = "Finish"
	StageSnapshotBuild StageID = "SnapshotBuild"
	StageSnapshotPrune StageID = "SnapshotPrune"

	// StageChainFreezer records the highest block whose num-keyed chain rows
	// (`b-<num>` and `tib-<num>`) are covered by the local ancient store and no
	// longer need hot KV copies. Hash-keyed lookup pruning has its own stage.
	StageChainFreezer StageID = "ChainFreezer"

	// StageSyncInventory records the highest block target observed from peer
	// CHAIN_INVENTORY messages. It is downloader progress, not canonical proof.
	StageSyncInventory StageID = "SyncInventory"
	// StageSyncBodies records the highest sync block body accepted into the
	// transient downloader staging table. It is not canonical Headers/Bodies
	// progress and does not imply the staged bodies are contiguous.
	StageSyncBodies StageID = "SyncBodies"
	// StageSyncBodiesReady records the highest contiguous sync block body that
	// can be drained from the current canonical head. Unlike StageSyncBodies,
	// this is a contiguous frontier and is the downloader-stage boundary future
	// execution/import stages can safely consume.
	StageSyncBodiesReady StageID = "SyncBodiesReady"
	// StageSyncImport records the latest block successfully imported by
	// SyncService. Canonical execution stages are advanced separately by chain
	// insertion and snapshot restore paths.
	StageSyncImport StageID = "SyncImport"
	// StageSyncExecution records the latest sync-imported block that completed
	// canonical execution. It is downloader/import diagnostics, not canonical
	// StageExecution progress.
	StageSyncExecution StageID = "SyncExecution"
	// StageSyncCommitment records the latest sync-imported block that completed
	// state commitment. It is downloader/import diagnostics, not canonical
	// StageCommitment progress.
	StageSyncCommitment StageID = "SyncCommitment"
	// StageSyncFinish records the latest sync-imported block that completed the
	// full canonical block pipeline. It is downloader/import diagnostics, not
	// canonical StageFinish progress.
	StageSyncFinish StageID = "SyncFinish"

	// StageSnapshotLatestBuild records the solidified block at which the last
	// production latest-snapshot build ran, so the LatestBuildBlocks cadence gate
	// resumes across restarts instead of re-seeding to the current head (which
	// would delay the next build by one interval). Block-valued, forward-only
	// (never rewound), mirroring StageSnapshotBuild. NOTE: if an operator raises
	// LatestBuildBlocks a lot, the next build may be far out even when state is
	// stale — that is expected (gate = block >= prev + interval), not a stuck stage.
	StageSnapshotLatestBuild StageID = "SnapshotLatestBuild"
	// StageSnapshotEventLogBuild records the highest source block whose
	// transaction logs have been published into registered cold event-log
	// segments and global event-log-index sidecars as a continuous prefix from
	// block 1. It is block-valued, unlike the txNum-valued state-domain snapshot
	// stages.
	StageSnapshotEventLogBuild StageID = "SnapshotEventLogBuild"

	StageSnapshotLatest          StageID = "SnapshotLatest"
	StageSnapshotHistory         StageID = "SnapshotHistory"
	StageSnapshotAccessor        StageID = "SnapshotAccessor"
	StageSnapshotCommitmentFlush StageID = "SnapshotCommitmentFlush"
	StageSnapshotHotPrune        StageID = "SnapshotHotPrune"
	// StageSnapshotChainLookupPrune records the highest chain-freezer block
	// whose hash-keyed hot lookup rows have been pruned after verified cold
	// chain-index coverage was published.
	StageSnapshotChainLookupPrune StageID = "SnapshotChainLookupPrune"
	// StageSnapshotSectionBloomPrune records the highest source block whose
	// hot section-bloom rows have been pruned after verified cold
	// section-bloom snapshot coverage was published.
	StageSnapshotSectionBloomPrune StageID = "SnapshotSectionBloomPrune"
	// StageSnapshotBalanceTracePrune records the highest source block whose
	// hot account/balance trace rows have been pruned after verified cold
	// balance-trace snapshot coverage was published.
	StageSnapshotBalanceTracePrune StageID = "SnapshotBalanceTracePrune"
	// StageSnapshotChainFreezerTailPrune records the highest local freezer
	// block hidden by the virtual tail after verified cold chain-freezer
	// coverage existed. In minimal mode this also means any fully-hidden data
	// shards have been physically reclaimed where the freezer supports it.
	StageSnapshotChainFreezerTailPrune StageID = "SnapshotChainFreezerTailPrune"

	// StageSnapshotInstall records the txNum boundary of an installed verified
	// snapshot. It is txNum-valued like the snapshot domain stages; canonical
	// Headers/Bodies/Execution stages must still be advanced only by chain data.
	StageSnapshotInstall StageID = "SnapshotInstall"
)

type StageProgress struct {
	Stage        StageID
	BlockNum     uint64
	BlockHash    common.Hash
	HasBlockHash bool
}

type SyncInventoryTargetRestore struct {
	Head      uint64
	Target    uint64
	Row       StageProgress
	HaveRow   bool
	Restored  bool
	ReadError error
}

func CanonicalExecutionStages() []StageID {
	return []StageID{
		StageHeaders,
		StageBodies,
		StageExecution,
		StageCommitment,
		StageFinish,
	}
}

// KnownStageProgressStages returns every stage id with a built-in meaning in
// the order operators expect to inspect the pipeline.
func KnownStageProgressStages() []StageID {
	return []StageID{
		StageHeaders,
		StageBodies,
		StageExecution,
		StageCommitment,
		StageFinish,
		StageSyncInventory,
		StageSyncBodies,
		StageSyncBodiesReady,
		StageSyncImport,
		StageSyncExecution,
		StageSyncCommitment,
		StageSyncFinish,
		StageSnapshotInstall,
		StageSnapshotBuild,
		StageSnapshotLatestBuild,
		StageSnapshotEventLogBuild,
		StageSnapshotLatest,
		StageSnapshotHistory,
		StageSnapshotAccessor,
		StageSnapshotCommitmentFlush,
		StageSnapshotHotPrune,
		StageSnapshotPrune,
		StageChainFreezer,
		StageSnapshotChainLookupPrune,
		StageSnapshotSectionBloomPrune,
		StageSnapshotBalanceTracePrune,
		StageSnapshotChainFreezerTailPrune,
	}
}

func WriteStageProgress(db ethdb.KeyValueWriter, stage StageID, blockNum uint64) error {
	if db == nil {
		return errors.New("rawdb: nil stage progress writer")
	}
	if stage == "" {
		return errors.New("rawdb: empty stage id")
	}
	return db.Put(stageProgressKey(stage), encodeStageProgress(blockNum, common.Hash{}, false))
}

func WriteStageProgressWithHash(db ethdb.KeyValueWriter, stage StageID, blockNum uint64, blockHash common.Hash) error {
	if db == nil {
		return errors.New("rawdb: nil stage progress writer")
	}
	if stage == "" {
		return errors.New("rawdb: empty stage id")
	}
	return db.Put(stageProgressKey(stage), encodeStageProgress(blockNum, blockHash, true))
}

// WriteStageProgressRows persists a group of stage progress rows. When the
// writer supports ethdb batches, the rows are flushed with one batch write.
func WriteStageProgressRows(db ethdb.KeyValueWriter, rows []StageProgress) error {
	if len(rows) == 0 {
		return nil
	}
	if db == nil {
		return errors.New("rawdb: nil stage progress writer")
	}
	for i, row := range rows {
		if row.Stage == "" {
			return fmt.Errorf("rawdb: empty stage id at row %d", i)
		}
	}
	writer := db
	var batch ethdb.Batch
	if batcher, ok := db.(ethdb.Batcher); ok {
		batch = batcher.NewBatch()
		defer batch.Reset()
		writer = batch
	}
	for _, row := range rows {
		if err := writer.Put(stageProgressKey(row.Stage), encodeStageProgress(row.BlockNum, row.BlockHash, row.HasBlockHash)); err != nil {
			return fmt.Errorf("rawdb: write stage progress %s at %d: %w", row.Stage, row.BlockNum, err)
		}
	}
	if batch != nil {
		if err := batch.Write(); err != nil {
			return fmt.Errorf("rawdb: write stage progress batch: %w", err)
		}
	}
	return nil
}

func WriteCanonicalStageProgress(db ethdb.KeyValueWriter, blockNum uint64) error {
	for _, stage := range CanonicalExecutionStages() {
		if err := WriteStageProgress(db, stage, blockNum); err != nil {
			return err
		}
	}
	return nil
}

func WriteCanonicalStageProgressWithHash(db ethdb.KeyValueWriter, blockNum uint64, blockHash common.Hash) error {
	for _, stage := range CanonicalExecutionStages() {
		if err := WriteStageProgressWithHash(db, stage, blockNum, blockHash); err != nil {
			return err
		}
	}
	return nil
}

func RewindStageProgress(db ethdb.KeyValueWriter, stage StageID, blockNum uint64) error {
	if db == nil {
		return errors.New("rawdb: nil stage progress writer")
	}
	if stage == "" {
		return errors.New("rawdb: empty stage id")
	}
	return WriteStageProgress(db, stage, blockNum)
}

func RewindCanonicalStageProgress(db ethdb.KeyValueWriter, blockNum uint64) error {
	for _, stage := range CanonicalExecutionStages() {
		if err := RewindStageProgress(db, stage, blockNum); err != nil {
			return err
		}
	}
	return nil
}

func RewindCanonicalStageProgressWithHash(db ethdb.KeyValueWriter, blockNum uint64, blockHash common.Hash) error {
	for _, stage := range CanonicalExecutionStages() {
		if err := WriteStageProgressWithHash(db, stage, blockNum, blockHash); err != nil {
			return err
		}
	}
	return nil
}

func ReadStageProgress(db ethdb.KeyValueReader, stage StageID) (uint64, bool, error) {
	row, ok, err := ReadStageProgressRow(db, stage)
	if err != nil || !ok {
		return 0, ok, err
	}
	return row.BlockNum, true, nil
}

func ReadStageProgressRow(db ethdb.KeyValueReader, stage StageID) (StageProgress, bool, error) {
	if db == nil || stage == "" {
		return StageProgress{}, false, nil
	}
	data, err := db.Get(stageProgressKey(stage))
	if err != nil {
		return StageProgress{}, false, nil
	}
	row, err := decodeStageProgress(stage, data)
	if err != nil {
		return StageProgress{}, false, err
	}
	return row, true, nil
}

// RestoreSyncInventoryTarget reads the peer-advertised sync target and returns
// it only when it is ahead of the current local head. SyncInventory is
// diagnostic downloader progress, so stale rows never move the local target
// backward.
func RestoreSyncInventoryTarget(db ethdb.KeyValueReader, head uint64) SyncInventoryTargetRestore {
	result := SyncInventoryTargetRestore{Head: head, Target: head}
	row, ok, err := ReadStageProgressRow(db, StageSyncInventory)
	if err != nil {
		result.ReadError = err
		return result
	}
	if !ok {
		return result
	}
	result.Row = row
	result.HaveRow = true
	if row.BlockNum > head {
		result.Target = row.BlockNum
		result.Restored = true
	}
	return result
}

func ReadVerifiedStageProgressBlock(db ethdb.KeyValueReader, stage StageID) (uint64, bool, error) {
	return ReadVerifiedStageProgressBlockWithHashReader(db, stage, func(number uint64) common.Hash {
		return ReadBlockHashByNumber(db, number)
	})
}

// ReadVerifiedStageProgressBlockWithHashReader reads a hash-bound stage row and
// verifies it against the caller's canonical hash source.
func ReadVerifiedStageProgressBlockWithHashReader(db ethdb.KeyValueReader, stage StageID, readCanonicalHash func(uint64) common.Hash) (uint64, bool, error) {
	row, ok, err := ReadStageProgressRow(db, stage)
	if err != nil || !ok {
		return 0, ok, err
	}
	stageName := strings.ToLower(string(stage))
	if !row.HasBlockHash {
		return 0, true, fmt.Errorf("rawdb: %s stage %d is not hash-bound", stageName, row.BlockNum)
	}
	if readCanonicalHash == nil {
		return 0, true, fmt.Errorf("rawdb: %s stage %d cannot be verified without a canonical hash reader", stageName, row.BlockNum)
	}
	canonical := readCanonicalHash(row.BlockNum)
	if canonical == (common.Hash{}) {
		return 0, true, fmt.Errorf("rawdb: %s stage %d has hash %x but canonical block is unavailable", stageName, row.BlockNum, row.BlockHash)
	}
	if canonical != row.BlockHash {
		return 0, true, fmt.Errorf("rawdb: %s stage %d hash %x does not match canonical hash %x", stageName, row.BlockNum, row.BlockHash, canonical)
	}
	return row.BlockNum, true, nil
}

func DeleteStageProgress(db ethdb.KeyValueWriter, stage StageID) error {
	if db == nil || stage == "" {
		return nil
	}
	return db.Delete(stageProgressKey(stage))
}

func IterateStageProgress(db ethdb.Iteratee, fn func(StageProgress) (bool, error)) error {
	if db == nil || fn == nil {
		return nil
	}
	it := db.NewIterator(stageProgressPrefix, nil)
	defer it.Release()
	for it.Next() {
		stage := StageID(string(it.Key()[len(stageProgressPrefix):]))
		if stage == "" {
			continue
		}
		row, err := decodeStageProgress(stage, it.Value())
		if err != nil {
			return err
		}
		cont, err := fn(StageProgress{
			Stage:        stage,
			BlockNum:     row.BlockNum,
			BlockHash:    row.BlockHash,
			HasBlockHash: row.HasBlockHash,
		})
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return it.Error()
}

func encodeStageProgress(blockNum uint64, blockHash common.Hash, withHash bool) []byte {
	if !withHash {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], blockNum)
		return append([]byte(nil), buf[:]...)
	}
	buf := make([]byte, 8+common.HashLength)
	binary.BigEndian.PutUint64(buf[:8], blockNum)
	copy(buf[8:], blockHash[:])
	return buf
}

func decodeStageProgress(stage StageID, data []byte) (StageProgress, error) {
	switch len(data) {
	case 8:
		return StageProgress{Stage: stage, BlockNum: binary.BigEndian.Uint64(data)}, nil
	case 8 + common.HashLength:
		var hash common.Hash
		copy(hash[:], data[8:])
		return StageProgress{
			Stage:        stage,
			BlockNum:     binary.BigEndian.Uint64(data[:8]),
			BlockHash:    hash,
			HasBlockHash: true,
		}, nil
	default:
		return StageProgress{}, fmt.Errorf("rawdb: stage progress %q has length %d", stage, len(data))
	}
}
