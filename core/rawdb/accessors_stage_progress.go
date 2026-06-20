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

// StageProgressOrderPair is one non-sync staged-pipeline invariant. A present
// downstream stage must not be ahead of its upstream stage; selected storage
// stages also require the upstream row to be present.
type StageProgressOrderPair struct {
	Downstream                  StageID
	Upstream                    StageID
	RequireUpstream             bool
	RequireUpstreamAfterGenesis bool
}

// StageProgressOrderIssue describes one canonical/storage stage ordering
// violation.
type StageProgressOrderIssue struct {
	Downstream      StageID
	DownstreamBlock uint64
	DownstreamHash  common.Hash
	Upstream        StageID
	UpstreamBlock   uint64
	UpstreamHash    common.Hash
	MissingUpstream bool
	HashMismatch    bool
}

func (i StageProgressOrderIssue) String() string {
	if i.MissingUpstream {
		return fmt.Sprintf("%s requires %s", i.Downstream, i.Upstream)
	}
	if i.HashMismatch {
		return fmt.Sprintf("%s=%d/%x hash does not match %s=%d/%x", i.Downstream, i.DownstreamBlock, i.DownstreamHash, i.Upstream, i.UpstreamBlock, i.UpstreamHash)
	}
	return fmt.Sprintf("%s=%d ahead of %s=%d", i.Downstream, i.DownstreamBlock, i.Upstream, i.UpstreamBlock)
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

// StageProgressOrderPairs returns canonical execution plus storage-maintenance
// stage dependencies. Downloader-only sync progress is checked by
// net/sync/downloader because that package owns sync-stage semantics.
func StageProgressOrderPairs() []StageProgressOrderPair {
	return []StageProgressOrderPair{
		{Downstream: StageBodies, Upstream: StageHeaders},
		{Downstream: StageExecution, Upstream: StageBodies},
		{Downstream: StageCommitment, Upstream: StageExecution},
		{Downstream: StageFinish, Upstream: StageCommitment},
		{Downstream: StageSnapshotBuild, Upstream: StageFinish, RequireUpstream: true},
		{Downstream: StageSnapshotLatestBuild, Upstream: StageFinish, RequireUpstream: true},
		{Downstream: StageSnapshotEventLogBuild, Upstream: StageFinish, RequireUpstream: true},
		{Downstream: StageSnapshotPrune, Upstream: StageFinish, RequireUpstream: true},
		{Downstream: StageChainFreezer, Upstream: StageFinish, RequireUpstream: true},
		{Downstream: StageSnapshotSectionBloomPrune, Upstream: StageFinish, RequireUpstream: true},
		{Downstream: StageSnapshotBalanceTracePrune, Upstream: StageFinish, RequireUpstream: true},
		{Downstream: StageSnapshotChainLookupPrune, Upstream: StageChainFreezer, RequireUpstream: true},
		{Downstream: StageSnapshotChainFreezerTailPrune, Upstream: StageSnapshotChainLookupPrune, RequireUpstream: true},
		{Downstream: StageSnapshotChainFreezerTailPrune, Upstream: StageSnapshotEventLogBuild, RequireUpstreamAfterGenesis: true},
	}
}

// CheckStageProgressOrder validates the rawdb-owned non-sync staged pipeline.
// Missing upstream rows are tolerated for canonical execution rows but rejected
// for storage stages whose persisted progress is only meaningful after the
// upstream coverage stage exists.
func CheckStageProgressOrder(rows map[StageID]StageProgress) []StageProgressOrderIssue {
	if len(rows) == 0 {
		return nil
	}
	var issues []StageProgressOrderIssue
	for _, pair := range StageProgressOrderPairs() {
		downstream, downstreamOK := rows[pair.Downstream]
		if !downstreamOK {
			continue
		}
		upstream, upstreamOK := rows[pair.Upstream]
		if !upstreamOK {
			if pair.RequiresUpstreamPresence(downstream.BlockNum) {
				issues = append(issues, StageProgressOrderIssue{
					Downstream:      pair.Downstream,
					DownstreamBlock: downstream.BlockNum,
					Upstream:        pair.Upstream,
					MissingUpstream: true,
				})
			}
			continue
		}
		if downstream.BlockNum <= upstream.BlockNum {
			if downstream.BlockNum == upstream.BlockNum &&
				downstream.HasBlockHash && upstream.HasBlockHash &&
				downstream.BlockHash != upstream.BlockHash {
				issues = append(issues, StageProgressOrderIssue{
					Downstream:      pair.Downstream,
					DownstreamBlock: downstream.BlockNum,
					DownstreamHash:  downstream.BlockHash,
					Upstream:        pair.Upstream,
					UpstreamBlock:   upstream.BlockNum,
					UpstreamHash:    upstream.BlockHash,
					HashMismatch:    true,
				})
			}
			continue
		}
		issues = append(issues, StageProgressOrderIssue{
			Downstream:      pair.Downstream,
			DownstreamBlock: downstream.BlockNum,
			DownstreamHash:  downstream.BlockHash,
			Upstream:        pair.Upstream,
			UpstreamBlock:   upstream.BlockNum,
			UpstreamHash:    upstream.BlockHash,
		})
	}
	return issues
}

// RequiresUpstreamPresence reports whether a downstream row is invalid without
// its upstream row.
func (p StageProgressOrderPair) RequiresUpstreamPresence(downstreamBlock uint64) bool {
	return p.RequireUpstream || (p.RequireUpstreamAfterGenesis && downstreamBlock > 0)
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
	key := stageProgressKey(stage)
	exists, err := db.Has(key)
	if err != nil {
		return StageProgress{}, false, err
	}
	if !exists {
		return StageProgress{}, false, nil
	}
	data, err := db.Get(key)
	if err != nil {
		return StageProgress{}, false, err
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
	return ReadVerifiedStageProgressBlockWithHashLookup(db, stage, func(number uint64) (common.Hash, bool, error) {
		return ReadBlockHashByNumberStrict(db, number)
	})
}

// ReadVerifiedStageProgressBlockWithHashReader reads a hash-bound stage row and
// verifies it against the caller's canonical hash source.
func ReadVerifiedStageProgressBlockWithHashReader(db ethdb.KeyValueReader, stage StageID, readCanonicalHash func(uint64) common.Hash) (uint64, bool, error) {
	if readCanonicalHash == nil {
		return ReadVerifiedStageProgressBlockWithHashLookup(db, stage, nil)
	}
	return ReadVerifiedStageProgressBlockWithHashLookup(db, stage, func(number uint64) (common.Hash, bool, error) {
		hash := readCanonicalHash(number)
		if hash == (common.Hash{}) {
			return common.Hash{}, false, nil
		}
		return hash, true, nil
	})
}

// ReadVerifiedStageProgressBlockWithHashLookup is the error-aware form of
// ReadVerifiedStageProgressBlockWithHashReader. It lets stage verifiers surface
// corrupt canonical block/hash sources instead of collapsing them into an
// unavailable zero hash.
func ReadVerifiedStageProgressBlockWithHashLookup(db ethdb.KeyValueReader, stage StageID, readCanonicalHash func(uint64) (common.Hash, bool, error)) (uint64, bool, error) {
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
	canonical, hashOK, err := readCanonicalHash(row.BlockNum)
	if err != nil {
		return 0, true, fmt.Errorf("rawdb: %s stage %d canonical hash lookup: %w", stageName, row.BlockNum, err)
	}
	if !hashOK || canonical == (common.Hash{}) {
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
