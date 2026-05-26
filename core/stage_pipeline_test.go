package core

import (
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestCanonicalStagePipelineWritesHashBoundStagesInOrder(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	hash := common.Hash{0x44}
	pipeline := newCanonicalStagePipeline(db, 44, hash)

	if err := pipeline.Advance(rawdb.StageHeaders, rawdb.StageBodies); err != nil {
		t.Fatalf("advance headers/bodies: %v", err)
	}
	if err := pipeline.Advance(rawdb.StageExecution); err != nil {
		t.Fatalf("advance execution: %v", err)
	}
	if err := pipeline.Advance(rawdb.StageCommitment, rawdb.StageFinish); err != nil {
		t.Fatalf("advance commitment/finish: %v", err)
	}
	for _, stage := range rawdb.CanonicalExecutionStages() {
		row, ok, err := rawdb.ReadStageProgressRow(db, stage)
		if err != nil || !ok || row.BlockNum != 44 || !row.HasBlockHash || row.BlockHash != hash {
			t.Fatalf("%s progress = %+v ok=%v err=%v, want block 44 hash-bound", stage, row, ok, err)
		}
	}
}

func TestCanonicalStagePipelineUsesStageProgressStore(t *testing.T) {
	hash := common.Hash{0x45}
	store := &recordingStageProgressStore{}
	pipeline := &canonicalStagePipeline{
		progress: store,
		blockNum: 45,
		hash:     hash,
		last:     -1,
	}

	if err := pipeline.Advance(rawdb.StageHeaders, rawdb.StageBodies, rawdb.StageExecution); err != nil {
		t.Fatalf("advance through store: %v", err)
	}
	if len(store.writes) != 3 {
		t.Fatalf("writes = %+v, want 3", store.writes)
	}
	for i, write := range store.writes {
		if write.blockNum != 45 || write.hash != hash {
			t.Fatalf("write %d = %+v, want block 45 hash %x", i, write, hash)
		}
	}
	if store.writes[0].stage != rawdb.StageHeaders || store.writes[1].stage != rawdb.StageBodies || store.writes[2].stage != rawdb.StageExecution {
		t.Fatalf("stage writes = %+v", store.writes)
	}
}

func TestCanonicalStagePipelineRejectsSkippedOrForeignStages(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	pipeline := newCanonicalStagePipeline(db, 3, common.Hash{0x03})

	if err := pipeline.Advance(rawdb.StageHeaders); err != nil {
		t.Fatalf("advance headers: %v", err)
	}
	if err := pipeline.Advance(rawdb.StageExecution); err == nil || !strings.Contains(err.Error(), "out of order") {
		t.Fatalf("advance skipped execution error = %v, want out of order", err)
	}
	if err := pipeline.Advance(rawdb.StageSnapshotPrune); err == nil || !strings.Contains(err.Error(), "not a canonical execution stage") {
		t.Fatalf("advance snapshot stage error = %v, want non-canonical rejection", err)
	}
	if err := pipeline.Advance(rawdb.StageBodies); err != nil {
		t.Fatalf("advance bodies after rejected stages: %v", err)
	}
}

type stageProgressWrite struct {
	stage    rawdb.StageID
	blockNum uint64
	hash     common.Hash
}

type recordingStageProgressStore struct {
	writes []stageProgressWrite
	rows   map[rawdb.StageID]rawdb.StageProgress
}

func (s *recordingStageProgressStore) WriteWithHash(stage rawdb.StageID, blockNum uint64, hash common.Hash) error {
	s.writes = append(s.writes, stageProgressWrite{stage: stage, blockNum: blockNum, hash: hash})
	if s.rows == nil {
		s.rows = make(map[rawdb.StageID]rawdb.StageProgress)
	}
	s.rows[stage] = rawdb.StageProgress{Stage: stage, BlockNum: blockNum, BlockHash: hash, HasBlockHash: true}
	return nil
}

func (s *recordingStageProgressStore) RewindCanonicalWithHash(blockNum uint64, hash common.Hash) error {
	for _, stage := range rawdb.CanonicalExecutionStages() {
		if err := s.WriteWithHash(stage, blockNum, hash); err != nil {
			return err
		}
	}
	return nil
}

func (s *recordingStageProgressStore) Read(stage rawdb.StageID) (rawdb.StageProgress, bool, error) {
	row, ok := s.rows[stage]
	return row, ok, nil
}

func TestRewindCanonicalStagePipelineWritesHashBoundRows(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	hash := common.Hash{0x07}
	if err := rewindCanonicalStagePipeline(db, 7, hash); err != nil {
		t.Fatalf("rewind canonical pipeline: %v", err)
	}
	for _, stage := range rawdb.CanonicalExecutionStages() {
		row, ok, err := rawdb.ReadStageProgressRow(db, stage)
		if err != nil || !ok || row.BlockNum != 7 || !row.HasBlockHash || row.BlockHash != hash {
			t.Fatalf("%s progress after rewind = %+v ok=%v err=%v, want block 7 hash-bound", stage, row, ok, err)
		}
	}
}

func TestVerifyCanonicalStagePipelineHeadRequiresHashBoundCurrentHead(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	hash := common.Hash{0x07}
	if err := rewindCanonicalStagePipeline(db, 7, hash); err != nil {
		t.Fatalf("write canonical stage progress: %v", err)
	}
	if err := verifyCanonicalStagePipelineHead(db, 7, hash); err != nil {
		t.Fatalf("verify canonical stage head: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, 7, common.Hash{0x08}); err != nil {
		t.Fatalf("corrupt finish stage: %v", err)
	}
	if err := verifyCanonicalStagePipelineHead(db, 7, hash); err == nil || !strings.Contains(err.Error(), "Finish stage progress") {
		t.Fatalf("verify corrupted finish stage error = %v, want Finish stage mismatch", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageFinish, 7); err != nil {
		t.Fatalf("write legacy finish stage: %v", err)
	}
	if err := verifyCanonicalStagePipelineHead(db, 7, hash); err == nil || !strings.Contains(err.Error(), "not hash-bound") {
		t.Fatalf("verify legacy finish stage error = %v, want not hash-bound", err)
	}
}

func TestCanonicalBlockExecutionValidateRequiresRangeOwnedPlan(t *testing.T) {
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 7},
		},
	})
	var nilPlan *canonicalBlockExecution
	if err := nilPlan.Validate(block, false); err == nil || !strings.Contains(err.Error(), "nil plan") {
		t.Fatalf("nil plan validate error = %v, want nil plan", err)
	}
	if err := (&canonicalBlockExecution{}).Validate(block, false); err == nil || !strings.Contains(err.Error(), "nil state") {
		t.Fatalf("missing state validate error = %v, want nil state", err)
	}
	base := &canonicalBlockExecution{
		state:    &state.StateDB{},
		pipeline: newCanonicalStagePipeline(rawdb.NewMemoryDatabase(), block.Number(), block.Hash()),
	}
	if err := base.Validate(block, true); err == nil || !strings.Contains(err.Error(), "missing planned state tx range") {
		t.Fatalf("missing tx range validate error = %v, want missing tx range", err)
	}
	base.txRange = &rawdb.StateTxRange{BlockNum: block.Number(), BlockHash: common.Hash{0xff}}
	if err := base.Validate(block, true); err == nil || !strings.Contains(err.Error(), "planned state tx range mismatch") {
		t.Fatalf("mismatched tx range validate error = %v, want mismatch", err)
	}
	base.txRange.BlockHash = block.Hash()
	if err := base.Validate(block, true); err != nil {
		t.Fatalf("valid history-enabled plan rejected: %v", err)
	}
	base.txRange = nil
	if err := base.Validate(block, false); err != nil {
		t.Fatalf("history-disabled plan without tx range rejected: %v", err)
	}
}

func TestCanonicalBlockExecutionCommitStateWritesCheckpointAndStage(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb, err := state.New(common.Hash{}, state.NewDatabase(db))
	if err != nil {
		t.Fatalf("new state: %v", err)
	}
	var addr common.Address
	addr[0] = 0x41
	addr[20] = 0x7a
	statedb.GetOrCreateAccount(addr)
	statedb.AddBalance(addr, 1)

	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 9},
		},
	})
	pipeline := newCanonicalStagePipeline(db, block.Number(), block.Hash())
	if err := pipeline.Advance(rawdb.StageHeaders, rawdb.StageBodies, rawdb.StageExecution); err != nil {
		t.Fatalf("advance pre-commit stages: %v", err)
	}
	plan := &canonicalBlockExecution{
		state:    statedb,
		pipeline: pipeline,
	}

	result, err := plan.CommitState(db, block, state.CommitOptions{}, true)
	if err != nil {
		t.Fatalf("commit state: %v", err)
	}
	if result.Root == (common.Hash{}) {
		t.Fatal("commit state returned zero root")
	}
	row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageCommitment)
	if err != nil || !ok || row.BlockNum != block.Number() || !row.HasBlockHash || row.BlockHash != block.Hash() {
		t.Fatalf("commitment stage row = %+v ok=%v err=%v, want block 9 hash-bound", row, ok, err)
	}
	checkpoint, ok, err := rawdb.ReadStateCommitmentCheckpoint(db, block.Number())
	if err != nil || !ok {
		t.Fatalf("checkpoint ok=%v err=%v", ok, err)
	}
	if checkpoint.BlockHash != block.Hash() || checkpoint.Root != result.Root || checkpoint.Scheme != rawdb.LatestDomainCommitmentScheme {
		t.Fatalf("checkpoint = %+v, want hash %x root %x scheme %s", checkpoint, block.Hash(), result.Root, rawdb.LatestDomainCommitmentScheme)
	}
}
