package downloader

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	tsync "github.com/tronprotocol/go-tron/net/sync"
	"github.com/tronprotocol/go-tron/p2p"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestRawBlockBytesCopiesWirePayload(t *testing.T) {
	block := testBufferedBlock(1)
	raw, err := block.Marshal()
	if err != nil {
		t.Fatalf("marshal block: %v", err)
	}
	got := RawBlockBytes(block, raw)
	if !bytes.Equal(got, raw) {
		t.Fatalf("raw copy differs from source")
	}
	raw[0] ^= 0xff
	if bytes.Equal(got, raw) {
		t.Fatal("raw copy aliases source slice")
	}
}

func TestRawBlockBytesRemarshalsWhenWirePayloadMissing(t *testing.T) {
	block := testBufferedBlock(2)
	got := RawBlockBytes(block, nil)
	decoded, err := types.UnmarshalBlock(got)
	if err != nil {
		t.Fatalf("decode remarshal bytes: %v", err)
	}
	if decoded.Hash() != block.Hash() || decoded.Number() != block.Number() {
		t.Fatalf("decoded block = #%d %x, want #%d %x", decoded.Number(), decoded.Hash(), block.Number(), block.Hash())
	}
}

func TestBufferedBatchDecodeBlocksKeepsPrefixOnError(t *testing.T) {
	block1 := testBufferedBlock(1)
	block3 := testBufferedBlock(3)
	raw1, err := block1.Marshal()
	if err != nil {
		t.Fatalf("marshal block1: %v", err)
	}
	raw3, err := block3.Marshal()
	if err != nil {
		t.Fatalf("marshal block3: %v", err)
	}
	batch := BufferedBatch{Buffered: []BufferedBlock{
		{Raw: raw1, Hash: block1.Hash(), Num: block1.Number()},
		{Raw: []byte{0x01, 0x02}, Hash: tcommon.Hash{0xee}, Num: 2},
		{Raw: raw3, Hash: block3.Hash(), Num: block3.Number()},
	}}

	dropped, err := batch.DecodeBlocks()
	if err == nil {
		t.Fatal("DecodeBlocks succeeded, want decode error")
	}
	if dropped.Num != 2 || dropped.Hash != (tcommon.Hash{0xee}) {
		t.Fatalf("dropped = #%d %x, want #2 ee", dropped.Num, dropped.Hash)
	}
	if len(batch.Blocks) != 1 || batch.Blocks[0].Hash() != block1.Hash() {
		t.Fatalf("decoded prefix = %d blocks, want only block1", len(batch.Blocks))
	}
}

func TestBufferedBatchDecodeBlocksRejectsMetadataMismatch(t *testing.T) {
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	raw1, err := block1.Marshal()
	if err != nil {
		t.Fatalf("marshal block1: %v", err)
	}
	raw2, err := block2.Marshal()
	if err != nil {
		t.Fatalf("marshal block2: %v", err)
	}
	expectedHash := tcommon.Hash{0xee}
	batch := BufferedBatch{Buffered: []BufferedBlock{
		{Raw: raw1, Hash: block1.Hash(), Num: block1.Number()},
		{Raw: raw2, Hash: expectedHash, Num: block2.Number()},
	}}

	dropped, err := batch.DecodeBlocks()
	var mismatch *BufferedBlockMetadataMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("DecodeBlocks err = %T %[1]v, want metadata mismatch", err)
	}
	if dropped.Num != block2.Number() || dropped.Hash != expectedHash {
		t.Fatalf("dropped = #%d %x, want block2 metadata hash %x", dropped.Num, dropped.Hash, expectedHash)
	}
	if mismatch.ExpectedNum != block2.Number() || mismatch.ExpectedHash != expectedHash || mismatch.GotNum != block2.Number() || mismatch.GotHash != block2.Hash() {
		t.Fatalf("mismatch = %+v, want expected staged metadata and decoded block2", mismatch)
	}
	if len(batch.Blocks) != 1 || batch.Blocks[0].Hash() != block1.Hash() {
		t.Fatalf("decoded prefix = %d blocks, want only block1", len(batch.Blocks))
	}
}

func TestValidateBufferedBlockMetadataRejectsNumberMismatch(t *testing.T) {
	block := testBufferedBlock(2)
	err := ValidateBufferedBlockMetadata(BufferedBlock{Num: 3, Hash: block.Hash()}, block)
	var mismatch *BufferedBlockMetadataMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("metadata validation err = %T %[1]v, want mismatch", err)
	}
	if mismatch.ExpectedNum != 3 || mismatch.GotNum != block.Number() || mismatch.ExpectedHash != block.Hash() || mismatch.GotHash != block.Hash() {
		t.Fatalf("mismatch = %+v, want expected #3 with decoded block2", mismatch)
	}
	if err := ValidateBufferedBlockMetadata(BufferedBlock{Num: block.Number(), Hash: block.Hash()}, block); err != nil {
		t.Fatalf("metadata validation with matching block failed: %v", err)
	}
}

func TestDecodeBufferedBatchAction(t *testing.T) {
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	raw1, err := block1.Marshal()
	if err != nil {
		t.Fatalf("marshal block1: %v", err)
	}
	raw2, err := block2.Marshal()
	if err != nil {
		t.Fatalf("marshal block2: %v", err)
	}

	full := BufferedBatch{Buffered: []BufferedBlock{
		{Raw: raw1, Hash: block1.Hash(), Num: block1.Number()},
		{Raw: raw2, Hash: block2.Hash(), Num: block2.Number()},
	}}
	if got := DecodeBufferedBatch(&full); got.Action != BufferedBatchDecodeImport || got.Err != nil || len(full.Blocks) != 2 {
		t.Fatalf("full decode = %+v blocks=%d, want import without error", got, len(full.Blocks))
	}

	firstBad := BufferedBatch{Buffered: []BufferedBlock{
		{Raw: []byte{0x01}, Hash: tcommon.Hash{0xee}, Num: 1},
		{Raw: raw2, Hash: block2.Hash(), Num: block2.Number()},
	}}
	if got := DecodeBufferedBatch(&firstBad); got.Action != BufferedBatchDecodeContinue || got.Err == nil || len(firstBad.Blocks) != 0 || got.Dropped.Num != 1 {
		t.Fatalf("first-bad decode = %+v blocks=%d, want continue with dropped #1", got, len(firstBad.Blocks))
	}

	prefix := BufferedBatch{Buffered: []BufferedBlock{
		{Raw: raw1, Hash: block1.Hash(), Num: block1.Number()},
		{Raw: []byte{0x02}, Hash: tcommon.Hash{0xdd}, Num: 2},
	}}
	if got := DecodeBufferedBatch(&prefix); got.Action != BufferedBatchDecodeImport || got.Err == nil || len(prefix.Blocks) != 1 || got.Dropped.Num != 2 {
		t.Fatalf("prefix decode = %+v blocks=%d, want import prefix with dropped #2", got, len(prefix.Blocks))
	}
}

func TestSummarizeAppliedBatch(t *testing.T) {
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	block2.Proto().Transactions = append(block2.Proto().Transactions, &corepb.Transaction{Signature: [][]byte{{0x02, 0x02}}})
	batch := BufferedBatch{
		Blocks: []*types.Block{block1, block2},
		Buffered: []BufferedBlock{
			{Hash: block1.Hash(), Num: block1.Number()},
			{Hash: block2.Hash(), Num: block2.Number()},
		},
	}

	got := SummarizeAppliedBatch(batch, 2)
	if !got.OK || got.Applied != 2 || got.TxCount != 3 || !got.HasStage {
		t.Fatalf("summary = %+v, want applied 2 txs 3 with stage", got)
	}
	if got.Last.Num != block2.Number() || got.Last.Hash != block2.Hash() {
		t.Fatalf("last = #%d %x, want block2 #%d %x", got.Last.Num, got.Last.Hash, block2.Number(), block2.Hash())
	}
}

func TestSummarizeAppliedBatchRejectsInvalidApplied(t *testing.T) {
	batch := BufferedBatch{Buffered: []BufferedBlock{{Num: 1}}}
	for _, applied := range []int{0, -1, 2} {
		if got := SummarizeAppliedBatch(batch, applied); got.OK {
			t.Fatalf("applied %d summary = %+v, want not ok", applied, got)
		}
	}
}

func TestSummarizeAppliedBatchCountsOnlyDecodedBlocks(t *testing.T) {
	block1 := testBufferedBlock(1)
	batch := BufferedBatch{
		Blocks: []*types.Block{block1},
		Buffered: []BufferedBlock{
			{Hash: block1.Hash(), Num: block1.Number()},
			{Hash: tcommon.Hash{0x02}, Num: 2},
		},
	}

	got := SummarizeAppliedBatch(batch, 2)
	if !got.OK || got.Applied != 2 || got.TxCount != 1 {
		t.Fatalf("summary = %+v, want applied 2 with tx count 1", got)
	}
	if got.Last.Num != 2 || !got.HasStage {
		t.Fatalf("last/stage = #%d %v, want block 2 stage", got.Last.Num, got.HasStage)
	}
}

func TestAppliedStagedBlockDeletes(t *testing.T) {
	first := BufferedBlock{Hash: tcommon.Hash{0x01}, Num: 1}
	second := BufferedBlock{Hash: tcommon.Hash{0x02}, Num: 2}
	third := BufferedBlock{Hash: tcommon.Hash{0x03}, Num: 3}

	got := AppliedStagedBlockDeletes(BufferedBatch{Buffered: []BufferedBlock{first, second, third}}, 2)
	if len(got) != 2 {
		t.Fatalf("deletes = %d, want 2", len(got))
	}
	if got[0].Number != 1 || got[0].Hash != first.Hash || got[1].Number != 2 || got[1].Hash != second.Hash {
		t.Fatalf("deletes = %+v, want first two buffered blocks", got)
	}
}

func TestAppliedStagedBlockDeletesClampsInvalidApplied(t *testing.T) {
	first := BufferedBlock{Hash: tcommon.Hash{0x01}, Num: 1}

	if got := AppliedStagedBlockDeletes(BufferedBatch{Buffered: []BufferedBlock{first}}, 0); got != nil {
		t.Fatalf("zero applied deletes = %+v, want nil", got)
	}
	got := AppliedStagedBlockDeletes(BufferedBatch{Buffered: []BufferedBlock{first}}, 3)
	if len(got) != 1 || got[0].Number != first.Num || got[0].Hash != first.Hash {
		t.Fatalf("clamped deletes = %+v, want first buffered block", got)
	}
}

func TestResolveImportFailureUsesRangeErrorIndex(t *testing.T) {
	first := BufferedBlock{Hash: tcommon.Hash{0x01}, Num: 1}
	second := BufferedBlock{Hash: tcommon.Hash{0x02}, Num: 2}
	err := &core.InsertBlocksError{Index: 1, BlockNumber: 2, Err: errors.New("bad block")}

	got := ResolveImportFailure(BufferedBatch{Buffered: []BufferedBlock{first, second}}, err)
	if !got.OK || got.Applied != 1 || got.FailedIndex != 1 || got.Failed.Num != 2 || got.FailedNum != 2 {
		t.Fatalf("resolution = %+v, want failed block2 with applied prefix 1", got)
	}
}

func TestResolveImportFailureFallsBackToFirstBlock(t *testing.T) {
	first := BufferedBlock{Hash: tcommon.Hash{0x01}, Num: 1}
	second := BufferedBlock{Hash: tcommon.Hash{0x02}, Num: 2}

	got := ResolveImportFailure(BufferedBatch{Buffered: []BufferedBlock{first, second}}, errors.New("plain insert failure"))
	if !got.OK || got.Applied != 0 || got.FailedIndex != 0 || got.Failed.Num != 1 || got.FailedNum != 1 {
		t.Fatalf("resolution = %+v, want failed first block", got)
	}
}

func TestResolveImportFailureFallsBackToRangeBlockNumber(t *testing.T) {
	err := &core.InsertBlocksError{Index: 0, BlockNumber: 99, Err: errors.New("bad block")}

	got := ResolveImportFailure(BufferedBatch{Buffered: []BufferedBlock{{Hash: tcommon.Hash{0x01}}}}, err)
	if !got.OK || got.FailedNum != 99 {
		t.Fatalf("resolution = %+v, want fallback block number 99", got)
	}
}

func TestResolveImportFailureRejectsNilOrEmpty(t *testing.T) {
	if got := ResolveImportFailure(BufferedBatch{}, errors.New("bad block")); got.OK {
		t.Fatalf("empty batch resolution = %+v, want not ok", got)
	}
	if got := ResolveImportFailure(BufferedBatch{Buffered: []BufferedBlock{{Num: 1}}}, nil); got.OK {
		t.Fatalf("nil error resolution = %+v, want not ok", got)
	}
}

func TestPlanImportOutcome(t *testing.T) {
	first := BufferedBlock{Hash: tcommon.Hash{0x01}, Num: 1}
	second := BufferedBlock{Hash: tcommon.Hash{0x02}, Num: 2}
	batch := BufferedBatch{
		Blocks:   []*types.Block{testBufferedBlock(1), testBufferedBlock(2)},
		Buffered: []BufferedBlock{first, second},
	}

	ok := PlanImportOutcome(batch, nil)
	if ok.Applied != 2 || !ok.RecordApplied || ok.Pause || ok.StopDrain {
		t.Fatalf("success outcome = %+v, want record full batch without pause", ok)
	}

	rangeErr := &core.InsertBlocksError{Index: 1, BlockNumber: 2, Err: errors.New("bad block")}
	partial := PlanImportOutcome(batch, rangeErr)
	if partial.Applied != 1 || !partial.RecordApplied || !partial.Pause || !partial.StopDrain || partial.PauseNum != 2 {
		t.Fatalf("partial outcome = %+v, want record prefix and pause at block2", partial)
	}

	firstErr := PlanImportOutcome(batch, errors.New("plain insert failure"))
	if firstErr.Applied != 0 || firstErr.RecordApplied || !firstErr.Pause || !firstErr.StopDrain || firstErr.PauseNum != 1 {
		t.Fatalf("first failure outcome = %+v, want pause at block1 without record", firstErr)
	}

	unmapped := PlanImportOutcome(BufferedBatch{}, errors.New("bad block"))
	if unmapped.RecordApplied || !unmapped.Pause || !unmapped.StopDrain || unmapped.PauseNum != 0 {
		t.Fatalf("unmapped outcome = %+v, want generic pause", unmapped)
	}
}

func TestPlanImportBatchRunSettlement(t *testing.T) {
	tests := map[string]struct {
		result ImportBatchRunResult
		want   ImportBatchRunSettlementPlan
	}{
		"successful import continues": {
			result: ImportBatchRunResult{Outcome: ImportOutcome{RecordApplied: true}},
			want:   ImportBatchRunSettlementPlan{ContinueDrain: true},
		},
		"decode drop continues": {
			result: ImportBatchRunResult{ContinueDrain: true},
			want:   ImportBatchRunSettlementPlan{ContinueDrain: true},
		},
		"canonical failure stops": {
			result: ImportBatchRunResult{StopDrain: true, Outcome: ImportOutcome{Pause: true, StopDrain: true}},
			want:   ImportBatchRunSettlementPlan{StopDrain: true},
		},
	}
	for name, test := range tests {
		if got := PlanImportBatchRunSettlement(test.result); !reflect.DeepEqual(got, test.want) {
			t.Fatalf("%s settlement = %+v, want %+v", name, got, test.want)
		}
	}
}

func TestPlanImportBatchExecutionSchedulesDecodedTarget(t *testing.T) {
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	batch := testImportRunBatch(t, block1, block2)
	if got := DecodeBufferedBatch(&batch); got.Action != BufferedBatchDecodeImport || got.Err != nil {
		t.Fatalf("decode = %+v, want import", got)
	}

	got := PlanImportBatchExecution(batch)
	if len(got.Blocks) != 2 || got.Blocks[0].Number() != block1.Number() || got.Blocks[0].Hash() != block1.Hash() || got.Blocks[1].Number() != block2.Number() || got.Blocks[1].Hash() != block2.Hash() {
		t.Fatalf("execution blocks = %+v, want decoded block1/block2", got.Blocks)
	}
	if !got.HasStageSchedule || got.Schedule.BlockNum != block2.Number() || got.Schedule.BlockHash != block2.Hash() {
		t.Fatalf("execution schedule = %+v has=%v, want block2", got.Schedule, got.HasStageSchedule)
	}
	if len(got.Schedules) != 2 || got.Schedules[0].BlockNum != block1.Number() || got.Schedules[1].BlockNum != block2.Number() {
		t.Fatalf("execution schedules = %+v, want block1/block2", got.Schedules)
	}
	if len(got.StagePlan.Schedules) != 2 || len(got.StagePlan.Bodies) != 2 || len(got.StagePlan.Execution) != 2 || len(got.StagePlan.Commitment) != 2 || len(got.StagePlan.Finish) != 2 {
		t.Fatalf("stage plan phases = schedules:%d bodies:%d execution:%d commitment:%d finish:%d, want two per phase",
			len(got.StagePlan.Schedules), len(got.StagePlan.Bodies), len(got.StagePlan.Execution), len(got.StagePlan.Commitment), len(got.StagePlan.Finish))
	}
	if phases := got.StagePlan.PhasePlans(); len(phases) != 4 || phases[0].Phase != ImportStagePhaseBodies || phases[1].Phase != ImportStagePhaseExecution || phases[2].Phase != ImportStagePhaseCommitment || phases[3].Phase != ImportStagePhaseFinish {
		t.Fatalf("stage phase plans = %+v, want bodies/execution/commitment/finish", phases)
	}
	if got.StagePhases.Empty() ||
		!got.StagePhases.HasBody ||
		!got.StagePhases.HasExecution ||
		!got.StagePhases.HasCommitment ||
		!got.StagePhases.HasFinish ||
		len(got.StagePhases.PostBody) != 3 ||
		len(got.StagePhases.PostBodyTasks) != 6 ||
		got.StagePhases.Execution.Tasks[1] != ImportExecutionStageTask(block2.Number(), block2.Hash()) ||
		got.StagePhases.Commitment.Tasks[1] != ImportCommitmentStageTask(block2.Number(), block2.Hash()) ||
		got.StagePhases.Finish.Tasks[1] != ImportFinishStageTask(block2.Number(), block2.Hash()) {
		t.Fatalf("stage phase schedule = %+v, want explicit bodies plus execution/commitment/finish phases", got.StagePhases)
	}
	phaseSchedule := got.PhaseSchedule()
	if phaseSchedule.Empty() || len(phaseSchedule.Phases) != 4 || phaseSchedule.Execution.Tasks[1] != ImportExecutionStageTask(block2.Number(), block2.Hash()) {
		t.Fatalf("PhaseSchedule = %+v, want explicit four-phase schedule", phaseSchedule)
	}
	if len(got.StagePlan.PostBody) != 6 || len(got.StagePlan.Tasks) != 8 {
		t.Fatalf("stage plan task counts = postBody:%d tasks:%d, want 6/8", len(got.StagePlan.PostBody), len(got.StagePlan.Tasks))
	}
	if got.Diagnostics.PlannedBlocks != 2 || got.Diagnostics.PlannedStages != 8 ||
		got.Diagnostics.PlannedBodyStages != 2 || got.Diagnostics.PlannedPostBodyStages != 6 ||
		got.Diagnostics.PlannedExecutionStages != 2 || got.Diagnostics.PlannedCommitmentStages != 2 || got.Diagnostics.PlannedFinishStages != 2 ||
		got.Diagnostics.FirstBlockNum != block1.Number() || got.Diagnostics.FirstBlockHash != block1.Hash() ||
		got.Diagnostics.LastBlockNum != block2.Number() || got.Diagnostics.LastBlockHash != block2.Hash() {
		t.Fatalf("execution diagnostics = %+v, want planned block1..block2 with 8 stages and 2 per phase", got.Diagnostics)
	}
	if got.StagePlan.Bodies[0] != ImportBodyStageTask(block1.Number(), block1.Hash()) || got.StagePlan.Execution[1] != ImportExecutionStageTask(block2.Number(), block2.Hash()) || got.StagePlan.Finish[1] != ImportFinishStageTask(block2.Number(), block2.Hash()) {
		t.Fatalf("stage plan grouped tasks = bodies:%+v execution:%+v finish:%+v, want block1 body and block2 execution/finish",
			got.StagePlan.Bodies, got.StagePlan.Execution, got.StagePlan.Finish)
	}
	applied1, ok := got.AppliedSchedule(1)
	if !ok || applied1.BlockNum != block1.Number() || applied1.BlockHash != block1.Hash() {
		t.Fatalf("applied schedule 1 = %+v ok=%v, want block1", applied1, ok)
	}
	applied2, ok := got.AppliedSchedule(2)
	if !ok || applied2.BlockNum != block2.Number() || applied2.BlockHash != block2.Hash() {
		t.Fatalf("applied schedule 2 = %+v ok=%v, want block2", applied2, ok)
	}
	appliedPlan1, ok := got.AppliedStagePlan(1)
	if !ok || len(appliedPlan1.Schedules) != 1 || len(appliedPlan1.Tasks) != 4 || appliedPlan1.Schedules[0].BlockNum != block1.Number() {
		t.Fatalf("applied stage plan 1 = %+v ok=%v, want one-block explicit stage plan", appliedPlan1, ok)
	}
	appliedPhases1, ok := got.AppliedPhaseSchedule(1)
	if !ok ||
		appliedPhases1.Empty() ||
		len(appliedPhases1.Phases) != 4 ||
		len(appliedPhases1.Execution.Tasks) != 1 ||
		appliedPhases1.Execution.Tasks[0] != ImportExecutionStageTask(block1.Number(), block1.Hash()) ||
		appliedPhases1.Commitment.Tasks[0] != ImportCommitmentStageTask(block1.Number(), block1.Hash()) ||
		appliedPhases1.Finish.Tasks[0] != ImportFinishStageTask(block1.Number(), block1.Hash()) {
		t.Fatalf("applied phase schedule 1 = %+v ok=%v, want one-block explicit phase schedule", appliedPhases1, ok)
	}
	appliedPlan2, ok := got.AppliedStagePlan(2)
	if !ok || len(appliedPlan2.Schedules) != 2 || len(appliedPlan2.Tasks) != 8 || appliedPlan2.Schedules[1].BlockNum != block2.Number() {
		t.Fatalf("applied stage plan 2 = %+v ok=%v, want two-block explicit stage plan", appliedPlan2, ok)
	}
	appliedPhases2, ok := got.AppliedPhaseSchedule(2)
	if !ok ||
		appliedPhases2.Empty() ||
		len(appliedPhases2.Phases) != 4 ||
		len(appliedPhases2.Execution.Tasks) != 2 ||
		appliedPhases2.Execution.Tasks[1] != ImportExecutionStageTask(block2.Number(), block2.Hash()) ||
		appliedPhases2.Commitment.Tasks[1] != ImportCommitmentStageTask(block2.Number(), block2.Hash()) ||
		appliedPhases2.Finish.Tasks[1] != ImportFinishStageTask(block2.Number(), block2.Hash()) {
		t.Fatalf("applied phase schedule 2 = %+v ok=%v, want two-block explicit phase schedule", appliedPhases2, ok)
	}
	for _, applied := range []int{0, 3} {
		if schedule, ok := got.AppliedSchedule(applied); ok {
			t.Fatalf("applied schedule %d = %+v ok=true, want false", applied, schedule)
		}
		if stagePlan, ok := got.AppliedStagePlan(applied); ok {
			t.Fatalf("applied stage plan %d = %+v ok=true, want false", applied, stagePlan)
		}
		if phaseSchedule, ok := got.AppliedPhaseSchedule(applied); ok {
			t.Fatalf("applied phase schedule %d = %+v ok=true, want false", applied, phaseSchedule)
		}
	}
	if !reflect.DeepEqual(got.Schedule.Tasks, ImportPipelineStageTasks(block2.Number(), block2.Hash())) {
		t.Fatalf("execution schedule tasks = %+v, want full import pipeline", got.Schedule.Tasks)
	}
}

func TestImportBatchExecutionPlanStageObserverFiltersToPlannedSchedules(t *testing.T) {
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	batch := testImportRunBatch(t, block1, block2)
	if got := DecodeBufferedBatch(&batch); got.Action != BufferedBatchDecodeImport || got.Err != nil {
		t.Fatalf("decode = %+v, want import", got)
	}
	execution := PlanImportBatchExecution(batch)

	type observedStage struct {
		stage rawdb.StageID
		num   uint64
		hash  tcommon.Hash
	}
	var observed []observedStage
	observer := execution.StageObserver(func(stage rawdb.StageID, blockNum uint64, blockHash tcommon.Hash) {
		observed = append(observed, observedStage{stage: stage, num: blockNum, hash: blockHash})
	})

	observer(rawdb.StageBodies, block1.Number(), block1.Hash())
	observer(rawdb.StageCommitment, block2.Number(), block2.Hash())
	observer(rawdb.StageExecution, block2.Number(), tcommon.Hash{0xee})
	observer(rawdb.StageFinish, block2.Number()+1, block2.Hash())
	observer(rawdb.StageHeaders, block2.Number(), block2.Hash())

	want := []observedStage{
		{stage: rawdb.StageBodies, num: block1.Number(), hash: block1.Hash()},
		{stage: rawdb.StageCommitment, num: block2.Number(), hash: block2.Hash()},
	}
	if !reflect.DeepEqual(observed, want) {
		t.Fatalf("observed = %+v, want only planned observations %+v", observed, want)
	}
	if !execution.PlansStageObservation(rawdb.StageFinish, block2.Number(), block2.Hash()) {
		t.Fatal("finish block2 should be planned")
	}
	observation, ok := execution.PlannedStageObservation(rawdb.StageCommitment, block2.Number(), block2.Hash())
	if !ok || observation.Task != ImportCommitmentStageTask(block2.Number(), block2.Hash()) || observation.Phase.Phase != ImportStagePhaseCommitment || observation.Phase.CanonicalStage != rawdb.StageCommitment || observation.Phase.SyncStage != rawdb.StageSyncCommitment || len(observation.Phase.Tasks) != 2 {
		t.Fatalf("planned stage observation = %+v ok=%v, want block2 commitment in two-task commitment phase", observation, ok)
	}
	if task, ok := execution.StagePlan.MatchCanonicalObservation(rawdb.StageCommitment, block2.Number(), block2.Hash()); !ok || task != ImportCommitmentStageTask(block2.Number(), block2.Hash()) {
		t.Fatalf("batch stage plan commitment match = %+v ok=%v, want block2 commitment task", task, ok)
	}
	if execution.PlansStageObservation(rawdb.StageFinish, block2.Number(), tcommon.Hash{0xee}) {
		t.Fatal("fork-hash finish should not be planned")
	}
	if observation, ok := execution.PlannedStageObservation(rawdb.StageFinish, block2.Number(), tcommon.Hash{0xee}); ok {
		t.Fatalf("fork-hash finish observation = %+v ok=true, want rejected", observation)
	}
}

func TestImportBatchExecutionPlanStageProgressObserverRecordsPlannedObservations(t *testing.T) {
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	batch := testImportRunBatch(t, block1, block2)
	if got := DecodeBufferedBatch(&batch); got.Action != BufferedBatchDecodeImport || got.Err != nil {
		t.Fatalf("decode = %+v, want import", got)
	}
	execution := PlanImportBatchExecution(batch)
	collector := NewStageProgressCollector()
	observer := execution.StageProgressObserver(collector)

	observer(rawdb.StageBodies, block2.Number(), block2.Hash())
	observer(rawdb.StageExecution, block2.Number(), block2.Hash())
	observer(rawdb.StageCommitment, block2.Number(), tcommon.Hash{0xee})

	want := []rawdb.StageProgress{
		{Stage: rawdb.StageSyncImport, BlockNum: block2.Number(), BlockHash: block2.Hash(), HasBlockHash: true},
		{Stage: rawdb.StageSyncExecution, BlockNum: block2.Number(), BlockHash: block2.Hash(), HasBlockHash: true},
	}
	if rows := collector.RowsForSchedule(NewImportStageSchedule(block2.Number(), block2.Hash())); !reflect.DeepEqual(rows, want) {
		t.Fatalf("planned progress observer rows = %+v, want %+v", rows, want)
	}
	if execution.StageProgressObserver(nil) != nil {
		t.Fatal("nil collector produced progress observer")
	}
}

func TestImportBatchExecutionPlanStageObserverDropsUnplannedObservations(t *testing.T) {
	var observed []rawdb.StageID
	observer := (ImportBatchExecutionPlan{}).StageObserver(func(stage rawdb.StageID, _ uint64, _ tcommon.Hash) {
		observed = append(observed, stage)
	})
	if observer == nil {
		t.Fatal("empty execution plan observer = nil, want no-op observer")
	}
	observer(rawdb.StageBodies, 1, tcommon.Hash{0x01})
	observer(rawdb.StageExecution, 1, tcommon.Hash{0x01})
	observer(rawdb.StageCommitment, 1, tcommon.Hash{0x01})
	observer(rawdb.StageFinish, 1, tcommon.Hash{0x01})
	if len(observed) != 0 {
		t.Fatalf("observed unplanned stages = %+v, want none", observed)
	}
	if (ImportBatchExecutionPlan{}).PlansStageObservation(rawdb.StageFinish, 1, tcommon.Hash{0x01}) {
		t.Fatal("empty execution plan reported a planned finish observation")
	}
}

func TestNewImportBatchRunPlanSchedulesExecutionPlanningBeforeExecute(t *testing.T) {
	block := testBufferedBlock(1)
	plan := NewImportBatchRunPlan(testImportRunBatch(t, block))

	var got []ImportBatchRunStepAction
	for _, step := range plan.Steps {
		got = append(got, step.Action)
	}
	want := []ImportBatchRunStepAction{
		ImportBatchRunDecode,
		ImportBatchRunRecordBufferWaits,
		ImportBatchRunPlanExecution,
		ImportBatchRunPlanStagePhases,
		ImportBatchRunExecute,
		ImportBatchRunSettle,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("run plan steps = %+v, want %+v", got, want)
	}
}

func TestApplyImportBatchRunPlanSuccess(t *testing.T) {
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	batch := testImportRunBatch(t, block1, block2)
	batch.BufferWaits = []time.Duration{time.Second, 2 * time.Second}
	applier := &recordingImportBatchRunApplier{elapsed: 3 * time.Millisecond}

	result := ApplyImportBatchRunPlan(NewImportBatchRunPlan(batch), applier)

	if result.ContinueDrain || result.StopDrain || result.Outcome.Applied != 2 || !result.Outcome.RecordApplied {
		t.Fatalf("result = %+v, want applied success without stop", result)
	}
	wantCalls := []ImportBatchRunStepAction{
		ImportBatchRunDecode,
		ImportBatchRunRecordBufferWaits,
		ImportBatchRunRecordBufferWaits,
		ImportBatchRunExecute,
		ImportBatchRunSettle,
	}
	if !reflect.DeepEqual(applier.calls, wantCalls) {
		t.Fatalf("calls = %+v, want %+v", applier.calls, wantCalls)
	}
	wantSteps := []ImportBatchRunStepAction{
		ImportBatchRunDecode,
		ImportBatchRunRecordBufferWaits,
		ImportBatchRunPlanExecution,
		ImportBatchRunPlanStagePhases,
		ImportBatchRunExecute,
		ImportBatchRunSettle,
	}
	if !reflect.DeepEqual(result.Steps, wantSteps) {
		t.Fatalf("result steps = %+v, want %+v", result.Steps, wantSteps)
	}
	if !reflect.DeepEqual(applier.waits, batch.BufferWaits) {
		t.Fatalf("waits = %v, want %v", applier.waits, batch.BufferWaits)
	}
	if result.Execution.Schedule.BlockNum != block2.Number() || !result.Execution.HasStageSchedule {
		t.Fatalf("result execution schedule = %+v has=%v, want block2", result.Execution.Schedule, result.Execution.HasStageSchedule)
	}
	if result.ExecutionDiagnostics.PlannedBlocks != 2 || result.ExecutionDiagnostics.PlannedStages != 8 ||
		result.ExecutionDiagnostics.PlannedBodyStages != 2 || result.ExecutionDiagnostics.PlannedPostBodyStages != 6 ||
		result.ExecutionDiagnostics.PlannedExecutionStages != 2 || result.ExecutionDiagnostics.PlannedCommitmentStages != 2 || result.ExecutionDiagnostics.PlannedFinishStages != 2 {
		t.Fatalf("result execution diagnostics = %+v, want planned two-block batch", result.ExecutionDiagnostics)
	}
	if len(result.ExecutionPhases) != 4 ||
		result.ExecutionPhases[0].Phase != ImportStagePhaseBodies ||
		result.ExecutionPhases[1].Phase != ImportStagePhaseExecution ||
		result.ExecutionPhases[2].Phase != ImportStagePhaseCommitment ||
		result.ExecutionPhases[3].Phase != ImportStagePhaseFinish ||
		len(result.ExecutionPhases[1].Tasks) != 2 ||
		result.ExecutionPhases[1].Tasks[1] != ImportExecutionStageTask(block2.Number(), block2.Hash()) {
		t.Fatalf("result execution phases = %+v, want two-block bodies/execution/commitment/finish plan", result.ExecutionPhases)
	}
	if result.StagePhaseSchedule.Empty() ||
		len(result.StagePhaseSchedule.PostBody) != 3 ||
		result.StagePhaseSchedule.Execution.Tasks[1] != ImportExecutionStageTask(block2.Number(), block2.Hash()) ||
		result.StagePhaseSchedule.Commitment.Tasks[1] != ImportCommitmentStageTask(block2.Number(), block2.Hash()) ||
		result.StagePhaseSchedule.Finish.Tasks[1] != ImportFinishStageTask(block2.Number(), block2.Hash()) {
		t.Fatalf("result stage phase schedule = %+v, want explicit two-block post-body phases", result.StagePhaseSchedule)
	}
	if !reflect.DeepEqual(applier.execution.Schedule.Tasks, ImportPipelineStageTasks(block2.Number(), block2.Hash())) {
		t.Fatalf("applier execution tasks = %+v, want block2 pipeline", applier.execution.Schedule.Tasks)
	}
	if !result.Progress.OK || result.Progress.Summary.Applied != 2 || !result.Progress.StagePlan.Complete {
		t.Fatalf("result progress plan = %+v, want complete applied block2 plan", result.Progress)
	}
	if !result.StageDiagnostics.Complete || result.StageDiagnostics.Completed != 8 || result.StageDiagnostics.Scheduled != 8 {
		t.Fatalf("result stage diagnostics = %+v, want complete 8/8", result.StageDiagnostics)
	}
	if !applier.recordPlan.OK || applier.recordPlan.ReportHead != block2.Number() || applier.recordPlan.StatsBlocks != 2 {
		t.Fatalf("applier progress plan = %+v, want block2 report with two stats blocks", applier.recordPlan)
	}
	if !applier.recordPlan.StageDiagnostics.Complete || applier.recordPlan.StageDiagnostics.Completed != 8 || applier.recordPlan.StageDiagnostics.Scheduled != 8 {
		t.Fatalf("applier stage diagnostics = %+v, want complete 8/8", applier.recordPlan.StageDiagnostics)
	}
	if applier.recordPlan.ExecutionDiagnostics.PlannedBlocks != 2 || applier.recordPlan.ExecutionDiagnostics.LastBlockNum != block2.Number() {
		t.Fatalf("applier execution diagnostics = %+v, want two planned blocks through block2", applier.recordPlan.ExecutionDiagnostics)
	}
	if applier.recordPlan.AppliedDiagnostics.PlannedBlocks != 2 || applier.recordPlan.AppliedDiagnostics.PlannedStages != 8 ||
		applier.recordPlan.AppliedDiagnostics.PlannedBodyStages != 2 || applier.recordPlan.AppliedDiagnostics.PlannedExecutionStages != 2 ||
		applier.recordPlan.AppliedDiagnostics.PlannedCommitmentStages != 2 || applier.recordPlan.AppliedDiagnostics.PlannedFinishStages != 2 ||
		applier.recordPlan.AppliedDiagnostics.LastBlockNum != block2.Number() {
		t.Fatalf("applier applied diagnostics = %+v, want two applied planned blocks through block2", applier.recordPlan.AppliedDiagnostics)
	}
	if len(applier.recordPlan.AppliedStagePlan.Tasks) != 8 || len(applier.recordPlan.AppliedStagePlan.Execution) != 2 {
		t.Fatalf("applier applied stage plan = %+v, want two-block execution/commitment/finish prefix", applier.recordPlan.AppliedStagePlan)
	}
	if result.Progress.AppliedStagePhases.Empty() ||
		len(result.Progress.AppliedPhases) != 4 ||
		result.Progress.AppliedStagePhases.Execution.Tasks[1] != ImportExecutionStageTask(block2.Number(), block2.Hash()) ||
		result.Progress.AppliedStagePhases.Commitment.Tasks[1] != ImportCommitmentStageTask(block2.Number(), block2.Hash()) ||
		result.Progress.AppliedStagePhases.Finish.Tasks[1] != ImportFinishStageTask(block2.Number(), block2.Hash()) {
		t.Fatalf("result applied phase schedule = %+v phases=%+v, want two-block phase prefix",
			result.Progress.AppliedStagePhases, result.Progress.AppliedPhases)
	}
	if applier.recordPlan.AppliedStagePhases.Empty() ||
		len(applier.recordPlan.AppliedPhases) != 4 ||
		applier.recordPlan.AppliedStagePhases.Finish.Tasks[1] != ImportFinishStageTask(block2.Number(), block2.Hash()) {
		t.Fatalf("applier applied phase schedule = %+v phases=%+v, want two-block phase prefix",
			applier.recordPlan.AppliedStagePhases, applier.recordPlan.AppliedPhases)
	}
	wantProgress := importPipelineProgressRows(block2.Number(), block2.Hash())
	if !reflect.DeepEqual(applier.progress, wantProgress) {
		t.Fatalf("progress = %+v, want %+v", applier.progress, wantProgress)
	}
	if applier.recordApplied != 2 || applier.recordElapsed != applier.elapsed {
		t.Fatalf("record applied/elapsed = %d/%s, want 2/%s", applier.recordApplied, applier.recordElapsed, applier.elapsed)
	}
	if !result.HasRecord ||
		result.RecordPlan.Elapsed != applier.elapsed ||
		!reflect.DeepEqual(result.RecordPlan.Progress, result.Progress) ||
		!reflect.DeepEqual(applier.recordRunPlan, result.RecordPlan) {
		t.Fatalf("record plan result=%+v has=%v applier=%+v, want downloader-owned plan for imported progress",
			result.RecordPlan, result.HasRecord, applier.recordRunPlan)
	}
}

func TestApplyImportBatchRunPlanSuccessWithHalfExecutedStageObservations(t *testing.T) {
	block := testBufferedBlock(1)
	applier := &recordingImportBatchRunApplier{
		observedStages: []rawdb.StageID{rawdb.StageBodies, rawdb.StageExecution},
	}

	result := ApplyImportBatchRunPlan(NewImportBatchRunPlan(testImportRunBatch(t, block)), applier)

	if result.ContinueDrain || result.StopDrain || result.Outcome.Applied != 1 || !result.Outcome.RecordApplied {
		t.Fatalf("result = %+v, want successful one-block import", result)
	}
	if !result.Progress.OK || result.Progress.Summary.Applied != 1 || !applier.recordPlan.OK {
		t.Fatalf("progress result=%+v record=%+v, want recorded one-block progress", result.Progress, applier.recordPlan)
	}
	if !result.HasRecord || !reflect.DeepEqual(result.RecordPlan.Progress, result.Progress) {
		t.Fatalf("record plan = %+v has=%v, want planned one-block progress record", result.RecordPlan, result.HasRecord)
	}
	wantProgress := []rawdb.StageProgress{
		{Stage: rawdb.StageSyncImport, BlockNum: block.Number(), BlockHash: block.Hash(), HasBlockHash: true},
		{Stage: rawdb.StageSyncExecution, BlockNum: block.Number(), BlockHash: block.Hash(), HasBlockHash: true},
	}
	if !reflect.DeepEqual(result.Progress.Progress, wantProgress) || !reflect.DeepEqual(applier.progress, wantProgress) {
		t.Fatalf("progress result=%+v recorded=%+v, want body/execution prefix %+v", result.Progress.Progress, applier.progress, wantProgress)
	}
	if result.StageDiagnostics.Complete || result.StageDiagnostics.Completed != 2 || result.StageDiagnostics.Scheduled != 4 ||
		!result.StageDiagnostics.HasBlocked ||
		result.StageDiagnostics.NextPhase != ImportStagePhaseCommitment ||
		result.StageDiagnostics.NextCanonicalStage != rawdb.StageCommitment ||
		result.StageDiagnostics.NextStage != rawdb.StageSyncCommitment ||
		result.StageDiagnostics.BlockedStatus != ImportStageProgressMissing {
		t.Fatalf("stage diagnostics = %+v, want blocked commitment after half execution", result.StageDiagnostics)
	}
	if !reflect.DeepEqual(applier.recordPlan.StageDiagnostics, result.StageDiagnostics) {
		t.Fatalf("recorded stage diagnostics = %+v, want result diagnostics %+v", applier.recordPlan.StageDiagnostics, result.StageDiagnostics)
	}
	if result.Progress.AppliedDiagnostics.PlannedBlocks != 1 ||
		result.Progress.AppliedDiagnostics.PlannedStages != 4 ||
		result.Progress.AppliedDiagnostics.PlannedCommitmentStages != 1 ||
		result.Progress.AppliedDiagnostics.PlannedFinishStages != 1 {
		t.Fatalf("applied diagnostics = %+v, want full one-block planned stage graph", result.Progress.AppliedDiagnostics)
	}
	for _, row := range applier.progress {
		if row.Stage == rawdb.StageSyncCommitment || row.Stage == rawdb.StageSyncFinish {
			t.Fatalf("published downstream row %+v after missing commitment/finish observations", row)
		}
	}
}

func TestApplyImportBatchRunPlanPartialFailureRecordsPrefixAndPauses(t *testing.T) {
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	insertErr := &core.InsertBlocksError{Index: 1, BlockNumber: block2.Number(), Err: errors.New("bad block")}
	applier := &recordingImportBatchRunApplier{insertErr: insertErr, appliedForObservations: 1}

	result := ApplyImportBatchRunPlan(NewImportBatchRunPlan(testImportRunBatch(t, block1, block2)), applier)

	if !result.StopDrain || !result.Outcome.Pause || result.Outcome.PauseNum != block2.Number() {
		t.Fatalf("result = %+v, want pause at block2 and stop drain", result)
	}
	if result.Outcome.Applied != 1 || !result.Outcome.RecordApplied || applier.recordApplied != 1 {
		t.Fatalf("applied = result %d record %d, want prefix 1", result.Outcome.Applied, applier.recordApplied)
	}
	if !result.Execution.HasStageSchedule || result.Execution.Schedule.BlockNum != block2.Number() {
		t.Fatalf("execution schedule = %+v has=%v, want attempted block2", result.Execution.Schedule, result.Execution.HasStageSchedule)
	}
	if len(result.ExecutionPhases) != 4 ||
		len(result.ExecutionPhases[0].Tasks) != 2 ||
		result.ExecutionPhases[0].Tasks[1] != ImportBodyStageTask(block2.Number(), block2.Hash()) ||
		len(result.ExecutionPhases[3].Tasks) != 2 ||
		result.ExecutionPhases[3].Tasks[1] != ImportFinishStageTask(block2.Number(), block2.Hash()) {
		t.Fatalf("partial result execution phases = %+v, want attempted two-block phase plan", result.ExecutionPhases)
	}
	if result.StagePhaseSchedule.Empty() ||
		len(result.StagePhaseSchedule.PostBody) != 3 ||
		result.StagePhaseSchedule.Finish.Tasks[1] != ImportFinishStageTask(block2.Number(), block2.Hash()) {
		t.Fatalf("partial result stage phase schedule = %+v, want attempted two-block post-body plan", result.StagePhaseSchedule)
	}
	if !result.Progress.OK || result.Progress.Summary.Applied != 1 || result.Progress.ReportHead != block1.Number() {
		t.Fatalf("result progress plan = %+v, want applied block1 prefix", result.Progress)
	}
	if !result.HasRecord || result.RecordPlan.Elapsed != applier.elapsed || !reflect.DeepEqual(result.RecordPlan.Progress, result.Progress) {
		t.Fatalf("partial record plan = %+v has=%v, want downloader-owned applied-prefix record plan", result.RecordPlan, result.HasRecord)
	}
	appliedSchedule, ok := result.Execution.AppliedSchedule(1)
	if !ok {
		t.Fatal("execution applied schedule 1 missing")
	}
	if !reflect.DeepEqual(result.Progress.Schedule.Tasks, appliedSchedule.Tasks) {
		t.Fatalf("progress schedule = %+v, want execution applied schedule %+v", result.Progress.Schedule.Tasks, appliedSchedule.Tasks)
	}
	if result.Progress.Schedule.BlockNum != block1.Number() || result.Progress.Schedule.BlockHash != block1.Hash() {
		t.Fatalf("progress schedule = %+v, want block1 applied prefix", result.Progress.Schedule)
	}
	if !result.StageDiagnostics.Complete || result.StageDiagnostics.Completed != 4 {
		t.Fatalf("partial result stage diagnostics = %+v, want complete applied-prefix plan", result.StageDiagnostics)
	}
	if result.Progress.ExecutionDiagnostics.PlannedBlocks != 2 || result.Progress.ExecutionDiagnostics.LastBlockNum != block2.Number() {
		t.Fatalf("partial execution diagnostics = %+v, want attempted two-block plan", result.Progress.ExecutionDiagnostics)
	}
	if result.Progress.AppliedDiagnostics.PlannedBlocks != 1 || result.Progress.AppliedDiagnostics.PlannedStages != 4 ||
		result.Progress.AppliedDiagnostics.PlannedBodyStages != 1 || result.Progress.AppliedDiagnostics.PlannedExecutionStages != 1 ||
		result.Progress.AppliedDiagnostics.PlannedCommitmentStages != 1 || result.Progress.AppliedDiagnostics.PlannedFinishStages != 1 ||
		result.Progress.AppliedDiagnostics.LastBlockNum != block1.Number() {
		t.Fatalf("partial applied diagnostics = %+v, want one-block applied prefix", result.Progress.AppliedDiagnostics)
	}
	if len(result.Progress.AppliedStagePlan.Tasks) != 4 || result.Progress.AppliedStagePlan.Finish[0] != ImportFinishStageTask(block1.Number(), block1.Hash()) {
		t.Fatalf("partial applied stage plan = %+v, want block1 execution/commitment/finish prefix", result.Progress.AppliedStagePlan)
	}
	if result.Progress.AppliedStagePhases.Empty() ||
		len(result.Progress.AppliedPhases) != 4 ||
		len(result.Progress.AppliedStagePhases.Finish.Tasks) != 1 ||
		result.Progress.AppliedStagePhases.Finish.Tasks[0] != ImportFinishStageTask(block1.Number(), block1.Hash()) {
		t.Fatalf("partial applied phase schedule = %+v phases=%+v, want block1 bodies/execution/commitment/finish prefix",
			result.Progress.AppliedStagePhases, result.Progress.AppliedPhases)
	}
	wantProgress := importPipelineProgressRows(block1.Number(), block1.Hash())
	if !reflect.DeepEqual(applier.progress, wantProgress) {
		t.Fatalf("progress = %+v, want %+v", applier.progress, wantProgress)
	}
	if applier.pauseNum != block2.Number() || !errors.Is(applier.pauseErr, insertErr) {
		t.Fatalf("pause = #%d err=%v, want block2 insert error", applier.pauseNum, applier.pauseErr)
	}
	wantCalls := []ImportBatchRunStepAction{
		ImportBatchRunDecode,
		ImportBatchRunExecute,
		ImportBatchRunSettle,
	}
	if !reflect.DeepEqual(applier.calls, wantCalls) {
		t.Fatalf("calls = %+v, want %+v", applier.calls, wantCalls)
	}
	wantSteps := []ImportBatchRunStepAction{
		ImportBatchRunDecode,
		ImportBatchRunRecordBufferWaits,
		ImportBatchRunPlanExecution,
		ImportBatchRunPlanStagePhases,
		ImportBatchRunExecute,
		ImportBatchRunSettle,
	}
	if !reflect.DeepEqual(result.Steps, wantSteps) {
		t.Fatalf("result steps = %+v, want %+v", result.Steps, wantSteps)
	}
}

func TestApplyImportBatchRunPlanPartialFailureIgnoresFailedBlockStageObservations(t *testing.T) {
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	insertErr := &core.InsertBlocksError{Index: 1, BlockNumber: block2.Number(), Err: errors.New("bad block")}
	applier := &recordingImportBatchRunApplier{
		insertErr:                 insertErr,
		appliedForObservations:    1,
		observeAllAttemptedStages: true,
	}

	result := ApplyImportBatchRunPlan(NewImportBatchRunPlan(testImportRunBatch(t, block1, block2)), applier)

	if !result.StopDrain || !result.Outcome.Pause || result.Outcome.Applied != 1 {
		t.Fatalf("result = %+v, want partial applied prefix and pause", result)
	}
	if result.Progress.Schedule.BlockNum != block1.Number() || result.Progress.Schedule.BlockHash != block1.Hash() {
		t.Fatalf("progress schedule = %+v, want applied block1 despite failed block observations", result.Progress.Schedule)
	}
	wantProgress := importPipelineProgressRows(block1.Number(), block1.Hash())
	if !reflect.DeepEqual(applier.progress, wantProgress) {
		t.Fatalf("progress = %+v, want applied block1 rows %+v", applier.progress, wantProgress)
	}
	for _, row := range applier.progress {
		if row.BlockNum == block2.Number() || row.BlockHash == block2.Hash() {
			t.Fatalf("progress includes failed block row %+v, want only applied prefix", row)
		}
	}
	if applier.recordApplied != 1 {
		t.Fatalf("recorded applied = %d, want 1", applier.recordApplied)
	}
	if applier.pauseNum != block2.Number() || !errors.Is(applier.pauseErr, insertErr) {
		t.Fatalf("pause = #%d err=%v, want failed block2", applier.pauseNum, applier.pauseErr)
	}
}

func TestApplyImportBatchRunPlanFirstBlockFailurePausesWithoutProgress(t *testing.T) {
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	insertErr := &core.InsertBlocksError{Index: 0, BlockNumber: block1.Number(), Err: errors.New("bad first block")}
	applier := &recordingImportBatchRunApplier{
		insertErr:                 insertErr,
		observeAllAttemptedStages: true,
	}

	result := ApplyImportBatchRunPlan(NewImportBatchRunPlan(testImportRunBatch(t, block1, block2)), applier)

	if !result.StopDrain || !result.Outcome.Pause || result.Outcome.PauseNum != block1.Number() {
		t.Fatalf("result = %+v, want pause at block1 and stop drain", result)
	}
	if result.Outcome.Applied != 0 || result.Outcome.RecordApplied {
		t.Fatalf("outcome = %+v, want no applied prefix recorded", result.Outcome)
	}
	if result.Progress.OK || applier.recordPlan.OK || applier.recordApplied != 0 || len(applier.progress) != 0 {
		t.Fatalf("progress result=%+v record=%+v applied=%d rows=%+v, want none", result.Progress, applier.recordPlan, applier.recordApplied, applier.progress)
	}
	if !result.Execution.HasStageSchedule || result.Execution.Schedule.BlockNum != block2.Number() {
		t.Fatalf("execution schedule = %+v has=%v, want attempted block2", result.Execution.Schedule, result.Execution.HasStageSchedule)
	}
	if len(result.ExecutionPhases) != 4 ||
		len(result.ExecutionPhases[0].Tasks) != 2 ||
		len(result.ExecutionPhases[3].Tasks) != 2 {
		t.Fatalf("execution phases = %+v, want attempted two-block phase plan", result.ExecutionPhases)
	}
	if result.StagePhaseSchedule.Empty() ||
		len(result.StagePhaseSchedule.PostBody) != 3 ||
		len(result.StagePhaseSchedule.Finish.Tasks) != 2 {
		t.Fatalf("stage phase schedule = %+v, want attempted two-block post-body plan", result.StagePhaseSchedule)
	}
	if applier.pauseNum != block1.Number() || !errors.Is(applier.pauseErr, insertErr) {
		t.Fatalf("pause = #%d err=%v, want failed block1", applier.pauseNum, applier.pauseErr)
	}
	wantCalls := []ImportBatchRunStepAction{
		ImportBatchRunDecode,
		ImportBatchRunExecute,
	}
	if !reflect.DeepEqual(applier.calls, wantCalls) {
		t.Fatalf("calls = %+v, want %+v", applier.calls, wantCalls)
	}
	wantSteps := []ImportBatchRunStepAction{
		ImportBatchRunDecode,
		ImportBatchRunRecordBufferWaits,
		ImportBatchRunPlanExecution,
		ImportBatchRunPlanStagePhases,
		ImportBatchRunExecute,
		ImportBatchRunSettle,
	}
	if !reflect.DeepEqual(result.Steps, wantSteps) {
		t.Fatalf("result steps = %+v, want %+v", result.Steps, wantSteps)
	}
}

func TestApplyImportBatchRunPlanFirstDecodeFailureContinuesDrain(t *testing.T) {
	block2 := testBufferedBlock(2)
	raw2, err := block2.Marshal()
	if err != nil {
		t.Fatalf("marshal block2: %v", err)
	}
	batch := BufferedBatch{Buffered: []BufferedBlock{
		{Raw: []byte{0x01}, Hash: tcommon.Hash{0xee}, Num: 1},
		{Raw: raw2, Hash: block2.Hash(), Num: block2.Number()},
	}}
	applier := &recordingImportBatchRunApplier{}

	result := ApplyImportBatchRunPlan(NewImportBatchRunPlan(batch), applier)

	if !result.ContinueDrain || result.StopDrain || result.Decode.Err == nil || result.Decode.Dropped.Num != 1 {
		t.Fatalf("result = %+v, want continue after first decode failure", result)
	}
	if !reflect.DeepEqual(applier.calls, []ImportBatchRunStepAction{ImportBatchRunDecode}) {
		t.Fatalf("calls = %+v, want only decode", applier.calls)
	}
	if !reflect.DeepEqual(result.Steps, []ImportBatchRunStepAction{ImportBatchRunDecode}) {
		t.Fatalf("result steps = %+v, want only decode", result.Steps)
	}
}

func TestStagedBodyDrainLimit(t *testing.T) {
	tests := []struct {
		name          string
		next          uint64
		max           int
		readyLimit    uint64
		hasReadyLimit bool
		wantLimit     int
		wantOK        bool
	}{
		{name: "no ready frontier uses max", next: 10, max: 32, wantLimit: 32, wantOK: true},
		{name: "ready frontier behind next stops drain", next: 10, max: 32, readyLimit: 9, hasReadyLimit: true},
		{name: "ready frontier clamps partial span", next: 10, max: 32, readyLimit: 12, hasReadyLimit: true, wantLimit: 3, wantOK: true},
		{name: "ready frontier beyond max keeps max", next: 10, max: 32, readyLimit: 99, hasReadyLimit: true, wantLimit: 32, wantOK: true},
		{name: "nonpositive max stops drain", next: 10, max: 0},
	}
	for _, tt := range tests {
		got, ok := StagedBodyDrainLimit(tt.next, tt.max, tt.readyLimit, tt.hasReadyLimit)
		if got != tt.wantLimit || ok != tt.wantOK {
			t.Fatalf("%s: limit=%d ok=%v, want %d %v", tt.name, got, ok, tt.wantLimit, tt.wantOK)
		}
	}
}

func TestImportBatchLimit(t *testing.T) {
	tests := []struct {
		name       string
		configured int
		want       int
	}{
		{name: "zero uses default", want: tsync.MaxImportBatch},
		{name: "negative uses default", configured: -1, want: tsync.MaxImportBatch},
		{name: "custom limit", configured: 12, want: 12},
		{name: "fetch cap", configured: tsync.MaxFetchBatch + 1, want: tsync.MaxFetchBatch},
	}
	for _, tt := range tests {
		if got := ImportBatchLimit(tt.configured); got != tt.want {
			t.Fatalf("%s: limit=%d, want %d", tt.name, got, tt.want)
		}
	}
}

func TestPlanStagedBodyDrain(t *testing.T) {
	tests := []struct {
		name  string
		next  uint64
		max   int
		ready StagedBodyReadyLimit
		want  StagedBodyDrainPlan
	}{
		{
			name:  "missing ready uses max",
			next:  10,
			max:   32,
			ready: StagedBodyReadyLimit{Status: StagedBodyReadyLimitMissing},
			want: StagedBodyDrainPlan{
				RestoreLimit: 32,
				CanDrain:     true,
				Steps: []StagedBodyDrainStep{
					{Action: StagedBodyDrainRestoreBodies, From: 10, Limit: 32},
					{Action: StagedBodyDrainPopBuffer, Next: 10, Limit: 32},
				},
			},
		},
		{
			name:  "valid ready clamps chunk",
			next:  10,
			max:   32,
			ready: StagedBodyReadyLimit{Status: StagedBodyReadyLimitValid, Limit: 12},
			want: StagedBodyDrainPlan{
				RestoreLimit:  3,
				CanDrain:      true,
				ReadyLimit:    12,
				HasReadyLimit: true,
				Steps: []StagedBodyDrainStep{
					{Action: StagedBodyDrainRestoreBodies, From: 10, Limit: 3},
					{Action: StagedBodyDrainPopBuffer, Next: 10, Limit: 3},
				},
			},
		},
		{
			name:  "valid ready beyond max keeps max",
			next:  10,
			max:   32,
			ready: StagedBodyReadyLimit{Status: StagedBodyReadyLimitValid, Limit: 99},
			want: StagedBodyDrainPlan{
				RestoreLimit:  32,
				CanDrain:      true,
				ReadyLimit:    99,
				HasReadyLimit: true,
				Steps: []StagedBodyDrainStep{
					{Action: StagedBodyDrainRestoreBodies, From: 10, Limit: 32},
					{Action: StagedBodyDrainPopBuffer, Next: 10, Limit: 32},
				},
			},
		},
		{
			name:  "stale ready requests refresh and uses max",
			next:  10,
			max:   32,
			ready: StagedBodyReadyLimit{Status: StagedBodyReadyLimitStale, Limit: 9},
			want: StagedBodyDrainPlan{
				RestoreLimit: 32,
				CanDrain:     true,
				RefreshReady: true,
				Steps: []StagedBodyDrainStep{
					{Action: StagedBodyDrainRefreshReady},
					{Action: StagedBodyDrainRestoreBodies, From: 10, Limit: 32},
					{Action: StagedBodyDrainPopBuffer, Next: 10, Limit: 32},
				},
			},
		},
		{
			name:  "invalid ready refreshes before using max",
			next:  10,
			max:   32,
			ready: StagedBodyReadyLimit{Status: StagedBodyReadyLimitHashMismatch, Limit: 12},
			want: StagedBodyDrainPlan{
				RestoreLimit: 32,
				CanDrain:     true,
				RefreshReady: true,
				Steps: []StagedBodyDrainStep{
					{Action: StagedBodyDrainRefreshReady},
					{Action: StagedBodyDrainRestoreBodies, From: 10, Limit: 32},
					{Action: StagedBodyDrainPopBuffer, Next: 10, Limit: 32},
				},
			},
		},
		{
			name:  "nonpositive max stops drain",
			next:  10,
			max:   0,
			ready: StagedBodyReadyLimit{Status: StagedBodyReadyLimitValid, Limit: 12},
			want:  StagedBodyDrainPlan{ReadyLimit: 12, HasReadyLimit: true},
		},
	}
	for _, tt := range tests {
		if got := PlanStagedBodyDrain(tt.next, tt.max, tt.ready); !reflect.DeepEqual(got, tt.want) {
			t.Fatalf("%s: plan = %+v, want %+v", tt.name, got, tt.want)
		}
	}
}

func TestShouldRefreshStagedBodyReadyBeforeDrain(t *testing.T) {
	tests := []struct {
		status StagedBodyReadyLimitStatus
		want   bool
	}{
		{StagedBodyReadyLimitMissing, false},
		{StagedBodyReadyLimitProgressReadError, true},
		{StagedBodyReadyLimitUnbound, true},
		{StagedBodyReadyLimitStale, true},
		{StagedBodyReadyLimitReadError, true},
		{StagedBodyReadyLimitStagedMissing, true},
		{StagedBodyReadyLimitHashMismatch, true},
		{StagedBodyReadyLimitValid, false},
	}
	for _, tt := range tests {
		if got := ShouldRefreshStagedBodyReadyBeforeDrain(tt.status); got != tt.want {
			t.Fatalf("ShouldRefreshStagedBodyReadyBeforeDrain(%v) = %v, want %v", tt.status, got, tt.want)
		}
	}
}

func TestApplyStagedBodyDrainPlan(t *testing.T) {
	popBatch := BufferedBatch{Buffered: []BufferedBlock{{Num: 10, Hash: tcommon.Hash{0x0a}}}}
	refresh := StagedBodyReadyProgressRefresh{
		Frontier: StagedBodyReadyFrontier{Have: true, Number: 12, Hash: tcommon.Hash{0x0c}},
		Updated:  true,
	}
	restore := StagedBodyRestoreResult{
		Restored:         2,
		NextExpected:     12,
		LastRestoredNum:  11,
		LastRestoredHash: tcommon.Hash{0x0b},
		HaveLastRestored: true,
	}
	plan := StagedBodyDrainPlan{
		Steps: []StagedBodyDrainStep{
			{Action: StagedBodyDrainRefreshReady},
			{Action: StagedBodyDrainRestoreBodies, From: 10, Limit: 3, PruneStaleTail: true},
			{Action: StagedBodyDrainStepAction(255)},
			{Action: StagedBodyDrainPopBuffer, Next: 10, Limit: 3},
		},
	}
	applier := &recordingStagedBodyDrainApplier{readyRefresh: refresh, restore: restore, popBatch: popBatch}

	got := ApplyStagedBodyDrainPlan(plan, applier)
	wantCalls := []recordedStagedBodyDrainCall{
		{action: StagedBodyDrainRefreshReady},
		{action: StagedBodyDrainRestoreBodies, from: 10, limit: 3, prune: true},
		{action: StagedBodyDrainPopBuffer, next: 10, limit: 3},
	}
	if !reflect.DeepEqual(applier.calls, wantCalls) {
		t.Fatalf("calls = %+v, want %+v", applier.calls, wantCalls)
	}
	wantApplied := []StagedBodyDrainStepAction{
		StagedBodyDrainRefreshReady,
		StagedBodyDrainRestoreBodies,
		StagedBodyDrainPopBuffer,
	}
	if !reflect.DeepEqual(got.AppliedSteps, wantApplied) {
		t.Fatalf("applied steps = %+v, want %+v", got.AppliedSteps, wantApplied)
	}
	if !reflect.DeepEqual(got.UnknownSteps, []StagedBodyDrainStepAction{StagedBodyDrainStepAction(255)}) {
		t.Fatalf("unknown steps = %+v, want [255]", got.UnknownSteps)
	}
	if !got.HasReadyRefresh || !reflect.DeepEqual(got.ReadyRefresh, refresh) {
		t.Fatalf("ready refresh = %+v set=%v, want %+v set", got.ReadyRefresh, got.HasReadyRefresh, refresh)
	}
	if !got.HasStagedBodyRestore || !reflect.DeepEqual(got.StagedBodyRestore, restore) {
		t.Fatalf("restore = %+v set=%v, want %+v set", got.StagedBodyRestore, got.HasStagedBodyRestore, restore)
	}
	if !reflect.DeepEqual(got.Batch, popBatch) {
		t.Fatalf("batch = %+v, want %+v", got.Batch, popBatch)
	}

	if got := ApplyStagedBodyDrainPlan(plan, nil); len(got.Batch.Buffered) != 0 || len(got.AppliedSteps) != 0 || len(got.UnknownSteps) != 0 {
		t.Fatalf("nil applier result = %+v, want empty", got)
	}
}

type recordedStagedBodyDrainCall struct {
	action StagedBodyDrainStepAction
	from   uint64
	next   uint64
	limit  int
	prune  bool
}

type recordingStagedBodyDrainApplier struct {
	calls        []recordedStagedBodyDrainCall
	readyRefresh StagedBodyReadyProgressRefresh
	restore      StagedBodyRestoreResult
	popBatch     BufferedBatch
}

func (a *recordingStagedBodyDrainApplier) RefreshSyncBodiesReady() StagedBodyReadyProgressRefresh {
	a.calls = append(a.calls, recordedStagedBodyDrainCall{action: StagedBodyDrainRefreshReady})
	return a.readyRefresh
}

func (a *recordingStagedBodyDrainApplier) RestoreStagedBodies(from uint64, limit int, pruneStaleTail bool) StagedBodyRestoreResult {
	a.calls = append(a.calls, recordedStagedBodyDrainCall{
		action: StagedBodyDrainRestoreBodies,
		from:   from,
		limit:  limit,
		prune:  pruneStaleTail,
	})
	return a.restore
}

func (a *recordingStagedBodyDrainApplier) PopBufferedBatch(next uint64, limit int) BufferedBatch {
	a.calls = append(a.calls, recordedStagedBodyDrainCall{
		action: StagedBodyDrainPopBuffer,
		next:   next,
		limit:  limit,
	})
	return a.popBatch
}

func TestPopBufferedBatchReleasesReservationsAndKeepsGap(t *testing.T) {
	now := time.Unix(100, 0)
	waitStart := now.Add(-3 * time.Second)
	var wait BufferWaitTracker
	wait.Begin(4, waitStart)

	h4 := tcommon.Hash{0x04}
	h5 := tcommon.Hash{0x05}
	h7 := tcommon.Hash{0x07}
	buffer := map[uint64]BufferedBlock{
		4: {Num: 4, Hash: h4},
		5: {Num: 5, Hash: h5},
		7: {Num: 7, Hash: h7},
	}
	bufferedHashes := map[tcommon.Hash]struct{}{
		h4: {},
		h5: {},
		h7: {},
	}
	path := BlockPath{
		4: h4,
		5: h5,
		7: h7,
	}

	batch := PopBufferedBatch(buffer, bufferedHashes, path, &wait, 4, 4, now)
	if len(batch.Buffered) != 2 {
		t.Fatalf("popped %d blocks, want 2", len(batch.Buffered))
	}
	if batch.Buffered[0].Num != 4 || batch.Buffered[1].Num != 5 {
		t.Fatalf("popped nums = %d,%d; want 4,5", batch.Buffered[0].Num, batch.Buffered[1].Num)
	}
	if len(batch.BufferWaits) != 2 || batch.BufferWaits[0] != 3*time.Second || batch.BufferWaits[1] != 0 {
		t.Fatalf("waits = %v, want [3s 0s]", batch.BufferWaits)
	}
	if _, ok := buffer[4]; ok {
		t.Fatal("block 4 still buffered")
	}
	if _, ok := buffer[5]; ok {
		t.Fatal("block 5 still buffered")
	}
	if _, ok := buffer[7]; !ok {
		t.Fatal("gap tail block 7 was removed")
	}
	if _, ok := bufferedHashes[h4]; ok {
		t.Fatal("hash 4 still reserved")
	}
	if _, ok := bufferedHashes[h5]; ok {
		t.Fatal("hash 5 still reserved")
	}
	if _, ok := bufferedHashes[h7]; !ok {
		t.Fatal("gap tail hash 7 was removed")
	}
	if _, ok := path[4]; ok {
		t.Fatal("path reservation 4 still present")
	}
	if _, ok := path[5]; ok {
		t.Fatal("path reservation 5 still present")
	}
	if _, ok := path[7]; !ok {
		t.Fatal("gap tail path reservation 7 was removed")
	}
}

func testBufferedBlock(num int64) *types.Block {
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData:          &corepb.BlockHeaderRaw{Number: num, Timestamp: num * 3000},
			WitnessSignature: make([]byte, 65),
		},
		Transactions: []*corepb.Transaction{
			{Signature: [][]byte{{byte(num)}}},
		},
	})
}

func testImportRunBatch(t *testing.T, blocks ...*types.Block) BufferedBatch {
	t.Helper()
	batch := BufferedBatch{
		Buffered: make([]BufferedBlock, 0, len(blocks)),
	}
	for _, block := range blocks {
		raw, err := block.Marshal()
		if err != nil {
			t.Fatalf("marshal block #%d: %v", block.Number(), err)
		}
		batch.Buffered = append(batch.Buffered, BufferedBlock{Raw: raw, Hash: block.Hash(), Num: block.Number()})
	}
	return batch
}

func importPipelineProgressRows(blockNum uint64, blockHash tcommon.Hash) []rawdb.StageProgress {
	return []rawdb.StageProgress{
		{Stage: rawdb.StageSyncImport, BlockNum: blockNum, BlockHash: blockHash, HasBlockHash: true},
		{Stage: rawdb.StageSyncExecution, BlockNum: blockNum, BlockHash: blockHash, HasBlockHash: true},
		{Stage: rawdb.StageSyncCommitment, BlockNum: blockNum, BlockHash: blockHash, HasBlockHash: true},
		{Stage: rawdb.StageSyncFinish, BlockNum: blockNum, BlockHash: blockHash, HasBlockHash: true},
	}
}

type recordingImportBatchRunApplier struct {
	calls                     []ImportBatchRunStepAction
	waits                     []time.Duration
	elapsed                   time.Duration
	insertErr                 error
	appliedForObservations    int
	observeAllAttemptedStages bool
	observedStages            []rawdb.StageID
	execution                 ImportBatchExecutionPlan
	recordRunPlan             ImportedBatchRecordPlan
	recordApply               ImportedBatchRecordApplyResult
	recordPlan                ImportedBatchProgressPlan
	recordApplied             int
	recordElapsed             time.Duration
	progress                  []rawdb.StageProgress
	pauseNum                  uint64
	pauseErr                  error
}

func (a *recordingImportBatchRunApplier) LogDecodeBatchResult(BufferedBatchDecodeResult) {
	a.calls = append(a.calls, ImportBatchRunDecode)
}

func (a *recordingImportBatchRunApplier) RecordBufferWait(wait time.Duration) {
	a.calls = append(a.calls, ImportBatchRunRecordBufferWaits)
	a.waits = append(a.waits, wait)
}

func (a *recordingImportBatchRunApplier) ExecuteImportBatch(execution ImportBatchExecutionPlan, observe StageProgressWriter) (time.Duration, error) {
	a.calls = append(a.calls, ImportBatchRunExecute)
	a.execution = execution
	stages := a.observedStages
	if stages == nil {
		stages = []rawdb.StageID{rawdb.StageBodies, rawdb.StageExecution, rawdb.StageCommitment, rawdb.StageFinish}
	}
	applied := len(execution.Blocks)
	if a.appliedForObservations > 0 && a.appliedForObservations < applied {
		applied = a.appliedForObservations
	}
	for i := 0; i < applied; i++ {
		block := execution.Blocks[i]
		for _, stage := range stages {
			observe(stage, block.Number(), block.Hash())
		}
	}
	if a.observeAllAttemptedStages {
		for _, block := range execution.Blocks[applied:] {
			for _, stage := range stages {
				observe(stage, block.Number(), block.Hash())
			}
		}
	}
	return a.elapsed, a.insertErr
}

func (a *recordingImportBatchRunApplier) ApplyImportedBatchRecord(plan ImportedBatchRecordPlan) ImportedBatchRecordApplyResult {
	a.calls = append(a.calls, ImportBatchRunSettle)
	a.recordRunPlan = plan
	a.recordPlan = plan.Progress
	a.recordApplied = plan.Progress.Summary.Applied
	a.recordElapsed = plan.Elapsed
	a.progress = append([]rawdb.StageProgress(nil), plan.Progress.Progress...)
	return a.recordApply
}

func (a *recordingImportBatchRunApplier) PauseImport(_ *p2p.Peer, blockNum uint64, err error) {
	a.pauseNum = blockNum
	a.pauseErr = err
}
