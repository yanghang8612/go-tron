package downloader

import (
	"errors"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestStageForCanonicalStage(t *testing.T) {
	tests := []struct {
		canonical rawdb.StageID
		sync      rawdb.StageID
		ok        bool
	}{
		{rawdb.StageHeaders, "", false},
		{rawdb.StageBodies, rawdb.StageSyncImport, true},
		{rawdb.StageExecution, rawdb.StageSyncExecution, true},
		{rawdb.StageCommitment, rawdb.StageSyncCommitment, true},
		{rawdb.StageFinish, rawdb.StageSyncFinish, true},
		{rawdb.StageSnapshotBuild, "", false},
	}
	for _, tt := range tests {
		got, ok := StageForCanonicalStage(tt.canonical)
		if got != tt.sync || ok != tt.ok {
			t.Fatalf("StageForCanonicalStage(%s) = %s/%v, want %s/%v", tt.canonical, got, ok, tt.sync, tt.ok)
		}
	}
}

func TestStageProgressCollectorWritesExplicitSchedulePrefix(t *testing.T) {
	collector := NewStageProgressCollector()
	collector.Observe(rawdb.StageBodies, 1, tcommon.Hash{0x01})
	collector.Observe(rawdb.StageBodies, 2, tcommon.Hash{0x02})
	collector.Observe(rawdb.StageExecution, 2, tcommon.Hash{0x02})
	collector.Observe(rawdb.StageCommitment, 3, tcommon.Hash{0x03})
	collector.Observe(rawdb.StageFinish, 2, tcommon.Hash{0x02})
	collector.Observe(rawdb.StageHeaders, 2, tcommon.Hash{0xff})

	var got []rawdb.StageProgress
	schedule := NewImportStageSchedule(2, tcommon.Hash{0x02})
	collector.WriteSchedule(schedule, func(stage rawdb.StageID, blockNum uint64, blockHash tcommon.Hash) {
		got = append(got, rawdb.StageProgress{
			Stage:        stage,
			BlockNum:     blockNum,
			BlockHash:    blockHash,
			HasBlockHash: true,
		})
	})

	want := []rawdb.StageProgress{
		{Stage: rawdb.StageSyncImport, BlockNum: 2, BlockHash: tcommon.Hash{0x02}, HasBlockHash: true},
		{Stage: rawdb.StageSyncExecution, BlockNum: 2, BlockHash: tcommon.Hash{0x02}, HasBlockHash: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("written progress = %+v, want %+v", got, want)
	}
	if rows := collector.RowsForSchedule(schedule); !reflect.DeepEqual(rows, want) {
		t.Fatalf("progress rows = %+v, want %+v", rows, want)
	}
}

func TestStageProgressCollectorRecordsPlannedObservation(t *testing.T) {
	collector := NewStageProgressCollector()
	hash := tcommon.Hash{0x02}
	bodyTask := ImportBodyStageTask(2, hash)
	task := ImportExecutionStageTask(2, hash)
	phase := ImportStagePhasePlan{
		Phase:          ImportStagePhaseExecution,
		CanonicalStage: rawdb.StageExecution,
		SyncStage:      rawdb.StageSyncExecution,
		Tasks:          []ImportStageTask{task},
	}

	collector.ObservePlanned(ImportStageObservation{Phase: ImportStagePhasePlan{
		Phase:          ImportStagePhaseBodies,
		CanonicalStage: rawdb.StageBodies,
		SyncStage:      rawdb.StageSyncImport,
		Tasks:          []ImportStageTask{bodyTask},
	}, Task: bodyTask})
	collector.ObservePlanned(ImportStageObservation{Phase: phase, Task: task})

	want := []rawdb.StageProgress{
		{Stage: rawdb.StageSyncImport, BlockNum: 2, BlockHash: hash, HasBlockHash: true},
		{Stage: rawdb.StageSyncExecution, BlockNum: 2, BlockHash: hash, HasBlockHash: true},
	}
	if rows := collector.RowsForSchedule(NewImportStageSchedule(2, hash)); !reflect.DeepEqual(rows, want) {
		t.Fatalf("planned observation rows = %+v, want %+v", rows, want)
	}
	var nilCollector *StageProgressCollector
	nilCollector.ObservePlanned(ImportStageObservation{Phase: phase, Task: task})
}

func TestStageProgressCollectorLegacyRowsDeriveBoundaryFromImportObservation(t *testing.T) {
	collector := NewStageProgressCollector()
	collector.Observe(rawdb.StageBodies, 2, tcommon.Hash{0x02})
	collector.Observe(rawdb.StageExecution, 2, tcommon.Hash{0x02})

	want := []rawdb.StageProgress{
		{Stage: rawdb.StageSyncImport, BlockNum: 2, BlockHash: tcommon.Hash{0x02}, HasBlockHash: true},
		{Stage: rawdb.StageSyncExecution, BlockNum: 2, BlockHash: tcommon.Hash{0x02}, HasBlockHash: true},
	}
	if rows := collector.Rows(2); !reflect.DeepEqual(rows, want) {
		t.Fatalf("legacy progress rows = %+v, want %+v", rows, want)
	}
}

func TestStageProgressCollectorDoesNotPublishWithoutImportBoundary(t *testing.T) {
	collector := NewStageProgressCollector()
	collector.Observe(rawdb.StageBodies, 1, tcommon.Hash{0x01})
	collector.Observe(rawdb.StageExecution, 2, tcommon.Hash{0x02})
	collector.Observe(rawdb.StageCommitment, 2, tcommon.Hash{0x02})
	collector.Observe(rawdb.StageFinish, 2, tcommon.Hash{0x02})

	if rows := collector.Rows(2); len(rows) != 0 {
		t.Fatalf("rows without import boundary = %+v, want none", rows)
	}
}

func TestStageProgressCollectorNilAndEmpty(t *testing.T) {
	var nilCollector *StageProgressCollector
	called := false
	nilCollector.Write(10, func(rawdb.StageID, uint64, tcommon.Hash) {
		called = true
	})
	if called {
		t.Fatal("nil collector wrote progress")
	}

	NewStageProgressCollector().Write(10, func(rawdb.StageID, uint64, tcommon.Hash) {
		called = true
	})
	if called {
		t.Fatal("empty collector wrote progress")
	}

	var zero StageProgressCollector
	zero.Observe(rawdb.StageFinish, 7, tcommon.Hash{0x07})
	zero.Write(7, func(stage rawdb.StageID, blockNum uint64, blockHash tcommon.Hash) {
		called = true
	})
	if called {
		t.Fatal("zero-value collector wrote downstream progress without import boundary")
	}

	zero.Observe(rawdb.StageBodies, 7, tcommon.Hash{0x07})
	zero.Observe(rawdb.StageExecution, 7, tcommon.Hash{0x07})
	zero.Write(7, func(stage rawdb.StageID, blockNum uint64, blockHash tcommon.Hash) {
		called = true
		if stage != rawdb.StageSyncImport && stage != rawdb.StageSyncExecution {
			t.Fatalf("zero-value collector wrote unexpected stage %s/%d/%x", stage, blockNum, blockHash)
		}
	})
	if !called {
		t.Fatal("zero-value collector did not write planned boundary progress")
	}
}

func TestPlanImportedBatchProgress(t *testing.T) {
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	block2.Proto().Transactions = append(block2.Proto().Transactions, &corepb.Transaction{Signature: [][]byte{{0x22}}})
	block3 := testBufferedBlock(3)
	batch := BufferedBatch{
		Blocks: []*types.Block{block1, block2, block3},
		Buffered: []BufferedBlock{
			{Num: block1.Number(), Hash: block1.Hash()},
			{Num: block2.Number(), Hash: block2.Hash()},
			{Num: block3.Number(), Hash: block3.Hash()},
		},
	}
	collector := NewStageProgressCollector()
	for _, stage := range []rawdb.StageID{rawdb.StageBodies, rawdb.StageExecution, rawdb.StageCommitment, rawdb.StageFinish} {
		collector.Observe(stage, block1.Number(), block1.Hash())
		collector.Observe(stage, block2.Number(), block2.Hash())
		collector.Observe(stage, block3.Number(), block3.Hash())
	}

	got := PlanImportedBatchProgress(batch, 2, collector)
	if !got.OK || got.Summary.Applied != 2 || got.Summary.Last.Num != block2.Number() {
		t.Fatalf("plan summary = %+v, want applied through block2", got.Summary)
	}
	wantStages := ImportPipelineStageTasks(block2.Number(), block2.Hash())
	if !reflect.DeepEqual(got.Stages, wantStages) {
		t.Fatalf("stages = %+v, want %+v", got.Stages, wantStages)
	}
	if got.Schedule.BlockNum != block2.Number() || got.Schedule.BlockHash != block2.Hash() || !reflect.DeepEqual(got.Schedule.Tasks, wantStages) {
		t.Fatalf("schedule = %+v, want block2 schedule %+v", got.Schedule, wantStages)
	}
	wantExecution := ImportExecutionStageTasks(block2.Number(), block2.Hash())
	if got.Schedule.Execution != wantExecution[0] || got.Schedule.Commitment != wantExecution[1] || got.Schedule.Finish != wantExecution[2] || !reflect.DeepEqual(got.Schedule.PostBody, wantExecution) {
		t.Fatalf("post-body schedule = %+v/%+v/%+v post=%+v, want %+v",
			got.Schedule.Execution, got.Schedule.Commitment, got.Schedule.Finish, got.Schedule.PostBody, wantExecution)
	}
	if !got.RefreshReady || got.StatsBlocks != 2 || got.StatsTransactions != 3 || got.ReportHead != block2.Number() {
		t.Fatalf("record metadata = refresh=%v blocks=%d txs=%d head=%d, want refresh/2/3/block2",
			got.RefreshReady, got.StatsBlocks, got.StatsTransactions, got.ReportHead)
	}
	if len(got.Deletes) != 2 || got.Deletes[0].Number != block1.Number() || got.Deletes[1].Number != block2.Number() {
		t.Fatalf("deletes = %+v, want block1/block2 staged rows", got.Deletes)
	}
	wantRows := []rawdb.StageProgress{
		{Stage: rawdb.StageSyncImport, BlockNum: block2.Number(), BlockHash: block2.Hash(), HasBlockHash: true},
		{Stage: rawdb.StageSyncExecution, BlockNum: block2.Number(), BlockHash: block2.Hash(), HasBlockHash: true},
		{Stage: rawdb.StageSyncCommitment, BlockNum: block2.Number(), BlockHash: block2.Hash(), HasBlockHash: true},
		{Stage: rawdb.StageSyncFinish, BlockNum: block2.Number(), BlockHash: block2.Hash(), HasBlockHash: true},
	}
	if !reflect.DeepEqual(got.Progress, wantRows) {
		t.Fatalf("progress = %+v, want %+v", got.Progress, wantRows)
	}
	if !got.StagePlan.Complete || got.StagePlan.HasNext || got.StagePlan.HasBlocked || len(got.StagePlan.Completed) != len(wantRows) {
		t.Fatalf("stage plan = %+v, want complete with no blocked stage", got.StagePlan)
	}
	wantSteps := []ImportedBatchProgressStep{
		{Action: ImportedBatchWriteProgress, Deletes: got.Deletes, Progress: wantRows},
		{Action: ImportedBatchRefreshBodiesReady},
	}
	if !reflect.DeepEqual(got.Steps, wantSteps) {
		t.Fatalf("steps = %+v, want %+v", got.Steps, wantSteps)
	}
	if len(got.Decisions) != len(wantRows) {
		t.Fatalf("decisions = %+v, want one per sync stage", got.Decisions)
	}
	for _, decision := range got.Decisions {
		if decision.Status != ImportStageProgressPlanned {
			t.Fatalf("decision = %+v, want planned", decision)
		}
	}
}

func TestPlanImportedBatchProgressForExecutionRequiresAppliedSchedule(t *testing.T) {
	block := testBufferedBlock(1)
	batch := BufferedBatch{
		Blocks: []*types.Block{block},
		Buffered: []BufferedBlock{
			{Num: block.Number(), Hash: block.Hash()},
		},
	}
	collector := NewStageProgressCollector()
	for _, stage := range []rawdb.StageID{rawdb.StageBodies, rawdb.StageExecution, rawdb.StageCommitment, rawdb.StageFinish} {
		collector.Observe(stage, block.Number(), block.Hash())
	}

	legacy := PlanImportedBatchProgress(batch, 1, collector)
	if len(legacy.Progress) != 4 || !legacy.StagePlan.Complete {
		t.Fatalf("legacy progress = %+v stagePlan=%+v, want hook-derived compatibility progress", legacy.Progress, legacy.StagePlan)
	}

	got := PlanImportedBatchProgressForExecution(batch, 1, ImportBatchExecutionPlan{}, collector)
	if !got.OK || got.Summary.Applied != 1 || !got.RefreshReady {
		t.Fatalf("execution progress plan = %+v, want applied cleanup plan with ready refresh", got)
	}
	if len(got.Progress) != 0 || len(got.Decisions) != 0 || len(got.Stages) != 0 || len(got.Schedule.Tasks) != 0 {
		t.Fatalf("execution progress = rows:%+v decisions:%+v stages:%+v schedule:%+v, want no stage progress without execution schedule",
			got.Progress, got.Decisions, got.Stages, got.Schedule)
	}
	if got.StagePlan.Complete || got.StageDiagnostics.Scheduled != 0 || got.StageDiagnostics.Completed != 0 {
		t.Fatalf("stage diagnostics = %+v plan=%+v, want no scheduled stage progress without execution schedule", got.StageDiagnostics, got.StagePlan)
	}
	if len(got.Deletes) != 1 || got.Deletes[0].Number != block.Number() || got.Deletes[0].Hash != block.Hash() {
		t.Fatalf("deletes = %+v, want staged block cleanup for applied block", got.Deletes)
	}
	wantSteps := []ImportedBatchProgressStep{
		{Action: ImportedBatchWriteProgress, Deletes: got.Deletes},
		{Action: ImportedBatchRefreshBodiesReady},
	}
	if !reflect.DeepEqual(got.Steps, wantSteps) {
		t.Fatalf("steps = %+v, want cleanup plus ready refresh", got.Steps)
	}
}

func TestPlanImportedBatchProgressForExecutionUsesBatchPhasePrefix(t *testing.T) {
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	batch := BufferedBatch{
		Blocks: []*types.Block{block1, block2},
		Buffered: []BufferedBlock{
			{Num: block1.Number(), Hash: block1.Hash()},
			{Num: block2.Number(), Hash: block2.Hash()},
		},
	}
	execution := PlanImportBatchExecution(batch)
	collector := NewStageProgressCollector()
	collector.Observe(rawdb.StageBodies, block1.Number(), block1.Hash())
	collector.Observe(rawdb.StageBodies, block2.Number(), block2.Hash())
	collector.Observe(rawdb.StageExecution, block1.Number(), block1.Hash())
	collector.Observe(rawdb.StageCommitment, block2.Number(), block2.Hash())
	collector.Observe(rawdb.StageFinish, block2.Number(), block2.Hash())

	got := PlanImportedBatchProgressForExecution(batch, 2, execution, collector)
	wantRows := []rawdb.StageProgress{
		{Stage: rawdb.StageSyncImport, BlockNum: block2.Number(), BlockHash: block2.Hash(), HasBlockHash: true},
		{Stage: rawdb.StageSyncExecution, BlockNum: block1.Number(), BlockHash: block1.Hash(), HasBlockHash: true},
	}
	if !reflect.DeepEqual(got.Progress, wantRows) {
		t.Fatalf("progress = %+v, want batch phase prefix %+v", got.Progress, wantRows)
	}
	if got.StageDiagnostics.Scheduled != 8 || got.StageDiagnostics.Completed != 3 || got.StageDiagnostics.Complete {
		t.Fatalf("stage diagnostics = %+v, want 3/8 incomplete", got.StageDiagnostics)
	}
	if got.StageDiagnostics.NextPhase != ImportStagePhaseExecution || got.StageDiagnostics.NextBlockNum != block2.Number() || got.StageDiagnostics.BlockedStatus != ImportStageProgressMismatch {
		t.Fatalf("next stage diagnostics = %+v, want execution block2 mismatch", got.StageDiagnostics)
	}
	wantStatuses := []ImportStageProgressStatus{
		ImportStageProgressPlanned,
		ImportStageProgressPlanned,
		ImportStageProgressPlanned,
		ImportStageProgressMismatch,
		ImportStageProgressBlocked,
		ImportStageProgressBlocked,
		ImportStageProgressBlocked,
		ImportStageProgressBlocked,
	}
	if len(got.Decisions) != len(wantStatuses) {
		t.Fatalf("decisions = %+v, want %d statuses", got.Decisions, len(wantStatuses))
	}
	for i, status := range wantStatuses {
		if got.Decisions[i].Status != status {
			t.Fatalf("decision %d = %+v, want status %v", i, got.Decisions[i], status)
		}
	}
}

func TestPlanImportedBatchProgressForExecutionDoesNotSkipMissingPhaseTask(t *testing.T) {
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	batch := BufferedBatch{
		Blocks: []*types.Block{block1, block2},
		Buffered: []BufferedBlock{
			{Num: block1.Number(), Hash: block1.Hash()},
			{Num: block2.Number(), Hash: block2.Hash()},
		},
	}
	execution := PlanImportBatchExecution(batch)
	collector := NewStageProgressCollector()
	collector.Observe(rawdb.StageBodies, block1.Number(), block1.Hash())
	collector.Observe(rawdb.StageBodies, block2.Number(), block2.Hash())
	collector.Observe(rawdb.StageExecution, block2.Number(), block2.Hash())
	collector.Observe(rawdb.StageCommitment, block2.Number(), block2.Hash())
	collector.Observe(rawdb.StageFinish, block2.Number(), block2.Hash())

	got := PlanImportedBatchProgressForExecution(batch, 2, execution, collector)
	wantRows := []rawdb.StageProgress{
		{Stage: rawdb.StageSyncImport, BlockNum: block2.Number(), BlockHash: block2.Hash(), HasBlockHash: true},
	}
	if !reflect.DeepEqual(got.Progress, wantRows) {
		t.Fatalf("progress = %+v, want only completed bodies phase %+v", got.Progress, wantRows)
	}
	if got.StageDiagnostics.Scheduled != 8 || got.StageDiagnostics.Completed != 2 || got.StageDiagnostics.Complete {
		t.Fatalf("stage diagnostics = %+v, want 2/8 incomplete", got.StageDiagnostics)
	}
	if got.StageDiagnostics.NextPhase != ImportStagePhaseExecution || got.StageDiagnostics.NextBlockNum != block1.Number() || got.StageDiagnostics.BlockedStatus != ImportStageProgressMismatch {
		t.Fatalf("next stage diagnostics = %+v, want execution block1 mismatch", got.StageDiagnostics)
	}
	for _, row := range got.Progress {
		if row.Stage == rawdb.StageSyncExecution {
			t.Fatalf("progress published skipped execution row %+v", row)
		}
	}
}

func TestPlanImportedBatchProgressForExecutionBlocksLaterPhasesAfterMissingExecution(t *testing.T) {
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	batch := BufferedBatch{
		Blocks: []*types.Block{block1, block2},
		Buffered: []BufferedBlock{
			{Num: block1.Number(), Hash: block1.Hash()},
			{Num: block2.Number(), Hash: block2.Hash()},
		},
	}
	execution := PlanImportBatchExecution(batch)
	collector := NewStageProgressCollector()
	collector.Observe(rawdb.StageBodies, block1.Number(), block1.Hash())
	collector.Observe(rawdb.StageBodies, block2.Number(), block2.Hash())
	collector.Observe(rawdb.StageCommitment, block2.Number(), block2.Hash())
	collector.Observe(rawdb.StageFinish, block2.Number(), block2.Hash())

	got := PlanImportedBatchProgressForExecution(batch, 2, execution, collector)
	wantRows := []rawdb.StageProgress{
		{Stage: rawdb.StageSyncImport, BlockNum: block2.Number(), BlockHash: block2.Hash(), HasBlockHash: true},
	}
	if !reflect.DeepEqual(got.Progress, wantRows) {
		t.Fatalf("progress = %+v, want only bodies phase progress %+v", got.Progress, wantRows)
	}
	wantStatuses := []ImportStageProgressStatus{
		ImportStageProgressPlanned,
		ImportStageProgressPlanned,
		ImportStageProgressMissing,
		ImportStageProgressBlocked,
		ImportStageProgressBlocked,
		ImportStageProgressBlocked,
		ImportStageProgressBlocked,
		ImportStageProgressBlocked,
	}
	if len(got.Decisions) != len(wantStatuses) {
		t.Fatalf("decisions = %+v, want %d statuses", got.Decisions, len(wantStatuses))
	}
	for i, status := range wantStatuses {
		if got.Decisions[i].Status != status {
			t.Fatalf("decision %d = %+v, want status %v", i, got.Decisions[i], status)
		}
	}
	if got.StageDiagnostics.Scheduled != 8 || got.StageDiagnostics.Completed != 2 || got.StageDiagnostics.Complete {
		t.Fatalf("stage diagnostics = %+v, want 2/8 incomplete", got.StageDiagnostics)
	}
	if got.StageDiagnostics.NextPhase != ImportStagePhaseExecution ||
		got.StageDiagnostics.NextBlockNum != block1.Number() ||
		got.StageDiagnostics.NextCanonicalStage != rawdb.StageExecution ||
		got.StageDiagnostics.NextStage != rawdb.StageSyncExecution ||
		got.StageDiagnostics.BlockedStatus != ImportStageProgressMissing {
		t.Fatalf("next stage diagnostics = %+v, want missing execution block1", got.StageDiagnostics)
	}
	for _, row := range got.Progress {
		if row.Stage == rawdb.StageSyncCommitment || row.Stage == rawdb.StageSyncFinish {
			t.Fatalf("progress published downstream phase row %+v after missing execution", row)
		}
	}
}

func TestApplyImportedBatchProgressPlan(t *testing.T) {
	deleteRow := rawdb.SyncStagedBlockDelete{Number: 2, Hash: tcommon.Hash{0x02}}
	progressRow := rawdb.StageProgress{
		Stage:        rawdb.StageSyncExecution,
		BlockNum:     2,
		BlockHash:    deleteRow.Hash,
		HasBlockHash: true,
	}
	plan := ImportedBatchProgressPlan{
		OK: true,
		Steps: []ImportedBatchProgressStep{
			{Action: ImportedBatchWriteProgress, Deletes: []rawdb.SyncStagedBlockDelete{deleteRow}, Progress: []rawdb.StageProgress{progressRow}},
			{Action: ImportedBatchProgressStepAction(255)},
			{Action: ImportedBatchRefreshBodiesReady},
		},
	}
	var applier recordingImportedBatchProgressApplier

	ApplyImportedBatchProgressPlan(plan, &applier)
	want := []recordedImportedBatchProgressCall{
		{action: ImportedBatchWriteProgress, deletes: []rawdb.SyncStagedBlockDelete{deleteRow}, progress: []rawdb.StageProgress{progressRow}},
		{action: ImportedBatchRefreshBodiesReady},
	}
	if !reflect.DeepEqual(applier.calls, want) {
		t.Fatalf("calls = %+v, want %+v", applier.calls, want)
	}

	ApplyImportedBatchProgressPlan(plan, nil)
	ApplyImportedBatchProgressPlan(ImportedBatchProgressPlan{}, &applier)
	if !reflect.DeepEqual(applier.calls, want) {
		t.Fatalf("calls after no-op plans = %+v, want unchanged %+v", applier.calls, want)
	}
}

type recordedImportedBatchProgressCall struct {
	action   ImportedBatchProgressStepAction
	deletes  []rawdb.SyncStagedBlockDelete
	progress []rawdb.StageProgress
}

type recordingImportedBatchProgressApplier struct {
	calls []recordedImportedBatchProgressCall
}

func (a *recordingImportedBatchProgressApplier) WriteImportedSyncProgress(deletes []rawdb.SyncStagedBlockDelete, rows []rawdb.StageProgress) {
	a.calls = append(a.calls, recordedImportedBatchProgressCall{
		action:   ImportedBatchWriteProgress,
		deletes:  append([]rawdb.SyncStagedBlockDelete(nil), deletes...),
		progress: append([]rawdb.StageProgress(nil), rows...),
	})
}

func (a *recordingImportedBatchProgressApplier) RefreshSyncBodiesReady() {
	a.calls = append(a.calls, recordedImportedBatchProgressCall{action: ImportedBatchRefreshBodiesReady})
}

func TestPlanImportedBatchProgressStopsAtStageMismatch(t *testing.T) {
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	batch := BufferedBatch{
		Blocks: []*types.Block{block1, block2},
		Buffered: []BufferedBlock{
			{Num: block1.Number(), Hash: block1.Hash()},
			{Num: block2.Number(), Hash: block2.Hash()},
		},
	}
	collector := NewStageProgressCollector()
	collector.Observe(rawdb.StageBodies, block2.Number(), block2.Hash())
	collector.Observe(rawdb.StageExecution, block1.Number(), block1.Hash())
	collector.Observe(rawdb.StageCommitment, block2.Number(), block2.Hash())
	collector.Observe(rawdb.StageFinish, block2.Number(), block2.Hash())

	got := PlanImportedBatchProgress(batch, 2, collector)
	wantRows := []rawdb.StageProgress{
		{Stage: rawdb.StageSyncImport, BlockNum: block2.Number(), BlockHash: block2.Hash(), HasBlockHash: true},
	}
	if !reflect.DeepEqual(got.Progress, wantRows) {
		t.Fatalf("progress = %+v, want only import row before execution mismatch", got.Progress)
	}
	wantStatuses := []ImportStageProgressStatus{
		ImportStageProgressPlanned,
		ImportStageProgressMismatch,
		ImportStageProgressBlocked,
		ImportStageProgressBlocked,
	}
	if len(got.Decisions) != len(wantStatuses) {
		t.Fatalf("decisions = %+v, want %d statuses", got.Decisions, len(wantStatuses))
	}
	for i, status := range wantStatuses {
		if got.Decisions[i].Status != status {
			t.Fatalf("decision %d = %+v, want status %v", i, got.Decisions[i], status)
		}
	}
	if got.Decisions[1].Row.BlockNum != block1.Number() || got.Decisions[1].Row.BlockHash != block1.Hash() {
		t.Fatalf("mismatch row = %+v, want execution at block1", got.Decisions[1].Row)
	}
}

func TestPlanImportedBatchProgressStopsAtCommitmentMismatch(t *testing.T) {
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	batch := BufferedBatch{
		Blocks: []*types.Block{block1, block2},
		Buffered: []BufferedBlock{
			{Num: block1.Number(), Hash: block1.Hash()},
			{Num: block2.Number(), Hash: block2.Hash()},
		},
	}
	collector := NewStageProgressCollector()
	collector.Observe(rawdb.StageBodies, block2.Number(), block2.Hash())
	collector.Observe(rawdb.StageExecution, block2.Number(), block2.Hash())
	collector.Observe(rawdb.StageCommitment, block1.Number(), block1.Hash())
	collector.Observe(rawdb.StageFinish, block2.Number(), block2.Hash())

	got := PlanImportedBatchProgress(batch, 2, collector)
	wantRows := []rawdb.StageProgress{
		{Stage: rawdb.StageSyncImport, BlockNum: block2.Number(), BlockHash: block2.Hash(), HasBlockHash: true},
		{Stage: rawdb.StageSyncExecution, BlockNum: block2.Number(), BlockHash: block2.Hash(), HasBlockHash: true},
	}
	if !reflect.DeepEqual(got.Progress, wantRows) {
		t.Fatalf("progress = %+v, want import+execution before commitment mismatch", got.Progress)
	}
	wantStatuses := []ImportStageProgressStatus{
		ImportStageProgressPlanned,
		ImportStageProgressPlanned,
		ImportStageProgressMismatch,
		ImportStageProgressBlocked,
	}
	if len(got.Decisions) != len(wantStatuses) {
		t.Fatalf("decisions = %+v, want %d statuses", got.Decisions, len(wantStatuses))
	}
	for i, status := range wantStatuses {
		if got.Decisions[i].Status != status {
			t.Fatalf("decision %d = %+v, want status %v", i, got.Decisions[i], status)
		}
	}
	if got.Decisions[2].Row.BlockNum != block1.Number() || got.Decisions[2].Row.BlockHash != block1.Hash() {
		t.Fatalf("mismatch row = %+v, want commitment at block1", got.Decisions[2].Row)
	}
}

func TestPlanImportedBatchProgressStopsAtStageGap(t *testing.T) {
	block := testBufferedBlock(2)
	batch := BufferedBatch{
		Blocks: []*types.Block{block},
		Buffered: []BufferedBlock{
			{Num: block.Number(), Hash: block.Hash()},
		},
	}
	collector := NewStageProgressCollector()
	collector.Observe(rawdb.StageBodies, block.Number(), block.Hash())
	collector.Observe(rawdb.StageCommitment, block.Number(), block.Hash())
	collector.Observe(rawdb.StageFinish, block.Number(), block.Hash())

	got := PlanImportedBatchProgress(batch, 1, collector)
	wantRows := []rawdb.StageProgress{
		{Stage: rawdb.StageSyncImport, BlockNum: block.Number(), BlockHash: block.Hash(), HasBlockHash: true},
	}
	if !reflect.DeepEqual(got.Progress, wantRows) {
		t.Fatalf("progress = %+v, want only import row before execution gap", got.Progress)
	}
	wantStatuses := []ImportStageProgressStatus{
		ImportStageProgressPlanned,
		ImportStageProgressMissing,
		ImportStageProgressBlocked,
		ImportStageProgressBlocked,
	}
	if len(got.Decisions) != len(wantStatuses) {
		t.Fatalf("decisions = %+v, want %d statuses", got.Decisions, len(wantStatuses))
	}
	for i, status := range wantStatuses {
		if got.Decisions[i].Status != status {
			t.Fatalf("decision %d = %+v, want status %v", i, got.Decisions[i], status)
		}
	}
}

func TestPlanImportedBatchProgressStopsAtFinishGap(t *testing.T) {
	block := testBufferedBlock(2)
	batch := BufferedBatch{
		Blocks: []*types.Block{block},
		Buffered: []BufferedBlock{
			{Num: block.Number(), Hash: block.Hash()},
		},
	}
	collector := NewStageProgressCollector()
	collector.Observe(rawdb.StageBodies, block.Number(), block.Hash())
	collector.Observe(rawdb.StageExecution, block.Number(), block.Hash())
	collector.Observe(rawdb.StageCommitment, block.Number(), block.Hash())

	got := PlanImportedBatchProgress(batch, 1, collector)
	wantRows := []rawdb.StageProgress{
		{Stage: rawdb.StageSyncImport, BlockNum: block.Number(), BlockHash: block.Hash(), HasBlockHash: true},
		{Stage: rawdb.StageSyncExecution, BlockNum: block.Number(), BlockHash: block.Hash(), HasBlockHash: true},
		{Stage: rawdb.StageSyncCommitment, BlockNum: block.Number(), BlockHash: block.Hash(), HasBlockHash: true},
	}
	if !reflect.DeepEqual(got.Progress, wantRows) {
		t.Fatalf("progress = %+v, want import+execution+commitment before finish gap", got.Progress)
	}
	if got.StagePlan.Complete || !got.StagePlan.HasNext || !got.StagePlan.HasBlocked {
		t.Fatalf("stage plan = %+v, want blocked finish stage", got.StagePlan)
	}
	if got.StagePlan.Next.Phase != ImportStagePhaseFinish || got.StagePlan.Blocked.Status != ImportStageProgressMissing {
		t.Fatalf("blocked stage = %+v next=%+v, want missing finish", got.StagePlan.Blocked, got.StagePlan.Next)
	}
	if len(got.StagePlan.Completed) != 3 {
		t.Fatalf("completed stages = %+v, want import/execution/commitment", got.StagePlan.Completed)
	}
	wantStatuses := []ImportStageProgressStatus{
		ImportStageProgressPlanned,
		ImportStageProgressPlanned,
		ImportStageProgressPlanned,
		ImportStageProgressMissing,
	}
	if len(got.Decisions) != len(wantStatuses) {
		t.Fatalf("decisions = %+v, want %d statuses", got.Decisions, len(wantStatuses))
	}
	for i, status := range wantStatuses {
		if got.Decisions[i].Status != status {
			t.Fatalf("decision %d = %+v, want status %v", i, got.Decisions[i], status)
		}
	}
}

func TestImportStagePlannerReportsNextIncompleteStage(t *testing.T) {
	hash := tcommon.Hash{0x42}
	schedule := NewImportStageSchedule(7, hash)
	collector := NewStageProgressCollector()
	collector.Observe(rawdb.StageBodies, 7, hash)
	collector.Observe(rawdb.StageExecution, 7, hash)
	collector.Observe(rawdb.StageFinish, 7, hash)

	stagePlan := collector.PlanSchedule(schedule)
	wantRows := []rawdb.StageProgress{
		{Stage: rawdb.StageSyncImport, BlockNum: 7, BlockHash: hash, HasBlockHash: true},
		{Stage: rawdb.StageSyncExecution, BlockNum: 7, BlockHash: hash, HasBlockHash: true},
	}
	if !reflect.DeepEqual(stagePlan.Progress, wantRows) {
		t.Fatalf("progress = %+v, want %+v", stagePlan.Progress, wantRows)
	}
	if stagePlan.Complete || !stagePlan.HasNext || !stagePlan.HasBlocked {
		t.Fatalf("stage plan = %+v, want blocked commitment stage", stagePlan)
	}
	if len(stagePlan.Completed) != 2 {
		t.Fatalf("completed stages = %+v, want import/execution", stagePlan.Completed)
	}
	if stagePlan.Next.Phase != ImportStagePhaseCommitment || stagePlan.Next.SyncStage != rawdb.StageSyncCommitment {
		t.Fatalf("next stage = %+v, want commitment", stagePlan.Next)
	}
	if stagePlan.Blocked.Status != ImportStageProgressMissing || stagePlan.Blocked.Task != stagePlan.Next {
		t.Fatalf("blocked decision = %+v, want missing next stage", stagePlan.Blocked)
	}

	progress, decisions := collector.Plan(schedule)
	if !reflect.DeepEqual(progress, stagePlan.Progress) || !reflect.DeepEqual(decisions, stagePlan.Decisions) {
		t.Fatalf("compat plan = %+v/%+v, want planner output %+v/%+v", progress, decisions, stagePlan.Progress, stagePlan.Decisions)
	}
}

func TestImportPipelineStageTasks(t *testing.T) {
	hash := tcommon.Hash{0x42}
	got := ImportPipelineStageTasks(7, hash)
	want := []ImportStageTask{
		{Phase: ImportStagePhaseBodies, CanonicalStage: rawdb.StageBodies, SyncStage: rawdb.StageSyncImport, BlockNum: 7, BlockHash: hash},
		{Phase: ImportStagePhaseExecution, CanonicalStage: rawdb.StageExecution, SyncStage: rawdb.StageSyncExecution, BlockNum: 7, BlockHash: hash},
		{Phase: ImportStagePhaseCommitment, CanonicalStage: rawdb.StageCommitment, SyncStage: rawdb.StageSyncCommitment, BlockNum: 7, BlockHash: hash},
		{Phase: ImportStagePhaseFinish, CanonicalStage: rawdb.StageFinish, SyncStage: rawdb.StageSyncFinish, BlockNum: 7, BlockHash: hash},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ImportPipelineStageTasks = %+v, want %+v", got, want)
	}
}

func TestImportExecutionStageTasks(t *testing.T) {
	hash := tcommon.Hash{0x42}
	got := ImportExecutionStageTasks(7, hash)
	want := []ImportStageTask{
		{Phase: ImportStagePhaseExecution, CanonicalStage: rawdb.StageExecution, SyncStage: rawdb.StageSyncExecution, BlockNum: 7, BlockHash: hash},
		{Phase: ImportStagePhaseCommitment, CanonicalStage: rawdb.StageCommitment, SyncStage: rawdb.StageSyncCommitment, BlockNum: 7, BlockHash: hash},
		{Phase: ImportStagePhaseFinish, CanonicalStage: rawdb.StageFinish, SyncStage: rawdb.StageSyncFinish, BlockNum: 7, BlockHash: hash},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ImportExecutionStageTasks = %+v, want %+v", got, want)
	}
	if ImportExecutionStageTask(7, hash) != want[0] {
		t.Fatalf("ImportExecutionStageTask = %+v, want %+v", ImportExecutionStageTask(7, hash), want[0])
	}
	if ImportCommitmentStageTask(7, hash) != want[1] {
		t.Fatalf("ImportCommitmentStageTask = %+v, want %+v", ImportCommitmentStageTask(7, hash), want[1])
	}
	if ImportFinishStageTask(7, hash) != want[2] {
		t.Fatalf("ImportFinishStageTask = %+v, want %+v", ImportFinishStageTask(7, hash), want[2])
	}
}

func TestNewImportStageSchedule(t *testing.T) {
	hash := tcommon.Hash{0x42}
	got := NewImportStageSchedule(7, hash)
	wantTasks := ImportPipelineStageTasks(7, hash)
	wantBody := ImportBodyStageTask(7, hash)
	wantPostBody := ImportExecutionStageTasks(7, hash)
	if got.BlockNum != 7 || got.BlockHash != hash || !reflect.DeepEqual(got.Tasks, wantTasks) || got.Body != wantBody || !reflect.DeepEqual(got.PostBody, wantPostBody) {
		t.Fatalf("schedule = %+v, want block/hash/tasks %+v", got, wantTasks)
	}
	if got.Execution != wantPostBody[0] || got.Commitment != wantPostBody[1] || got.Finish != wantPostBody[2] {
		t.Fatalf("named post-body tasks = %+v/%+v/%+v, want %+v", got.Execution, got.Commitment, got.Finish, wantPostBody)
	}

	progress, decisions := NewStageProgressCollector().Plan(ImportStageSchedule{})
	if progress != nil || decisions != nil {
		t.Fatalf("empty schedule plan = %+v/%+v, want nil/nil", progress, decisions)
	}
}

func TestNewImportBatchStagePlan(t *testing.T) {
	hash1 := tcommon.Hash{0x41}
	hash2 := tcommon.Hash{0x42}
	schedules := []ImportStageSchedule{
		NewImportStageSchedule(1, hash1),
		NewImportStageSchedule(2, hash2),
	}

	got := NewImportBatchStagePlan(schedules)
	if got.Empty() {
		t.Fatal("batch stage plan is empty, want scheduled tasks")
	}
	if !reflect.DeepEqual(got.Schedules, schedules) {
		t.Fatalf("schedules = %+v, want %+v", got.Schedules, schedules)
	}
	wantBodies := []ImportStageTask{
		ImportBodyStageTask(1, hash1),
		ImportBodyStageTask(2, hash2),
	}
	wantExecution := []ImportStageTask{
		ImportExecutionStageTask(1, hash1),
		ImportExecutionStageTask(2, hash2),
	}
	wantCommitment := []ImportStageTask{
		ImportCommitmentStageTask(1, hash1),
		ImportCommitmentStageTask(2, hash2),
	}
	wantFinish := []ImportStageTask{
		ImportFinishStageTask(1, hash1),
		ImportFinishStageTask(2, hash2),
	}
	if !reflect.DeepEqual(got.Bodies, wantBodies) || !reflect.DeepEqual(got.Execution, wantExecution) || !reflect.DeepEqual(got.Commitment, wantCommitment) || !reflect.DeepEqual(got.Finish, wantFinish) {
		t.Fatalf("phase groups = bodies:%+v execution:%+v commitment:%+v finish:%+v, want grouped block1/block2 tasks",
			got.Bodies, got.Execution, got.Commitment, got.Finish)
	}
	wantPhases := []ImportStagePhasePlan{
		{Phase: ImportStagePhaseBodies, CanonicalStage: rawdb.StageBodies, SyncStage: rawdb.StageSyncImport, Tasks: wantBodies},
		{Phase: ImportStagePhaseExecution, CanonicalStage: rawdb.StageExecution, SyncStage: rawdb.StageSyncExecution, Tasks: wantExecution},
		{Phase: ImportStagePhaseCommitment, CanonicalStage: rawdb.StageCommitment, SyncStage: rawdb.StageSyncCommitment, Tasks: wantCommitment},
		{Phase: ImportStagePhaseFinish, CanonicalStage: rawdb.StageFinish, SyncStage: rawdb.StageSyncFinish, Tasks: wantFinish},
	}
	if !reflect.DeepEqual(got.Phases, wantPhases) {
		t.Fatalf("phase plans = %+v, want %+v", got.Phases, wantPhases)
	}
	if !reflect.DeepEqual(got.PhasePlans(), wantPhases) {
		t.Fatalf("PhasePlans = %+v, want %+v", got.PhasePlans(), wantPhases)
	}
	wantPostBody := append([]ImportStageTask{}, wantExecution...)
	wantPostBody = append(wantPostBody, wantCommitment...)
	wantPostBody = append(wantPostBody, wantFinish...)
	if !reflect.DeepEqual(got.PostBody, wantPostBody) {
		t.Fatalf("post-body tasks = %+v, want %+v", got.PostBody, wantPostBody)
	}
	wantTasks := append([]ImportStageTask{}, wantBodies...)
	wantTasks = append(wantTasks, wantPostBody...)
	if !reflect.DeepEqual(got.Tasks, wantTasks) {
		t.Fatalf("tasks = %+v, want %+v", got.Tasks, wantTasks)
	}
	for _, tt := range []struct {
		phase ImportStagePhase
		want  []ImportStageTask
	}{
		{ImportStagePhaseBodies, wantBodies},
		{ImportStagePhaseExecution, wantExecution},
		{ImportStagePhaseCommitment, wantCommitment},
		{ImportStagePhaseFinish, wantFinish},
	} {
		phasePlan, ok := got.PhasePlan(tt.phase)
		if !ok || !reflect.DeepEqual(phasePlan.Tasks, tt.want) {
			t.Fatalf("PhasePlan(%s) = %+v ok=%v, want tasks %+v", tt.phase, phasePlan, ok, tt.want)
		}
		if phaseTasks := got.TasksForPhase(tt.phase); !reflect.DeepEqual(phaseTasks, tt.want) {
			t.Fatalf("TasksForPhase(%s) = %+v, want %+v", tt.phase, phaseTasks, tt.want)
		}
	}
	phaseCopy := got.PhasePlans()
	phaseCopy[0].Tasks[0].BlockNum = 99
	if got.Phases[0].Tasks[0].BlockNum == 99 {
		t.Fatal("PhasePlans returned aliased task slice")
	}
	if phaseTasks := got.TasksForPhase(ImportStagePhase("unknown")); phaseTasks != nil {
		t.Fatalf("unknown phase tasks = %+v, want nil", phaseTasks)
	}
	if phasePlan, ok := got.PhasePlan(ImportStagePhase("unknown")); ok || len(phasePlan.Tasks) != 0 {
		t.Fatalf("unknown phase plan = %+v ok=%v, want empty/false", phasePlan, ok)
	}
	task, ok := got.MatchCanonicalObservation(rawdb.StageCommitment, 2, hash2)
	if !ok || task != ImportCommitmentStageTask(2, hash2) {
		t.Fatalf("matched task = %+v ok=%v, want block2 commitment", task, ok)
	}
	observation, ok := got.MatchPhaseObservation(rawdb.StageCommitment, 2, hash2)
	if !ok || observation.Task != ImportCommitmentStageTask(2, hash2) || observation.Phase.Phase != ImportStagePhaseCommitment || len(observation.Phase.Tasks) != 2 {
		t.Fatalf("phase observation = %+v ok=%v, want block2 commitment in two-task commitment phase", observation, ok)
	}
	if task, ok := got.MatchCanonicalObservation(rawdb.StageCommitment, 2, hash1); ok {
		t.Fatalf("fork hash matched task %+v, want rejected", task)
	}
	if observation, ok := got.MatchPhaseObservation(rawdb.StageCommitment, 2, hash1); ok {
		t.Fatalf("fork hash matched phase observation %+v, want rejected", observation)
	}
	empty := NewImportBatchStagePlan(nil)
	if !empty.Empty() {
		t.Fatalf("empty plan = %+v, want empty", empty)
	}
}

func TestImportStageScheduleMatchCanonicalObservation(t *testing.T) {
	hash := tcommon.Hash{0x42}
	schedule := NewImportStageSchedule(7, hash)

	task, ok := schedule.MatchCanonicalObservation(rawdb.StageCommitment, 7, hash)
	if !ok || task.Phase != ImportStagePhaseCommitment || task.SyncStage != rawdb.StageSyncCommitment {
		t.Fatalf("matched task = %+v ok=%v, want commitment task", task, ok)
	}

	for name, test := range map[string]struct {
		stage rawdb.StageID
		num   uint64
		hash  tcommon.Hash
	}{
		"unknown stage": {stage: rawdb.StageHeaders, num: 7, hash: hash},
		"wrong number":  {stage: rawdb.StageCommitment, num: 8, hash: hash},
		"wrong hash":    {stage: rawdb.StageCommitment, num: 7, hash: tcommon.Hash{0xee}},
	} {
		if task, ok := schedule.MatchCanonicalObservation(test.stage, test.num, test.hash); ok {
			t.Fatalf("%s matched %+v, want rejected", name, task)
		}
	}
}

func TestSyncPipelineProgressStagesOrder(t *testing.T) {
	got := SyncPipelineProgressStages()
	want := []rawdb.StageID{
		rawdb.StageSyncImport,
		rawdb.StageSyncExecution,
		rawdb.StageSyncCommitment,
		rawdb.StageSyncFinish,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SyncPipelineProgressStages = %v, want %v", got, want)
	}
}

func TestRepairSyncStageProgress(t *testing.T) {
	stage := rawdb.StageSyncImport
	canonical := map[uint64]tcommon.Hash{
		2: {0x02},
		3: {0x03},
	}
	readCanonical := func(number uint64) (tcommon.Hash, bool) {
		hash, ok := canonical[number]
		return hash, ok
	}
	tests := []struct {
		name   string
		row    *rawdb.StageProgress
		head   uint64
		status SyncStageProgressRepairStatus
		kept   bool
	}{
		{name: "missing", head: 3, status: SyncStageProgressMissing},
		{name: "valid", row: &rawdb.StageProgress{Stage: stage, BlockNum: 2, BlockHash: canonical[2], HasBlockHash: true}, head: 3, status: SyncStageProgressKept, kept: true},
		{name: "unbound", row: &rawdb.StageProgress{Stage: stage, BlockNum: 2}, head: 3, status: SyncStageProgressDeleted},
		{name: "ahead", row: &rawdb.StageProgress{Stage: stage, BlockNum: 4, BlockHash: tcommon.Hash{0x04}, HasBlockHash: true}, head: 3, status: SyncStageProgressDeleted},
		{name: "fork hash", row: &rawdb.StageProgress{Stage: stage, BlockNum: 2, BlockHash: tcommon.Hash{0xee}, HasBlockHash: true}, head: 3, status: SyncStageProgressDeleted},
		{name: "missing canonical", row: &rawdb.StageProgress{Stage: stage, BlockNum: 1, BlockHash: tcommon.Hash{0x01}, HasBlockHash: true}, head: 3, status: SyncStageProgressDeleted},
	}
	for _, tt := range tests {
		db := rawdb.NewMemoryDatabase()
		if tt.row != nil {
			if tt.row.HasBlockHash {
				if err := rawdb.WriteStageProgressWithHash(db, stage, tt.row.BlockNum, tt.row.BlockHash); err != nil {
					t.Fatalf("%s: write progress: %v", tt.name, err)
				}
			} else if err := rawdb.WriteStageProgress(db, stage, tt.row.BlockNum); err != nil {
				t.Fatalf("%s: write progress: %v", tt.name, err)
			}
		}
		got := RepairSyncStageProgress(db, stage, tt.head, readCanonical)
		if got.Status != tt.status {
			t.Fatalf("%s: status = %v result %+v, want %v", tt.name, got.Status, got, tt.status)
		}
		_, ok, err := rawdb.ReadStageProgressRow(db, stage)
		if err != nil {
			t.Fatalf("%s: read progress after repair: %v", tt.name, err)
		}
		if ok != tt.kept {
			t.Fatalf("%s: kept row = %v, want %v", tt.name, ok, tt.kept)
		}
	}
}

func TestRepairSyncPipelineProgressDeletesDownstreamAheadOfUpstream(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	canonical := map[uint64]tcommon.Hash{
		1: {0x01},
		2: {0x02},
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncImport, 1, canonical[1]); err != nil {
		t.Fatalf("write import progress: %v", err)
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncExecution, rawdb.StageSyncCommitment, rawdb.StageSyncFinish} {
		if err := rawdb.WriteStageProgressWithHash(db, stage, 2, canonical[2]); err != nil {
			t.Fatalf("write %s progress: %v", stage, err)
		}
	}

	got := RepairSyncPipelineProgress(db, 2, func(number uint64) (tcommon.Hash, bool) {
		hash, ok := canonical[number]
		return hash, ok
	})
	wantStatuses := []SyncStageProgressRepairStatus{
		SyncStageProgressKept,
		SyncStageProgressDeleted,
		SyncStageProgressDeleted,
		SyncStageProgressDeleted,
	}
	if len(got) != len(wantStatuses) {
		t.Fatalf("repairs = %+v, want %d", got, len(wantStatuses))
	}
	for i, status := range wantStatuses {
		if got[i].Status != status {
			t.Fatalf("repair %d = %+v, want status %v", i, got[i], status)
		}
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncImport); err != nil || !ok || row.BlockNum != 1 {
		t.Fatalf("import progress = %+v ok=%v err=%v, want block1 kept", row, ok, err)
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncExecution, rawdb.StageSyncCommitment, rawdb.StageSyncFinish} {
		if row, ok, err := rawdb.ReadStageProgressRow(db, stage); err != nil || ok {
			t.Fatalf("%s progress = %+v ok=%v err=%v, want deleted", stage, row, ok, err)
		}
	}
}

func TestRepairSyncPipelineProgressDeletesDownstreamAfterMiddleForkHashMismatch(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	canonical := tcommon.Hash{0x01}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncImport, 1, canonical); err != nil {
		t.Fatalf("write import progress: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncExecution, 1, canonical); err != nil {
		t.Fatalf("write execution progress: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncCommitment, 1, tcommon.Hash{0xee}); err != nil {
		t.Fatalf("write forked commitment progress: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncFinish, 1, canonical); err != nil {
		t.Fatalf("write finish progress: %v", err)
	}

	got := RepairSyncPipelineProgress(db, 1, func(number uint64) (tcommon.Hash, bool) {
		if number == 1 {
			return canonical, true
		}
		return tcommon.Hash{}, false
	})
	wantStatuses := []SyncStageProgressRepairStatus{
		SyncStageProgressKept,
		SyncStageProgressKept,
		SyncStageProgressDeleted,
		SyncStageProgressDeleted,
	}
	if len(got) != len(wantStatuses) {
		t.Fatalf("repairs = %+v, want %d", got, len(wantStatuses))
	}
	for i, status := range wantStatuses {
		if got[i].Status != status {
			t.Fatalf("repair %d = %+v, want status %v", i, got[i], status)
		}
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncImport, rawdb.StageSyncExecution} {
		if row, ok, err := rawdb.ReadStageProgressRow(db, stage); err != nil || !ok || row.BlockNum != 1 || row.BlockHash != canonical {
			t.Fatalf("%s progress = %+v ok=%v err=%v, want kept canonical row", stage, row, ok, err)
		}
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncCommitment, rawdb.StageSyncFinish} {
		if row, ok, err := rawdb.ReadStageProgressRow(db, stage); err != nil || ok {
			t.Fatalf("%s progress = %+v ok=%v err=%v, want deleted", stage, row, ok, err)
		}
	}
}

func TestRepairSyncPipelineProgressDeletesDownstreamWithoutUpstream(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	hash := tcommon.Hash{0x01}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncExecution, 1, hash); err != nil {
		t.Fatalf("write execution progress: %v", err)
	}

	got := RepairSyncPipelineProgress(db, 1, func(number uint64) (tcommon.Hash, bool) {
		if number == 1 {
			return hash, true
		}
		return tcommon.Hash{}, false
	})
	wantStatuses := []SyncStageProgressRepairStatus{
		SyncStageProgressMissing,
		SyncStageProgressDeleted,
		SyncStageProgressMissing,
		SyncStageProgressMissing,
	}
	if len(got) != len(wantStatuses) {
		t.Fatalf("repairs = %+v, want %d", got, len(wantStatuses))
	}
	for i, status := range wantStatuses {
		if got[i].Status != status {
			t.Fatalf("repair %d = %+v, want status %v", i, got[i], status)
		}
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncExecution); err != nil || ok {
		t.Fatalf("execution progress = %+v ok=%v err=%v, want deleted", row, ok, err)
	}
}

func TestRepairSyncPipelineProgressWithResultSummarizesBoundaries(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	hash := tcommon.Hash{0x01}
	for _, stage := range SyncPipelineProgressStages() {
		if err := rawdb.WriteStageProgressWithHash(db, stage, 1, hash); err != nil {
			t.Fatalf("write %s progress: %v", stage, err)
		}
	}

	complete := RepairSyncPipelineProgressWithResult(db, 1, func(number uint64) (tcommon.Hash, bool) {
		if number == 1 {
			return hash, true
		}
		return tcommon.Hash{}, false
	})
	if !complete.Complete || complete.HasBlocked || complete.Interrupted ||
		complete.Kept != len(SyncPipelineProgressStages()) ||
		complete.Missing != 0 || complete.Deleted != 0 ||
		len(complete.Repairs) != len(SyncPipelineProgressStages()) {
		t.Fatalf("complete repair result = %+v, want full kept pipeline", complete)
	}

	db = rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncExecution, 1, hash); err != nil {
		t.Fatalf("write orphan execution progress: %v", err)
	}
	blocked := RepairSyncPipelineProgressWithResult(db, 1, func(number uint64) (tcommon.Hash, bool) {
		if number == 1 {
			return hash, true
		}
		return tcommon.Hash{}, false
	})
	if blocked.Complete || !blocked.HasBlocked || blocked.FirstBlockedStage != rawdb.StageSyncImport ||
		blocked.Interrupted || blocked.Kept != 0 || blocked.Missing != 3 || blocked.Deleted != 1 {
		t.Fatalf("blocked repair result = %+v, want missing import with deleted downstream execution", blocked)
	}
}

func TestRepairSyncPipelineProgressStopsBeforeDownstreamOnReadError(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncImport, 1, tcommon.Hash{0x01}); err != nil {
		t.Fatalf("write import progress: %v", err)
	}
	if err := corruptOnlyStageProgressRow(db); err != nil {
		t.Fatalf("corrupt import progress: %v", err)
	}
	forkHash := tcommon.Hash{0xee}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncExecution, 1, forkHash); err != nil {
		t.Fatalf("write forked execution progress: %v", err)
	}

	got := RepairSyncPipelineProgress(db, 1, func(number uint64) (tcommon.Hash, bool) {
		if number == 1 {
			return tcommon.Hash{0x01}, true
		}
		return tcommon.Hash{}, false
	})
	if len(got) != 1 || got[0].Stage != rawdb.StageSyncImport || got[0].Status != SyncStageProgressReadError {
		t.Fatalf("repairs = %+v, want only import read error", got)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncExecution); err != nil || !ok || row.BlockHash != forkHash {
		t.Fatalf("execution progress after import read error = %+v ok=%v err=%v, want retained fork row", row, ok, err)
	}

	result := RepairSyncPipelineProgressWithResult(db, 1, func(number uint64) (tcommon.Hash, bool) {
		if number == 1 {
			return tcommon.Hash{0x01}, true
		}
		return tcommon.Hash{}, false
	})
	if !result.Interrupted || result.ErrorStage != rawdb.StageSyncImport || result.Complete ||
		len(result.Repairs) != 1 || result.Repairs[0].Status != SyncStageProgressReadError {
		t.Fatalf("read-error result = %+v, want interrupted at import", result)
	}
}

func corruptOnlyStageProgressRow(db ethdb.KeyValueStore) error {
	it := db.NewIterator(nil, nil)
	defer it.Release()
	if !it.Next() {
		if err := it.Error(); err != nil {
			return err
		}
		return errors.New("stage progress row not found")
	}
	key := append([]byte(nil), it.Key()...)
	if it.Next() {
		return ethdb.ErrTooManyKeys
	}
	if err := it.Error(); err != nil {
		return err
	}
	return db.Put(key, []byte{0x01})
}
