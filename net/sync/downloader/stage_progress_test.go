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

func TestStageProgressCollectorRejectsUnownedPlannedObservation(t *testing.T) {
	hash := tcommon.Hash{0x02}
	task := ImportExecutionStageTask(2, hash)
	phase := ImportStagePhasePlan{
		Phase:          ImportStagePhaseExecution,
		CanonicalStage: rawdb.StageExecution,
		SyncStage:      rawdb.StageSyncExecution,
		Tasks:          []ImportStageTask{task},
	}
	if observation := (ImportStageObservation{Phase: phase, Task: task}); !observation.Valid() {
		t.Fatalf("valid planned observation reported invalid: %+v", observation)
	}

	for name, observation := range map[string]ImportStageObservation{
		"empty": {},
		"phase mismatch": {
			Phase: ImportStagePhasePlan{
				Phase:          ImportStagePhaseCommitment,
				CanonicalStage: rawdb.StageCommitment,
				SyncStage:      rawdb.StageSyncCommitment,
				Tasks:          []ImportStageTask{task},
			},
			Task: task,
		},
		"task not owned": {
			Phase: ImportStagePhasePlan{
				Phase:          ImportStagePhaseExecution,
				CanonicalStage: rawdb.StageExecution,
				SyncStage:      rawdb.StageSyncExecution,
				Tasks:          []ImportStageTask{ImportExecutionStageTask(3, tcommon.Hash{0x03})},
			},
			Task: task,
		},
		"task stage mismatch": {
			Phase: phase,
			Task:  ImportStageTask{Phase: ImportStagePhaseExecution, CanonicalStage: rawdb.StageCommitment, SyncStage: rawdb.StageSyncExecution, BlockNum: task.BlockNum, BlockHash: task.BlockHash},
		},
	} {
		if observation.Valid() {
			t.Fatalf("%s observation reported valid: %+v", name, observation)
		}
		collector := NewStageProgressCollector()
		collector.ObservePlanned(observation)
		if rows := collector.RowsForSchedule(NewImportStageSchedule(task.BlockNum, task.BlockHash)); len(rows) != 0 {
			t.Fatalf("%s observation produced rows %+v, want none", name, rows)
		}
	}
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
	if !got.AppliedStagePhases.Empty() || len(got.AppliedPhases) != 0 {
		t.Fatalf("applied phase schedule = %+v phases=%+v, want empty without execution schedule", got.AppliedStagePhases, got.AppliedPhases)
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
	if cursor := got.StagePhaseCursor; cursor.ScheduledPhases != 4 ||
		cursor.CompletedPhases != 1 ||
		cursor.ScheduledTasks != 8 ||
		cursor.CompletedTasks != 3 ||
		cursor.Complete ||
		!cursor.HasCurrent ||
		cursor.CurrentPhase != ImportStagePhaseExecution ||
		cursor.CurrentCanonicalStage != rawdb.StageExecution ||
		cursor.CurrentSyncStage != rawdb.StageSyncExecution ||
		cursor.CurrentTaskIndex != 1 ||
		!cursor.HasNextTask ||
		cursor.NextTask != ImportExecutionStageTask(block2.Number(), block2.Hash()) ||
		!cursor.HasBlocked ||
		cursor.BlockedStatus != ImportStageProgressMismatch {
		t.Fatalf("stage phase cursor = %+v, want execution phase cursor at block2 mismatch", cursor)
	}
	if got.AppliedStagePhases.Empty() ||
		len(got.AppliedPhases) != 4 ||
		got.AppliedStagePhases.Execution.Tasks[1] != ImportExecutionStageTask(block2.Number(), block2.Hash()) ||
		got.AppliedStagePhases.Commitment.Tasks[1] != ImportCommitmentStageTask(block2.Number(), block2.Hash()) ||
		got.AppliedStagePhases.Finish.Tasks[1] != ImportFinishStageTask(block2.Number(), block2.Hash()) {
		t.Fatalf("applied phase schedule = %+v phases=%+v, want two-block bodies/execution/commitment/finish prefix",
			got.AppliedStagePhases, got.AppliedPhases)
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

	result := ApplyImportedBatchProgressPlan(plan, &applier)
	want := []recordedImportedBatchProgressCall{
		{action: ImportedBatchWriteProgress, deletes: []rawdb.SyncStagedBlockDelete{deleteRow}, progress: []rawdb.StageProgress{progressRow}},
		{action: ImportedBatchRefreshBodiesReady},
	}
	if !reflect.DeepEqual(applier.calls, want) {
		t.Fatalf("calls = %+v, want %+v", applier.calls, want)
	}
	if !reflect.DeepEqual(result.AppliedSteps, []ImportedBatchProgressStepAction{ImportedBatchWriteProgress, ImportedBatchRefreshBodiesReady}) {
		t.Fatalf("applied steps = %+v, want write/refresh", result.AppliedSteps)
	}
	if !reflect.DeepEqual(result.UnknownSteps, []ImportedBatchProgressStepAction{ImportedBatchProgressStepAction(255)}) {
		t.Fatalf("unknown steps = %+v, want [255]", result.UnknownSteps)
	}
	if !result.HasWriteResult || result.WriteDeletes != 1 || result.WriteProgressRows != 1 || result.WriteResult.Deleted != 1 || result.WriteResult.ProgressRows != 1 {
		t.Fatalf("write result = %+v deletes=%d rows=%d set=%v, want 1 delete and 1 progress row", result.WriteResult, result.WriteDeletes, result.WriteProgressRows, result.HasWriteResult)
	}
	if !result.HasReadyRefresh || !result.ReadyRefresh.Updated {
		t.Fatalf("ready refresh = %+v set=%v, want updated", result.ReadyRefresh, result.HasReadyRefresh)
	}

	if empty := ApplyImportedBatchProgressPlan(plan, nil); len(empty.AppliedSteps) != 0 || len(empty.UnknownSteps) != 0 || empty.HasWriteResult || empty.HasReadyRefresh {
		t.Fatalf("nil applier result = %+v, want empty", empty)
	}
	if empty := ApplyImportedBatchProgressPlan(ImportedBatchProgressPlan{}, &applier); len(empty.AppliedSteps) != 0 || len(empty.UnknownSteps) != 0 || empty.HasWriteResult || empty.HasReadyRefresh {
		t.Fatalf("empty plan result = %+v, want empty", empty)
	}
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

func (a *recordingImportedBatchProgressApplier) WriteImportedSyncProgress(deletes []rawdb.SyncStagedBlockDelete, rows []rawdb.StageProgress) rawdb.SyncImportProgressWriteResult {
	a.calls = append(a.calls, recordedImportedBatchProgressCall{
		action:   ImportedBatchWriteProgress,
		deletes:  append([]rawdb.SyncStagedBlockDelete(nil), deletes...),
		progress: append([]rawdb.StageProgress(nil), rows...),
	})
	return rawdb.SyncImportProgressWriteResult{
		Deleted:      len(deletes),
		ProgressRows: len(rows),
	}
}

func (a *recordingImportedBatchProgressApplier) RefreshSyncBodiesReady() StagedBodyReadyProgressRefresh {
	a.calls = append(a.calls, recordedImportedBatchProgressCall{action: ImportedBatchRefreshBodiesReady})
	return StagedBodyReadyProgressRefresh{Updated: true}
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

func TestImportStageSpecsDefinePlannerOrder(t *testing.T) {
	got := ImportStageSpecs()
	want := []ImportStageSpec{
		{Phase: ImportStagePhaseBodies, CanonicalStage: rawdb.StageBodies, SyncStage: rawdb.StageSyncImport},
		{Phase: ImportStagePhaseExecution, CanonicalStage: rawdb.StageExecution, SyncStage: rawdb.StageSyncExecution},
		{Phase: ImportStagePhaseCommitment, CanonicalStage: rawdb.StageCommitment, SyncStage: rawdb.StageSyncCommitment},
		{Phase: ImportStagePhaseFinish, CanonicalStage: rawdb.StageFinish, SyncStage: rawdb.StageSyncFinish},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ImportStageSpecs = %+v, want %+v", got, want)
	}
	got[1].Phase = ImportStagePhaseFinish
	if again := ImportStageSpecs(); !reflect.DeepEqual(again, want) {
		t.Fatalf("ImportStageSpecs returned aliased planner spec: %+v", again)
	}
	hash := tcommon.Hash{0x42}
	if got := want[2].Task(7, hash); got != ImportCommitmentStageTask(7, hash) {
		t.Fatalf("commitment spec task = %+v, want %+v", got, ImportCommitmentStageTask(7, hash))
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

func TestNewImportBatchStagePhaseSchedule(t *testing.T) {
	hash1 := tcommon.Hash{0x41}
	hash2 := tcommon.Hash{0x42}
	stagePlan := NewImportBatchStagePlan([]ImportStageSchedule{
		NewImportStageSchedule(1, hash1),
		NewImportStageSchedule(2, hash2),
	})

	got := NewImportBatchStagePhaseSchedule(stagePlan)
	if got.Empty() {
		t.Fatal("phase schedule is empty, want scheduled phases")
	}
	if !got.HasBody || !got.HasExecution || !got.HasCommitment || !got.HasFinish {
		t.Fatalf("phase presence = body:%v execution:%v commitment:%v finish:%v, want all present",
			got.HasBody, got.HasExecution, got.HasCommitment, got.HasFinish)
	}
	if len(got.Phases) != 4 || len(got.PostBody) != 3 || len(got.Tasks) != 8 || len(got.PostBodyTasks) != 6 {
		t.Fatalf("phase counts = phases:%d postBody:%d tasks:%d postBodyTasks:%d, want 4/3/8/6",
			len(got.Phases), len(got.PostBody), len(got.Tasks), len(got.PostBodyTasks))
	}
	if got.Body.Phase != ImportStagePhaseBodies ||
		got.Execution.Phase != ImportStagePhaseExecution ||
		got.Commitment.Phase != ImportStagePhaseCommitment ||
		got.Finish.Phase != ImportStagePhaseFinish {
		t.Fatalf("named phases = body:%s execution:%s commitment:%s finish:%s, want canonical order",
			got.Body.Phase, got.Execution.Phase, got.Commitment.Phase, got.Finish.Phase)
	}
	if got.PostBody[0].Phase != ImportStagePhaseExecution ||
		got.PostBody[1].Phase != ImportStagePhaseCommitment ||
		got.PostBody[2].Phase != ImportStagePhaseFinish {
		t.Fatalf("post-body phases = %+v, want execution/commitment/finish", got.PostBody)
	}
	if got.Execution.Tasks[1] != ImportExecutionStageTask(2, hash2) ||
		got.Commitment.Tasks[1] != ImportCommitmentStageTask(2, hash2) ||
		got.Finish.Tasks[1] != ImportFinishStageTask(2, hash2) {
		t.Fatalf("post-body phase tasks = execution:%+v commitment:%+v finish:%+v, want block2 tasks",
			got.Execution.Tasks, got.Commitment.Tasks, got.Finish.Tasks)
	}
	phasePlan, ok := got.PhasePlan(ImportStagePhaseCommitment)
	if !ok || len(phasePlan.Tasks) != 2 || phasePlan.Tasks[1] != ImportCommitmentStageTask(2, hash2) {
		t.Fatalf("PhasePlan(commitment) = %+v ok=%v, want block2 commitment task", phasePlan, ok)
	}
	phasePlan.Tasks[1].BlockNum = 99
	if got.Commitment.Tasks[1].BlockNum == 99 {
		t.Fatal("PhasePlan returned aliased task slice")
	}
	phaseCopy := got.PhasePlans()
	phaseCopy[1].Tasks[1].BlockNum = 99
	if got.Execution.Tasks[1].BlockNum == 99 {
		t.Fatal("PhasePlans returned aliased task slice")
	}
	observation, ok := got.MatchPhaseObservation(rawdb.StageFinish, 2, hash2)
	if !ok || observation.Task != ImportFinishStageTask(2, hash2) || observation.Phase.Phase != ImportStagePhaseFinish || len(observation.Phase.Tasks) != 2 {
		t.Fatalf("finish observation = %+v ok=%v, want block2 finish in finish phase", observation, ok)
	}
	if observation, ok := got.MatchPhaseObservation(rawdb.StageFinish, 2, tcommon.Hash{0xee}); ok {
		t.Fatalf("fork hash observation = %+v ok=true, want rejected", observation)
	}
	empty := NewImportBatchStagePhaseSchedule(ImportBatchStagePlan{})
	if !empty.Empty() || len(empty.PhasePlans()) != 0 {
		t.Fatalf("empty phase schedule = %+v, want empty", empty)
	}
}

func TestPlanImportStagePhaseCursor(t *testing.T) {
	hash1 := tcommon.Hash{0x41}
	hash2 := tcommon.Hash{0x42}
	stagePlan := NewImportBatchStagePlan([]ImportStageSchedule{
		NewImportStageSchedule(1, hash1),
		NewImportStageSchedule(2, hash2),
	})
	schedule := NewImportBatchStagePhaseSchedule(stagePlan)
	collector := NewStageProgressCollector()
	collector.Observe(rawdb.StageBodies, 1, hash1)
	collector.Observe(rawdb.StageBodies, 2, hash2)
	collector.Observe(rawdb.StageExecution, 1, hash1)

	cursor := PlanImportStagePhaseCursor(schedule, collector.PlanBatch(stagePlan))
	if cursor.ScheduledPhases != 4 ||
		cursor.CompletedPhases != 1 ||
		cursor.ScheduledTasks != 8 ||
		cursor.CompletedTasks != 3 ||
		cursor.Complete ||
		!cursor.HasCurrent ||
		cursor.CurrentPhase != ImportStagePhaseExecution ||
		cursor.CurrentCanonicalStage != rawdb.StageExecution ||
		cursor.CurrentSyncStage != rawdb.StageSyncExecution ||
		len(cursor.CurrentTasks) != 2 ||
		cursor.CurrentTaskIndex != 1 ||
		!cursor.HasNextTask ||
		cursor.NextTask != ImportExecutionStageTask(2, hash2) ||
		!cursor.HasBlocked ||
		cursor.BlockedStatus != ImportStageProgressMismatch {
		t.Fatalf("cursor = %+v, want current execution phase at block2", cursor)
	}
	cursor.CurrentTasks[1].BlockNum = 99
	if schedule.Execution.Tasks[1].BlockNum == 99 {
		t.Fatal("phase cursor returned aliased current task slice")
	}

	completeCollector := NewStageProgressCollector()
	for _, task := range stagePlan.Tasks {
		completeCollector.Observe(task.CanonicalStage, task.BlockNum, task.BlockHash)
	}
	complete := PlanImportStagePhaseCursor(schedule, completeCollector.PlanBatch(stagePlan))
	if !complete.Complete ||
		complete.CompletedPhases != 4 ||
		complete.CompletedTasks != 8 ||
		complete.HasCurrent ||
		complete.HasNextTask ||
		complete.HasBlocked {
		t.Fatalf("complete cursor = %+v, want complete without current phase", complete)
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

func TestFullSyncPipelineProgressStagesOrder(t *testing.T) {
	got := FullSyncPipelineProgressStages()
	want := []rawdb.StageID{
		rawdb.StageSyncBodies,
		rawdb.StageSyncBodiesReady,
		rawdb.StageSyncImport,
		rawdb.StageSyncExecution,
		rawdb.StageSyncCommitment,
		rawdb.StageSyncFinish,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("FullSyncPipelineProgressStages = %v, want %v", got, want)
	}
}

func TestCheckSyncPipelineProgressOrder(t *testing.T) {
	progress := map[rawdb.StageID]rawdb.StageProgress{
		rawdb.StageSyncBodies:      {Stage: rawdb.StageSyncBodies, BlockNum: 7},
		rawdb.StageSyncBodiesReady: {Stage: rawdb.StageSyncBodiesReady, BlockNum: 8},
		rawdb.StageSyncImport:      {Stage: rawdb.StageSyncImport, BlockNum: 3},
		rawdb.StageSyncExecution:   {Stage: rawdb.StageSyncExecution, BlockNum: 4},
		rawdb.StageSyncCommitment:  {Stage: rawdb.StageSyncCommitment, BlockNum: 4},
		rawdb.StageSyncFinish:      {Stage: rawdb.StageSyncFinish, BlockNum: 4},
	}

	issues := CheckSyncPipelineProgressOrder(progress, SyncPipelineProgressOrderOptions{})
	want := []string{
		"SyncBodiesReady=8 ahead of SyncBodies=7",
		"SyncExecution=4 ahead of SyncImport=3",
	}
	if len(issues) != len(want) {
		t.Fatalf("issues = %+v, want %d entries", issues, len(want))
	}
	for i, issue := range issues {
		if issue.String() != want[i] {
			t.Fatalf("issue %d = %q, want %q", i, issue.String(), want[i])
		}
	}

	issues = CheckSyncPipelineProgressOrder(map[rawdb.StageID]rawdb.StageProgress{
		rawdb.StageSyncExecution: {Stage: rawdb.StageSyncExecution, BlockNum: 4},
	}, SyncPipelineProgressOrderOptions{})
	if len(issues) != 0 {
		t.Fatalf("non-strict missing upstream issues = %+v, want none", issues)
	}

	issues = CheckSyncPipelineProgressOrder(map[rawdb.StageID]rawdb.StageProgress{
		rawdb.StageSyncExecution: {Stage: rawdb.StageSyncExecution, BlockNum: 4},
	}, SyncPipelineProgressOrderOptions{RequireUpstream: true})
	if len(issues) != 1 || !issues[0].MissingUpstream || issues[0].String() != "SyncExecution requires SyncImport" {
		t.Fatalf("strict missing upstream issues = %+v, want SyncExecution requires SyncImport", issues)
	}
}

func TestCheckSyncPipelineProgressOrderFromDB(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	writes := []struct {
		stage rawdb.StageID
		block uint64
	}{
		{stage: rawdb.StageSyncExecution, block: 9},
		{stage: rawdb.StageSyncBodiesReady, block: 8},
		{stage: rawdb.StageSyncImport, block: 6},
		{stage: rawdb.StageSyncBodies, block: 7},
	}
	for _, write := range writes {
		if err := rawdb.WriteStageProgress(db, write.stage, write.block); err != nil {
			t.Fatalf("write %s progress: %v", write.stage, err)
		}
	}

	got := CheckSyncPipelineProgressOrderFromDB(db, SyncPipelineProgressOrderOptions{})
	if len(got.ReadErrors) != 0 {
		t.Fatalf("read errors = %+v, want none", got.ReadErrors)
	}
	wantRows := []rawdb.StageProgress{
		{Stage: rawdb.StageSyncBodies, BlockNum: 7},
		{Stage: rawdb.StageSyncBodiesReady, BlockNum: 8},
		{Stage: rawdb.StageSyncImport, BlockNum: 6},
		{Stage: rawdb.StageSyncExecution, BlockNum: 9},
	}
	if !reflect.DeepEqual(got.Rows, wantRows) {
		t.Fatalf("rows = %+v, want full pipeline order %+v", got.Rows, wantRows)
	}
	wantIssues := []string{
		"SyncBodiesReady=8 ahead of SyncBodies=7",
		"SyncExecution=9 ahead of SyncImport=6",
	}
	if len(got.Issues) != len(wantIssues) {
		t.Fatalf("issues = %+v, want %d entries", got.Issues, len(wantIssues))
	}
	for i, issue := range got.Issues {
		if issue.String() != wantIssues[i] {
			t.Fatalf("issue %d = %q, want %q", i, issue.String(), wantIssues[i])
		}
	}

	if empty := CheckSyncPipelineProgressOrderFromDB(nil, SyncPipelineProgressOrderOptions{}); len(empty.Rows) != 0 || len(empty.Issues) != 0 || len(empty.ReadErrors) != 0 {
		t.Fatalf("nil db result = %+v, want empty", empty)
	}
}

func TestCheckSyncPipelineProgressOrderFromDBReportsReadErrors(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStageProgress(db, rawdb.StageSyncBodies, 7); err != nil {
		t.Fatalf("write bodies progress: %v", err)
	}
	if err := corruptOnlyStageProgressRow(db); err != nil {
		t.Fatalf("corrupt bodies progress: %v", err)
	}

	got := CheckSyncPipelineProgressOrderFromDB(db, SyncPipelineProgressOrderOptions{})
	if len(got.ReadErrors) != 1 || got.ReadErrors[0].Stage != rawdb.StageSyncBodies || got.ReadErrors[0].Err == nil {
		t.Fatalf("read errors = %+v, want SyncBodies decode error", got.ReadErrors)
	}
	if len(got.Rows) != 0 || len(got.Issues) != 0 {
		t.Fatalf("result = %+v, want no rows or order issues after read error", got)
	}
}

func TestPlanSyncPipelineProgressCursor(t *testing.T) {
	hash := tcommon.Hash{0x0a}
	check := SyncPipelineProgressOrderCheckResult{
		Rows: []rawdb.StageProgress{
			{Stage: rawdb.StageSyncBodies, BlockNum: 12, BlockHash: hash, HasBlockHash: true},
			{Stage: rawdb.StageSyncImport, BlockNum: 10, BlockHash: hash, HasBlockHash: true},
			{Stage: rawdb.StageSyncExecution, BlockNum: 10, BlockHash: hash, HasBlockHash: true},
		},
	}

	got := PlanSyncPipelineProgressCursor(check)
	if got.StageRows != 3 || !got.HasLast || got.LastStage != rawdb.StageSyncExecution ||
		got.LastBlock != 10 || got.LastHash != hash || !got.LastHasHash ||
		!got.HasNext || got.NextStage != rawdb.StageSyncCommitment ||
		got.Complete || got.HasBlocked || got.Interrupted {
		t.Fatalf("cursor = %+v, want execution cursor continuing at commitment", got)
	}

	check.Rows = append(check.Rows,
		rawdb.StageProgress{Stage: rawdb.StageSyncCommitment, BlockNum: 10, BlockHash: hash, HasBlockHash: true},
		rawdb.StageProgress{Stage: rawdb.StageSyncFinish, BlockNum: 10, BlockHash: hash, HasBlockHash: true},
	)
	got = PlanSyncPipelineProgressCursor(check)
	if !got.Complete || got.HasNext || got.LastStage != rawdb.StageSyncFinish {
		t.Fatalf("complete cursor = %+v, want finish complete", got)
	}

	check = SyncPipelineProgressOrderCheckResult{
		Rows: []rawdb.StageProgress{
			{Stage: rawdb.StageSyncBodies, BlockNum: 12},
			{Stage: rawdb.StageSyncBodiesReady, BlockNum: 12},
			{Stage: rawdb.StageSyncImport, BlockNum: 12},
			{Stage: rawdb.StageSyncExecution, BlockNum: 8},
			{Stage: rawdb.StageSyncCommitment, BlockNum: 10},
		},
		Issues: []SyncPipelineProgressOrderIssue{{
			Downstream:      rawdb.StageSyncCommitment,
			DownstreamBlock: 10,
			Upstream:        rawdb.StageSyncExecution,
			UpstreamBlock:   8,
		}},
	}
	got = PlanSyncPipelineProgressCursor(check)
	if !got.HasBlocked || !got.HasNext || got.NextStage != rawdb.StageSyncCommitment ||
		!got.HasLast || got.LastStage != rawdb.StageSyncExecution || got.Complete {
		t.Fatalf("blocked cursor = %+v, want blocked at commitment with execution as last stage", got)
	}

	got = PlanSyncPipelineProgressCursor(SyncPipelineProgressOrderCheckResult{
		ReadErrors: []SyncPipelineProgressOrderReadError{{Stage: rawdb.StageSyncBodies}},
	})
	if !got.Interrupted || got.ErrorStage != rawdb.StageSyncBodies || got.HasNext || got.HasLast {
		t.Fatalf("interrupted cursor = %+v, want read-error cursor", got)
	}
}

func TestRepairSyncPipelineProgressOrderFromDBDeletesDownstreamTail(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	hash := tcommon.Hash{0x07}
	writes := []struct {
		stage rawdb.StageID
		block uint64
	}{
		{stage: rawdb.StageSyncBodies, block: 7},
		{stage: rawdb.StageSyncBodiesReady, block: 8},
		{stage: rawdb.StageSyncImport, block: 8},
		{stage: rawdb.StageSyncExecution, block: 8},
	}
	for _, write := range writes {
		if err := rawdb.WriteStageProgressWithHash(db, write.stage, write.block, hash); err != nil {
			t.Fatalf("write %s progress: %v", write.stage, err)
		}
	}

	got := RepairSyncPipelineProgressOrderFromDB(db, SyncPipelineProgressOrderOptions{})
	if !got.Complete || got.Interrupted || got.Deleted != 3 || len(got.Repairs) != 3 {
		t.Fatalf("repair result = %+v, want complete deletion of ready/import/execution tail", got)
	}
	if len(got.Before.Issues) != 1 || got.Before.Issues[0].Downstream != rawdb.StageSyncBodiesReady {
		t.Fatalf("before issues = %+v, want bodies-ready ahead of bodies", got.Before.Issues)
	}
	if len(got.After.Issues) != 0 || len(got.After.ReadErrors) != 0 {
		t.Fatalf("after check = %+v, want no remaining order issues", got.After)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncBodies); err != nil || !ok || row.BlockNum != 7 {
		t.Fatalf("SyncBodies after repair = %+v ok=%v err=%v, want kept block7", row, ok, err)
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncBodiesReady, rawdb.StageSyncImport, rawdb.StageSyncExecution} {
		if row, ok, err := rawdb.ReadStageProgressRow(db, stage); err != nil || ok {
			t.Fatalf("%s after repair = %+v ok=%v err=%v, want deleted", stage, row, ok, err)
		}
	}
}

func TestRepairSyncPipelineProgressOrderFromDBAdvancesBodiesFromVerifiedReady(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	block7 := testBufferedBlock(7)
	block8 := testBufferedBlock(8)
	for _, block := range []*types.Block{block7, block8} {
		if err := rawdb.WriteSyncStagedBlock(db, block); err != nil {
			t.Fatalf("write staged block %d: %v", block.Number(), err)
		}
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodies, block7.Number(), block7.Hash()); err != nil {
		t.Fatalf("write bodies progress: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodiesReady, block8.Number(), block8.Hash()); err != nil {
		t.Fatalf("write ready progress: %v", err)
	}

	got := RepairSyncPipelineProgressOrderFromDB(db, SyncPipelineProgressOrderOptions{})
	if !got.Complete || got.Interrupted || got.Deleted != 0 || got.Updated != 1 || len(got.Repairs) != 1 || !got.Repairs[0].Updated {
		t.Fatalf("repair result = %+v, want one SyncBodies update from verified ready", got)
	}
	if len(got.Before.Issues) != 1 || got.Before.Issues[0].Downstream != rawdb.StageSyncBodiesReady {
		t.Fatalf("before issues = %+v, want ready ahead of bodies", got.Before.Issues)
	}
	if len(got.After.Issues) != 0 || len(got.After.ReadErrors) != 0 {
		t.Fatalf("after check = %+v, want repaired order", got.After)
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncBodies, rawdb.StageSyncBodiesReady} {
		row, ok, err := rawdb.ReadStageProgressRow(db, stage)
		if err != nil || !ok || row.BlockNum != block8.Number() || row.BlockHash != block8.Hash() {
			t.Fatalf("%s after repair = %+v ok=%v err=%v, want block8", stage, row, ok, err)
		}
	}
}

func TestRepairSyncPipelineProgressOrderFromDBDeletesImportTailAfterReadyLag(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	hash := tcommon.Hash{0x0a}
	writes := []struct {
		stage rawdb.StageID
		block uint64
	}{
		{stage: rawdb.StageSyncBodies, block: 12},
		{stage: rawdb.StageSyncBodiesReady, block: 8},
		{stage: rawdb.StageSyncImport, block: 10},
		{stage: rawdb.StageSyncExecution, block: 10},
		{stage: rawdb.StageSyncCommitment, block: 10},
		{stage: rawdb.StageSyncFinish, block: 10},
	}
	for _, write := range writes {
		if err := rawdb.WriteStageProgressWithHash(db, write.stage, write.block, hash); err != nil {
			t.Fatalf("write %s progress: %v", write.stage, err)
		}
	}

	got := RepairSyncPipelineProgressOrderFromDB(db, SyncPipelineProgressOrderOptions{})
	if !got.Complete || got.Deleted != 4 || len(got.Repairs) != 4 {
		t.Fatalf("repair result = %+v, want import/execution/commitment/finish tail deleted", got)
	}
	if len(got.Before.Issues) != 1 || got.Before.Issues[0].Downstream != rawdb.StageSyncImport {
		t.Fatalf("before issues = %+v, want import ahead of ready", got.Before.Issues)
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncBodies, rawdb.StageSyncBodiesReady} {
		if row, ok, err := rawdb.ReadStageProgressRow(db, stage); err != nil || !ok {
			t.Fatalf("%s after repair = %+v ok=%v err=%v, want kept", stage, row, ok, err)
		}
	}
	for _, stage := range SyncPipelineProgressStages() {
		if row, ok, err := rawdb.ReadStageProgressRow(db, stage); err != nil || ok {
			t.Fatalf("%s after repair = %+v ok=%v err=%v, want deleted", stage, row, ok, err)
		}
	}
}

func TestRepairSyncPipelineProgressOrderFromDBDeletesCommitmentTailAfterExecutionLag(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	hash := tcommon.Hash{0x0c}
	writes := []struct {
		stage rawdb.StageID
		block uint64
	}{
		{stage: rawdb.StageSyncBodies, block: 12},
		{stage: rawdb.StageSyncBodiesReady, block: 12},
		{stage: rawdb.StageSyncImport, block: 12},
		{stage: rawdb.StageSyncExecution, block: 8},
		{stage: rawdb.StageSyncCommitment, block: 10},
		{stage: rawdb.StageSyncFinish, block: 10},
	}
	for _, write := range writes {
		if err := rawdb.WriteStageProgressWithHash(db, write.stage, write.block, hash); err != nil {
			t.Fatalf("write %s progress: %v", write.stage, err)
		}
	}

	got := RepairSyncPipelineProgressOrderFromDB(db, SyncPipelineProgressOrderOptions{})
	if !got.Complete || got.Deleted != 2 || len(got.Repairs) != 2 {
		t.Fatalf("repair result = %+v, want commitment/finish tail deleted", got)
	}
	if len(got.Before.Issues) != 1 || got.Before.Issues[0].Downstream != rawdb.StageSyncCommitment {
		t.Fatalf("before issues = %+v, want commitment ahead of execution", got.Before.Issues)
	}
	if len(got.After.Issues) != 0 || len(got.After.ReadErrors) != 0 {
		t.Fatalf("after check = %+v, want clean order after deleting commitment tail", got.After)
	}
	for _, stage := range []rawdb.StageID{
		rawdb.StageSyncBodies,
		rawdb.StageSyncBodiesReady,
		rawdb.StageSyncImport,
		rawdb.StageSyncExecution,
	} {
		if row, ok, err := rawdb.ReadStageProgressRow(db, stage); err != nil || !ok {
			t.Fatalf("%s after repair = %+v ok=%v err=%v, want kept", stage, row, ok, err)
		}
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncCommitment, rawdb.StageSyncFinish} {
		if row, ok, err := rawdb.ReadStageProgressRow(db, stage); err != nil || ok {
			t.Fatalf("%s after repair = %+v ok=%v err=%v, want deleted", stage, row, ok, err)
		}
	}
}

func TestRepairSyncPipelineProgressOrderFromDBDeletesFinishTailAfterCommitmentLag(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	hash := tcommon.Hash{0x0d}
	writes := []struct {
		stage rawdb.StageID
		block uint64
	}{
		{stage: rawdb.StageSyncBodies, block: 12},
		{stage: rawdb.StageSyncBodiesReady, block: 12},
		{stage: rawdb.StageSyncImport, block: 12},
		{stage: rawdb.StageSyncExecution, block: 12},
		{stage: rawdb.StageSyncCommitment, block: 8},
		{stage: rawdb.StageSyncFinish, block: 10},
	}
	for _, write := range writes {
		if err := rawdb.WriteStageProgressWithHash(db, write.stage, write.block, hash); err != nil {
			t.Fatalf("write %s progress: %v", write.stage, err)
		}
	}

	got := RepairSyncPipelineProgressOrderFromDB(db, SyncPipelineProgressOrderOptions{})
	if !got.Complete || got.Deleted != 1 || len(got.Repairs) != 1 {
		t.Fatalf("repair result = %+v, want finish tail deleted", got)
	}
	if len(got.Before.Issues) != 1 || got.Before.Issues[0].Downstream != rawdb.StageSyncFinish {
		t.Fatalf("before issues = %+v, want finish ahead of commitment", got.Before.Issues)
	}
	if len(got.After.Issues) != 0 || len(got.After.ReadErrors) != 0 {
		t.Fatalf("after check = %+v, want clean order after deleting finish tail", got.After)
	}
	for _, stage := range []rawdb.StageID{
		rawdb.StageSyncBodies,
		rawdb.StageSyncBodiesReady,
		rawdb.StageSyncImport,
		rawdb.StageSyncExecution,
		rawdb.StageSyncCommitment,
	} {
		if row, ok, err := rawdb.ReadStageProgressRow(db, stage); err != nil || !ok {
			t.Fatalf("%s after repair = %+v ok=%v err=%v, want kept", stage, row, ok, err)
		}
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncFinish); err != nil || ok {
		t.Fatalf("SyncFinish after repair = %+v ok=%v err=%v, want deleted", row, ok, err)
	}
}

func TestRepairSyncPipelineProgressOrderFromDBAllowsMissingUpstreamByDefault(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	hash := tcommon.Hash{0x0a}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncImport, rawdb.StageSyncExecution} {
		if err := rawdb.WriteStageProgressWithHash(db, stage, 10, hash); err != nil {
			t.Fatalf("write %s progress: %v", stage, err)
		}
	}

	got := RepairSyncPipelineProgressOrderFromDB(db, SyncPipelineProgressOrderOptions{})
	if !got.Complete || got.Deleted != 0 || len(got.Repairs) != 0 || len(got.Before.Issues) != 0 {
		t.Fatalf("repair result = %+v, want no-op for missing upstream in non-strict mode", got)
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncImport, rawdb.StageSyncExecution} {
		if row, ok, err := rawdb.ReadStageProgressRow(db, stage); err != nil || !ok || row.BlockNum != 10 {
			t.Fatalf("%s after no-op repair = %+v ok=%v err=%v, want kept block10", stage, row, ok, err)
		}
	}
}

func TestRepairSyncPipelineProgressOrderFromDBStopsOnReadError(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStageProgress(db, rawdb.StageSyncBodies, 7); err != nil {
		t.Fatalf("write bodies progress: %v", err)
	}
	if err := corruptOnlyStageProgressRow(db); err != nil {
		t.Fatalf("corrupt bodies progress: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSyncBodiesReady, 8); err != nil {
		t.Fatalf("write ready progress: %v", err)
	}

	got := RepairSyncPipelineProgressOrderFromDB(db, SyncPipelineProgressOrderOptions{})
	if !got.Interrupted || got.ErrorStage != rawdb.StageSyncBodies || got.Complete || got.Deleted != 0 || len(got.Repairs) != 0 {
		t.Fatalf("repair result = %+v, want interrupted before deletion on read error", got)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncBodiesReady); err != nil || !ok || row.BlockNum != 8 {
		t.Fatalf("ready progress after interrupted repair = %+v ok=%v err=%v, want retained", row, ok, err)
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
