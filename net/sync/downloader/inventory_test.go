package downloader

import (
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
)

func TestPlanChainInventoryFiltersCandidatesAndAdvancesTarget(t *testing.T) {
	id10 := queueID(10)
	id11 := queueID(11)
	id12 := queueID(12)
	got := PlanChainInventory(ChainInventoryInput{
		CurrentTarget:  5,
		ExistingQueued: 1,
		RemainNum:      3,
		InventoryLimit: 2,
		Candidates: []InventoryCandidate{
			{ID: id10, Facts: InventoryCandidateFacts{KnownOrRequested: true, ReservedPath: true}},
			{ID: id11, Facts: InventoryCandidateFacts{ReservedPath: true}},
			{ID: id12, Facts: InventoryCandidateFacts{PeerRequested: true, ReservedPath: true}},
		},
	})

	if !reflect.DeepEqual(got.Accepted, []types.BlockID{id11}) {
		t.Fatalf("accepted = %+v, want only id11", got.Accepted)
	}
	if got.QueuedAfter != 2 || got.RemainNum != 3 || got.Done {
		t.Fatalf("queued/remain/done = %d/%d/%v, want 2/3/false", got.QueuedAfter, got.RemainNum, got.Done)
	}
	if !got.HasTarget || got.Target.Target != 15 || got.Target.Observed != 15 || got.Target.Window.Min != 8 || got.Target.Window.Max != 12 {
		t.Fatalf("target = %+v has=%v, want observed target 15 window 8-12", got.Target, got.HasTarget)
	}
	if !got.HasStageTarget || got.StageTarget != 15 {
		t.Fatalf("stage target = %d has=%v, want 15", got.StageTarget, got.HasStageTarget)
	}
	if len(got.Steps) != 2 ||
		got.Steps[0].Action != ChainInventoryAppendAccepted ||
		!reflect.DeepEqual(got.Steps[0].Accepted, []types.BlockID{id11}) ||
		got.Steps[1].Action != ChainInventoryUpdateProgress ||
		got.Steps[1].RemainNum != 3 ||
		!got.Steps[1].HasTarget ||
		got.Steps[1].Target.Target != 15 ||
		!got.Steps[1].HasStageTarget ||
		got.Steps[1].StageTarget != 15 {
		t.Fatalf("steps = %+v, want append id11 then target progress", got.Steps)
	}
}

func TestPlanChainInventoryDoneRequiresNoQueuedWork(t *testing.T) {
	id10 := queueID(10)
	done := PlanChainInventory(ChainInventoryInput{
		Candidates: []InventoryCandidate{
			{ID: id10, Facts: InventoryCandidateFacts{KnownOrRequested: true}},
		},
	})
	if !done.Done || done.QueuedAfter != 0 {
		t.Fatalf("done plan = %+v, want done with empty queue", done)
	}
	if len(done.Steps) != 3 || done.Steps[2].Action != ChainInventoryMarkDone {
		t.Fatalf("done steps = %+v, want mark done", done.Steps)
	}

	notDone := PlanChainInventory(ChainInventoryInput{
		ExistingQueued: 1,
		Candidates: []InventoryCandidate{
			{ID: id10, Facts: InventoryCandidateFacts{KnownOrRequested: true}},
		},
	})
	if notDone.Done || notDone.QueuedAfter != 1 {
		t.Fatalf("queued plan = %+v, want not done with existing queue", notDone)
	}
	if len(notDone.Steps) != 2 {
		t.Fatalf("queued steps = %+v, want no mark-done step", notDone.Steps)
	}
}

func TestPlanChainInventoryEmptyResponseMarksDoneWithoutTarget(t *testing.T) {
	got := PlanChainInventory(ChainInventoryInput{CurrentTarget: 7})
	if !got.Done || got.HasTarget || got.HasStageTarget || got.QueuedAfter != 0 || got.Target.Target != 0 {
		t.Fatalf("empty inventory plan = %+v, want done without target update", got)
	}
}

func TestPlanChainInventoryKeepsStaleTarget(t *testing.T) {
	id25 := queueID(25)
	got := PlanChainInventory(ChainInventoryInput{
		CurrentTarget:  50,
		RemainNum:      7,
		InventoryLimit: 5,
		Candidates: []InventoryCandidate{
			{ID: id25, Facts: InventoryCandidateFacts{ReservedPath: true}},
		},
	})
	if got.Target.Target != 50 || got.Target.Observed != 32 || got.Target.Advanced {
		t.Fatalf("stale target = %+v, want keep current target 50 observed 32", got.Target)
	}
	if !got.HasStageTarget || got.StageTarget != 50 {
		t.Fatalf("stage target = %d has=%v, want current target 50", got.StageTarget, got.HasStageTarget)
	}
}

func TestPlanChainInventoryResponseBuildsBoundedWindow(t *testing.T) {
	got := PlanChainInventoryResponse(ChainInventoryResponseInput{
		CommonBlock:    3,
		HeadBlock:      7,
		InventoryLimit: 3,
		ReadBlockID: chainInventoryResponseReader(map[uint64]types.BlockID{
			4: queueID(4),
			5: queueID(5),
			6: queueID(6),
			7: queueID(7),
		}),
	})

	if !reflect.DeepEqual(got.IDs, []types.BlockID{queueID(4), queueID(5), queueID(6)}) {
		t.Fatalf("ids = %+v, want 4,5,6", got.IDs)
	}
	if got.RemainNum != 1 || got.FromBlock != 4 || got.NextBlock != 7 || got.MissingBlock || got.MissingAt != 0 {
		t.Fatalf("plan = %+v, want remain 1, from 4, next 7, no missing", got)
	}
}

func TestPlanChainInventoryResponseStopsAtHead(t *testing.T) {
	got := PlanChainInventoryResponse(ChainInventoryResponseInput{
		CommonBlock:    3,
		HeadBlock:      5,
		InventoryLimit: 10,
		ReadBlockID: chainInventoryResponseReader(map[uint64]types.BlockID{
			4: queueID(4),
			5: queueID(5),
		}),
	})

	if !reflect.DeepEqual(got.IDs, []types.BlockID{queueID(4), queueID(5)}) {
		t.Fatalf("ids = %+v, want 4,5", got.IDs)
	}
	if got.RemainNum != 0 || got.FromBlock != 4 || got.NextBlock != 6 || got.MissingBlock {
		t.Fatalf("plan = %+v, want exhausted head with no remain", got)
	}
}

func TestPlanChainInventoryResponseStopsAtMissingBlock(t *testing.T) {
	got := PlanChainInventoryResponse(ChainInventoryResponseInput{
		CommonBlock:    3,
		HeadBlock:      7,
		InventoryLimit: 10,
		ReadBlockID: chainInventoryResponseReader(map[uint64]types.BlockID{
			4: queueID(4),
			6: queueID(6),
		}),
	})

	if !reflect.DeepEqual(got.IDs, []types.BlockID{queueID(4)}) {
		t.Fatalf("ids = %+v, want only block 4", got.IDs)
	}
	if got.RemainNum != 3 || !got.MissingBlock || got.MissingAt != 5 || got.NextBlock != 5 {
		t.Fatalf("plan = %+v, want missing block 5 with remain 3", got)
	}
}

func TestPlanChainInventoryResponseNoProgress(t *testing.T) {
	atHead := PlanChainInventoryResponse(ChainInventoryResponseInput{
		CommonBlock:    7,
		HeadBlock:      7,
		InventoryLimit: 10,
		ReadBlockID:    chainInventoryResponseReader(nil),
	})
	if len(atHead.IDs) != 0 || atHead.RemainNum != 0 || atHead.FromBlock != 0 || atHead.NextBlock != 0 || atHead.MissingBlock {
		t.Fatalf("at-head plan = %+v, want empty plan", atHead)
	}

	limited := PlanChainInventoryResponse(ChainInventoryResponseInput{
		CommonBlock:    3,
		HeadBlock:      7,
		InventoryLimit: 0,
		ReadBlockID: chainInventoryResponseReader(map[uint64]types.BlockID{
			4: queueID(4),
		}),
	})
	if len(limited.IDs) != 0 || limited.RemainNum != 4 || limited.FromBlock != 4 || limited.NextBlock != 4 || limited.MissingBlock {
		t.Fatalf("limit-zero plan = %+v, want no ids and all blocks remaining", limited)
	}

	missingReader := PlanChainInventoryResponse(ChainInventoryResponseInput{
		CommonBlock:    3,
		HeadBlock:      7,
		InventoryLimit: 10,
	})
	if len(missingReader.IDs) != 0 || missingReader.RemainNum != 4 || !missingReader.MissingBlock || missingReader.MissingAt != 4 || missingReader.NextBlock != 4 {
		t.Fatalf("missing-reader plan = %+v, want missing block 4 with all blocks remaining", missingReader)
	}
}

func TestApplyChainInventoryPlan(t *testing.T) {
	id11 := queueID(11)
	applier := new(recordingChainInventoryApplier)
	plan := ChainInventoryPlan{Steps: []ChainInventoryStep{
		{Action: ChainInventoryAppendAccepted, Accepted: []types.BlockID{id11}},
		{Action: ChainInventoryStepAction(255)},
		{
			Action:         ChainInventoryUpdateProgress,
			RemainNum:      3,
			Target:         InventoryTargetUpdate{Target: 15},
			HasTarget:      true,
			StageTarget:    15,
			HasStageTarget: true,
		},
		{Action: ChainInventoryMarkDone},
	}}
	result := ApplyChainInventoryPlan(plan, applier)

	if !reflect.DeepEqual(result.Plan, plan) {
		t.Fatalf("result plan = %+v, want original plan %+v", result.Plan, plan)
	}
	wantCalls := []ChainInventoryStepAction{
		ChainInventoryAppendAccepted,
		ChainInventoryUpdateProgress,
		ChainInventoryMarkDone,
	}
	if !reflect.DeepEqual(result.AppliedSteps, wantCalls) ||
		!reflect.DeepEqual(result.UnknownSteps, []ChainInventoryStepAction{ChainInventoryStepAction(255)}) ||
		result.StageTarget != 15 || !result.HasStageTarget || !result.Done {
		t.Fatalf("apply result = %+v, want applied calls, unknown step, stage target 15, done", result)
	}
	if !reflect.DeepEqual(applier.calls, wantCalls) {
		t.Fatalf("calls = %+v, want %+v", applier.calls, wantCalls)
	}
	if !reflect.DeepEqual(applier.accepted, []types.BlockID{id11}) || applier.remainNum != 3 || applier.target.Target != 15 || !applier.hasTarget || applier.stageTarget != 15 || !applier.hasStageTarget || !applier.done {
		t.Fatalf("applied state = accepted:%+v remain:%d target:%+v has:%v stage:%d/%v done:%v, want full inventory state",
			applier.accepted, applier.remainNum, applier.target, applier.hasTarget, applier.stageTarget, applier.hasStageTarget, applier.done)
	}

	applier = new(recordingChainInventoryApplier)
	fallbackPlan := ChainInventoryPlan{
		Accepted:       []types.BlockID{id11},
		RemainNum:      4,
		Target:         InventoryTargetUpdate{Target: 16},
		HasTarget:      true,
		StageTarget:    16,
		HasStageTarget: true,
		Done:           true,
	}
	result = ApplyChainInventoryPlan(fallbackPlan, applier)
	if !reflect.DeepEqual(result.Plan, fallbackPlan.withSteps()) {
		t.Fatalf("fallback result plan = %+v, want generated step plan", result.Plan)
	}
	if !reflect.DeepEqual(result.AppliedSteps, wantCalls) || len(result.UnknownSteps) != 0 || result.StageTarget != 16 || !result.HasStageTarget || !result.Done {
		t.Fatalf("fallback apply result = %+v, want generated applied calls and stage target 16", result)
	}
	if !reflect.DeepEqual(applier.calls, wantCalls) || !reflect.DeepEqual(applier.accepted, []types.BlockID{id11}) || applier.remainNum != 4 || applier.target.Target != 16 || applier.stageTarget != 16 || !applier.done {
		t.Fatalf("fallback applied state = calls:%+v accepted:%+v remain:%d target:%+v stage:%d done:%v, want generated steps",
			applier.calls, applier.accepted, applier.remainNum, applier.target, applier.stageTarget, applier.done)
	}
	if nilResult := ApplyChainInventoryPlan(plan, nil); !reflect.DeepEqual(nilResult.Plan, plan) || len(nilResult.AppliedSteps) != 0 || len(nilResult.UnknownSteps) != 0 || nilResult.HasStageTarget || nilResult.Done {
		t.Fatalf("nil apply result = %+v, want original plan without applied state", nilResult)
	}
}

func TestApplyChainInventoryBuildsAndAppliesPlan(t *testing.T) {
	id10 := queueID(10)
	id11 := queueID(11)
	input := ChainInventoryInput{
		CurrentTarget:  5,
		ExistingQueued: 1,
		RemainNum:      3,
		InventoryLimit: 2,
		Candidates: []InventoryCandidate{
			{ID: id10, Facts: InventoryCandidateFacts{KnownOrRequested: true, ReservedPath: true}},
			{ID: id11, Facts: InventoryCandidateFacts{ReservedPath: true}},
		},
	}
	wantPlan := PlanChainInventory(input)
	applier := new(recordingChainInventoryApplier)
	result := ApplyChainInventory(input, applier)

	if !reflect.DeepEqual(result.Plan, wantPlan) {
		t.Fatalf("result plan = %+v, want %+v", result.Plan, wantPlan)
	}
	wantCalls := []ChainInventoryStepAction{
		ChainInventoryAppendAccepted,
		ChainInventoryUpdateProgress,
	}
	if !reflect.DeepEqual(result.AppliedSteps, wantCalls) || len(result.UnknownSteps) != 0 || result.Done {
		t.Fatalf("apply result = %+v, want append/update only", result)
	}
	if !reflect.DeepEqual(applier.calls, wantCalls) ||
		!reflect.DeepEqual(applier.accepted, []types.BlockID{id11}) ||
		applier.remainNum != wantPlan.RemainNum ||
		!reflect.DeepEqual(applier.target, wantPlan.Target) ||
		applier.hasTarget != wantPlan.HasTarget ||
		applier.stageTarget != wantPlan.StageTarget ||
		applier.hasStageTarget != wantPlan.HasStageTarget ||
		applier.done {
		t.Fatalf("applied state = calls:%+v accepted:%+v remain:%d target:%+v has:%v stage:%d/%v done:%v, want plan state %+v",
			applier.calls, applier.accepted, applier.remainNum, applier.target, applier.hasTarget, applier.stageTarget, applier.hasStageTarget, applier.done, wantPlan)
	}

	doneApplier := new(recordingChainInventoryApplier)
	doneResult := ApplyChainInventory(ChainInventoryInput{}, doneApplier)
	if !doneResult.Done || !reflect.DeepEqual(doneApplier.calls, []ChainInventoryStepAction{
		ChainInventoryAppendAccepted,
		ChainInventoryUpdateProgress,
		ChainInventoryMarkDone,
	}) || !doneApplier.done {
		t.Fatalf("done apply result = %+v calls:%+v done:%v, want generated mark-done path", doneResult, doneApplier.calls, doneApplier.done)
	}
}

func TestPlanChainInventoryPostLock(t *testing.T) {
	empty := PlanChainInventoryPostLock(ChainInventoryApplyResult{})
	if len(empty.Steps) != 0 {
		t.Fatalf("empty post-lock plan = %+v, want no steps", empty)
	}

	got := PlanChainInventoryPostLock(ChainInventoryApplyResult{
		HasStageTarget: true,
		StageTarget:    55,
	})
	want := ChainInventoryPostLockPlan{Steps: []ChainInventoryPostLockStep{{
		Action:      ChainInventoryWriteStageProgress,
		Stage:       rawdb.StageSyncInventory,
		StageTarget: 55,
	}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("post-lock plan = %+v, want %+v", got, want)
	}
}

func TestApplyChainInventoryPostLockPlan(t *testing.T) {
	applier := new(recordingChainInventoryPostLockApplier)
	plan := ChainInventoryPostLockPlan{Steps: []ChainInventoryPostLockStep{
		{Action: ChainInventoryPostLockStepAction(255)},
		{Action: ChainInventoryWriteStageProgress, Stage: rawdb.StageSyncInventory, StageTarget: 75},
	}}
	got := ApplyChainInventoryPostLockPlan(plan, applier)

	if !reflect.DeepEqual(got.AppliedSteps, []ChainInventoryPostLockStepAction{ChainInventoryWriteStageProgress}) ||
		!reflect.DeepEqual(got.UnknownSteps, []ChainInventoryPostLockStepAction{ChainInventoryPostLockStepAction(255)}) ||
		!got.WroteStageProgress || got.Stage != rawdb.StageSyncInventory || got.StageTarget != 75 {
		t.Fatalf("post-lock apply result = %+v, want stage progress write and unknown step", got)
	}
	if !reflect.DeepEqual(applier.calls, []ChainInventoryPostLockStepAction{ChainInventoryWriteStageProgress}) ||
		applier.stage != rawdb.StageSyncInventory || applier.target != 75 {
		t.Fatalf("post-lock applier = calls:%+v stage:%s target:%d, want inventory stage 75",
			applier.calls, applier.stage, applier.target)
	}

	nilResult := ApplyChainInventoryPostLockPlan(plan, nil)
	if len(nilResult.AppliedSteps) != 0 || len(nilResult.UnknownSteps) != 0 || nilResult.WroteStageProgress {
		t.Fatalf("nil post-lock apply result = %+v, want empty", nilResult)
	}
}

type recordingChainInventoryApplier struct {
	calls          []ChainInventoryStepAction
	accepted       []types.BlockID
	remainNum      int64
	target         InventoryTargetUpdate
	hasTarget      bool
	stageTarget    uint64
	hasStageTarget bool
	done           bool
}

func (a *recordingChainInventoryApplier) AppendAcceptedInventory(ids []types.BlockID) {
	a.calls = append(a.calls, ChainInventoryAppendAccepted)
	a.accepted = append(a.accepted, ids...)
}

func (a *recordingChainInventoryApplier) UpdateInventoryProgress(remainNum int64, target InventoryTargetUpdate, hasTarget bool, stageTarget uint64, hasStageTarget bool) {
	a.calls = append(a.calls, ChainInventoryUpdateProgress)
	a.remainNum = remainNum
	a.target = target
	a.hasTarget = hasTarget
	a.stageTarget = stageTarget
	a.hasStageTarget = hasStageTarget
}

func (a *recordingChainInventoryApplier) MarkInventoryDone() {
	a.calls = append(a.calls, ChainInventoryMarkDone)
	a.done = true
}

type recordingChainInventoryPostLockApplier struct {
	calls  []ChainInventoryPostLockStepAction
	stage  rawdb.StageID
	target uint64
}

func (a *recordingChainInventoryPostLockApplier) WriteInventoryStageProgress(stage rawdb.StageID, target uint64) {
	a.calls = append(a.calls, ChainInventoryWriteStageProgress)
	a.stage = stage
	a.target = target
}

func chainInventoryResponseReader(ids map[uint64]types.BlockID) func(uint64) (types.BlockID, bool) {
	return func(number uint64) (types.BlockID, bool) {
		id, ok := ids[number]
		return id, ok
	}
}
