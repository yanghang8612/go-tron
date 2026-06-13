package downloader

import (
	"reflect"
	"testing"

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
	ApplyChainInventoryPlan(plan, applier)

	wantCalls := []ChainInventoryStepAction{
		ChainInventoryAppendAccepted,
		ChainInventoryUpdateProgress,
		ChainInventoryMarkDone,
	}
	if !reflect.DeepEqual(applier.calls, wantCalls) {
		t.Fatalf("calls = %+v, want %+v", applier.calls, wantCalls)
	}
	if !reflect.DeepEqual(applier.accepted, []types.BlockID{id11}) || applier.remainNum != 3 || applier.target.Target != 15 || !applier.hasTarget || applier.stageTarget != 15 || !applier.hasStageTarget || !applier.done {
		t.Fatalf("applied state = accepted:%+v remain:%d target:%+v has:%v stage:%d/%v done:%v, want full inventory state",
			applier.accepted, applier.remainNum, applier.target, applier.hasTarget, applier.stageTarget, applier.hasStageTarget, applier.done)
	}

	applier = new(recordingChainInventoryApplier)
	ApplyChainInventoryPlan(ChainInventoryPlan{
		Accepted:       []types.BlockID{id11},
		RemainNum:      4,
		Target:         InventoryTargetUpdate{Target: 16},
		HasTarget:      true,
		StageTarget:    16,
		HasStageTarget: true,
		Done:           true,
	}, applier)
	if !reflect.DeepEqual(applier.calls, wantCalls) || !reflect.DeepEqual(applier.accepted, []types.BlockID{id11}) || applier.remainNum != 4 || applier.target.Target != 16 || applier.stageTarget != 16 || !applier.done {
		t.Fatalf("fallback applied state = calls:%+v accepted:%+v remain:%d target:%+v stage:%d done:%v, want generated steps",
			applier.calls, applier.accepted, applier.remainNum, applier.target, applier.stageTarget, applier.done)
	}
	ApplyChainInventoryPlan(plan, nil)
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
