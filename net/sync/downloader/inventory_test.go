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

	notDone := PlanChainInventory(ChainInventoryInput{
		ExistingQueued: 1,
		Candidates: []InventoryCandidate{
			{ID: id10, Facts: InventoryCandidateFacts{KnownOrRequested: true}},
		},
	})
	if notDone.Done || notDone.QueuedAfter != 1 {
		t.Fatalf("queued plan = %+v, want not done with existing queue", notDone)
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
