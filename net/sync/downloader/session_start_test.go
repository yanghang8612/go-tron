package downloader

import (
	"reflect"
	"testing"
)

func TestPlanSessionStartup(t *testing.T) {
	got := PlanSessionStartup(SessionStartupInput{
		Head:         99,
		RestoreLimit: 32,
	})

	if got.InventoryFloor != 99 {
		t.Fatalf("inventory floor = %d, want 99", got.InventoryFloor)
	}
	if got.DeleteImportedThrough != 99 {
		t.Fatalf("delete imported through = %d, want 99", got.DeleteImportedThrough)
	}
	if got.RestoreStagedBodiesFrom != 100 {
		t.Fatalf("restore staged bodies from = %d, want 100", got.RestoreStagedBodiesFrom)
	}
	if got.RestoreLimit != 32 {
		t.Fatalf("restore limit = %d, want 32", got.RestoreLimit)
	}
	if !got.PruneStaleTail {
		t.Fatalf("prune stale tail = false, want true")
	}
	if !got.ResetPeerJoinThrottle {
		t.Fatalf("reset peer join throttle = false, want true")
	}
	wantSteps := []SessionStartupStep{
		{Action: SessionStartupRepairSyncPipeline},
		{Action: SessionStartupRestoreInventoryTarget, InventoryFloor: 99},
		{Action: SessionStartupDeleteImportedBodies, DeleteImportedThrough: 99},
		{Action: SessionStartupRestoreStagedBodies, RestoreStagedBodiesFrom: 100, RestoreLimit: 32, PruneStaleTail: true},
		{Action: SessionStartupRefreshBodiesReady},
	}
	if !reflect.DeepEqual(got.Steps, wantSteps) {
		t.Fatalf("startup steps = %+v, want %+v", got.Steps, wantSteps)
	}
}

func TestPlanSessionStartupClampsNegativeRestoreLimit(t *testing.T) {
	got := PlanSessionStartup(SessionStartupInput{
		Head:         7,
		RestoreLimit: -1,
	})

	if got.RestoreLimit != 0 {
		t.Fatalf("restore limit = %d, want 0", got.RestoreLimit)
	}
	if got.PruneStaleTail {
		t.Fatalf("prune stale tail = true, want false")
	}
	if got.RestoreStagedBodiesFrom != 8 {
		t.Fatalf("restore staged bodies from = %d, want 8", got.RestoreStagedBodiesFrom)
	}
	for _, step := range got.Steps {
		if step.Action == SessionStartupRestoreStagedBodies && step.PruneStaleTail {
			t.Fatalf("restore step prune stale tail = true, want false: %+v", step)
		}
	}
}

func TestPlanSessionStartupSaturatesRestoreStart(t *testing.T) {
	const maxUint64 = ^uint64(0)

	got := PlanSessionStartup(SessionStartupInput{
		Head:         maxUint64,
		RestoreLimit: 32,
	})

	if got.RestoreStagedBodiesFrom != maxUint64 {
		t.Fatalf("restore staged bodies from = %d, want %d", got.RestoreStagedBodiesFrom, maxUint64)
	}
	if got.DeleteImportedThrough != maxUint64 {
		t.Fatalf("delete imported through = %d, want %d", got.DeleteImportedThrough, maxUint64)
	}
}

func TestApplySessionStartupPlan(t *testing.T) {
	plan := SessionStartupPlan{
		Steps: []SessionStartupStep{
			{Action: SessionStartupRepairSyncPipeline},
			{Action: SessionStartupRestoreInventoryTarget, InventoryFloor: 9},
			{Action: SessionStartupStepAction(255)},
			{Action: SessionStartupDeleteImportedBodies, DeleteImportedThrough: 8},
			{Action: SessionStartupRestoreStagedBodies, RestoreStagedBodiesFrom: 10, RestoreLimit: 32, PruneStaleTail: true},
			{Action: SessionStartupRefreshBodiesReady},
		},
	}
	var applier recordingSessionStartupApplier
	ApplySessionStartupPlan(plan, &applier)
	want := []recordedSessionStartupCall{
		{action: SessionStartupRepairSyncPipeline},
		{action: SessionStartupRestoreInventoryTarget, first: 9},
		{action: SessionStartupDeleteImportedBodies, first: 8},
		{action: SessionStartupRestoreStagedBodies, first: 10, limit: 32, prune: true},
		{action: SessionStartupRefreshBodiesReady},
	}
	if !reflect.DeepEqual(applier.calls, want) {
		t.Fatalf("calls = %+v, want %+v", applier.calls, want)
	}

	ApplySessionStartupPlan(plan, nil)
}

type recordedSessionStartupCall struct {
	action SessionStartupStepAction
	first  uint64
	limit  int
	prune  bool
}

type recordingSessionStartupApplier struct {
	calls []recordedSessionStartupCall
}

func (a *recordingSessionStartupApplier) RepairSyncPipeline() {
	a.calls = append(a.calls, recordedSessionStartupCall{action: SessionStartupRepairSyncPipeline})
}

func (a *recordingSessionStartupApplier) RestoreInventoryTarget(inventoryFloor uint64) {
	a.calls = append(a.calls, recordedSessionStartupCall{action: SessionStartupRestoreInventoryTarget, first: inventoryFloor})
}

func (a *recordingSessionStartupApplier) DeleteImportedBodies(through uint64) {
	a.calls = append(a.calls, recordedSessionStartupCall{action: SessionStartupDeleteImportedBodies, first: through})
}

func (a *recordingSessionStartupApplier) RestoreStagedBodies(from uint64, limit int, pruneStaleTail bool) {
	a.calls = append(a.calls, recordedSessionStartupCall{
		action: SessionStartupRestoreStagedBodies,
		first:  from,
		limit:  limit,
		prune:  pruneStaleTail,
	})
}

func (a *recordingSessionStartupApplier) RefreshBodiesReady() {
	a.calls = append(a.calls, recordedSessionStartupCall{action: SessionStartupRefreshBodiesReady})
}
