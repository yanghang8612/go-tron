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
