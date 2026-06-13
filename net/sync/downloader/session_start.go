package downloader

// SessionStartupInput is the canonical chain boundary and operator knobs needed
// to plan one downloader session restart.
type SessionStartupInput struct {
	Head         uint64
	RestoreLimit int
}

// SessionStartupStepAction names one persistence/runtime repair step required
// before a sync session asks peers for more inventory.
type SessionStartupStepAction uint8

const (
	SessionStartupRepairSyncPipeline SessionStartupStepAction = iota
	SessionStartupRestoreInventoryTarget
	SessionStartupDeleteImportedBodies
	SessionStartupRestoreStagedBodies
	SessionStartupRefreshBodiesReady
)

// SessionStartupStep is one explicit startup recovery operation. Fields are
// populated only when the action needs that boundary.
type SessionStartupStep struct {
	Action                  SessionStartupStepAction
	InventoryFloor          uint64
	DeleteImportedThrough   uint64
	RestoreStagedBodiesFrom uint64
	RestoreLimit            int
	PruneStaleTail          bool
}

// SessionStartupPlan describes the persistence and local-runtime boundaries a
// sync session should apply before asking peers for more inventory.
type SessionStartupPlan struct {
	InventoryFloor          uint64
	DeleteImportedThrough   uint64
	RestoreStagedBodiesFrom uint64
	RestoreLimit            int
	PruneStaleTail          bool
	Steps                   []SessionStartupStep
	ResetPeerJoinThrottle   bool
}

// PlanSessionStartup derives restart boundaries from the current canonical
// head. SyncService owns DB writes and timers; the downloader package owns the
// staging/import boundary decisions.
func PlanSessionStartup(in SessionStartupInput) SessionStartupPlan {
	restoreLimit := in.RestoreLimit
	if restoreLimit < 0 {
		restoreLimit = 0
	}
	restoreFrom := in.Head
	if restoreFrom != ^uint64(0) {
		restoreFrom++
	}
	plan := SessionStartupPlan{
		InventoryFloor:          in.Head,
		DeleteImportedThrough:   in.Head,
		RestoreStagedBodiesFrom: restoreFrom,
		RestoreLimit:            restoreLimit,
		PruneStaleTail:          restoreLimit > 0,
		ResetPeerJoinThrottle:   true,
	}
	plan.Steps = []SessionStartupStep{
		{Action: SessionStartupRepairSyncPipeline},
		{Action: SessionStartupRestoreInventoryTarget, InventoryFloor: plan.InventoryFloor},
		{Action: SessionStartupDeleteImportedBodies, DeleteImportedThrough: plan.DeleteImportedThrough},
		{
			Action:                  SessionStartupRestoreStagedBodies,
			RestoreStagedBodiesFrom: plan.RestoreStagedBodiesFrom,
			RestoreLimit:            plan.RestoreLimit,
			PruneStaleTail:          plan.PruneStaleTail,
		},
		{Action: SessionStartupRefreshBodiesReady},
	}
	return plan
}
