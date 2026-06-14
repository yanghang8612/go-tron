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
	SessionStartupCheckSyncPipelineOrder
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

// SessionStartupPlanApplier performs the persistence/runtime operations named
// by a startup plan. The service owns DB handles; downloader owns the ordered
// stage-recovery schedule.
type SessionStartupPlanApplier interface {
	RepairSyncPipeline() SyncPipelineProgressRepairResult
	RestoreInventoryTarget(inventoryFloor uint64)
	DeleteImportedBodies(through uint64)
	RestoreStagedBodies(from uint64, limit int, pruneStaleTail bool) StagedBodyRestoreResult
	RefreshBodiesReady()
	CheckSyncPipelineProgressOrder() SyncPipelineProgressOrderCheckResult
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

// SessionStartupApplyResult records the startup recovery steps that were
// actually dispatched. Unknown steps are surfaced so tests and diagnostics can
// catch plan/apply drift without teaching SyncService about every action.
type SessionStartupApplyResult struct {
	AppliedSteps             []SessionStartupStepAction
	UnknownSteps             []SessionStartupStepAction
	SyncPipelineRepairResult SyncPipelineProgressRepairResult
	HasSyncPipelineRepair    bool
	SyncPipelineRepairs      []SyncStageProgressRepair
	SyncPipelineOrderCheck   SyncPipelineProgressOrderCheckResult
	SyncPipelineOrderIssues  []SyncPipelineProgressOrderIssue
	SyncPipelineOrderErrors  []SyncPipelineProgressOrderReadError
	HasSyncPipelineOrder     bool
	StagedBodyRestore        StagedBodyRestoreResult
	HasStagedBodyRestore     bool
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
		{Action: SessionStartupCheckSyncPipelineOrder},
	}
	return plan
}

// ApplySessionStartupPlan executes a downloader-owned startup recovery
// schedule against the caller's persistence/runtime adapter.
func ApplySessionStartupPlan(plan SessionStartupPlan, applier SessionStartupPlanApplier) SessionStartupApplyResult {
	var result SessionStartupApplyResult
	if applier == nil {
		return result
	}
	for _, step := range plan.Steps {
		switch step.Action {
		case SessionStartupRepairSyncPipeline:
			result.SyncPipelineRepairResult = applier.RepairSyncPipeline()
			result.HasSyncPipelineRepair = true
			result.SyncPipelineRepairs = result.SyncPipelineRepairResult.Repairs
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case SessionStartupRestoreInventoryTarget:
			applier.RestoreInventoryTarget(step.InventoryFloor)
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case SessionStartupDeleteImportedBodies:
			applier.DeleteImportedBodies(step.DeleteImportedThrough)
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case SessionStartupRestoreStagedBodies:
			result.StagedBodyRestore = applier.RestoreStagedBodies(step.RestoreStagedBodiesFrom, step.RestoreLimit, step.PruneStaleTail)
			result.HasStagedBodyRestore = true
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case SessionStartupRefreshBodiesReady:
			applier.RefreshBodiesReady()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case SessionStartupCheckSyncPipelineOrder:
			result.SyncPipelineOrderCheck = applier.CheckSyncPipelineProgressOrder()
			result.SyncPipelineOrderIssues = result.SyncPipelineOrderCheck.Issues
			result.SyncPipelineOrderErrors = result.SyncPipelineOrderCheck.ReadErrors
			result.HasSyncPipelineOrder = true
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		default:
			result.UnknownSteps = append(result.UnknownSteps, step.Action)
		}
	}
	return result
}
