package downloader

// SessionStartupInput is the canonical chain boundary and operator knobs needed
// to plan one downloader session restart.
type SessionStartupInput struct {
	Head         uint64
	RestoreLimit int
}

// SyncStartSkipReason names why an inbound StartSync request should not attach
// the peer to the current downloader session.
type SyncStartSkipReason string

const (
	SyncStartSkipNone            SyncStartSkipReason = ""
	SyncStartSkipNoPeer          SyncStartSkipReason = "no-peer"
	SyncStartSkipStopping        SyncStartSkipReason = "stopping"
	SyncStartSkipPaused          SyncStartSkipReason = "paused"
	SyncStartSkipPeerUnavailable SyncStartSkipReason = "peer-unavailable"
	SyncStartSkipAlreadyJoined   SyncStartSkipReason = "already-joined"
)

// SyncStartGateInput is the side-effect-free pre-lock state needed to decide
// whether StartSync should proceed far enough to touch session state.
type SyncStartGateInput struct {
	PeerPresent         bool
	Stopping            bool
	Paused              bool
	AvailabilityChecked bool
	PeerCanServe        bool
}

// SyncStartGatePlan reports whether StartSync may continue past its pre-lock
// gate and, if not, why it stopped.
type SyncStartGatePlan struct {
	Allowed    bool
	SkipReason SyncStartSkipReason
}

// SyncPeerAttachInput is the locked session state needed to decide whether a
// candidate peer should be attached to the downloader session.
type SyncPeerAttachInput struct {
	Syncing           bool
	PeerAlreadyJoined bool
}

// SyncPeerAttachPlan is the session-local side-effect schedule for attaching a
// peer. SyncService still owns the concrete maps, peer objects, and messages.
type SyncPeerAttachPlan struct {
	Attach             bool
	InitSession        bool
	MarkChainRequested bool
	MirrorLegacy       bool
	SendChainSummary   bool
	JoinAvailablePeers bool
	LogStarted         bool
	SkipReason         SyncStartSkipReason
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
	SessionStartupRepairSyncPipelineOrder
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
	RepairSyncPipelineProgressOrder() SyncPipelineProgressOrderRepairResult
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
	AppliedSteps               []SessionStartupStepAction
	UnknownSteps               []SessionStartupStepAction
	SyncPipelineRepairResult   SyncPipelineProgressRepairResult
	HasSyncPipelineRepair      bool
	SyncPipelineRepairs        []SyncStageProgressRepair
	SyncPipelineOrderCheck     SyncPipelineProgressOrderCheckResult
	SyncPipelineOrderIssues    []SyncPipelineProgressOrderIssue
	SyncPipelineOrderErrors    []SyncPipelineProgressOrderReadError
	HasSyncPipelineOrder       bool
	SyncPipelineCursor         SyncPipelineProgressCursor
	HasSyncPipelineCursor      bool
	SyncPipelineOrderRepair    SyncPipelineProgressOrderRepairResult
	HasSyncPipelineOrderRepair bool
	StagedBodyRestore          StagedBodyRestoreResult
	HasStagedBodyRestore       bool
}

// PlanSyncStartGate applies the StartSync pre-lock gate without observing or
// mutating service-owned session state.
func PlanSyncStartGate(in SyncStartGateInput) SyncStartGatePlan {
	switch {
	case !in.PeerPresent:
		return SyncStartGatePlan{SkipReason: SyncStartSkipNoPeer}
	case in.Stopping:
		return SyncStartGatePlan{SkipReason: SyncStartSkipStopping}
	case in.Paused:
		return SyncStartGatePlan{SkipReason: SyncStartSkipPaused}
	case in.AvailabilityChecked && !in.PeerCanServe:
		return SyncStartGatePlan{SkipReason: SyncStartSkipPeerUnavailable}
	default:
		return SyncStartGatePlan{Allowed: true}
	}
}

// PlanSyncPeerAttach derives the locked StartSync session actions. A stale
// pre-existing peer map is ignored when Syncing=false because session startup
// reinitializes the maps before attaching the peer.
func PlanSyncPeerAttach(in SyncPeerAttachInput) SyncPeerAttachPlan {
	if in.Syncing && in.PeerAlreadyJoined {
		return SyncPeerAttachPlan{SkipReason: SyncStartSkipAlreadyJoined}
	}
	return SyncPeerAttachPlan{
		Attach:             true,
		InitSession:        !in.Syncing,
		MarkChainRequested: true,
		MirrorLegacy:       true,
		SendChainSummary:   true,
		JoinAvailablePeers: true,
		LogStarted:         !in.Syncing,
	}
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
		{Action: SessionStartupRepairSyncPipelineOrder},
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
		case SessionStartupRepairSyncPipelineOrder:
			result.SyncPipelineOrderRepair = applier.RepairSyncPipelineProgressOrder()
			result.HasSyncPipelineOrderRepair = true
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case SessionStartupCheckSyncPipelineOrder:
			result.SyncPipelineOrderCheck = applier.CheckSyncPipelineProgressOrder()
			result.SyncPipelineOrderIssues = result.SyncPipelineOrderCheck.Issues
			result.SyncPipelineOrderErrors = result.SyncPipelineOrderCheck.ReadErrors
			result.HasSyncPipelineOrder = true
			result.SyncPipelineCursor = PlanSyncPipelineProgressCursor(result.SyncPipelineOrderCheck)
			result.HasSyncPipelineCursor = true
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		default:
			result.UnknownSteps = append(result.UnknownSteps, step.Action)
		}
	}
	return result
}
