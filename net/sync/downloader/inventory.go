package downloader

import (
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
)

// InventoryCandidate is one block ID from a peer CHAIN_INVENTORY response plus
// the service-collected facts needed to decide whether it enters the fetch
// queue.
type InventoryCandidate struct {
	ID    types.BlockID
	Facts InventoryCandidateFacts
}

// InventoryCandidateFactReader supplies SyncService-owned facts while
// downloader owns the order in which they are evaluated.
type InventoryCandidateFactReader interface {
	HasCanonicalInventoryBlock(id types.BlockID) bool
	HasKhaosInventoryBlock(id types.BlockID) bool
	HasBufferedInventoryBlock(id types.BlockID) bool
	HasRequestedInventoryBlock(id types.BlockID) bool
	PeerRequestedInventoryBlock(id types.BlockID) bool
	ReserveInventoryBlockPath(id types.BlockID) bool
}

// ChainInventoryInput is the side-effect-free state needed to plan a
// CHAIN_INVENTORY response.
type ChainInventoryInput struct {
	CurrentTarget  uint64
	ExistingQueued int
	RemainNum      int64
	InventoryLimit int
	Candidates     []InventoryCandidate
}

// ChainInventoryResponseInput is the side-effect-free state needed to answer a
// peer SYNC_BLOCK_CHAIN summary with the next sequential CHAIN_INVENTORY
// window.
type ChainInventoryResponseInput struct {
	CommonBlock    uint64
	HeadBlock      uint64
	InventoryLimit int
	ReadBlockID    func(number uint64) (types.BlockID, bool)
}

// ChainInventoryPlan is the downloader-owned result of one CHAIN_INVENTORY
// response. The service applies Accepted IDs and target/window state to its
// peer record while downloader owns the decision rules.
type ChainInventoryPlan struct {
	Accepted       []types.BlockID
	QueuedAfter    int
	RemainNum      int64
	Target         InventoryTargetUpdate
	HasTarget      bool
	StageTarget    uint64
	HasStageTarget bool
	Done           bool
	Steps          []ChainInventoryStep
}

// ChainInventoryResponsePlan is the downloader-owned response window for one
// SYNC_BLOCK_CHAIN request.
type ChainInventoryResponsePlan struct {
	IDs          []types.BlockID
	RemainNum    int64
	FromBlock    uint64
	NextBlock    uint64
	MissingBlock bool
	MissingAt    uint64
}

// ChainInventoryStepAction names one peer/session state mutation after a
// CHAIN_INVENTORY response has been classified.
type ChainInventoryStepAction uint8

const (
	ChainInventoryAppendAccepted ChainInventoryStepAction = iota
	ChainInventoryUpdateProgress
	ChainInventoryMarkDone
)

// ChainInventoryStep is one downloader-owned inventory response state update.
type ChainInventoryStep struct {
	Action         ChainInventoryStepAction
	Accepted       []types.BlockID
	RemainNum      int64
	Target         InventoryTargetUpdate
	HasTarget      bool
	StageTarget    uint64
	HasStageTarget bool
}

// ChainInventoryPostLockStepAction names one post-lock side effect after a
// CHAIN_INVENTORY response has updated peer/session state.
type ChainInventoryPostLockStepAction uint8

const (
	ChainInventoryWriteStageProgress ChainInventoryPostLockStepAction = iota
)

// ChainInventoryPostLockStep is one downloader-owned operation that must run
// after SyncService releases its session lock.
type ChainInventoryPostLockStep struct {
	Action      ChainInventoryPostLockStepAction
	Stage       rawdb.StageID
	StageTarget uint64
}

// ChainInventoryPlanApplier performs lock-held state mutations named by a
// chain inventory plan. SyncService owns peer/session fields; downloader owns
// the update ordering.
type ChainInventoryPlanApplier interface {
	AppendAcceptedInventory(ids []types.BlockID)
	UpdateInventoryProgress(remainNum int64, target InventoryTargetUpdate, hasTarget bool, stageTarget uint64, hasStageTarget bool)
	MarkInventoryDone()
}

// ChainInventoryApplyResult records the downloader-owned state updates applied
// for one CHAIN_INVENTORY response.
type ChainInventoryApplyResult struct {
	Plan           ChainInventoryPlan
	AppliedSteps   []ChainInventoryStepAction
	UnknownSteps   []ChainInventoryStepAction
	StageTarget    uint64
	HasStageTarget bool
	Done           bool
}

// ChainInventoryPostLockPlan carries post-lock side effects derived from the
// applied inventory plan. The service supplies the DB writer; downloader owns
// whether the inventory stage frontier should be persisted.
type ChainInventoryPostLockPlan struct {
	Steps []ChainInventoryPostLockStep
}

// ChainInventoryPostLockPlanApplier performs post-lock inventory side effects.
type ChainInventoryPostLockPlanApplier interface {
	WriteInventoryStageProgress(stage rawdb.StageID, target uint64)
}

// ChainInventoryPostLockApplyResult records post-lock inventory side effects.
type ChainInventoryPostLockApplyResult struct {
	AppliedSteps       []ChainInventoryPostLockStepAction
	UnknownSteps       []ChainInventoryPostLockStepAction
	WroteStageProgress bool
	Stage              rawdb.StageID
	StageTarget        uint64
}

// ChainInventoryRunPlan groups the lock-held inventory update with the
// post-lock persistence plan derived from applying it.
type ChainInventoryRunPlan struct {
	Inventory ChainInventoryPlan
	PostLock  ChainInventoryPostLockPlan
}

// ChainInventoryRunApplyResult groups the applied lock-held inventory update
// with the post-lock side effects the service should run after releasing the
// session lock.
type ChainInventoryRunApplyResult struct {
	Plan      ChainInventoryRunPlan
	Inventory ChainInventoryApplyResult
	PostLock  ChainInventoryPostLockPlan
}

// ChainInventorySessionRunInput is the lock-held state for accepting a
// CHAIN_INVENTORY response before fetch slots refill.
type ChainInventorySessionRunInput struct {
	Inventory ChainInventoryInput
}

// ChainInventorySessionRunPlan groups the inventory response run with the
// post-inventory session settlement run.
type ChainInventorySessionRunPlan struct {
	Inventory     ChainInventoryRunPlan
	PostInventory PostInventoryRunPlan
}

// ChainInventorySessionRunApplyResult groups the lock-held inventory apply
// result with the lock-held post-inventory settlement result. The caller still
// owns post-lock stage-progress writes, network dispatch, and after-dispatch
// settlement.
type ChainInventorySessionRunApplyResult struct {
	Plan             ChainInventorySessionRunPlan
	Inventory        ChainInventoryRunApplyResult
	OutboundRequests int
	PostInventory    PostInventoryRunApplyResult
}

// ChainInventorySessionRunPlanApplier performs the lock-held side effects
// needed to accept inventory, refill fetch slots, and settle the session.
type ChainInventorySessionRunPlanApplier interface {
	ChainInventoryPlanApplier
	PostInventorySettlementPlanApplier
	RefillFetchSlotsAfterInventory() int
	PostInventoryRunProgress() SessionProgress
}

// ChainInventorySessionRunObserver optionally observes lock-held milestones in
// the inventory session run without changing the planner's decisions.
type ChainInventorySessionRunObserver interface {
	ChainInventoryApplied()
}

// BuildInventoryCandidates gathers the downloader facts for advertised block
// IDs in the java-tron-compatible order: drop anything already known/requested,
// then reject same-peer duplicates, then reserve the canonical block path.
func BuildInventoryCandidates(ids []types.BlockID, reader InventoryCandidateFactReader) []InventoryCandidate {
	candidates := make([]InventoryCandidate, 0, len(ids))
	for _, id := range ids {
		facts := InventoryCandidateFacts{}
		if reader != nil {
			facts.KnownOrRequested =
				reader.HasCanonicalInventoryBlock(id) ||
					reader.HasKhaosInventoryBlock(id) ||
					reader.HasBufferedInventoryBlock(id) ||
					reader.HasRequestedInventoryBlock(id)
			if !facts.KnownOrRequested {
				facts.PeerRequested = reader.PeerRequestedInventoryBlock(id)
			}
			if !facts.KnownOrRequested && !facts.PeerRequested {
				facts.ReservedPath = reader.ReserveInventoryBlockPath(id)
			}
		}
		candidates = append(candidates, InventoryCandidate{ID: id, Facts: facts})
	}
	return candidates
}

// PlanChainInventory filters one CHAIN_INVENTORY response, advances target
// diagnostics, and recognizes java-tron's one-id completion signal.
func PlanChainInventory(in ChainInventoryInput) ChainInventoryPlan {
	plan := ChainInventoryPlan{
		Accepted:    make([]types.BlockID, 0, len(in.Candidates)),
		QueuedAfter: in.ExistingQueued,
		RemainNum:   in.RemainNum,
	}
	for _, candidate := range in.Candidates {
		if AcceptInventoryCandidate(candidate.Facts) {
			plan.Accepted = append(plan.Accepted, candidate.ID)
		}
	}
	plan.QueuedAfter += len(plan.Accepted)
	if len(in.Candidates) > 0 {
		last := in.Candidates[len(in.Candidates)-1].ID
		if last.Num > 0 {
			plan.Target = ObserveInventoryTarget(in.CurrentTarget, last.Num, in.RemainNum, in.InventoryLimit)
			plan.HasTarget = true
			if plan.Target.StageTarget > 0 {
				plan.StageTarget = plan.Target.StageTarget
				plan.HasStageTarget = true
			}
		}
	}
	plan.Done = ShouldMarkInventoryDone(len(in.Candidates), plan.QueuedAfter, in.RemainNum)
	return plan.withSteps()
}

// PlanChainInventoryResponse builds the bounded sequential block-id window to
// return for a peer SYNC_BLOCK_CHAIN request.
func PlanChainInventoryResponse(in ChainInventoryResponseInput) ChainInventoryResponsePlan {
	plan := ChainInventoryResponsePlan{}
	if in.CommonBlock >= in.HeadBlock {
		return plan
	}
	from := in.CommonBlock + 1
	plan.FromBlock = from
	plan.NextBlock = from
	limit := in.InventoryLimit
	if in.ReadBlockID == nil {
		plan.MissingBlock = true
		plan.MissingAt = from
		plan.RemainNum = int64(in.HeadBlock - in.CommonBlock)
		return plan
	}
	for num := from; limit > 0 && num <= in.HeadBlock; num++ {
		bid, ok := in.ReadBlockID(num)
		if !ok {
			plan.MissingBlock = true
			plan.MissingAt = num
			plan.NextBlock = num
			break
		}
		plan.IDs = append(plan.IDs, bid)
		plan.NextBlock = num + 1
		limit--
		if num == ^uint64(0) {
			break
		}
	}
	if missing := in.HeadBlock - in.CommonBlock; uint64(len(plan.IDs)) < missing {
		plan.RemainNum = int64(missing - uint64(len(plan.IDs)))
	}
	return plan
}

func (p ChainInventoryPlan) withSteps() ChainInventoryPlan {
	p.Steps = []ChainInventoryStep{
		{Action: ChainInventoryAppendAccepted, Accepted: append([]types.BlockID(nil), p.Accepted...)},
		{
			Action:         ChainInventoryUpdateProgress,
			RemainNum:      p.RemainNum,
			Target:         p.Target,
			HasTarget:      p.HasTarget,
			StageTarget:    p.StageTarget,
			HasStageTarget: p.HasStageTarget,
		},
	}
	if p.Done {
		p.Steps = append(p.Steps, ChainInventoryStep{Action: ChainInventoryMarkDone})
	}
	return p
}

// ApplyChainInventoryPlan executes downloader-owned state updates for one
// CHAIN_INVENTORY response.
func ApplyChainInventoryPlan(plan ChainInventoryPlan, applier ChainInventoryPlanApplier) ChainInventoryApplyResult {
	result := ChainInventoryApplyResult{Plan: plan}
	if applier == nil {
		return result
	}
	if len(plan.Steps) == 0 {
		plan = plan.withSteps()
		result.Plan = plan
	}
	for _, step := range plan.Steps {
		switch step.Action {
		case ChainInventoryAppendAccepted:
			applier.AppendAcceptedInventory(step.Accepted)
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case ChainInventoryUpdateProgress:
			applier.UpdateInventoryProgress(step.RemainNum, step.Target, step.HasTarget, step.StageTarget, step.HasStageTarget)
			if step.HasStageTarget {
				result.StageTarget = step.StageTarget
				result.HasStageTarget = true
			}
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case ChainInventoryMarkDone:
			applier.MarkInventoryDone()
			result.Done = true
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		default:
			result.UnknownSteps = append(result.UnknownSteps, step.Action)
		}
	}
	return result
}

// ApplyChainInventory creates and applies the downloader-owned response plan
// from side-effect-free inventory facts.
func ApplyChainInventory(in ChainInventoryInput, applier ChainInventoryPlanApplier) ChainInventoryApplyResult {
	return ApplyChainInventoryPlan(PlanChainInventory(in), applier)
}

// ApplyChainInventoryRun creates and applies the lock-held inventory plan, then
// derives the post-lock persistence plan from the actual applied result.
func ApplyChainInventoryRun(in ChainInventoryInput, applier ChainInventoryPlanApplier) ChainInventoryRunApplyResult {
	return ApplyChainInventoryRunPlan(ChainInventoryRunPlan{Inventory: PlanChainInventory(in)}, applier)
}

// ApplyChainInventoryRunPlan applies a prebuilt inventory run plan.
func ApplyChainInventoryRunPlan(plan ChainInventoryRunPlan, applier ChainInventoryPlanApplier) ChainInventoryRunApplyResult {
	result := ChainInventoryRunApplyResult{Plan: plan}
	result.Inventory = ApplyChainInventoryPlan(plan.Inventory, applier)
	result.PostLock = PlanChainInventoryPostLock(result.Inventory)
	result.Plan.PostLock = result.PostLock
	return result
}

// PlanChainInventorySessionRun derives the downloader-owned lock-held plan for
// an accepted CHAIN_INVENTORY response.
func PlanChainInventorySessionRun(in ChainInventorySessionRunInput) ChainInventorySessionRunPlan {
	return ChainInventorySessionRunPlan{
		Inventory: ChainInventoryRunPlan{
			Inventory: PlanChainInventory(in.Inventory),
		},
	}
}

// ApplyChainInventorySessionRun applies the lock-held inventory update and
// post-inventory settlement in downloader-owned order.
func ApplyChainInventorySessionRun(in ChainInventorySessionRunInput, applier ChainInventorySessionRunPlanApplier) ChainInventorySessionRunApplyResult {
	return ApplyChainInventorySessionRunPlan(PlanChainInventorySessionRun(in), applier)
}

// ApplyChainInventorySessionRunPlan applies a prebuilt inventory session run.
func ApplyChainInventorySessionRunPlan(plan ChainInventorySessionRunPlan, applier ChainInventorySessionRunPlanApplier) ChainInventorySessionRunApplyResult {
	result := ChainInventorySessionRunApplyResult{Plan: plan}
	result.Inventory = ApplyChainInventoryRunPlan(plan.Inventory, applier)
	result.Plan.Inventory = result.Inventory.Plan
	if applier == nil {
		return result
	}
	if observer, ok := applier.(ChainInventorySessionRunObserver); ok {
		observer.ChainInventoryApplied()
	}
	result.OutboundRequests = applier.RefillFetchSlotsAfterInventory()
	postInventory := PlanPostInventoryRun(PostInventoryRunInput{
		OutboundRequests: result.OutboundRequests,
		Progress:         applier.PostInventoryRunProgress(),
	})
	result.PostInventory = ApplyPostInventoryRunLockedPlan(postInventory, applier)
	result.Plan.PostInventory = result.PostInventory.Plan
	return result
}

// PlanChainInventoryPostLock returns the post-lock persistence steps for an
// applied inventory response.
func PlanChainInventoryPostLock(result ChainInventoryApplyResult) ChainInventoryPostLockPlan {
	if !result.HasStageTarget {
		return ChainInventoryPostLockPlan{}
	}
	return ChainInventoryPostLockPlan{
		Steps: []ChainInventoryPostLockStep{{
			Action:      ChainInventoryWriteStageProgress,
			Stage:       rawdb.StageSyncInventory,
			StageTarget: result.StageTarget,
		}},
	}
}

// ApplyChainInventoryPostLockPlan executes downloader-owned post-lock
// inventory side effects.
func ApplyChainInventoryPostLockPlan(plan ChainInventoryPostLockPlan, applier ChainInventoryPostLockPlanApplier) ChainInventoryPostLockApplyResult {
	var result ChainInventoryPostLockApplyResult
	if applier == nil {
		return result
	}
	for _, step := range plan.Steps {
		switch step.Action {
		case ChainInventoryWriteStageProgress:
			applier.WriteInventoryStageProgress(step.Stage, step.StageTarget)
			result.WroteStageProgress = true
			result.Stage = step.Stage
			result.StageTarget = step.StageTarget
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		default:
			result.UnknownSteps = append(result.UnknownSteps, step.Action)
		}
	}
	return result
}
