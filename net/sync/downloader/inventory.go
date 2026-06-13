package downloader

import "github.com/tronprotocol/go-tron/core/types"

// InventoryCandidate is one block ID from a peer CHAIN_INVENTORY response plus
// the service-collected facts needed to decide whether it enters the fetch
// queue.
type InventoryCandidate struct {
	ID    types.BlockID
	Facts InventoryCandidateFacts
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

// ChainInventoryPlanApplier performs state mutations named by a chain
// inventory plan. SyncService owns peer/session fields and DB stage writes;
// downloader owns the update ordering.
type ChainInventoryPlanApplier interface {
	AppendAcceptedInventory(ids []types.BlockID)
	UpdateInventoryProgress(remainNum int64, target InventoryTargetUpdate, hasTarget bool, stageTarget uint64, hasStageTarget bool)
	MarkInventoryDone()
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
func ApplyChainInventoryPlan(plan ChainInventoryPlan, applier ChainInventoryPlanApplier) {
	if applier == nil {
		return
	}
	if len(plan.Steps) == 0 {
		plan = plan.withSteps()
	}
	for _, step := range plan.Steps {
		switch step.Action {
		case ChainInventoryAppendAccepted:
			applier.AppendAcceptedInventory(step.Accepted)
		case ChainInventoryUpdateProgress:
			applier.UpdateInventoryProgress(step.RemainNum, step.Target, step.HasTarget, step.StageTarget, step.HasStageTarget)
		case ChainInventoryMarkDone:
			applier.MarkInventoryDone()
		}
	}
}
