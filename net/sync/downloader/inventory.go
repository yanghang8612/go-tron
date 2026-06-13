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
	return plan
}
