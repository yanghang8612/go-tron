package downloader

import (
	"time"

	"github.com/tronprotocol/go-tron/core/types"
)

// FetchSlotEligibilityInput is the side-effect-free peer state needed to decide
// whether a peer can receive more block fetch work.
type FetchSlotEligibilityInput struct {
	PeerPresent    bool
	Done           bool
	ChainRequested bool
	Inflight       int
}

// FetchSlotEligibilityPlan is the downloader-owned refill eligibility decision
// for one peer.
type FetchSlotEligibilityPlan struct {
	Eligible bool
}

// FetchSlotInput is the side-effect-free peer state needed to decide the next
// fetch-slot operation for one eligible peer.
type FetchSlotInput struct {
	Batch        []types.BlockID
	FetchWait    time.Duration
	Done         bool
	InventoryTip uint64
	CurrentHead  uint64
	Now          time.Time
	MinInterval  time.Duration
}

// FetchSlotPlan is the downloader-owned scheduler decision for one peer fetch
// slot. SyncService owns timers, pending-map assignment, and network sends.
type FetchSlotPlan struct {
	Action      PeerFetchAction
	Batch       []types.BlockID
	Wait        time.Duration
	Request     FetchRequestState
	NextFetchAt time.Time
	Steps       []FetchSlotStep
}

// FetchSlotStepAction names one peer-local fetch-slot operation.
type FetchSlotStepAction uint8

const (
	FetchSlotWaitLocalHead FetchSlotStepAction = iota
	FetchSlotRequestInventory
	FetchSlotDelay
	FetchSlotSend
)

// FetchSlotStep is one downloader-owned operation for an eligible peer fetch
// slot.
type FetchSlotStep struct {
	Action FetchSlotStepAction
}

// FetchSlotPlanApplier performs the runtime operations named by a fetch-slot
// plan. SyncService owns timers, peer state, requested marks, and network
// dispatch accumulation; downloader owns the action ordering.
type FetchSlotPlanApplier interface {
	WaitLocalHead(plan FetchSlotPlan)
	RequestInventory(plan FetchSlotPlan)
	DelayFetch(plan FetchSlotPlan)
	SendFetch(plan FetchSlotPlan)
}

// FetchSlotApplyResult records the peer-local actions applied for one fetch
// slot refill and the network dispatch intent produced by those actions.
type FetchSlotApplyResult struct {
	AppliedSteps     []FetchSlotStepAction
	UnknownSteps     []FetchSlotStepAction
	RequestInventory bool
	SendFetch        bool
}

// PlanFetchSlotEligibility decides whether one peer can be considered for fetch
// slot refill.
func PlanFetchSlotEligibility(in FetchSlotEligibilityInput) FetchSlotEligibilityPlan {
	return FetchSlotEligibilityPlan{
		Eligible: in.PeerPresent && !in.Done && !in.ChainRequested && in.Inflight == 0,
	}
}

// PlanFetchSlot combines the peer-local fetch action and outbound request
// bookkeeping for one peer that is otherwise eligible to fetch.
func PlanFetchSlot(in FetchSlotInput) FetchSlotPlan {
	action := PlanReadyPeerFetch(ReadyPeerFetchInput{
		BatchLen:     len(in.Batch),
		FetchWait:    in.FetchWait,
		Done:         in.Done,
		InventoryTip: in.InventoryTip,
		CurrentHead:  in.CurrentHead,
	})
	plan := FetchSlotPlan{
		Action: action.Action,
		Batch:  append([]types.BlockID(nil), in.Batch...),
		Wait:   action.Wait,
	}
	if action.Action != PeerFetchSend {
		return plan.withSteps()
	}
	plan.Request = NewFetchRequestState(in.Batch)
	if in.MinInterval > 0 {
		plan.NextFetchAt = in.Now.Add(in.MinInterval)
	}
	return plan.withSteps()
}

func (p FetchSlotPlan) withSteps() FetchSlotPlan {
	switch p.Action {
	case PeerFetchWaitLocalHead:
		p.Steps = []FetchSlotStep{{Action: FetchSlotWaitLocalHead}}
	case PeerFetchRequestInventory:
		p.Steps = []FetchSlotStep{{Action: FetchSlotRequestInventory}}
	case PeerFetchDelay:
		p.Steps = []FetchSlotStep{{Action: FetchSlotDelay}}
	case PeerFetchSend:
		p.Steps = []FetchSlotStep{{Action: FetchSlotSend}}
	}
	return p
}

// ApplyFetchSlotPlan executes the downloader-owned peer fetch-slot schedule.
func ApplyFetchSlotPlan(plan FetchSlotPlan, applier FetchSlotPlanApplier) FetchSlotApplyResult {
	var result FetchSlotApplyResult
	if applier == nil {
		return result
	}
	if len(plan.Steps) == 0 {
		plan = plan.withSteps()
	}
	for _, step := range plan.Steps {
		switch step.Action {
		case FetchSlotWaitLocalHead:
			applier.WaitLocalHead(plan)
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case FetchSlotRequestInventory:
			applier.RequestInventory(plan)
			result.RequestInventory = true
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case FetchSlotDelay:
			applier.DelayFetch(plan)
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case FetchSlotSend:
			applier.SendFetch(plan)
			result.SendFetch = true
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		default:
			result.UnknownSteps = append(result.UnknownSteps, step.Action)
		}
	}
	return result
}
