package downloader

import (
	"time"

	"github.com/tronprotocol/go-tron/core/types"
)

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
		return plan
	}
	plan.Request = NewFetchRequestState(in.Batch)
	if in.MinInterval > 0 {
		plan.NextFetchAt = in.Now.Add(in.MinInterval)
	}
	return plan
}
