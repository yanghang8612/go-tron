package downloader

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

// FetchRequestState is the in-flight bookkeeping derived from one outbound
// FETCH_INV_DATA batch. It does not own peer timers or network sends.
type FetchRequestState struct {
	Inflight        int
	Pending         map[tcommon.Hash]uint64
	PendingIDs      map[tcommon.Hash]types.BlockID
	RequestedHashes []tcommon.Hash
}

// NewFetchRequestState builds the peer-local pending maps and requested-hash
// marks for an outbound block batch.
func NewFetchRequestState(batch []types.BlockID) FetchRequestState {
	state := FetchRequestState{Inflight: len(batch)}
	if len(batch) == 0 {
		return state
	}
	state.Pending = make(map[tcommon.Hash]uint64, len(batch))
	state.PendingIDs = make(map[tcommon.Hash]types.BlockID, len(batch))
	state.RequestedHashes = make([]tcommon.Hash, 0, len(batch))
	for _, bid := range batch {
		state.Pending[bid.Hash] = bid.Num
		state.PendingIDs[bid.Hash] = bid
		state.RequestedHashes = append(state.RequestedHashes, bid.Hash)
	}
	return state
}

// FetchReceiptState is the peer-local in-flight state updated when a requested
// block body arrives.
type FetchReceiptState struct {
	Inflight   int
	Pending    map[tcommon.Hash]uint64
	PendingIDs map[tcommon.Hash]types.BlockID
}

// FetchReceiptResult describes the effect of acknowledging one received block.
type FetchReceiptResult struct {
	Accepted  bool
	Inflight  int
	BatchDone bool
}

// FetchReceiptSettlement is the downloader-owned state-machine decision after
// acknowledging one received FETCH_INV_DATA block.
type FetchReceiptSettlement struct {
	Accepted            bool
	Inflight            int
	BatchDone           bool
	DeleteRequestedHash bool
	AdvanceFetchSeq     bool
	StopFetchTimer      bool
	RearmFetchTimer     bool
	FillFetchSlots      bool
	DrainBuffered       bool
}

// FetchReceiptDispatchInput is the post-drain state needed before sending
// follow-up outbound fetch requests after a received block body.
type FetchReceiptDispatchInput struct {
	OutboundRequests int
	Syncing          bool
	Paused           bool
}

// FetchReceiptDispatchPlan describes whether follow-up fetch requests should
// be sent after the local drain has had a chance to settle the session.
type FetchReceiptDispatchPlan struct {
	SendOutboundRequests bool
}

// FetchedBlockBufferAction names the local buffer/stage decision for one
// received block body after its fetch receipt was accepted.
type FetchedBlockBufferAction uint8

const (
	FetchedBlockBufferIgnore FetchedBlockBufferAction = iota
	FetchedBlockBufferStage
	FetchedBlockBufferConflict
)

// FetchedBlockBufferFacts are the side-effect-free facts needed to decide
// whether a received block body should be staged and inserted into the local
// contiguous drain buffer.
type FetchedBlockBufferFacts struct {
	ID                   types.BlockID
	CurrentHead          uint64
	ExistingBuffered     bool
	ExistingBufferedHash tcommon.Hash
	HashBuffered         bool
	ReservedPath         bool
}

// FetchedBlockBufferPlan is the downloader-owned local buffer/stage decision.
type FetchedBlockBufferPlan struct {
	Action FetchedBlockBufferAction
	ID     types.BlockID
	Kept   tcommon.Hash
}

// AcknowledgeFetchReceipt removes a matching received block from the in-flight
// request state and decrements the outstanding count without underflowing.
func AcknowledgeFetchReceipt(state FetchReceiptState, hash tcommon.Hash, num uint64) FetchReceiptResult {
	result := FetchReceiptResult{Inflight: state.Inflight, BatchDone: state.Inflight == 0}
	expectedNum, ok := state.Pending[hash]
	if !ok || expectedNum != num {
		return result
	}
	delete(state.Pending, hash)
	delete(state.PendingIDs, hash)
	if state.Inflight > 0 {
		state.Inflight--
	}
	return FetchReceiptResult{
		Accepted:  true,
		Inflight:  state.Inflight,
		BatchDone: state.Inflight == 0,
	}
}

// PlanFetchReceiptSettlement maps an accepted fetch receipt to the service
// actions required to keep timers, global requested marks, and follow-up fetch
// scheduling consistent.
func PlanFetchReceiptSettlement(receipt FetchReceiptResult) FetchReceiptSettlement {
	if !receipt.Accepted {
		return FetchReceiptSettlement{}
	}
	return FetchReceiptSettlement{
		Accepted:            true,
		Inflight:            receipt.Inflight,
		BatchDone:           receipt.BatchDone,
		DeleteRequestedHash: true,
		AdvanceFetchSeq:     true,
		StopFetchTimer:      true,
		RearmFetchTimer:     !receipt.BatchDone,
		FillFetchSlots:      receipt.BatchDone,
		DrainBuffered:       true,
	}
}

// PlanFetchReceiptDispatch decides whether follow-up outbound fetch requests
// should be sent after receipt settlement and any local drain.
func PlanFetchReceiptDispatch(in FetchReceiptDispatchInput) FetchReceiptDispatchPlan {
	return FetchReceiptDispatchPlan{
		SendOutboundRequests: in.OutboundRequests > 0 && in.Syncing && !in.Paused,
	}
}

// PlanFetchedBlockBuffer decides whether an accepted sync block body should be
// persisted in the staged-body table and local drain buffer.
func PlanFetchedBlockBuffer(f FetchedBlockBufferFacts) FetchedBlockBufferPlan {
	plan := FetchedBlockBufferPlan{ID: f.ID}
	if f.ID.Num <= f.CurrentHead {
		return plan
	}
	if f.ExistingBuffered {
		if f.ExistingBufferedHash != f.ID.Hash {
			plan.Action = FetchedBlockBufferConflict
			plan.Kept = f.ExistingBufferedHash
		}
		return plan
	}
	if f.HashBuffered || !f.ReservedPath {
		return plan
	}
	plan.Action = FetchedBlockBufferStage
	return plan
}
