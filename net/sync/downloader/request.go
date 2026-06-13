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
