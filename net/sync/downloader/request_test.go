package downloader

import (
	"reflect"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

func TestNewFetchRequestStateEmpty(t *testing.T) {
	state := NewFetchRequestState(nil)
	if state.Inflight != 0 {
		t.Fatalf("inflight = %d, want 0", state.Inflight)
	}
	if state.Pending != nil || state.PendingIDs != nil || state.RequestedHashes != nil {
		t.Fatalf("empty state has maps/lists: %+v", state)
	}
}

func TestNewFetchRequestStateBuildsPendingMaps(t *testing.T) {
	batch := []types.BlockID{queueID(1), queueID(2)}
	state := NewFetchRequestState(batch)

	if state.Inflight != 2 {
		t.Fatalf("inflight = %d, want 2", state.Inflight)
	}
	if got, want := state.Pending[batch[0].Hash], uint64(1); got != want {
		t.Fatalf("pending first = %d, want %d", got, want)
	}
	if got, want := state.Pending[batch[1].Hash], uint64(2); got != want {
		t.Fatalf("pending second = %d, want %d", got, want)
	}
	if !reflect.DeepEqual(state.PendingIDs[batch[0].Hash], batch[0]) {
		t.Fatalf("pending id first = %+v, want %+v", state.PendingIDs[batch[0].Hash], batch[0])
	}
	if !reflect.DeepEqual(state.RequestedHashes, []tcommon.Hash{batch[0].Hash, batch[1].Hash}) {
		t.Fatalf("requested hashes = %x, want batch order", state.RequestedHashes)
	}
}

func TestNewFetchRequestStateKeepsInflightForDuplicateHashes(t *testing.T) {
	first := types.BlockID{Hash: tcommon.Hash{0xaa}, Num: 1}
	second := types.BlockID{Hash: first.Hash, Num: 2}

	state := NewFetchRequestState([]types.BlockID{first, second})
	if state.Inflight != 2 {
		t.Fatalf("inflight = %d, want original batch length 2", state.Inflight)
	}
	if got, want := state.Pending[first.Hash], uint64(2); got != want {
		t.Fatalf("duplicate pending num = %d, want last value %d", got, want)
	}
	if !reflect.DeepEqual(state.PendingIDs[first.Hash], second) {
		t.Fatalf("duplicate pending id = %+v, want last value %+v", state.PendingIDs[first.Hash], second)
	}
	if got, want := len(state.RequestedHashes), 2; got != want {
		t.Fatalf("requested hashes = %d, want %d duplicate marks", got, want)
	}
}

func TestAcknowledgeFetchReceiptRejectsUnknownHash(t *testing.T) {
	request := NewFetchRequestState([]types.BlockID{queueID(1)})
	got := AcknowledgeFetchReceipt(FetchReceiptState{
		Inflight:   request.Inflight,
		Pending:    request.Pending,
		PendingIDs: request.PendingIDs,
	}, queueID(2).Hash, 2)

	if got.Accepted {
		t.Fatal("unknown hash was accepted")
	}
	if got.Inflight != 1 || got.BatchDone {
		t.Fatalf("result = %+v, want inflight 1 and batch not done", got)
	}
	if _, ok := request.Pending[queueID(1).Hash]; !ok {
		t.Fatal("pending entry was deleted for unknown hash")
	}
}

func TestAcknowledgeFetchReceiptRejectsWrongNumber(t *testing.T) {
	request := NewFetchRequestState([]types.BlockID{queueID(1)})
	got := AcknowledgeFetchReceipt(FetchReceiptState{
		Inflight:   request.Inflight,
		Pending:    request.Pending,
		PendingIDs: request.PendingIDs,
	}, queueID(1).Hash, 2)

	if got.Accepted {
		t.Fatal("wrong block number was accepted")
	}
	if got.Inflight != 1 || got.BatchDone {
		t.Fatalf("result = %+v, want inflight 1 and batch not done", got)
	}
	if _, ok := request.PendingIDs[queueID(1).Hash]; !ok {
		t.Fatal("pending id was deleted for wrong number")
	}
}

func TestAcknowledgeFetchReceiptDeletesPendingAndDecrementsInflight(t *testing.T) {
	batch := []types.BlockID{queueID(1), queueID(2)}
	request := NewFetchRequestState(batch)
	got := AcknowledgeFetchReceipt(FetchReceiptState{
		Inflight:   request.Inflight,
		Pending:    request.Pending,
		PendingIDs: request.PendingIDs,
	}, batch[0].Hash, batch[0].Num)

	if !got.Accepted {
		t.Fatal("matching receipt was rejected")
	}
	if got.Inflight != 1 || got.BatchDone {
		t.Fatalf("result = %+v, want inflight 1 and batch not done", got)
	}
	if _, ok := request.Pending[batch[0].Hash]; ok {
		t.Fatal("acked pending entry was not deleted")
	}
	if _, ok := request.PendingIDs[batch[0].Hash]; ok {
		t.Fatal("acked pending id was not deleted")
	}
	if got, want := request.Pending[batch[1].Hash], batch[1].Num; got != want {
		t.Fatalf("remaining pending = %d, want %d", got, want)
	}
}

func TestAcknowledgeFetchReceiptDoesNotUnderflowInflight(t *testing.T) {
	bid := queueID(1)
	pending := map[tcommon.Hash]uint64{bid.Hash: bid.Num}
	pendingIDs := map[tcommon.Hash]types.BlockID{bid.Hash: bid}

	got := AcknowledgeFetchReceipt(FetchReceiptState{
		Pending:    pending,
		PendingIDs: pendingIDs,
	}, bid.Hash, bid.Num)

	if !got.Accepted || got.Inflight != 0 || !got.BatchDone {
		t.Fatalf("result = %+v, want accepted done with inflight 0", got)
	}
	if len(pending) != 0 || len(pendingIDs) != 0 {
		t.Fatalf("pending maps were not cleared: %d/%d", len(pending), len(pendingIDs))
	}
}
