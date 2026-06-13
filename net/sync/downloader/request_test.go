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
