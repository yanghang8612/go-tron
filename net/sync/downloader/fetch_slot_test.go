package downloader

import (
	"reflect"
	"testing"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

func TestPlanFetchSlotDrainedActions(t *testing.T) {
	tests := []struct {
		name string
		in   FetchSlotInput
		want PeerFetchAction
	}{
		{
			name: "done idles",
			in:   FetchSlotInput{Done: true, InventoryTip: 10, CurrentHead: 10},
			want: PeerFetchIdle,
		},
		{
			name: "wait local head",
			in:   FetchSlotInput{InventoryTip: 10, CurrentHead: 9},
			want: PeerFetchWaitLocalHead,
		},
		{
			name: "request inventory",
			in:   FetchSlotInput{InventoryTip: 10, CurrentHead: 10},
			want: PeerFetchRequestInventory,
		},
	}
	for _, tt := range tests {
		got := PlanFetchSlot(tt.in)
		if got.Action != tt.want || len(got.Batch) != 0 || got.Request.Inflight != 0 || got.Wait != 0 {
			t.Fatalf("%s: plan = %+v, want action %v without request", tt.name, got, tt.want)
		}
	}
}

func TestPlanFetchSlotDelaysAndKeepsBatch(t *testing.T) {
	batch := []types.BlockID{queueID(1), queueID(2)}
	got := PlanFetchSlot(FetchSlotInput{
		Batch:     batch,
		FetchWait: 2 * time.Second,
	})
	if got.Action != PeerFetchDelay || got.Wait != 2*time.Second {
		t.Fatalf("delay plan = %+v, want delay 2s", got)
	}
	if !reflect.DeepEqual(got.Batch, batch) || len(got.Request.RequestedHashes) != 0 {
		t.Fatalf("delay plan batch/request = %+v/%+v, want original batch without request", got.Batch, got.Request)
	}
	batch[0] = queueID(99)
	if got.Batch[0].Num != 1 {
		t.Fatalf("plan batch aliases caller slice: got first num %d", got.Batch[0].Num)
	}
}

func TestPlanFetchSlotBuildsRequestState(t *testing.T) {
	now := time.Unix(100, 0)
	batch := []types.BlockID{queueID(1), queueID(2)}
	got := PlanFetchSlot(FetchSlotInput{
		Batch:       batch,
		Now:         now,
		MinInterval: 3 * time.Second,
	})

	if got.Action != PeerFetchSend {
		t.Fatalf("action = %v, want send", got.Action)
	}
	if got.NextFetchAt != now.Add(3*time.Second) {
		t.Fatalf("next fetch = %s, want %s", got.NextFetchAt, now.Add(3*time.Second))
	}
	if got.Request.Inflight != 2 {
		t.Fatalf("inflight = %d, want 2", got.Request.Inflight)
	}
	if got.Request.Pending[batch[0].Hash] != batch[0].Num || got.Request.Pending[batch[1].Hash] != batch[1].Num {
		t.Fatalf("pending = %+v, want batch ids", got.Request.Pending)
	}
	if !reflect.DeepEqual(got.Request.RequestedHashes, []tcommon.Hash{batch[0].Hash, batch[1].Hash}) {
		t.Fatalf("requested hashes = %x, want batch order", got.Request.RequestedHashes)
	}
	if !reflect.DeepEqual(got.Batch, batch) {
		t.Fatalf("batch = %+v, want original batch", got.Batch)
	}
}
