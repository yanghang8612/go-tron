package downloader

import (
	"reflect"
	"testing"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

func TestPlanFetchSlotEligibility(t *testing.T) {
	tests := []struct {
		name string
		in   FetchSlotEligibilityInput
		want bool
	}{
		{
			name: "missing peer",
			in:   FetchSlotEligibilityInput{},
			want: false,
		},
		{
			name: "done",
			in: FetchSlotEligibilityInput{
				PeerPresent: true,
				Done:        true,
			},
			want: false,
		},
		{
			name: "chain requested",
			in: FetchSlotEligibilityInput{
				PeerPresent:    true,
				ChainRequested: true,
			},
			want: false,
		},
		{
			name: "inflight",
			in: FetchSlotEligibilityInput{
				PeerPresent: true,
				Inflight:    1,
			},
			want: false,
		},
		{
			name: "eligible",
			in: FetchSlotEligibilityInput{
				PeerPresent: true,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		if got := PlanFetchSlotEligibility(tt.in); got.Eligible != tt.want {
			t.Fatalf("%s: eligible = %v, want %v", tt.name, got.Eligible, tt.want)
		}
	}
}

func TestPlanFetchSlotDrainedActions(t *testing.T) {
	tests := []struct {
		name      string
		in        FetchSlotInput
		want      PeerFetchAction
		wantSteps []FetchSlotStepAction
	}{
		{
			name: "done idles",
			in:   FetchSlotInput{Done: true, InventoryTip: 10, CurrentHead: 10},
			want: PeerFetchIdle,
		},
		{
			name:      "wait local head",
			in:        FetchSlotInput{InventoryTip: 10, CurrentHead: 9},
			want:      PeerFetchWaitLocalHead,
			wantSteps: []FetchSlotStepAction{FetchSlotWaitLocalHead},
		},
		{
			name:      "request inventory",
			in:        FetchSlotInput{InventoryTip: 10, CurrentHead: 10},
			want:      PeerFetchRequestInventory,
			wantSteps: []FetchSlotStepAction{FetchSlotRequestInventory},
		},
	}
	for _, tt := range tests {
		got := PlanFetchSlot(tt.in)
		if got.Action != tt.want || len(got.Batch) != 0 || got.Request.Inflight != 0 || got.Wait != 0 {
			t.Fatalf("%s: plan = %+v, want action %v without request", tt.name, got, tt.want)
		}
		if actions := fetchSlotStepActions(got.Steps); !reflect.DeepEqual(actions, tt.wantSteps) {
			t.Fatalf("%s: steps = %+v, want %+v", tt.name, actions, tt.wantSteps)
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
	if actions := fetchSlotStepActions(got.Steps); !reflect.DeepEqual(actions, []FetchSlotStepAction{FetchSlotDelay}) {
		t.Fatalf("delay steps = %+v, want delay", actions)
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
	if actions := fetchSlotStepActions(got.Steps); !reflect.DeepEqual(actions, []FetchSlotStepAction{FetchSlotSend}) {
		t.Fatalf("send steps = %+v, want send", actions)
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

func TestApplyFetchSlotPlan(t *testing.T) {
	applier := new(recordingFetchSlotApplier)
	plan := FetchSlotPlan{Steps: []FetchSlotStep{
		{Action: FetchSlotWaitLocalHead},
		{Action: FetchSlotStepAction(255)},
		{Action: FetchSlotRequestInventory},
		{Action: FetchSlotDelay},
		{Action: FetchSlotSend},
	}}
	ApplyFetchSlotPlan(plan, applier)

	want := []FetchSlotStepAction{
		FetchSlotWaitLocalHead,
		FetchSlotRequestInventory,
		FetchSlotDelay,
		FetchSlotSend,
	}
	if !reflect.DeepEqual(applier.calls, want) {
		t.Fatalf("fetch slot calls = %+v, want %+v", applier.calls, want)
	}

	applier.calls = nil
	ApplyFetchSlotPlan(FetchSlotPlan{Action: PeerFetchSend}, applier)
	if !reflect.DeepEqual(applier.calls, []FetchSlotStepAction{FetchSlotSend}) {
		t.Fatalf("fallback fetch slot calls = %+v, want send", applier.calls)
	}
	ApplyFetchSlotPlan(FetchSlotPlan{Action: PeerFetchSend}, nil)
}

func fetchSlotStepActions(steps []FetchSlotStep) []FetchSlotStepAction {
	if len(steps) == 0 {
		return nil
	}
	actions := make([]FetchSlotStepAction, 0, len(steps))
	for _, step := range steps {
		actions = append(actions, step.Action)
	}
	return actions
}

type recordingFetchSlotApplier struct {
	calls []FetchSlotStepAction
}

func (a *recordingFetchSlotApplier) WaitLocalHead(FetchSlotPlan) {
	a.calls = append(a.calls, FetchSlotWaitLocalHead)
}

func (a *recordingFetchSlotApplier) RequestInventory(FetchSlotPlan) {
	a.calls = append(a.calls, FetchSlotRequestInventory)
}

func (a *recordingFetchSlotApplier) DelayFetch(FetchSlotPlan) {
	a.calls = append(a.calls, FetchSlotDelay)
}

func (a *recordingFetchSlotApplier) SendFetch(FetchSlotPlan) {
	a.calls = append(a.calls, FetchSlotSend)
}
