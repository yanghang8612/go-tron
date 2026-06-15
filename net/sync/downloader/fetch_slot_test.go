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

func TestPlanFetchSlotRefill(t *testing.T) {
	if got := PlanFetchSlotRefill(FetchSlotRefillInput{}); got.Eligible || len(got.Steps) != 0 {
		t.Fatalf("ineligible refill plan = %+v, want no steps", got)
	}

	got := PlanFetchSlotRefill(FetchSlotRefillInput{
		Eligibility: FetchSlotEligibilityInput{PeerPresent: true},
	})
	wantSteps := []FetchSlotRefillStepAction{
		FetchSlotRefillAssignRetry,
		FetchSlotRefillNextBatch,
		FetchSlotRefillApplySlot,
	}
	if !got.Eligible || !reflect.DeepEqual(fetchSlotRefillStepActions(got.Steps), wantSteps) {
		t.Fatalf("eligible refill plan = %+v, want steps %+v", got, wantSteps)
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

func TestApplyFetchSlotRefillPlan(t *testing.T) {
	now := time.Unix(100, 0)
	batch := []types.BlockID{queueID(1), queueID(2)}
	applier := &recordingFetchSlotRefillApplier{
		batch: batch,
		slotInput: FetchSlotInput{
			Now:         now,
			MinInterval: 3 * time.Second,
		},
	}
	plan := FetchSlotRefillPlan{Steps: []FetchSlotRefillStep{
		{Action: FetchSlotRefillAssignRetry},
		{Action: FetchSlotRefillStepAction(255)},
		{Action: FetchSlotRefillNextBatch},
		{Action: FetchSlotRefillApplySlot},
	}}
	result := ApplyFetchSlotRefillPlan(plan, applier)

	if !reflect.DeepEqual(result.Plan, plan) {
		t.Fatalf("refill result plan = %+v, want original plan %+v", result.Plan, plan)
	}
	wantRefillCalls := []FetchSlotRefillStepAction{
		FetchSlotRefillAssignRetry,
		FetchSlotRefillNextBatch,
		FetchSlotRefillApplySlot,
	}
	if !reflect.DeepEqual(applier.refillCalls, wantRefillCalls) {
		t.Fatalf("refill calls = %+v, want %+v", applier.refillCalls, wantRefillCalls)
	}
	if !reflect.DeepEqual(applier.slotCalls, []FetchSlotStepAction{FetchSlotSend}) {
		t.Fatalf("slot calls = %+v, want send", applier.slotCalls)
	}
	if !reflect.DeepEqual(result.AppliedSteps, wantRefillCalls) ||
		!reflect.DeepEqual(result.UnknownSteps, []FetchSlotRefillStepAction{FetchSlotRefillStepAction(255)}) ||
		!reflect.DeepEqual(result.Batch, batch) ||
		result.SlotPlan.Action != PeerFetchSend ||
		!result.SendFetch || result.RequestInventory {
		t.Fatalf("refill result = %+v, want applied refill with send slot", result)
	}
	if !reflect.DeepEqual(result.SlotPlan.Batch, batch) || result.SlotPlan.NextFetchAt != now.Add(3*time.Second) {
		t.Fatalf("slot plan batch/next = %+v/%s, want batch and next fetch", result.SlotPlan.Batch, result.SlotPlan.NextFetchAt)
	}

	batch[0] = queueID(99)
	if result.Batch[0].Num != 1 || result.SlotPlan.Batch[0].Num != 1 {
		t.Fatalf("refill result aliases source batch: result=%+v slot=%+v", result.Batch, result.SlotPlan.Batch)
	}

	applier = &recordingFetchSlotRefillApplier{}
	result = ApplyFetchSlotRefillPlan(FetchSlotRefillPlan{Eligible: true}, applier)
	if !reflect.DeepEqual(fetchSlotRefillStepActions(result.Plan.Steps), wantRefillCalls) {
		t.Fatalf("fallback refill result plan steps = %+v, want %+v", fetchSlotRefillStepActions(result.Plan.Steps), wantRefillCalls)
	}
	if !reflect.DeepEqual(applier.refillCalls, wantRefillCalls) {
		t.Fatalf("fallback refill calls = %+v, want %+v", applier.refillCalls, wantRefillCalls)
	}
	if result.SlotPlan.Action != PeerFetchRequestInventory || !result.RequestInventory || result.SendFetch {
		t.Fatalf("fallback refill result = %+v, want inventory request", result)
	}

	nilPlan := FetchSlotRefillPlan{Eligible: true}
	nilResult := ApplyFetchSlotRefillPlan(nilPlan, nil)
	if !reflect.DeepEqual(nilResult.Plan, nilPlan) ||
		len(nilResult.AppliedSteps) != 0 || len(nilResult.UnknownSteps) != 0 || nilResult.SendFetch || nilResult.RequestInventory {
		t.Fatalf("nil refill result = %+v, want empty", nilResult)
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
	result := ApplyFetchSlotPlan(plan, applier)

	want := []FetchSlotStepAction{
		FetchSlotWaitLocalHead,
		FetchSlotRequestInventory,
		FetchSlotDelay,
		FetchSlotSend,
	}
	if !reflect.DeepEqual(applier.calls, want) {
		t.Fatalf("fetch slot calls = %+v, want %+v", applier.calls, want)
	}
	if !reflect.DeepEqual(result.AppliedSteps, want) ||
		!reflect.DeepEqual(result.UnknownSteps, []FetchSlotStepAction{FetchSlotStepAction(255)}) ||
		!result.RequestInventory || !result.SendFetch {
		t.Fatalf("fetch slot apply result = %+v, want applied calls, unknown step, inventory and fetch dispatch", result)
	}

	applier.calls = nil
	result = ApplyFetchSlotPlan(FetchSlotPlan{Action: PeerFetchSend}, applier)
	if !reflect.DeepEqual(applier.calls, []FetchSlotStepAction{FetchSlotSend}) {
		t.Fatalf("fallback fetch slot calls = %+v, want send", applier.calls)
	}
	if !reflect.DeepEqual(result.AppliedSteps, []FetchSlotStepAction{FetchSlotSend}) || len(result.UnknownSteps) != 0 || result.RequestInventory || !result.SendFetch {
		t.Fatalf("fallback fetch slot apply result = %+v, want send dispatch", result)
	}
	if nilResult := ApplyFetchSlotPlan(FetchSlotPlan{Action: PeerFetchSend}, nil); len(nilResult.AppliedSteps) != 0 || len(nilResult.UnknownSteps) != 0 || nilResult.RequestInventory || nilResult.SendFetch {
		t.Fatalf("nil fetch slot apply result = %+v, want empty", nilResult)
	}
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

func fetchSlotRefillStepActions(steps []FetchSlotRefillStep) []FetchSlotRefillStepAction {
	if len(steps) == 0 {
		return nil
	}
	actions := make([]FetchSlotRefillStepAction, 0, len(steps))
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

type recordingFetchSlotRefillApplier struct {
	refillCalls []FetchSlotRefillStepAction
	slotCalls   []FetchSlotStepAction
	batch       []types.BlockID
	slotInput   FetchSlotInput
}

func (a *recordingFetchSlotRefillApplier) AssignRetry() {
	a.refillCalls = append(a.refillCalls, FetchSlotRefillAssignRetry)
}

func (a *recordingFetchSlotRefillApplier) NextFetchBatch() []types.BlockID {
	a.refillCalls = append(a.refillCalls, FetchSlotRefillNextBatch)
	return a.batch
}

func (a *recordingFetchSlotRefillApplier) FetchSlotInput(batch []types.BlockID) FetchSlotInput {
	a.refillCalls = append(a.refillCalls, FetchSlotRefillApplySlot)
	in := a.slotInput
	in.Batch = batch
	return in
}

func (a *recordingFetchSlotRefillApplier) WaitLocalHead(FetchSlotPlan) {
	a.slotCalls = append(a.slotCalls, FetchSlotWaitLocalHead)
}

func (a *recordingFetchSlotRefillApplier) RequestInventory(FetchSlotPlan) {
	a.slotCalls = append(a.slotCalls, FetchSlotRequestInventory)
}

func (a *recordingFetchSlotRefillApplier) DelayFetch(FetchSlotPlan) {
	a.slotCalls = append(a.slotCalls, FetchSlotDelay)
}

func (a *recordingFetchSlotRefillApplier) SendFetch(FetchSlotPlan) {
	a.slotCalls = append(a.slotCalls, FetchSlotSend)
}
