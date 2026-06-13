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

func TestPlanFetchReceiptSettlement(t *testing.T) {
	rejected := PlanFetchReceiptSettlement(FetchReceiptResult{Inflight: 2})
	if rejected.Accepted || rejected.DeleteRequestedHash || rejected.DrainBuffered {
		t.Fatalf("rejected settlement = %+v, want no side effects", rejected)
	}

	partial := PlanFetchReceiptSettlement(FetchReceiptResult{Accepted: true, Inflight: 1})
	if !partial.Accepted || partial.Inflight != 1 || partial.BatchDone || !partial.DeleteRequestedHash || !partial.AdvanceFetchSeq || !partial.StopFetchTimer || !partial.RearmFetchTimer || partial.FillFetchSlots || !partial.DrainBuffered {
		t.Fatalf("partial settlement = %+v, want rearm without fill", partial)
	}
	if got, want := fetchReceiptStepActions(partial.LockedPreBuffer), []FetchReceiptSettlementStepAction{
		FetchReceiptDeleteRequestedHash,
		FetchReceiptAdvanceFetchSeq,
		FetchReceiptUpdateInflight,
		FetchReceiptStopFetchTimer,
		FetchReceiptRearmFetchTimer,
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("partial pre-buffer steps = %+v, want %+v", got, want)
	}
	if got, want := fetchReceiptStepActions(partial.LockedPostBuffer), []FetchReceiptSettlementStepAction{FetchReceiptMirrorLegacy}; !reflect.DeepEqual(got, want) {
		t.Fatalf("partial post-buffer steps = %+v, want %+v", got, want)
	}
	if got, want := fetchReceiptStepActions(partial.AfterUnlock), []FetchReceiptSettlementStepAction{FetchReceiptDrainBuffered}; !reflect.DeepEqual(got, want) {
		t.Fatalf("partial after-unlock steps = %+v, want %+v", got, want)
	}

	done := PlanFetchReceiptSettlement(FetchReceiptResult{Accepted: true, BatchDone: true})
	if !done.Accepted || done.Inflight != 0 || !done.BatchDone || !done.DeleteRequestedHash || !done.AdvanceFetchSeq || !done.StopFetchTimer || done.RearmFetchTimer || !done.FillFetchSlots || !done.DrainBuffered {
		t.Fatalf("done settlement = %+v, want fill without rearm", done)
	}
	if got, want := fetchReceiptStepActions(done.LockedPreBuffer), []FetchReceiptSettlementStepAction{
		FetchReceiptDeleteRequestedHash,
		FetchReceiptAdvanceFetchSeq,
		FetchReceiptUpdateInflight,
		FetchReceiptStopFetchTimer,
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("done pre-buffer steps = %+v, want %+v", got, want)
	}
	if got, want := fetchReceiptStepActions(done.LockedPostBuffer), []FetchReceiptSettlementStepAction{FetchReceiptFillFetchSlots, FetchReceiptMirrorLegacy}; !reflect.DeepEqual(got, want) {
		t.Fatalf("done post-buffer steps = %+v, want %+v", got, want)
	}
	if got, want := fetchReceiptStepActions(done.AfterUnlock), []FetchReceiptSettlementStepAction{FetchReceiptDrainBuffered}; !reflect.DeepEqual(got, want) {
		t.Fatalf("done after-unlock steps = %+v, want %+v", got, want)
	}
	if len(done.LockedPreBuffer) < 3 || done.LockedPreBuffer[2].Inflight != 0 || len(partial.LockedPreBuffer) < 3 || partial.LockedPreBuffer[2].Inflight != 1 {
		t.Fatalf("update-inflight steps = done %+v partial %+v, want 0/1", done.LockedPreBuffer, partial.LockedPreBuffer)
	}
}

func TestApplyFetchReceiptSettlementPlan(t *testing.T) {
	applier := new(recordingFetchReceiptSettlementApplier)
	plan := FetchReceiptSettlement{
		LockedPreBuffer: []FetchReceiptSettlementStep{
			{Action: FetchReceiptDeleteRequestedHash},
			{Action: FetchReceiptUpdateInflight, Inflight: 3},
			{Action: FetchReceiptSettlementStepAction(255)},
		},
		LockedPostBuffer: []FetchReceiptSettlementStep{
			{Action: FetchReceiptFillFetchSlots},
			{Action: FetchReceiptMirrorLegacy},
		},
		AfterUnlock: []FetchReceiptSettlementStep{
			{Action: FetchReceiptDrainBuffered},
		},
	}
	ApplyFetchReceiptSettlementLockedPreBufferPlan(plan, applier)
	ApplyFetchReceiptSettlementLockedPostBufferPlan(plan, applier)
	ApplyFetchReceiptSettlementAfterUnlockPlan(plan, applier)

	want := []FetchReceiptSettlementStepAction{
		FetchReceiptDeleteRequestedHash,
		FetchReceiptUpdateInflight,
		FetchReceiptFillFetchSlots,
		FetchReceiptMirrorLegacy,
		FetchReceiptDrainBuffered,
	}
	if !reflect.DeepEqual(applier.calls, want) || applier.inflight != 3 {
		t.Fatalf("settlement calls/inflight = %+v/%d, want %+v/3", applier.calls, applier.inflight, want)
	}

	applier.calls = nil
	ApplyFetchReceiptSettlementLockedPreBufferPlan(FetchReceiptSettlement{
		Accepted:            true,
		Inflight:            4,
		DeleteRequestedHash: true,
		AdvanceFetchSeq:     true,
		StopFetchTimer:      true,
		RearmFetchTimer:     true,
	}, applier)
	want = []FetchReceiptSettlementStepAction{
		FetchReceiptDeleteRequestedHash,
		FetchReceiptAdvanceFetchSeq,
		FetchReceiptUpdateInflight,
		FetchReceiptStopFetchTimer,
		FetchReceiptRearmFetchTimer,
	}
	if !reflect.DeepEqual(applier.calls, want) || applier.inflight != 4 {
		t.Fatalf("fallback pre-buffer calls/inflight = %+v/%d, want %+v/4", applier.calls, applier.inflight, want)
	}
	ApplyFetchReceiptSettlementLockedPreBufferPlan(FetchReceiptSettlement{Accepted: true}, nil)
	ApplyFetchReceiptSettlementLockedPostBufferPlan(FetchReceiptSettlement{Accepted: true}, nil)
	ApplyFetchReceiptSettlementAfterUnlockPlan(FetchReceiptSettlement{Accepted: true}, nil)
}

func TestPlanFetchReceiptDispatch(t *testing.T) {
	tests := map[string]struct {
		input FetchReceiptDispatchInput
		want  FetchReceiptDispatchPlan
	}{
		"active outbound": {
			input: FetchReceiptDispatchInput{OutboundRequests: 1, Progress: SessionProgress{Syncing: true}},
			want: FetchReceiptDispatchPlan{
				SendOutboundRequests: true,
				Steps:                []FetchReceiptDispatchStep{{Action: FetchReceiptDispatchSendOutbound}},
			},
		},
		"no outbound": {
			input: FetchReceiptDispatchInput{Progress: SessionProgress{Syncing: true}},
			want:  FetchReceiptDispatchPlan{},
		},
		"not syncing": {
			input: FetchReceiptDispatchInput{OutboundRequests: 1},
			want:  FetchReceiptDispatchPlan{},
		},
		"paused": {
			input: FetchReceiptDispatchInput{OutboundRequests: 1, Progress: SessionProgress{Syncing: true, Paused: true}},
			want:  FetchReceiptDispatchPlan{},
		},
	}
	for name, test := range tests {
		if got := PlanFetchReceiptDispatch(test.input); !reflect.DeepEqual(got, test.want) {
			t.Fatalf("%s dispatch = %+v, want %+v", name, got, test.want)
		}
	}
}

func TestApplyFetchReceiptDispatchPlan(t *testing.T) {
	applier := new(recordingFetchReceiptDispatchApplier)
	ApplyFetchReceiptDispatchPlan(FetchReceiptDispatchPlan{Steps: []FetchReceiptDispatchStep{
		{Action: FetchReceiptDispatchSendOutbound},
		{Action: FetchReceiptDispatchStepAction(255)},
	}}, applier)
	if applier.sent != 1 {
		t.Fatalf("dispatch sends = %d, want 1", applier.sent)
	}

	applier.sent = 0
	ApplyFetchReceiptDispatchPlan(FetchReceiptDispatchPlan{SendOutboundRequests: true}, applier)
	if applier.sent != 1 {
		t.Fatalf("fallback dispatch sends = %d, want 1", applier.sent)
	}
	ApplyFetchReceiptDispatchPlan(FetchReceiptDispatchPlan{SendOutboundRequests: true}, nil)
}

func TestPlanFetchedBlockBuffer(t *testing.T) {
	bid := queueID(10)
	tests := []struct {
		name  string
		facts FetchedBlockBufferFacts
		want  FetchedBlockBufferPlan
	}{
		{
			name:  "at current head ignored",
			facts: FetchedBlockBufferFacts{ID: bid, CurrentHead: bid.Num},
			want:  FetchedBlockBufferPlan{ID: bid},
		},
		{
			name: "existing same height fork conflicts",
			facts: FetchedBlockBufferFacts{
				ID:                   bid,
				CurrentHead:          bid.Num - 1,
				ExistingBuffered:     true,
				ExistingBufferedHash: tcommon.Hash{0xee},
			},
			want: FetchedBlockBufferPlan{Action: FetchedBlockBufferConflict, ID: bid, Kept: tcommon.Hash{0xee}},
		},
		{
			name: "existing same block ignored",
			facts: FetchedBlockBufferFacts{
				ID:                   bid,
				CurrentHead:          bid.Num - 1,
				ExistingBuffered:     true,
				ExistingBufferedHash: bid.Hash,
			},
			want: FetchedBlockBufferPlan{ID: bid},
		},
		{
			name:  "duplicate hash ignored",
			facts: FetchedBlockBufferFacts{ID: bid, CurrentHead: bid.Num - 1, HashBuffered: true, ReservedPath: true},
			want:  FetchedBlockBufferPlan{ID: bid},
		},
		{
			name:  "path reservation failure ignored",
			facts: FetchedBlockBufferFacts{ID: bid, CurrentHead: bid.Num - 1},
			want:  FetchedBlockBufferPlan{ID: bid},
		},
		{
			name:  "fresh future block staged",
			facts: FetchedBlockBufferFacts{ID: bid, CurrentHead: bid.Num - 1, ReservedPath: true},
			want:  FetchedBlockBufferPlan{Action: FetchedBlockBufferStage, ID: bid},
		},
	}
	for _, tt := range tests {
		if got := PlanFetchedBlockBuffer(tt.facts); got != tt.want {
			t.Fatalf("%s: plan = %+v, want %+v", tt.name, got, tt.want)
		}
	}
}

func TestApplyFetchedBlockBufferPlan(t *testing.T) {
	applier := new(recordingFetchedBlockBufferApplier)
	conflict := FetchedBlockBufferPlan{
		Action: FetchedBlockBufferConflict,
		ID:     queueID(10),
		Kept:   tcommon.Hash{0xee},
	}
	stage := FetchedBlockBufferPlan{
		Action: FetchedBlockBufferStage,
		ID:     queueID(11),
	}

	ApplyFetchedBlockBufferPlan(FetchedBlockBufferPlan{ID: queueID(9)}, applier)
	ApplyFetchedBlockBufferPlan(conflict, applier)
	ApplyFetchedBlockBufferPlan(stage, applier)

	if !reflect.DeepEqual(applier.conflicts, []FetchedBlockBufferPlan{conflict}) {
		t.Fatalf("conflicts = %+v, want %+v", applier.conflicts, []FetchedBlockBufferPlan{conflict})
	}
	if !reflect.DeepEqual(applier.staged, []FetchedBlockBufferPlan{stage}) {
		t.Fatalf("staged = %+v, want %+v", applier.staged, []FetchedBlockBufferPlan{stage})
	}
	ApplyFetchedBlockBufferPlan(stage, nil)
}

func fetchReceiptStepActions(steps []FetchReceiptSettlementStep) []FetchReceiptSettlementStepAction {
	actions := make([]FetchReceiptSettlementStepAction, 0, len(steps))
	for _, step := range steps {
		actions = append(actions, step.Action)
	}
	return actions
}

type recordingFetchReceiptSettlementApplier struct {
	calls    []FetchReceiptSettlementStepAction
	inflight int
}

func (a *recordingFetchReceiptSettlementApplier) DeleteRequestedHash() {
	a.calls = append(a.calls, FetchReceiptDeleteRequestedHash)
}

func (a *recordingFetchReceiptSettlementApplier) AdvanceFetchSeq() {
	a.calls = append(a.calls, FetchReceiptAdvanceFetchSeq)
}

func (a *recordingFetchReceiptSettlementApplier) UpdateInflight(inflight int) {
	a.calls = append(a.calls, FetchReceiptUpdateInflight)
	a.inflight = inflight
}

func (a *recordingFetchReceiptSettlementApplier) StopFetchTimer() {
	a.calls = append(a.calls, FetchReceiptStopFetchTimer)
}

func (a *recordingFetchReceiptSettlementApplier) RearmFetchTimer() {
	a.calls = append(a.calls, FetchReceiptRearmFetchTimer)
}

func (a *recordingFetchReceiptSettlementApplier) FillFetchSlots() {
	a.calls = append(a.calls, FetchReceiptFillFetchSlots)
}

func (a *recordingFetchReceiptSettlementApplier) MirrorLegacyLocked() {
	a.calls = append(a.calls, FetchReceiptMirrorLegacy)
}

func (a *recordingFetchReceiptSettlementApplier) DrainBuffered() {
	a.calls = append(a.calls, FetchReceiptDrainBuffered)
}

type recordingFetchReceiptDispatchApplier struct {
	sent int
}

func (a *recordingFetchReceiptDispatchApplier) SendOutboundRequests() {
	a.sent++
}

type recordingFetchedBlockBufferApplier struct {
	conflicts []FetchedBlockBufferPlan
	staged    []FetchedBlockBufferPlan
}

func (a *recordingFetchedBlockBufferApplier) DropConflictingFetchedBlock(plan FetchedBlockBufferPlan) {
	a.conflicts = append(a.conflicts, plan)
}

func (a *recordingFetchedBlockBufferApplier) StageFetchedBlock(plan FetchedBlockBufferPlan) {
	a.staged = append(a.staged, plan)
}
