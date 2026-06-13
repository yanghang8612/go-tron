package downloader

import (
	"reflect"
	"testing"
)

func TestSessionProgressEstimatedRemainingUsesTargetHeadWhenAhead(t *testing.T) {
	progress := SessionProgress{
		CurrentHead:    10,
		TargetHead:     15,
		RetryListLen:   3,
		BlockBufferLen: 4,
		Peers: []PeerProgress{{
			FetchListLen: 2,
			Inflight:     1,
			RemainNum:    20,
		}},
	}

	if got := progress.EstimatedRemaining(); got != 5 {
		t.Fatalf("EstimatedRemaining = %d, want target-head diff 5", got)
	}
}

func TestSessionProgressEstimatedRemainingFallsBackToQueues(t *testing.T) {
	progress := SessionProgress{
		CurrentHead:    15,
		TargetHead:     10,
		RetryListLen:   3,
		BlockBufferLen: 4,
		Peers: []PeerProgress{
			{FetchListLen: 2, Inflight: 1, RemainNum: 20},
			{FetchListLen: 5, Inflight: 6, RemainNum: -9},
		},
	}

	if got := progress.EstimatedRemaining(); got != 41 {
		t.Fatalf("EstimatedRemaining = %d, want 41", got)
	}
}

func TestPlanIdleDrainAfterRefill(t *testing.T) {
	complete := SessionProgress{Syncing: true, CurrentHead: 9, TargetHead: 9, Peers: []PeerProgress{{Done: true}}}
	finish := IdleDrainPlan{
		Finish: true,
		Steps:  []IdleDrainStep{{Action: IdleDrainFinish}},
	}
	if got := PlanIdleDrainAfterRefill(IdleDrainAfterRefillInput{Progress: complete, JoinAvailablePeersAllowed: true}); !reflect.DeepEqual(got, finish) {
		t.Fatalf("complete idle plan = %+v, want %+v", got, finish)
	}
	join := IdleDrainPlan{
		JoinAvailablePeers: true,
		Steps:              []IdleDrainStep{{Action: IdleDrainJoinAvailablePeers}},
	}
	if got := PlanIdleDrainAfterRefill(IdleDrainAfterRefillInput{JoinAvailablePeersAllowed: true}); !reflect.DeepEqual(got, join) {
		t.Fatalf("joinable idle plan = %+v, want %+v", got, join)
	}
	if got := PlanIdleDrainAfterRefill(IdleDrainAfterRefillInput{}); !reflect.DeepEqual(got, IdleDrainPlan{}) {
		t.Fatalf("incomplete idle plan = %+v, want no action", got)
	}
}

func TestPlanFetchRefillDispatch(t *testing.T) {
	tests := map[string]struct {
		input FetchRefillDispatchInput
		want  FetchRefillDispatchPlan
	}{
		"active outbound": {
			input: FetchRefillDispatchInput{OutboundRequests: 1, Progress: SessionProgress{Syncing: true}},
			want: FetchRefillDispatchPlan{
				SendOutboundRequests: true,
				Steps:                []FetchRefillDispatchStep{{Action: FetchRefillDispatchSendOutbound}},
			},
		},
		"no outbound": {
			input: FetchRefillDispatchInput{Progress: SessionProgress{Syncing: true}},
			want:  FetchRefillDispatchPlan{},
		},
		"not syncing": {
			input: FetchRefillDispatchInput{OutboundRequests: 1},
			want:  FetchRefillDispatchPlan{},
		},
		"paused": {
			input: FetchRefillDispatchInput{OutboundRequests: 1, Progress: SessionProgress{Syncing: true, Paused: true}},
			want:  FetchRefillDispatchPlan{},
		},
	}
	for name, test := range tests {
		if got := PlanFetchRefillDispatch(test.input); !reflect.DeepEqual(got, test.want) {
			t.Fatalf("%s plan = %+v, want %+v", name, got, test.want)
		}
	}
}

func TestPlanEmptyDrainRefill(t *testing.T) {
	complete := SessionProgress{Syncing: true, CurrentHead: 9, TargetHead: 9, Peers: []PeerProgress{{Done: true}}}
	got := PlanEmptyDrainRefill(EmptyDrainRefillInput{
		OutboundRequests:          2,
		Progress:                  complete,
		JoinAvailablePeersAllowed: true,
	})
	want := EmptyDrainRefillPlan{
		Idle: IdleDrainPlan{
			Finish: true,
			Steps:  []IdleDrainStep{{Action: IdleDrainFinish}},
		},
		Dispatch: FetchRefillDispatchPlan{
			SendOutboundRequests: true,
			Steps:                []FetchRefillDispatchStep{{Action: FetchRefillDispatchSendOutbound}},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("complete empty-drain refill plan = %+v, want %+v", got, want)
	}

	got = PlanEmptyDrainRefill(EmptyDrainRefillInput{
		Progress:                  SessionProgress{Syncing: true, CurrentHead: 1, TargetHead: 2},
		JoinAvailablePeersAllowed: true,
	})
	want = EmptyDrainRefillPlan{
		Idle: IdleDrainPlan{
			JoinAvailablePeers: true,
			Steps:              []IdleDrainStep{{Action: IdleDrainJoinAvailablePeers}},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("joinable empty-drain refill plan = %+v, want %+v", got, want)
	}

	got = PlanEmptyDrainRefill(EmptyDrainRefillInput{
		OutboundRequests: 3,
		Progress:         SessionProgress{Syncing: true, Paused: true},
	})
	if !reflect.DeepEqual(got, EmptyDrainRefillPlan{}) {
		t.Fatalf("paused empty-drain refill plan = %+v, want no action", got)
	}
}

func TestApplyIdleDrainAfterRefillPlan(t *testing.T) {
	applier := new(recordingIdleDrainApplier)
	ApplyIdleDrainAfterRefillPlan(IdleDrainPlan{Steps: []IdleDrainStep{
		{Action: IdleDrainJoinAvailablePeers},
		{Action: IdleDrainStepAction(255)},
		{Action: IdleDrainFinish},
	}}, applier)

	want := []IdleDrainStepAction{IdleDrainJoinAvailablePeers, IdleDrainFinish}
	if !reflect.DeepEqual(applier.calls, want) {
		t.Fatalf("idle drain calls = %+v, want %+v", applier.calls, want)
	}
	applier.calls = nil
	ApplyIdleDrainAfterRefillPlan(IdleDrainPlan{Finish: true}, applier)
	if !reflect.DeepEqual(applier.calls, []IdleDrainStepAction{IdleDrainFinish}) {
		t.Fatalf("fallback idle drain calls = %+v, want finish", applier.calls)
	}
	ApplyIdleDrainAfterRefillPlan(IdleDrainPlan{Steps: []IdleDrainStep{{Action: IdleDrainFinish}}}, nil)
}

func TestApplyFetchRefillDispatchPlan(t *testing.T) {
	applier := new(recordingFetchRefillDispatchApplier)
	ApplyFetchRefillDispatchPlan(FetchRefillDispatchPlan{Steps: []FetchRefillDispatchStep{
		{Action: FetchRefillDispatchSendOutbound},
		{Action: FetchRefillDispatchStepAction(255)},
	}}, applier)
	if applier.sent != 1 {
		t.Fatalf("refill dispatch sends = %d, want 1", applier.sent)
	}

	applier.sent = 0
	ApplyFetchRefillDispatchPlan(FetchRefillDispatchPlan{SendOutboundRequests: true}, applier)
	if applier.sent != 1 {
		t.Fatalf("fallback refill dispatch sends = %d, want 1", applier.sent)
	}
	ApplyFetchRefillDispatchPlan(FetchRefillDispatchPlan{SendOutboundRequests: true}, nil)
}

func TestPlanPostInventorySettlement(t *testing.T) {
	tests := map[string]struct {
		input PostInventorySettlementInput
		want  PostInventorySettlementPlan
	}{
		"stalled retries with no outbound resets": {
			input: PostInventorySettlementInput{Progress: SessionProgress{
				Syncing:      true,
				RetryListLen: 1,
				Peers:        []PeerProgress{{Done: true}},
			}},
			want: PostInventorySettlementPlan{
				Reset:              true,
				TryFindPeer:        true,
				LockedSteps:        []PostInventorySettlementStep{{Action: PostInventoryReset}},
				AfterDispatchSteps: []PostInventorySettlementStep{{Action: PostInventoryTryFindPeer}},
			},
		},
		"outbound requests suppress stalled reset": {
			input: PostInventorySettlementInput{
				OutboundRequests: 1,
				Progress: SessionProgress{
					Syncing:      true,
					RetryListLen: 1,
					Peers:        []PeerProgress{{Done: true}},
				},
			},
			want: PostInventorySettlementPlan{
				Mirror:      true,
				LockedSteps: []PostInventorySettlementStep{{Action: PostInventoryMirror}},
			},
		},
		"complete session finishes": {
			input: PostInventorySettlementInput{Progress: SessionProgress{
				Syncing:     true,
				CurrentHead: 9,
				TargetHead:  9,
				Peers:       []PeerProgress{{Done: true}},
			}},
			want: PostInventorySettlementPlan{
				Mirror:             true,
				Finish:             true,
				LockedSteps:        []PostInventorySettlementStep{{Action: PostInventoryMirror}},
				AfterDispatchSteps: []PostInventorySettlementStep{{Action: PostInventoryFinish}},
			},
		},
		"incomplete session mirrors legacy queues": {
			input: PostInventorySettlementInput{},
			want: PostInventorySettlementPlan{
				Mirror:      true,
				LockedSteps: []PostInventorySettlementStep{{Action: PostInventoryMirror}},
			},
		},
	}

	for name, test := range tests {
		if got := PlanPostInventorySettlement(test.input); !reflect.DeepEqual(got, test.want) {
			t.Fatalf("%s plan = %+v, want %+v", name, got, test.want)
		}
	}
}

func TestApplyPostInventorySettlementPlan(t *testing.T) {
	applier := new(recordingPostInventorySettlementApplier)
	plan := PostInventorySettlementPlan{
		LockedSteps: []PostInventorySettlementStep{
			{Action: PostInventoryReset},
			{Action: PostInventorySettlementStepAction(255)},
			{Action: PostInventoryMirror},
		},
		AfterDispatchSteps: []PostInventorySettlementStep{
			{Action: PostInventoryTryFindPeer},
			{Action: PostInventorySettlementStepAction(255)},
			{Action: PostInventoryFinish},
		},
	}
	ApplyPostInventorySettlementLockedPlan(plan, applier)
	ApplyPostInventorySettlementAfterDispatchPlan(plan, applier)

	want := []PostInventorySettlementStepAction{
		PostInventoryReset,
		PostInventoryMirror,
		PostInventoryTryFindPeer,
		PostInventoryFinish,
	}
	if !reflect.DeepEqual(applier.calls, want) {
		t.Fatalf("post-inventory calls = %+v, want %+v", applier.calls, want)
	}

	applier.calls = nil
	ApplyPostInventorySettlementLockedPlan(PostInventorySettlementPlan{Mirror: true}, applier)
	ApplyPostInventorySettlementAfterDispatchPlan(PostInventorySettlementPlan{Finish: true}, applier)
	want = []PostInventorySettlementStepAction{PostInventoryMirror, PostInventoryFinish}
	if !reflect.DeepEqual(applier.calls, want) {
		t.Fatalf("fallback post-inventory calls = %+v, want %+v", applier.calls, want)
	}
	ApplyPostInventorySettlementLockedPlan(PostInventorySettlementPlan{Reset: true}, nil)
	ApplyPostInventorySettlementAfterDispatchPlan(PostInventorySettlementPlan{TryFindPeer: true}, nil)
}

func TestSessionProgressShouldFinish(t *testing.T) {
	done := SessionProgress{
		Syncing:     true,
		CurrentHead: 9,
		TargetHead:  9,
		Peers: []PeerProgress{
			{Done: true},
			{Done: true},
		},
	}
	if !done.ShouldFinish() {
		t.Fatal("drained session at target should finish")
	}

	for name, progress := range map[string]SessionProgress{
		"paused":         {Syncing: true, Paused: true, CurrentHead: 9, TargetHead: 9, Peers: []PeerProgress{{Done: true}}},
		"not-syncing":    {Syncing: false, CurrentHead: 9, TargetHead: 9, Peers: []PeerProgress{{Done: true}}},
		"retry":          {Syncing: true, CurrentHead: 9, TargetHead: 9, RetryListLen: 1, Peers: []PeerProgress{{Done: true}}},
		"buffer":         {Syncing: true, CurrentHead: 9, TargetHead: 9, BlockBufferLen: 1, Peers: []PeerProgress{{Done: true}}},
		"peer-fetch":     {Syncing: true, CurrentHead: 9, TargetHead: 9, Peers: []PeerProgress{{Done: true, FetchListLen: 1}}},
		"peer-inflight":  {Syncing: true, CurrentHead: 9, TargetHead: 9, Peers: []PeerProgress{{Done: true, Inflight: 1}}},
		"peer-requested": {Syncing: true, CurrentHead: 9, TargetHead: 9, Peers: []PeerProgress{{Done: true, ChainRequested: true}}},
		"peer-not-done":  {Syncing: true, CurrentHead: 9, TargetHead: 9, Peers: []PeerProgress{{Done: false}}},
		"below-target":   {Syncing: true, CurrentHead: 8, TargetHead: 9, Peers: []PeerProgress{{Done: true}}},
	} {
		if progress.ShouldFinish() {
			t.Fatalf("%s progress unexpectedly finished", name)
		}
	}
}

func TestSessionProgressShouldRestartForStalledRetries(t *testing.T) {
	progress := SessionProgress{
		Syncing:      true,
		RetryListLen: 2,
		Peers:        []PeerProgress{{Done: false}, {Done: true}},
	}
	if !progress.ShouldRestartForStalledRetries() {
		t.Fatal("idle peers with retries should restart")
	}

	for name, progress := range map[string]SessionProgress{
		"paused":         {Syncing: true, Paused: true, RetryListLen: 1},
		"not-syncing":    {Syncing: false, RetryListLen: 1},
		"no-retry":       {Syncing: true},
		"buffer":         {Syncing: true, RetryListLen: 1, BlockBufferLen: 1},
		"peer-fetch":     {Syncing: true, RetryListLen: 1, Peers: []PeerProgress{{FetchListLen: 1}}},
		"peer-inflight":  {Syncing: true, RetryListLen: 1, Peers: []PeerProgress{{Inflight: 1}}},
		"peer-requested": {Syncing: true, RetryListLen: 1, Peers: []PeerProgress{{ChainRequested: true}}},
	} {
		if progress.ShouldRestartForStalledRetries() {
			t.Fatalf("%s progress unexpectedly restarted", name)
		}
	}
}

type recordingIdleDrainApplier struct {
	calls []IdleDrainStepAction
}

func (a *recordingIdleDrainApplier) FinishSync() {
	a.calls = append(a.calls, IdleDrainFinish)
}

func (a *recordingIdleDrainApplier) JoinAvailablePeers() {
	a.calls = append(a.calls, IdleDrainJoinAvailablePeers)
}

type recordingFetchRefillDispatchApplier struct {
	sent int
}

func (a *recordingFetchRefillDispatchApplier) SendOutboundRequests() {
	a.sent++
}

type recordingPostInventorySettlementApplier struct {
	calls []PostInventorySettlementStepAction
}

func (a *recordingPostInventorySettlementApplier) ResetSyncUnderLock() {
	a.calls = append(a.calls, PostInventoryReset)
}

func (a *recordingPostInventorySettlementApplier) MirrorLegacyUnderLock() {
	a.calls = append(a.calls, PostInventoryMirror)
}

func (a *recordingPostInventorySettlementApplier) TryFindSyncPeer() {
	a.calls = append(a.calls, PostInventoryTryFindPeer)
}

func (a *recordingPostInventorySettlementApplier) FinishSync() {
	a.calls = append(a.calls, PostInventoryFinish)
}
