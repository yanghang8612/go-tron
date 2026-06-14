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

func TestPlanFetchRefillRun(t *testing.T) {
	active := SessionProgress{Syncing: true, CurrentHead: 5, TargetHead: 9}
	got := PlanFetchRefillRun(FetchRefillRunInput{
		OutboundRequests: 2,
		Progress:         active,
	})
	want := FetchRefillRunPlan{
		Dispatch: FetchRefillDispatchPlan{
			SendOutboundRequests: true,
			Steps:                []FetchRefillDispatchStep{{Action: FetchRefillDispatchSendOutbound}},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("active refill run = %+v, want %+v", got, want)
	}

	got = PlanFetchRefillRun(FetchRefillRunInput{
		OutboundRequests: 2,
		Progress:         SessionProgress{Syncing: true, Paused: true},
	})
	if !reflect.DeepEqual(got, FetchRefillRunPlan{}) {
		t.Fatalf("paused refill run = %+v, want no action", got)
	}
}

func TestPlanEmptyDrainJoinProbe(t *testing.T) {
	complete := SessionProgress{Syncing: true, CurrentHead: 9, TargetHead: 9, Peers: []PeerProgress{{Done: true}}}
	if got := PlanEmptyDrainJoinProbe(EmptyDrainJoinProbeInput{Progress: complete}); got.CheckJoinAvailablePeers {
		t.Fatalf("complete join probe = %+v, want no peer-join check", got)
	}
	incomplete := SessionProgress{Syncing: true, CurrentHead: 1, TargetHead: 2}
	if got := PlanEmptyDrainJoinProbe(EmptyDrainJoinProbeInput{Progress: incomplete}); !got.CheckJoinAvailablePeers {
		t.Fatalf("incomplete join probe = %+v, want peer-join check", got)
	}
	for name, progress := range map[string]SessionProgress{
		"not syncing": {},
		"paused":      {Syncing: true, Paused: true, CurrentHead: 1, TargetHead: 2},
	} {
		if got := PlanEmptyDrainJoinProbe(EmptyDrainJoinProbeInput{Progress: progress}); got.CheckJoinAvailablePeers {
			t.Fatalf("%s join probe = %+v, want no peer-join check", name, got)
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

func TestPlanEmptyDrainRun(t *testing.T) {
	complete := SessionProgress{Syncing: true, CurrentHead: 9, TargetHead: 9, Peers: []PeerProgress{{Done: true}}}
	gate := &recordingEmptyDrainJoinGate{allowed: true}
	got := PlanEmptyDrainRun(EmptyDrainRunInput{
		OutboundRequests: 2,
		Progress:         complete,
	}, gate)
	want := EmptyDrainRunPlan{
		JoinProbe: EmptyDrainJoinProbePlan{},
		Refill: EmptyDrainRefillPlan{
			Idle: IdleDrainPlan{
				Finish: true,
				Steps:  []IdleDrainStep{{Action: IdleDrainFinish}},
			},
			Dispatch: FetchRefillDispatchPlan{
				SendOutboundRequests: true,
				Steps:                []FetchRefillDispatchStep{{Action: FetchRefillDispatchSendOutbound}},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("complete empty-drain run = %+v, want %+v", got, want)
	}
	if gate.calls != 0 {
		t.Fatalf("complete empty-drain called join gate %d times, want 0", gate.calls)
	}

	incomplete := SessionProgress{Syncing: true, CurrentHead: 1, TargetHead: 3}
	gate = &recordingEmptyDrainJoinGate{allowed: true}
	got = PlanEmptyDrainRun(EmptyDrainRunInput{Progress: incomplete}, gate)
	want = EmptyDrainRunPlan{
		JoinProbe:                 EmptyDrainJoinProbePlan{CheckJoinAvailablePeers: true},
		JoinAvailablePeersAllowed: true,
		Refill: EmptyDrainRefillPlan{
			Idle: IdleDrainPlan{
				JoinAvailablePeers: true,
				Steps:              []IdleDrainStep{{Action: IdleDrainJoinAvailablePeers}},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("joinable empty-drain run = %+v, want %+v", got, want)
	}
	if gate.calls != 1 || !reflect.DeepEqual(gate.progress, incomplete) {
		t.Fatalf("join gate calls/progress = %d/%+v, want one call with incomplete progress", gate.calls, gate.progress)
	}

	gate = &recordingEmptyDrainJoinGate{allowed: false}
	got = PlanEmptyDrainRun(EmptyDrainRunInput{
		OutboundRequests: 1,
		Progress:         incomplete,
	}, gate)
	want = EmptyDrainRunPlan{
		JoinProbe: EmptyDrainJoinProbePlan{CheckJoinAvailablePeers: true},
		Refill: EmptyDrainRefillPlan{
			Dispatch: FetchRefillDispatchPlan{
				SendOutboundRequests: true,
				Steps:                []FetchRefillDispatchStep{{Action: FetchRefillDispatchSendOutbound}},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("non-joinable empty-drain run = %+v, want %+v", got, want)
	}

	gate = &recordingEmptyDrainJoinGate{allowed: true}
	got = PlanEmptyDrainRun(EmptyDrainRunInput{Progress: SessionProgress{Syncing: true, Paused: true}}, gate)
	if !reflect.DeepEqual(got, EmptyDrainRunPlan{}) {
		t.Fatalf("paused empty-drain run = %+v, want no action", got)
	}
	if gate.calls != 0 {
		t.Fatalf("paused empty-drain called join gate %d times, want 0", gate.calls)
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

func TestPlanPostInventoryRun(t *testing.T) {
	active := SessionProgress{Syncing: true, CurrentHead: 5, TargetHead: 9}
	got := PlanPostInventoryRun(PostInventoryRunInput{
		OutboundRequests: 2,
		Progress:         active,
	})
	want := PostInventoryRunPlan{
		Settlement: PostInventorySettlementPlan{
			Mirror:      true,
			LockedSteps: []PostInventorySettlementStep{{Action: PostInventoryMirror}},
		},
		Dispatch: FetchRefillDispatchPlan{
			SendOutboundRequests: true,
			Steps:                []FetchRefillDispatchStep{{Action: FetchRefillDispatchSendOutbound}},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("active outbound run = %+v, want %+v", got, want)
	}

	stalled := SessionProgress{Syncing: true, RetryListLen: 1, Peers: []PeerProgress{{Done: true}}}
	got = PlanPostInventoryRun(PostInventoryRunInput{Progress: stalled})
	want = PostInventoryRunPlan{
		Settlement: PostInventorySettlementPlan{
			Reset:              true,
			TryFindPeer:        true,
			LockedSteps:        []PostInventorySettlementStep{{Action: PostInventoryReset}},
			AfterDispatchSteps: []PostInventorySettlementStep{{Action: PostInventoryTryFindPeer}},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("stalled retry run = %+v, want %+v", got, want)
	}

	complete := SessionProgress{Syncing: true, CurrentHead: 9, TargetHead: 9, Peers: []PeerProgress{{Done: true}}}
	got = PlanPostInventoryRun(PostInventoryRunInput{
		OutboundRequests: 1,
		Progress:         complete,
	})
	want = PostInventoryRunPlan{
		Settlement: PostInventorySettlementPlan{
			Mirror:             true,
			Finish:             true,
			LockedSteps:        []PostInventorySettlementStep{{Action: PostInventoryMirror}},
			AfterDispatchSteps: []PostInventorySettlementStep{{Action: PostInventoryFinish}},
		},
		Dispatch: FetchRefillDispatchPlan{
			SendOutboundRequests: true,
			Steps:                []FetchRefillDispatchStep{{Action: FetchRefillDispatchSendOutbound}},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("complete outbound run = %+v, want %+v", got, want)
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

type recordingEmptyDrainJoinGate struct {
	allowed  bool
	calls    int
	progress SessionProgress
}

func (g *recordingEmptyDrainJoinGate) CheckJoinAvailablePeers(progress SessionProgress) bool {
	g.calls++
	g.progress = progress
	return g.allowed
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
