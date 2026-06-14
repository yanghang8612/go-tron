package downloader

import (
	"reflect"
	"testing"
)

func TestPlanPeerFailover(t *testing.T) {
	tests := []struct {
		name string
		in   PeerFailoverInput
		want PeerFailoverPlan
	}{
		{
			name: "no remaining peers resets and finds peer",
			in:   PeerFailoverInput{},
			want: PeerFailoverPlan{
				Reset:              true,
				TryFindPeer:        true,
				LockedSteps:        []PeerFailoverStep{{Action: PeerFailoverReset}},
				AfterDispatchSteps: []PeerFailoverStep{{Action: PeerFailoverTryFindPeer}},
			},
		},
		{
			name: "stalled retries reset and finds peer",
			in: PeerFailoverInput{Progress: SessionProgress{
				Syncing:      true,
				RetryListLen: 1,
				Peers:        []PeerProgress{{Done: true}, {Done: false}},
			}},
			want: PeerFailoverPlan{
				Reset:              true,
				TryFindPeer:        true,
				LockedSteps:        []PeerFailoverStep{{Action: PeerFailoverReset}},
				AfterDispatchSteps: []PeerFailoverStep{{Action: PeerFailoverTryFindPeer}},
			},
		},
		{
			name: "outbound requests continue session",
			in: PeerFailoverInput{
				OutboundRequests: 1,
				Progress: SessionProgress{
					Syncing:      true,
					RetryListLen: 1,
					Peers:        []PeerProgress{{Done: true}, {Done: false}},
				},
			},
			want: PeerFailoverPlan{
				Mirror:               true,
				SendOutboundRequests: true,
				LockedSteps:          []PeerFailoverStep{{Action: PeerFailoverMirror}},
				DispatchSteps:        []PeerFailoverStep{{Action: PeerFailoverSendOutbound}},
			},
		},
		{
			name: "idle peers without stalled retries mirror state",
			in:   PeerFailoverInput{Progress: SessionProgress{Syncing: true, Peers: []PeerProgress{{Done: true}, {Done: false}}}},
			want: PeerFailoverPlan{
				Mirror:      true,
				LockedSteps: []PeerFailoverStep{{Action: PeerFailoverMirror}},
			},
		},
	}
	for _, tt := range tests {
		if got := PlanPeerFailover(tt.in); !reflect.DeepEqual(got, tt.want) {
			t.Fatalf("%s: plan = %+v, want %+v", tt.name, got, tt.want)
		}
	}
}

func TestApplyPeerFailoverPlan(t *testing.T) {
	applier := new(recordingPeerFailoverApplier)
	plan := PeerFailoverPlan{
		LockedSteps: []PeerFailoverStep{
			{Action: PeerFailoverReset},
			{Action: PeerFailoverStepAction(255)},
			{Action: PeerFailoverMirror},
		},
		DispatchSteps: []PeerFailoverStep{
			{Action: PeerFailoverSendOutbound},
			{Action: PeerFailoverStepAction(255)},
		},
		AfterDispatchSteps: []PeerFailoverStep{
			{Action: PeerFailoverTryFindPeer},
			{Action: PeerFailoverStepAction(255)},
		},
	}
	lockedResult := ApplyPeerFailoverLockedPlan(plan, applier)
	dispatchResult := ApplyPeerFailoverDispatchPlan(plan, applier)
	afterResult := ApplyPeerFailoverAfterDispatchPlan(plan, applier)

	want := []PeerFailoverStepAction{PeerFailoverReset, PeerFailoverMirror, PeerFailoverSendOutbound, PeerFailoverTryFindPeer}
	if !reflect.DeepEqual(applier.calls, want) {
		t.Fatalf("failover calls = %+v, want %+v", applier.calls, want)
	}
	if !reflect.DeepEqual(lockedResult.AppliedSteps, []PeerFailoverStepAction{PeerFailoverReset, PeerFailoverMirror}) ||
		!reflect.DeepEqual(lockedResult.UnknownSteps, []PeerFailoverStepAction{PeerFailoverStepAction(255)}) {
		t.Fatalf("locked failover result = %+v, want reset/mirror applied and unknown [255]", lockedResult)
	}
	if !reflect.DeepEqual(dispatchResult.AppliedSteps, []PeerFailoverStepAction{PeerFailoverSendOutbound}) ||
		!reflect.DeepEqual(dispatchResult.UnknownSteps, []PeerFailoverStepAction{PeerFailoverStepAction(255)}) {
		t.Fatalf("dispatch failover result = %+v, want send applied and unknown [255]", dispatchResult)
	}
	if !reflect.DeepEqual(afterResult.AppliedSteps, []PeerFailoverStepAction{PeerFailoverTryFindPeer}) ||
		!reflect.DeepEqual(afterResult.UnknownSteps, []PeerFailoverStepAction{PeerFailoverStepAction(255)}) {
		t.Fatalf("after-dispatch failover result = %+v, want try-find applied and unknown [255]", afterResult)
	}

	applier.calls = nil
	lockedResult = ApplyPeerFailoverLockedPlan(PeerFailoverPlan{Mirror: true}, applier)
	dispatchResult = ApplyPeerFailoverDispatchPlan(PeerFailoverPlan{SendOutboundRequests: true}, applier)
	afterResult = ApplyPeerFailoverAfterDispatchPlan(PeerFailoverPlan{TryFindPeer: true}, applier)
	want = []PeerFailoverStepAction{PeerFailoverMirror, PeerFailoverSendOutbound, PeerFailoverTryFindPeer}
	if !reflect.DeepEqual(applier.calls, want) {
		t.Fatalf("fallback failover calls = %+v, want %+v", applier.calls, want)
	}
	if !reflect.DeepEqual(lockedResult.AppliedSteps, []PeerFailoverStepAction{PeerFailoverMirror}) ||
		len(lockedResult.UnknownSteps) != 0 {
		t.Fatalf("fallback locked failover result = %+v, want mirror applied", lockedResult)
	}
	if !reflect.DeepEqual(dispatchResult.AppliedSteps, []PeerFailoverStepAction{PeerFailoverSendOutbound}) ||
		len(dispatchResult.UnknownSteps) != 0 {
		t.Fatalf("fallback dispatch failover result = %+v, want send applied", dispatchResult)
	}
	if !reflect.DeepEqual(afterResult.AppliedSteps, []PeerFailoverStepAction{PeerFailoverTryFindPeer}) ||
		len(afterResult.UnknownSteps) != 0 {
		t.Fatalf("fallback after-dispatch failover result = %+v, want try-find applied", afterResult)
	}
	if nilResult := ApplyPeerFailoverLockedPlan(PeerFailoverPlan{Reset: true}, nil); len(nilResult.AppliedSteps) != 0 || len(nilResult.UnknownSteps) != 0 {
		t.Fatalf("nil locked failover result = %+v, want empty", nilResult)
	}
	if nilResult := ApplyPeerFailoverDispatchPlan(PeerFailoverPlan{SendOutboundRequests: true}, nil); len(nilResult.AppliedSteps) != 0 || len(nilResult.UnknownSteps) != 0 {
		t.Fatalf("nil dispatch failover result = %+v, want empty", nilResult)
	}
	if nilResult := ApplyPeerFailoverAfterDispatchPlan(PeerFailoverPlan{TryFindPeer: true}, nil); len(nilResult.AppliedSteps) != 0 || len(nilResult.UnknownSteps) != 0 {
		t.Fatalf("nil after-dispatch failover result = %+v, want empty", nilResult)
	}
}

type recordingPeerFailoverApplier struct {
	calls []PeerFailoverStepAction
}

func (a *recordingPeerFailoverApplier) ResetSyncUnderLock() {
	a.calls = append(a.calls, PeerFailoverReset)
}

func (a *recordingPeerFailoverApplier) MirrorLegacyUnderLock() {
	a.calls = append(a.calls, PeerFailoverMirror)
}

func (a *recordingPeerFailoverApplier) SendOutboundRequests() {
	a.calls = append(a.calls, PeerFailoverSendOutbound)
}

func (a *recordingPeerFailoverApplier) TryFindSyncPeer() {
	a.calls = append(a.calls, PeerFailoverTryFindPeer)
}
