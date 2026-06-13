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
	ApplyPeerFailoverLockedPlan(plan, applier)
	ApplyPeerFailoverDispatchPlan(plan, applier)
	ApplyPeerFailoverAfterDispatchPlan(plan, applier)

	want := []PeerFailoverStepAction{PeerFailoverReset, PeerFailoverMirror, PeerFailoverSendOutbound, PeerFailoverTryFindPeer}
	if !reflect.DeepEqual(applier.calls, want) {
		t.Fatalf("failover calls = %+v, want %+v", applier.calls, want)
	}

	applier.calls = nil
	ApplyPeerFailoverLockedPlan(PeerFailoverPlan{Mirror: true}, applier)
	ApplyPeerFailoverDispatchPlan(PeerFailoverPlan{SendOutboundRequests: true}, applier)
	ApplyPeerFailoverAfterDispatchPlan(PeerFailoverPlan{TryFindPeer: true}, applier)
	want = []PeerFailoverStepAction{PeerFailoverMirror, PeerFailoverSendOutbound, PeerFailoverTryFindPeer}
	if !reflect.DeepEqual(applier.calls, want) {
		t.Fatalf("fallback failover calls = %+v, want %+v", applier.calls, want)
	}
	ApplyPeerFailoverLockedPlan(PeerFailoverPlan{Reset: true}, nil)
	ApplyPeerFailoverDispatchPlan(PeerFailoverPlan{SendOutboundRequests: true}, nil)
	ApplyPeerFailoverAfterDispatchPlan(PeerFailoverPlan{TryFindPeer: true}, nil)
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
