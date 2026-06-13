package downloader

import "testing"

func TestPlanPeerFailover(t *testing.T) {
	tests := []struct {
		name string
		in   PeerFailoverInput
		want PeerFailoverPlan
	}{
		{
			name: "no remaining peers resets and finds peer",
			in:   PeerFailoverInput{},
			want: PeerFailoverPlan{Reset: true, TryFindPeer: true},
		},
		{
			name: "stalled retries reset and finds peer",
			in:   PeerFailoverInput{RemainingPeers: 2, StalledRetries: true},
			want: PeerFailoverPlan{Reset: true, TryFindPeer: true},
		},
		{
			name: "outbound requests continue session",
			in:   PeerFailoverInput{RemainingPeers: 2, OutboundRequests: 1, StalledRetries: true},
			want: PeerFailoverPlan{Mirror: true},
		},
		{
			name: "idle peers without stalled retries mirror state",
			in:   PeerFailoverInput{RemainingPeers: 2},
			want: PeerFailoverPlan{Mirror: true},
		},
	}
	for _, tt := range tests {
		if got := PlanPeerFailover(tt.in); got != tt.want {
			t.Fatalf("%s: plan = %+v, want %+v", tt.name, got, tt.want)
		}
	}
}
