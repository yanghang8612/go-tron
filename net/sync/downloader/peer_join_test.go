package downloader

import (
	"testing"
	"time"
)

func TestPeerJoinCapacity(t *testing.T) {
	tests := []struct {
		name    string
		current int
		max     int
		want    int
	}{
		{name: "needs peers", current: 1, max: 4, want: 3},
		{name: "at capacity", current: 4, max: 4},
		{name: "over capacity clamps", current: 5, max: 4},
	}
	for _, tt := range tests {
		if got := PeerJoinCapacity(tt.current, tt.max); got != tt.want {
			t.Fatalf("%s: capacity = %d, want %d", tt.name, got, tt.want)
		}
	}
}

func TestPlanPeerJoinAttempt(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	base := PeerJoinAttemptInput{
		HandlerAvailable: true,
		Syncing:          true,
		CurrentPeers:     1,
		MaxPeers:         4,
		LastAttempt:      now.Add(-2 * time.Second),
		Now:              now,
		MinInterval:      time.Second,
	}

	got := PlanPeerJoinAttempt(base)
	if !got.Allowed || got.Need != 3 {
		t.Fatalf("plan = %+v, want allowed with need 3", got)
	}

	for name, mutate := range map[string]func(*PeerJoinAttemptInput){
		"no-handler":  func(in *PeerJoinAttemptInput) { in.HandlerAvailable = false },
		"not-syncing": func(in *PeerJoinAttemptInput) { in.Syncing = false },
		"paused":      func(in *PeerJoinAttemptInput) { in.Paused = true },
		"at-capacity": func(in *PeerJoinAttemptInput) { in.CurrentPeers = in.MaxPeers },
		"throttled": func(in *PeerJoinAttemptInput) {
			in.LastAttempt = in.Now.Add(-in.MinInterval / 2)
		},
	} {
		in := base
		mutate(&in)
		if got := PlanPeerJoinAttempt(in); got.Allowed || got.Need != 0 {
			t.Fatalf("%s: plan = %+v, want disallowed", name, got)
		}
	}
}

func TestPlanPeerJoinAttemptAllowsFirstAttempt(t *testing.T) {
	got := PlanPeerJoinAttempt(PeerJoinAttemptInput{
		HandlerAvailable: true,
		Syncing:          true,
		MaxPeers:         2,
		Now:              time.Unix(1_700_000_000, 0),
		MinInterval:      time.Second,
	})
	if !got.Allowed || got.Need != 2 {
		t.Fatalf("plan = %+v, want first attempt allowed with need 2", got)
	}
}
