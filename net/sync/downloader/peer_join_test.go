package downloader

import (
	"reflect"
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
		Progress: SessionProgress{
			Syncing: true,
			Peers:   []PeerProgress{{Done: false}},
		},
		MaxPeers:    4,
		LastAttempt: now.Add(-2 * time.Second),
		Now:         now,
		MinInterval: time.Second,
	}

	got := PlanPeerJoinAttempt(base)
	if !got.Allowed || got.Need != 3 {
		t.Fatalf("plan = %+v, want allowed with need 3", got)
	}

	for name, mutate := range map[string]func(*PeerJoinAttemptInput){
		"no-handler":  func(in *PeerJoinAttemptInput) { in.HandlerAvailable = false },
		"not-syncing": func(in *PeerJoinAttemptInput) { in.Progress.Syncing = false },
		"paused":      func(in *PeerJoinAttemptInput) { in.Progress.Paused = true },
		"at-capacity": func(in *PeerJoinAttemptInput) {
			in.Progress.Peers = []PeerProgress{{}, {}, {}, {}}
		},
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
		Progress:         SessionProgress{Syncing: true},
		MaxPeers:         2,
		Now:              time.Unix(1_700_000_000, 0),
		MinInterval:      time.Second,
	})
	if !got.Allowed || got.Need != 2 {
		t.Fatalf("plan = %+v, want first attempt allowed with need 2", got)
	}
}

func TestPlanPeerJoinSelectionUsesPrimaryThenFallback(t *testing.T) {
	got := PlanPeerJoinSelection(PeerJoinSelectionInput{
		Need:     4,
		Existing: []string{"existing"},
		Primary:  []string{"primary-a", "existing", "primary-b", "primary-a"},
		Fallback: []PeerJoinFallbackCandidate{
			{ID: "fallback-blocked"},
			{ID: "primary-b", CanServe: true},
			{ID: "fallback-a", CanServe: true},
			{ID: "fallback-b", CanServe: true},
		},
	})
	want := []string{"primary-a", "primary-b", "fallback-a", "fallback-b"}
	if !reflect.DeepEqual(got.Selected, want) {
		t.Fatalf("selected = %+v, want %+v", got.Selected, want)
	}
}

func TestPlanPeerJoinSelectionClampsToNeed(t *testing.T) {
	got := PlanPeerJoinSelection(PeerJoinSelectionInput{
		Need:    2,
		Primary: []string{"primary-a", "primary-b", "primary-c"},
		Fallback: []PeerJoinFallbackCandidate{
			{ID: "fallback-a", CanServe: true},
		},
	})
	want := []string{"primary-a", "primary-b"}
	if !reflect.DeepEqual(got.Selected, want) {
		t.Fatalf("selected = %+v, want %+v", got.Selected, want)
	}
}

func TestPlanPeerJoinSelectionSkipsEmptyAndUnavailablePeers(t *testing.T) {
	got := PlanPeerJoinSelection(PeerJoinSelectionInput{
		Need:     3,
		Existing: []string{""},
		Primary:  []string{"", "primary"},
		Fallback: []PeerJoinFallbackCandidate{
			{ID: "", CanServe: true},
			{ID: "blocked"},
			{ID: "served", CanServe: true},
		},
	})
	want := []string{"primary", "served"}
	if !reflect.DeepEqual(got.Selected, want) {
		t.Fatalf("selected = %+v, want %+v", got.Selected, want)
	}

	if zero := PlanPeerJoinSelection(PeerJoinSelectionInput{Need: 0, Primary: []string{"primary"}}); len(zero.Selected) != 0 {
		t.Fatalf("zero-need selected = %+v, want none", zero.Selected)
	}
}
