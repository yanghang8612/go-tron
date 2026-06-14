package downloader

import "time"

// PeerJoinCapacity returns how many additional peers a downloader session can
// join before reaching its configured parallelism limit.
func PeerJoinCapacity(currentPeers, maxPeers int) int {
	need := maxPeers - currentPeers
	if need < 0 {
		return 0
	}
	return need
}

// PeerJoinAttemptInput is the side-effect-free state needed to decide whether
// a sync session should ask the handler for more peers.
type PeerJoinAttemptInput struct {
	HandlerAvailable bool
	Progress         SessionProgress
	MaxPeers         int
	LastAttempt      time.Time
	Now              time.Time
	MinInterval      time.Duration
}

// PeerJoinAttemptPlan describes whether a join attempt should run and how many
// additional peers it may request.
type PeerJoinAttemptPlan struct {
	Allowed bool
	Need    int
}

// PeerJoinFallbackCandidate is one handshaked fallback peer and whether it can
// serve the downloader's required block range.
type PeerJoinFallbackCandidate struct {
	ID       string
	CanServe bool
}

// PeerJoinSelectionInput is the side-effect-free peer set used to select the
// next peers to join. Primary candidates are already handler-ranked; fallback
// candidates preserve handler iteration order.
type PeerJoinSelectionInput struct {
	Need     int
	Existing []string
	Primary  []string
	Fallback []PeerJoinFallbackCandidate
}

// PeerJoinSelectionPlan is the ordered peer-id list SyncService should join.
type PeerJoinSelectionPlan struct {
	Selected []string
}

// PlanPeerJoinAttempt applies the downloader join throttle and parallel-peer
// budget without mutating service state.
func PlanPeerJoinAttempt(in PeerJoinAttemptInput) PeerJoinAttemptPlan {
	if !in.HandlerAvailable || !in.Progress.Syncing || in.Progress.Paused {
		return PeerJoinAttemptPlan{}
	}
	need := PeerJoinCapacity(len(in.Progress.Peers), in.MaxPeers)
	if need == 0 {
		return PeerJoinAttemptPlan{}
	}
	if !in.LastAttempt.IsZero() && in.Now.Sub(in.LastAttempt) < in.MinInterval {
		return PeerJoinAttemptPlan{}
	}
	return PeerJoinAttemptPlan{Allowed: true, Need: need}
}

// PlanPeerJoinSelection selects primary candidates first and fills remaining
// capacity from handshaked peers that can serve the required block range.
func PlanPeerJoinSelection(in PeerJoinSelectionInput) PeerJoinSelectionPlan {
	if in.Need <= 0 {
		return PeerJoinSelectionPlan{}
	}
	seen := make(map[string]struct{}, len(in.Existing)+len(in.Primary)+len(in.Fallback))
	for _, id := range in.Existing {
		if id != "" {
			seen[id] = struct{}{}
		}
	}
	plan := PeerJoinSelectionPlan{Selected: make([]string, 0, in.Need)}
	add := func(id string) bool {
		if id == "" {
			return false
		}
		if _, ok := seen[id]; ok {
			return false
		}
		seen[id] = struct{}{}
		plan.Selected = append(plan.Selected, id)
		return len(plan.Selected) >= in.Need
	}
	for _, id := range in.Primary {
		if add(id) {
			return plan
		}
	}
	for _, candidate := range in.Fallback {
		if !candidate.CanServe {
			continue
		}
		if add(candidate.ID) {
			return plan
		}
	}
	return plan
}
