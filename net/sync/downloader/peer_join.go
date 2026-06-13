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
