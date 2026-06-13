package downloader

// PeerProgress is the per-peer queue state needed for sync-session progress
// decisions. It deliberately carries counts, not references to peer objects.
type PeerProgress struct {
	FetchListLen   int
	Inflight       int
	RemainNum      int64
	ChainRequested bool
	Done           bool
}

// SessionProgress is a lock-free snapshot of the downloader session state
// used to decide remaining work, completion, and stalled-retry restart.
type SessionProgress struct {
	Syncing        bool
	Paused         bool
	CurrentHead    uint64
	TargetHead     uint64
	RetryListLen   int
	BlockBufferLen int
	Peers          []PeerProgress
}

// IdleDrainAfterRefillInput is the lock-free state needed after a local drain
// found no buffered batch and fetch slots have been refilled.
type IdleDrainAfterRefillInput struct {
	Complete                  bool
	JoinAvailablePeersAllowed bool
}

// IdleDrainPlan describes the session-level action after a local drain found no
// buffered batch and fetch slots have been refilled.
type IdleDrainPlan struct {
	Finish             bool
	JoinAvailablePeers bool
}

// PostInventorySettlementInput is the lock-free state needed after an
// inventory message refilled fetch slots.
type PostInventorySettlementInput struct {
	OutboundRequests int
	StalledRetries   bool
	Complete         bool
}

// PostInventorySettlementPlan describes the session-level action after an
// inventory response has been queued and fetch slots have been refilled.
type PostInventorySettlementPlan struct {
	Reset       bool
	Mirror      bool
	Finish      bool
	TryFindPeer bool
}

// PlanIdleDrainAfterRefill decides how the sync loop should settle an empty
// local drain after existing peers were given a chance to fetch more bodies.
func PlanIdleDrainAfterRefill(in IdleDrainAfterRefillInput) IdleDrainPlan {
	if in.Complete {
		return IdleDrainPlan{Finish: true}
	}
	if in.JoinAvailablePeersAllowed {
		return IdleDrainPlan{JoinAvailablePeers: true}
	}
	return IdleDrainPlan{}
}

// PlanPostInventorySettlement decides how the service should settle a sync
// session after accepting an inventory response and refilling fetch slots.
func PlanPostInventorySettlement(in PostInventorySettlementInput) PostInventorySettlementPlan {
	if in.OutboundRequests == 0 && in.StalledRetries {
		return PostInventorySettlementPlan{Reset: true, TryFindPeer: true}
	}
	if in.Complete {
		return PostInventorySettlementPlan{Mirror: true, Finish: true}
	}
	return PostInventorySettlementPlan{Mirror: true}
}

// EstimatedRemaining reports the advisory remaining block count for status and
// import-summary logs.
func (s SessionProgress) EstimatedRemaining() int64 {
	if s.TargetHead > s.CurrentHead {
		return int64(s.TargetHead - s.CurrentHead)
	}
	remain := int64(s.RetryListLen + s.BlockBufferLen)
	for _, peer := range s.Peers {
		remain += int64(peer.FetchListLen + peer.Inflight)
		if peer.RemainNum > 0 {
			remain += peer.RemainNum
		}
	}
	return remain
}

// ShouldFinish reports whether every peer queue is drained and the target head
// has been reached.
func (s SessionProgress) ShouldFinish() bool {
	if !s.Syncing || s.Paused {
		return false
	}
	if s.RetryListLen != 0 || s.BlockBufferLen != 0 {
		return false
	}
	for _, peer := range s.Peers {
		if peer.ChainRequested || peer.Inflight != 0 || peer.FetchListLen != 0 {
			return false
		}
		if !peer.Done {
			return false
		}
	}
	return s.TargetHead == 0 || s.CurrentHead >= s.TargetHead
}

// ShouldRestartForStalledRetries reports whether no peer has useful in-flight
// work but retry candidates remain, so the service should restart the session.
func (s SessionProgress) ShouldRestartForStalledRetries() bool {
	if !s.Syncing || s.Paused || s.RetryListLen == 0 || s.BlockBufferLen != 0 {
		return false
	}
	for _, peer := range s.Peers {
		if peer.ChainRequested || peer.Inflight != 0 || peer.FetchListLen != 0 {
			return false
		}
	}
	return true
}
