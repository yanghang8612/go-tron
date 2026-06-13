package downloader

// PeerFailoverInput is the side-effect-free state after a peer has been
// removed from the active downloader session.
type PeerFailoverInput struct {
	RemainingPeers   int
	OutboundRequests int
	StalledRetries   bool
}

// PeerFailoverPlan describes how the service should settle a failed-peer
// transition after any remaining peers had a chance to fill fetch slots.
type PeerFailoverPlan struct {
	Reset       bool
	Mirror      bool
	TryFindPeer bool
}

// PlanPeerFailover decides whether a failed-peer transition should reset the
// current sync session, mirror surviving peer state, and/or ask the handler for
// a fresh sync peer.
func PlanPeerFailover(in PeerFailoverInput) PeerFailoverPlan {
	if in.RemainingPeers == 0 {
		return PeerFailoverPlan{Reset: true, TryFindPeer: true}
	}
	if in.OutboundRequests == 0 && in.StalledRetries {
		return PeerFailoverPlan{Reset: true, TryFindPeer: true}
	}
	return PeerFailoverPlan{Mirror: true}
}
