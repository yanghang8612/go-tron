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
	Reset                bool
	Mirror               bool
	SendOutboundRequests bool
	TryFindPeer          bool
	LockedSteps          []PeerFailoverStep
	DispatchSteps        []PeerFailoverStep
	AfterDispatchSteps   []PeerFailoverStep
}

// PeerFailoverStepAction names one operation for settling a failed-peer
// transition.
type PeerFailoverStepAction uint8

const (
	PeerFailoverReset PeerFailoverStepAction = iota
	PeerFailoverMirror
	PeerFailoverSendOutbound
	PeerFailoverTryFindPeer
)

// PeerFailoverStep is one downloader-owned failed-peer settlement operation.
// Locked steps run while SyncService still holds its state lock; after-dispatch
// steps run after logging and any outbound request sends.
type PeerFailoverStep struct {
	Action PeerFailoverStepAction
}

// PeerFailoverPlanApplier performs the runtime operations named by a failed
// peer settlement plan.
type PeerFailoverPlanApplier interface {
	ResetSyncUnderLock()
	MirrorLegacyUnderLock()
	SendOutboundRequests()
	TryFindSyncPeer()
}

// PlanPeerFailover decides whether a failed-peer transition should reset the
// current sync session, mirror surviving peer state, and/or ask the handler for
// a fresh sync peer.
func PlanPeerFailover(in PeerFailoverInput) PeerFailoverPlan {
	if in.RemainingPeers == 0 {
		return PeerFailoverPlan{Reset: true, TryFindPeer: true}.withSteps()
	}
	if in.OutboundRequests == 0 && in.StalledRetries {
		return PeerFailoverPlan{Reset: true, TryFindPeer: true}.withSteps()
	}
	return PeerFailoverPlan{Mirror: true, SendOutboundRequests: in.OutboundRequests > 0}.withSteps()
}

func (p PeerFailoverPlan) withSteps() PeerFailoverPlan {
	if p.Reset {
		p.LockedSteps = []PeerFailoverStep{{Action: PeerFailoverReset}}
	} else if p.Mirror {
		p.LockedSteps = []PeerFailoverStep{{Action: PeerFailoverMirror}}
	}
	if p.SendOutboundRequests {
		p.DispatchSteps = []PeerFailoverStep{{Action: PeerFailoverSendOutbound}}
	}
	if p.TryFindPeer {
		p.AfterDispatchSteps = []PeerFailoverStep{{Action: PeerFailoverTryFindPeer}}
	}
	return p
}

// ApplyPeerFailoverLockedPlan executes the lock-held settlement steps for a
// failed-peer transition.
func ApplyPeerFailoverLockedPlan(plan PeerFailoverPlan, applier PeerFailoverPlanApplier) {
	if applier == nil {
		return
	}
	if len(plan.LockedSteps) == 0 {
		plan = plan.withSteps()
	}
	for _, step := range plan.LockedSteps {
		switch step.Action {
		case PeerFailoverReset:
			applier.ResetSyncUnderLock()
		case PeerFailoverMirror:
			applier.MirrorLegacyUnderLock()
		}
	}
}

// ApplyPeerFailoverDispatchPlan executes post-log network dispatch steps for a
// failed-peer transition.
func ApplyPeerFailoverDispatchPlan(plan PeerFailoverPlan, applier PeerFailoverPlanApplier) {
	if applier == nil {
		return
	}
	if len(plan.DispatchSteps) == 0 {
		plan = plan.withSteps()
	}
	for _, step := range plan.DispatchSteps {
		switch step.Action {
		case PeerFailoverSendOutbound:
			applier.SendOutboundRequests()
		}
	}
}

// ApplyPeerFailoverAfterDispatchPlan executes post-dispatch settlement steps
// for a failed-peer transition.
func ApplyPeerFailoverAfterDispatchPlan(plan PeerFailoverPlan, applier PeerFailoverPlanApplier) {
	if applier == nil {
		return
	}
	if len(plan.AfterDispatchSteps) == 0 {
		plan = plan.withSteps()
	}
	for _, step := range plan.AfterDispatchSteps {
		switch step.Action {
		case PeerFailoverTryFindPeer:
			applier.TryFindSyncPeer()
		}
	}
}
