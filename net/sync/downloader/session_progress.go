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
	Progress                  SessionProgress
	JoinAvailablePeersAllowed bool
}

// FetchRefillDispatchInput is the lock-free state needed after refilling peer
// fetch slots outside a receipt-settlement path.
type FetchRefillDispatchInput struct {
	OutboundRequests int
	Syncing          bool
	Paused           bool
}

// IdleDrainStepAction names one session-level action after a local drain found
// no importable buffered bodies and existing peers were refilled.
type IdleDrainStepAction uint8

const (
	IdleDrainFinish IdleDrainStepAction = iota
	IdleDrainJoinAvailablePeers
)

// FetchRefillDispatchStepAction names one network dispatch action after peer
// fetch slots were refilled.
type FetchRefillDispatchStepAction uint8

const (
	FetchRefillDispatchSendOutbound FetchRefillDispatchStepAction = iota
)

// IdleDrainStep is one downloader-owned empty-drain settlement action.
type IdleDrainStep struct {
	Action IdleDrainStepAction
}

// FetchRefillDispatchStep is one downloader-owned dispatch operation after a
// fetch-slot refill.
type FetchRefillDispatchStep struct {
	Action FetchRefillDispatchStepAction
}

// IdleDrainPlan describes the session-level action after a local drain found no
// buffered batch and fetch slots have been refilled.
type IdleDrainPlan struct {
	Finish             bool
	JoinAvailablePeers bool
	Steps              []IdleDrainStep
}

// FetchRefillDispatchPlan describes whether refilled outbound requests should
// be sent.
type FetchRefillDispatchPlan struct {
	SendOutboundRequests bool
	Steps                []FetchRefillDispatchStep
}

// IdleDrainPlanApplier performs the session-level runtime actions named by an
// empty-drain plan.
type IdleDrainPlanApplier interface {
	FinishSync()
	JoinAvailablePeers()
}

// FetchRefillDispatchPlanApplier performs network sends named by a refill
// dispatch plan.
type FetchRefillDispatchPlanApplier interface {
	SendOutboundRequests()
}

// PostInventorySettlementInput is the lock-free state needed after an
// inventory message refilled fetch slots.
type PostInventorySettlementInput struct {
	OutboundRequests int
	Progress         SessionProgress
}

// PostInventorySettlementStepAction names one session action after accepting a
// peer inventory response and refilling local fetch slots.
type PostInventorySettlementStepAction uint8

const (
	PostInventoryReset PostInventorySettlementStepAction = iota
	PostInventoryMirror
	PostInventoryTryFindPeer
	PostInventoryFinish
)

// PostInventorySettlementStep is one downloader-owned post-inventory
// settlement action. Locked steps must run while SyncService still holds its
// state lock; after-dispatch steps run after stage progress and network sends.
type PostInventorySettlementStep struct {
	Action PostInventorySettlementStepAction
}

// PostInventorySettlementPlan describes the session-level action after an
// inventory response has been queued and fetch slots have been refilled.
type PostInventorySettlementPlan struct {
	Reset              bool
	Mirror             bool
	Finish             bool
	TryFindPeer        bool
	LockedSteps        []PostInventorySettlementStep
	AfterDispatchSteps []PostInventorySettlementStep
}

// PostInventorySettlementPlanApplier performs the runtime actions named by a
// post-inventory settlement plan.
type PostInventorySettlementPlanApplier interface {
	ResetSyncUnderLock()
	MirrorLegacyUnderLock()
	TryFindSyncPeer()
	FinishSync()
}

// PlanIdleDrainAfterRefill decides how the sync loop should settle an empty
// local drain after existing peers were given a chance to fetch more bodies.
func PlanIdleDrainAfterRefill(in IdleDrainAfterRefillInput) IdleDrainPlan {
	if in.Progress.ShouldFinish() {
		return IdleDrainPlan{Finish: true}.withSteps()
	}
	if in.JoinAvailablePeersAllowed {
		return IdleDrainPlan{JoinAvailablePeers: true}.withSteps()
	}
	return IdleDrainPlan{}
}

// PlanFetchRefillDispatch decides whether refilled outbound requests should be
// sent after a timer/manual refill. Receipt settlement has its own dispatch
// plan because it may run after an off-lock drain.
func PlanFetchRefillDispatch(in FetchRefillDispatchInput) FetchRefillDispatchPlan {
	return FetchRefillDispatchPlan{
		SendOutboundRequests: in.OutboundRequests > 0 && in.Syncing && !in.Paused,
	}.withSteps()
}

func (p IdleDrainPlan) withSteps() IdleDrainPlan {
	switch {
	case p.Finish:
		p.Steps = []IdleDrainStep{{Action: IdleDrainFinish}}
	case p.JoinAvailablePeers:
		p.Steps = []IdleDrainStep{{Action: IdleDrainJoinAvailablePeers}}
	}
	return p
}

func (p FetchRefillDispatchPlan) withSteps() FetchRefillDispatchPlan {
	if p.SendOutboundRequests {
		p.Steps = []FetchRefillDispatchStep{{Action: FetchRefillDispatchSendOutbound}}
	}
	return p
}

// ApplyIdleDrainAfterRefillPlan executes the downloader-owned empty-drain
// settlement schedule.
func ApplyIdleDrainAfterRefillPlan(plan IdleDrainPlan, applier IdleDrainPlanApplier) {
	if applier == nil {
		return
	}
	if len(plan.Steps) == 0 {
		plan = plan.withSteps()
	}
	for _, step := range plan.Steps {
		switch step.Action {
		case IdleDrainFinish:
			applier.FinishSync()
		case IdleDrainJoinAvailablePeers:
			applier.JoinAvailablePeers()
		}
	}
}

// ApplyFetchRefillDispatchPlan executes downloader-owned refill dispatch
// operations.
func ApplyFetchRefillDispatchPlan(plan FetchRefillDispatchPlan, applier FetchRefillDispatchPlanApplier) {
	if applier == nil {
		return
	}
	if len(plan.Steps) == 0 {
		plan = plan.withSteps()
	}
	for _, step := range plan.Steps {
		switch step.Action {
		case FetchRefillDispatchSendOutbound:
			applier.SendOutboundRequests()
		}
	}
}

// PlanPostInventorySettlement decides how the service should settle a sync
// session after accepting an inventory response and refilling fetch slots.
func PlanPostInventorySettlement(in PostInventorySettlementInput) PostInventorySettlementPlan {
	if in.OutboundRequests == 0 && in.Progress.ShouldRestartForStalledRetries() {
		return PostInventorySettlementPlan{Reset: true, TryFindPeer: true}.withSteps()
	}
	if in.Progress.ShouldFinish() {
		return PostInventorySettlementPlan{Mirror: true, Finish: true}.withSteps()
	}
	return PostInventorySettlementPlan{Mirror: true}.withSteps()
}

func (p PostInventorySettlementPlan) withSteps() PostInventorySettlementPlan {
	if p.Reset {
		p.LockedSteps = []PostInventorySettlementStep{{Action: PostInventoryReset}}
	} else if p.Mirror {
		p.LockedSteps = []PostInventorySettlementStep{{Action: PostInventoryMirror}}
	}
	if p.TryFindPeer {
		p.AfterDispatchSteps = []PostInventorySettlementStep{{Action: PostInventoryTryFindPeer}}
	} else if p.Finish {
		p.AfterDispatchSteps = []PostInventorySettlementStep{{Action: PostInventoryFinish}}
	}
	return p
}

// ApplyPostInventorySettlementLockedPlan executes the lock-held settlement
// steps for a post-inventory plan.
func ApplyPostInventorySettlementLockedPlan(plan PostInventorySettlementPlan, applier PostInventorySettlementPlanApplier) {
	if applier == nil {
		return
	}
	if len(plan.LockedSteps) == 0 {
		plan = plan.withSteps()
	}
	for _, step := range plan.LockedSteps {
		switch step.Action {
		case PostInventoryReset:
			applier.ResetSyncUnderLock()
		case PostInventoryMirror:
			applier.MirrorLegacyUnderLock()
		}
	}
}

// ApplyPostInventorySettlementAfterDispatchPlan executes the post-dispatch
// settlement steps for a post-inventory plan.
func ApplyPostInventorySettlementAfterDispatchPlan(plan PostInventorySettlementPlan, applier PostInventorySettlementPlanApplier) {
	if applier == nil {
		return
	}
	if len(plan.AfterDispatchSteps) == 0 {
		plan = plan.withSteps()
	}
	for _, step := range plan.AfterDispatchSteps {
		switch step.Action {
		case PostInventoryTryFindPeer:
			applier.TryFindSyncPeer()
		case PostInventoryFinish:
			applier.FinishSync()
		}
	}
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
