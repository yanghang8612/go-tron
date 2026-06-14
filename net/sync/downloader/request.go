package downloader

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

// FetchRequestState is the in-flight bookkeeping derived from one outbound
// FETCH_INV_DATA batch. It does not own peer timers or network sends.
type FetchRequestState struct {
	Inflight        int
	Pending         map[tcommon.Hash]uint64
	PendingIDs      map[tcommon.Hash]types.BlockID
	RequestedHashes []tcommon.Hash
}

// NewFetchRequestState builds the peer-local pending maps and requested-hash
// marks for an outbound block batch.
func NewFetchRequestState(batch []types.BlockID) FetchRequestState {
	state := FetchRequestState{Inflight: len(batch)}
	if len(batch) == 0 {
		return state
	}
	state.Pending = make(map[tcommon.Hash]uint64, len(batch))
	state.PendingIDs = make(map[tcommon.Hash]types.BlockID, len(batch))
	state.RequestedHashes = make([]tcommon.Hash, 0, len(batch))
	for _, bid := range batch {
		state.Pending[bid.Hash] = bid.Num
		state.PendingIDs[bid.Hash] = bid
		state.RequestedHashes = append(state.RequestedHashes, bid.Hash)
	}
	return state
}

// FetchReceiptState is the peer-local in-flight state updated when a requested
// block body arrives.
type FetchReceiptState struct {
	Inflight   int
	Pending    map[tcommon.Hash]uint64
	PendingIDs map[tcommon.Hash]types.BlockID
}

// FetchReceiptResult describes the effect of acknowledging one received block.
type FetchReceiptResult struct {
	Accepted  bool
	Inflight  int
	BatchDone bool
}

// FetchReceiptRunInput is the side-effect-free state for settling one received
// block body against the peer's in-flight fetch request.
type FetchReceiptRunInput struct {
	Receipt FetchReceiptResult
}

// FetchReceiptSettlement is the downloader-owned state-machine decision after
// acknowledging one received FETCH_INV_DATA block.
type FetchReceiptSettlement struct {
	Accepted            bool
	Inflight            int
	BatchDone           bool
	DeleteRequestedHash bool
	AdvanceFetchSeq     bool
	StopFetchTimer      bool
	RearmFetchTimer     bool
	FillFetchSlots      bool
	DrainBuffered       bool
	LockedPreBuffer     []FetchReceiptSettlementStep
	LockedPostBuffer    []FetchReceiptSettlementStep
	AfterUnlock         []FetchReceiptSettlementStep
}

// FetchReceiptSettlementStepAction names one ordered local action after a
// requested block body has been acknowledged.
type FetchReceiptSettlementStepAction uint8

const (
	FetchReceiptDeleteRequestedHash FetchReceiptSettlementStepAction = iota
	FetchReceiptAdvanceFetchSeq
	FetchReceiptUpdateInflight
	FetchReceiptStopFetchTimer
	FetchReceiptRearmFetchTimer
	FetchReceiptFillFetchSlots
	FetchReceiptMirrorLegacy
	FetchReceiptDrainBuffered
)

// FetchReceiptSettlementStep is one downloader-owned fetch-receipt settlement
// action. Inflight is populated for FetchReceiptUpdateInflight.
type FetchReceiptSettlementStep struct {
	Action   FetchReceiptSettlementStepAction
	Inflight int
}

// FetchReceiptSettlementPlanApplier performs the runtime operations named by a
// fetch-receipt settlement plan. SyncService owns peer timers, request maps,
// fetch-slot output, and local drains; downloader owns the ordering.
type FetchReceiptSettlementPlanApplier interface {
	DeleteRequestedHash()
	AdvanceFetchSeq()
	UpdateInflight(inflight int)
	StopFetchTimer()
	RearmFetchTimer()
	FillFetchSlots() int
	MirrorLegacyLocked()
	DrainBuffered()
}

// FetchReceiptSettlementApplyResult records the receipt-settlement steps
// applied in one lock phase and any follow-up fetch slots produced there.
type FetchReceiptSettlementApplyResult struct {
	AppliedSteps     []FetchReceiptSettlementStepAction
	UnknownSteps     []FetchReceiptSettlementStepAction
	OutboundRequests int
	FilledFetchSlots bool
}

// FetchReceiptDispatchInput is the post-drain state needed before sending
// follow-up outbound fetch requests after a received block body.
type FetchReceiptDispatchInput struct {
	OutboundRequests int
	Progress         SessionProgress
}

// FetchReceiptRunAfterDrainInput is the state available after receipt
// settlement has filled any fetch slots and local staged bodies have drained.
type FetchReceiptRunAfterDrainInput struct {
	OutboundRequests int
	Progress         SessionProgress
}

// FetchReceiptDispatchPlan describes whether follow-up fetch requests should
// be sent after the local drain has had a chance to settle the session.
type FetchReceiptDispatchPlan struct {
	SendOutboundRequests bool
	Steps                []FetchReceiptDispatchStep
}

// FetchReceiptRunPlan groups the downloader-owned settlement and post-drain
// dispatch decisions for one accepted fetched block body.
type FetchReceiptRunPlan struct {
	Settlement FetchReceiptSettlement
	Buffer     FetchedBlockBufferPlan
	Dispatch   FetchReceiptDispatchPlan
}

// FetchReceiptRunApplyResult groups the applied phases for one accepted
// fetched block body.
type FetchReceiptRunApplyResult struct {
	LockedPreBuffer  FetchReceiptSettlementApplyResult
	Buffer           FetchedBlockBufferApplyResult
	LockedPostBuffer FetchReceiptSettlementApplyResult
	AfterUnlock      FetchReceiptSettlementApplyResult
	Dispatch         FetchReceiptDispatchApplyResult
}

// FetchReceiptDispatchStepAction names one post-drain dispatch operation after
// a fetch receipt has settled.
type FetchReceiptDispatchStepAction uint8

const (
	FetchReceiptDispatchSendOutbound FetchReceiptDispatchStepAction = iota
)

// FetchReceiptDispatchStep is one downloader-owned post-drain dispatch step.
type FetchReceiptDispatchStep struct {
	Action FetchReceiptDispatchStepAction
}

// FetchReceiptDispatchApplyResult records post-drain dispatch steps applied
// after accepting a fetched block body.
type FetchReceiptDispatchApplyResult struct {
	AppliedSteps []FetchReceiptDispatchStepAction
	UnknownSteps []FetchReceiptDispatchStepAction
}

// FetchReceiptDispatchPlanApplier performs post-drain dispatch operations
// named by a fetch receipt dispatch plan.
type FetchReceiptDispatchPlanApplier interface {
	SendOutboundRequests()
}

// FetchedBlockBufferAction names the local buffer/stage decision for one
// received block body after its fetch receipt was accepted.
type FetchedBlockBufferAction uint8

const (
	FetchedBlockBufferIgnore FetchedBlockBufferAction = iota
	FetchedBlockBufferStage
	FetchedBlockBufferConflict
)

// FetchedBlockBufferFacts are the side-effect-free facts needed to decide
// whether a received block body should be staged and inserted into the local
// contiguous drain buffer.
type FetchedBlockBufferFacts struct {
	ID                   types.BlockID
	CurrentHead          uint64
	ExistingBuffered     bool
	ExistingBufferedHash tcommon.Hash
	HashBuffered         bool
	ReservedPath         bool
}

// FetchedBlockBufferPlan is the downloader-owned local buffer/stage decision.
type FetchedBlockBufferPlan struct {
	Action FetchedBlockBufferAction
	ID     types.BlockID
	Kept   tcommon.Hash
}

// FetchedBlockBufferApplyResult records local buffer/stage actions applied for
// one accepted fetched block body.
type FetchedBlockBufferApplyResult struct {
	AppliedActions []FetchedBlockBufferAction
	UnknownActions []FetchedBlockBufferAction
}

// FetchedBlockBufferPlanApplier performs the side effects named by a fetched
// block buffer plan. SyncService owns logs, staged-body writes, and in-memory
// buffer maps; downloader owns the action dispatch.
type FetchedBlockBufferPlanApplier interface {
	DropConflictingFetchedBlock(plan FetchedBlockBufferPlan)
	StageFetchedBlock(plan FetchedBlockBufferPlan)
}

// AcknowledgeFetchReceipt removes a matching received block from the in-flight
// request state and decrements the outstanding count without underflowing.
func AcknowledgeFetchReceipt(state FetchReceiptState, hash tcommon.Hash, num uint64) FetchReceiptResult {
	result := FetchReceiptResult{Inflight: state.Inflight, BatchDone: state.Inflight == 0}
	expectedNum, ok := state.Pending[hash]
	if !ok || expectedNum != num {
		return result
	}
	delete(state.Pending, hash)
	delete(state.PendingIDs, hash)
	if state.Inflight > 0 {
		state.Inflight--
	}
	return FetchReceiptResult{
		Accepted:  true,
		Inflight:  state.Inflight,
		BatchDone: state.Inflight == 0,
	}
}

// PlanFetchReceiptRun derives the settlement half of a fetched-block receipt
// run. The dispatch half is planned after the caller applies settlement and
// drains local staged bodies, because only then are follow-up outbound requests
// known.
func PlanFetchReceiptRun(in FetchReceiptRunInput) FetchReceiptRunPlan {
	return FetchReceiptRunPlan{
		Settlement: PlanFetchReceiptSettlement(in.Receipt),
	}
}

// PlanFetchReceiptRunBuffer attaches the local buffer/stage decision to an
// accepted fetch-receipt run.
func PlanFetchReceiptRunBuffer(plan FetchReceiptRunPlan, buffer FetchedBlockBufferPlan) FetchReceiptRunPlan {
	if !plan.Settlement.Accepted {
		return plan
	}
	plan.Buffer = buffer
	return plan
}

// PlanFetchReceiptSettlement maps an accepted fetch receipt to the service
// actions required to keep timers, global requested marks, and follow-up fetch
// scheduling consistent.
func PlanFetchReceiptSettlement(receipt FetchReceiptResult) FetchReceiptSettlement {
	if !receipt.Accepted {
		return FetchReceiptSettlement{}
	}
	return FetchReceiptSettlement{
		Accepted:            true,
		Inflight:            receipt.Inflight,
		BatchDone:           receipt.BatchDone,
		DeleteRequestedHash: true,
		AdvanceFetchSeq:     true,
		StopFetchTimer:      true,
		RearmFetchTimer:     !receipt.BatchDone,
		FillFetchSlots:      receipt.BatchDone,
		DrainBuffered:       true,
	}.withSteps()
}

func (s FetchReceiptSettlement) withSteps() FetchReceiptSettlement {
	if !s.Accepted {
		return s
	}
	if s.DeleteRequestedHash {
		s.LockedPreBuffer = append(s.LockedPreBuffer, FetchReceiptSettlementStep{Action: FetchReceiptDeleteRequestedHash})
	}
	if s.AdvanceFetchSeq {
		s.LockedPreBuffer = append(s.LockedPreBuffer, FetchReceiptSettlementStep{Action: FetchReceiptAdvanceFetchSeq})
	}
	s.LockedPreBuffer = append(s.LockedPreBuffer, FetchReceiptSettlementStep{
		Action:   FetchReceiptUpdateInflight,
		Inflight: s.Inflight,
	})
	if s.StopFetchTimer {
		s.LockedPreBuffer = append(s.LockedPreBuffer, FetchReceiptSettlementStep{Action: FetchReceiptStopFetchTimer})
	}
	if s.RearmFetchTimer {
		s.LockedPreBuffer = append(s.LockedPreBuffer, FetchReceiptSettlementStep{Action: FetchReceiptRearmFetchTimer})
	}
	if s.FillFetchSlots {
		s.LockedPostBuffer = append(s.LockedPostBuffer, FetchReceiptSettlementStep{Action: FetchReceiptFillFetchSlots})
	}
	s.LockedPostBuffer = append(s.LockedPostBuffer, FetchReceiptSettlementStep{Action: FetchReceiptMirrorLegacy})
	if s.DrainBuffered {
		s.AfterUnlock = []FetchReceiptSettlementStep{{Action: FetchReceiptDrainBuffered}}
	}
	return s
}

// ApplyFetchReceiptRunLockedPreBufferPlan executes the lock-held pre-buffer
// settlement phase for a fetched-block receipt run.
func ApplyFetchReceiptRunLockedPreBufferPlan(plan FetchReceiptRunPlan, applier FetchReceiptSettlementPlanApplier) FetchReceiptRunApplyResult {
	return FetchReceiptRunApplyResult{
		LockedPreBuffer: ApplyFetchReceiptSettlementLockedPreBufferPlan(plan.Settlement, applier),
	}
}

// ApplyFetchReceiptRunLockedBufferPlan executes the lock-held local
// buffer/stage phase for a fetched-block receipt run.
func ApplyFetchReceiptRunLockedBufferPlan(plan FetchReceiptRunPlan, applier FetchedBlockBufferPlanApplier) FetchReceiptRunApplyResult {
	return FetchReceiptRunApplyResult{
		Buffer: ApplyFetchedBlockBufferPlan(plan.Buffer, applier),
	}
}

// ApplyFetchReceiptRunLockedPostBufferPlan executes the lock-held post-buffer
// settlement phase for a fetched-block receipt run.
func ApplyFetchReceiptRunLockedPostBufferPlan(plan FetchReceiptRunPlan, applier FetchReceiptSettlementPlanApplier) FetchReceiptRunApplyResult {
	return FetchReceiptRunApplyResult{
		LockedPostBuffer: ApplyFetchReceiptSettlementLockedPostBufferPlan(plan.Settlement, applier),
	}
}

// ApplyFetchReceiptRunAfterUnlockPlan executes the post-lock local-drain phase
// for a fetched-block receipt run.
func ApplyFetchReceiptRunAfterUnlockPlan(plan FetchReceiptRunPlan, applier FetchReceiptSettlementPlanApplier) FetchReceiptRunApplyResult {
	return FetchReceiptRunApplyResult{
		AfterUnlock: ApplyFetchReceiptSettlementAfterUnlockPlan(plan.Settlement, applier),
	}
}

// ApplyFetchReceiptRunDispatchPlan executes the final post-drain dispatch
// phase for a fetched-block receipt run.
func ApplyFetchReceiptRunDispatchPlan(plan FetchReceiptRunPlan, applier FetchReceiptDispatchPlanApplier) FetchReceiptRunApplyResult {
	return FetchReceiptRunApplyResult{
		Dispatch: ApplyFetchReceiptDispatchPlan(plan.Dispatch, applier),
	}
}

// ApplyFetchReceiptSettlementLockedPreBufferPlan executes the lock-held
// peer/request/timer update steps that must run before staging the received
// block body.
func ApplyFetchReceiptSettlementLockedPreBufferPlan(settlement FetchReceiptSettlement, applier FetchReceiptSettlementPlanApplier) FetchReceiptSettlementApplyResult {
	if applier == nil {
		return FetchReceiptSettlementApplyResult{}
	}
	if len(settlement.LockedPreBuffer) == 0 {
		settlement = settlement.withSteps()
	}
	return applyFetchReceiptSettlementSteps(settlement.LockedPreBuffer, applier)
}

// ApplyFetchReceiptSettlementLockedPostBufferPlan executes the lock-held fetch
// scheduling and mirror steps that run after the received block body has been
// staged or ignored.
func ApplyFetchReceiptSettlementLockedPostBufferPlan(settlement FetchReceiptSettlement, applier FetchReceiptSettlementPlanApplier) FetchReceiptSettlementApplyResult {
	if applier == nil {
		return FetchReceiptSettlementApplyResult{}
	}
	if len(settlement.LockedPostBuffer) == 0 {
		settlement = settlement.withSteps()
	}
	return applyFetchReceiptSettlementSteps(settlement.LockedPostBuffer, applier)
}

// ApplyFetchReceiptSettlementAfterUnlockPlan executes post-lock local drain
// actions for an accepted fetch receipt.
func ApplyFetchReceiptSettlementAfterUnlockPlan(settlement FetchReceiptSettlement, applier FetchReceiptSettlementPlanApplier) FetchReceiptSettlementApplyResult {
	if applier == nil {
		return FetchReceiptSettlementApplyResult{}
	}
	if len(settlement.AfterUnlock) == 0 {
		settlement = settlement.withSteps()
	}
	return applyFetchReceiptSettlementSteps(settlement.AfterUnlock, applier)
}

func applyFetchReceiptSettlementSteps(steps []FetchReceiptSettlementStep, applier FetchReceiptSettlementPlanApplier) FetchReceiptSettlementApplyResult {
	var result FetchReceiptSettlementApplyResult
	for _, step := range steps {
		switch step.Action {
		case FetchReceiptDeleteRequestedHash:
			applier.DeleteRequestedHash()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case FetchReceiptAdvanceFetchSeq:
			applier.AdvanceFetchSeq()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case FetchReceiptUpdateInflight:
			applier.UpdateInflight(step.Inflight)
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case FetchReceiptStopFetchTimer:
			applier.StopFetchTimer()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case FetchReceiptRearmFetchTimer:
			applier.RearmFetchTimer()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case FetchReceiptFillFetchSlots:
			result.OutboundRequests = applier.FillFetchSlots()
			result.FilledFetchSlots = true
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case FetchReceiptMirrorLegacy:
			applier.MirrorLegacyLocked()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case FetchReceiptDrainBuffered:
			applier.DrainBuffered()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		default:
			result.UnknownSteps = append(result.UnknownSteps, step.Action)
		}
	}
	return result
}

// PlanFetchReceiptDispatch decides whether follow-up outbound fetch requests
// should be sent after receipt settlement and any local drain.
func PlanFetchReceiptDispatch(in FetchReceiptDispatchInput) FetchReceiptDispatchPlan {
	return FetchReceiptDispatchPlan{
		SendOutboundRequests: in.OutboundRequests > 0 && in.Progress.Syncing && !in.Progress.Paused,
	}.withSteps()
}

// PlanFetchReceiptRunAfterDrain completes a fetched-block receipt run with the
// post-drain dispatch decision.
func PlanFetchReceiptRunAfterDrain(plan FetchReceiptRunPlan, in FetchReceiptRunAfterDrainInput) FetchReceiptRunPlan {
	if !plan.Settlement.Accepted {
		return plan
	}
	plan.Dispatch = PlanFetchReceiptDispatch(FetchReceiptDispatchInput{
		OutboundRequests: in.OutboundRequests,
		Progress:         in.Progress,
	})
	return plan
}

func (p FetchReceiptDispatchPlan) withSteps() FetchReceiptDispatchPlan {
	if p.SendOutboundRequests {
		p.Steps = []FetchReceiptDispatchStep{{Action: FetchReceiptDispatchSendOutbound}}
	}
	return p
}

// ApplyFetchReceiptDispatchPlan executes downloader-owned post-drain dispatch
// operations after a fetch receipt.
func ApplyFetchReceiptDispatchPlan(plan FetchReceiptDispatchPlan, applier FetchReceiptDispatchPlanApplier) FetchReceiptDispatchApplyResult {
	var result FetchReceiptDispatchApplyResult
	if applier == nil {
		return result
	}
	if len(plan.Steps) == 0 {
		plan = plan.withSteps()
	}
	for _, step := range plan.Steps {
		switch step.Action {
		case FetchReceiptDispatchSendOutbound:
			applier.SendOutboundRequests()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		default:
			result.UnknownSteps = append(result.UnknownSteps, step.Action)
		}
	}
	return result
}

// PlanFetchedBlockBuffer decides whether an accepted sync block body should be
// persisted in the staged-body table and local drain buffer.
func PlanFetchedBlockBuffer(f FetchedBlockBufferFacts) FetchedBlockBufferPlan {
	plan := FetchedBlockBufferPlan{ID: f.ID}
	if f.ID.Num <= f.CurrentHead {
		return plan
	}
	if f.ExistingBuffered {
		if f.ExistingBufferedHash != f.ID.Hash {
			plan.Action = FetchedBlockBufferConflict
			plan.Kept = f.ExistingBufferedHash
		}
		return plan
	}
	if f.HashBuffered || !f.ReservedPath {
		return plan
	}
	plan.Action = FetchedBlockBufferStage
	return plan
}

// ApplyFetchedBlockBufferPlan executes the downloader-owned local
// buffer/stage action for one accepted fetched block body.
func ApplyFetchedBlockBufferPlan(plan FetchedBlockBufferPlan, applier FetchedBlockBufferPlanApplier) FetchedBlockBufferApplyResult {
	var result FetchedBlockBufferApplyResult
	if applier == nil {
		return result
	}
	switch plan.Action {
	case FetchedBlockBufferConflict:
		applier.DropConflictingFetchedBlock(plan)
		result.AppliedActions = append(result.AppliedActions, plan.Action)
	case FetchedBlockBufferStage:
		applier.StageFetchedBlock(plan)
		result.AppliedActions = append(result.AppliedActions, plan.Action)
	case FetchedBlockBufferIgnore:
	default:
		result.UnknownActions = append(result.UnknownActions, plan.Action)
	}
	return result
}
