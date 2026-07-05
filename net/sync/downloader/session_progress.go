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
	Progress         SessionProgress
}

// FetchRefillRunInput is the lock-free state after a timer/manual peer-ready
// pass has refilled peer fetch slots.
type FetchRefillRunInput struct {
	OutboundRequests int
	Progress         SessionProgress
}

// EmptyDrainRefillInput is the lock-free state after a local drain found no
// importable bodies and existing peers were refilled.
type EmptyDrainRefillInput struct {
	OutboundRequests          int
	Progress                  SessionProgress
	JoinAvailablePeersAllowed bool
}

// EmptyDrainJoinProbeInput is the lock-free state needed to decide whether an
// empty local drain should spend time probing additional peers.
type EmptyDrainJoinProbeInput struct {
	Progress SessionProgress
}

// EmptyDrainRunInput is the lock-free state after a drain iteration found no
// local batch and existing peer fetch slots were refilled.
type EmptyDrainRunInput struct {
	OutboundRequests int
	Progress         SessionProgress
}

// EmptyDrainPreparationInput is the lock-held state at the start of an empty
// local drain branch, before fetch slots are refilled.
type EmptyDrainPreparationInput struct {
	Progress SessionProgress
}

// LocalDrainEntryInput is the lock-held session state at the top of one local
// drain iteration, before the caller reads staged-body rows.
type LocalDrainEntryInput struct {
	Progress SessionProgress
}

// LocalDrainIterationInput is the lock-held state for one local staged-body
// drain iteration after the caller has restored/popped a possible batch.
type LocalDrainIterationInput struct {
	Progress         SessionProgress
	BufferedBatchLen int
}

// LocalDrainRunInput is the lock-held state after staged-body drain
// preparation has either produced an importable batch or found an empty drain.
type LocalDrainRunInput struct {
	Progress SessionProgress
	Drain    StagedBodyDrainRunResult
}

// LocalDrainSessionRunInput is the lock-held state needed to run one local
// drain iteration: first decide whether staged bodies should be read, then
// read/apply that staged-body drain and branch on the refreshed progress.
type LocalDrainSessionRunInput struct {
	Progress SessionProgress
	Next     uint64
	Max      int
}

// IdleDrainStepAction names one session-level action after a local drain found
// no importable buffered bodies and existing peers were refilled.
type IdleDrainStepAction uint8

const (
	IdleDrainFinish IdleDrainStepAction = iota
	IdleDrainJoinAvailablePeers
)

// LocalDrainIterationStepAction names the next high-level operation for one
// local drain loop iteration.
type LocalDrainIterationStepAction uint8

const (
	LocalDrainIterationStop LocalDrainIterationStepAction = iota
	LocalDrainIterationEmpty
	LocalDrainIterationImport
)

// LocalDrainEntryStepAction names the entry operation for one local drain
// iteration before staged-body rows are read.
type LocalDrainEntryStepAction uint8

const (
	LocalDrainEntryStop LocalDrainEntryStepAction = iota
	LocalDrainEntryReadStagedBodies
)

// SessionResetStepAction names one ordered operation for clearing a sync
// session. The service owns concrete timers and buffers; downloader owns the
// reset schedule so finish, restart, and failover paths converge on the same
// cleanup.
type SessionResetStepAction uint8

const (
	SessionResetStopPeerTimers SessionResetStepAction = iota
	SessionResetDeactivateSession
	SessionResetClearLegacyFetchState
	SessionResetAdvanceFetchSequence
	SessionResetStopLegacyFetchTimer
	SessionResetClearPeerState
	SessionResetClearBlockTracking
	SessionResetClearTarget
	SessionResetResetBufferWait
	SessionResetDeleteStagedBodies
)

// FetchRefillDispatchStepAction names one network dispatch action after peer
// fetch slots were refilled.
type FetchRefillDispatchStepAction uint8

const (
	FetchRefillDispatchSendOutbound FetchRefillDispatchStepAction = iota
)

// FetchRefillRunStepAction names one lock-held operation for a timer/manual
// peer-ready fetch-refill run.
type FetchRefillRunStepAction uint8

const (
	FetchRefillRunMirrorLegacy FetchRefillRunStepAction = iota
)

// IdleDrainStep is one downloader-owned empty-drain settlement action.
type IdleDrainStep struct {
	Action IdleDrainStepAction
}

// IdleDrainApplyResult records empty-drain settlement steps dispatched by the
// downloader planner after refill.
type IdleDrainApplyResult struct {
	AppliedSteps []IdleDrainStepAction
	UnknownSteps []IdleDrainStepAction
}

// LocalDrainIterationStep is one downloader-owned local drain loop operation.
type LocalDrainIterationStep struct {
	Action LocalDrainIterationStepAction
}

// LocalDrainIterationApplyResult records the drain-loop branch selected from
// the downloader-owned local iteration step list.
type LocalDrainIterationApplyResult struct {
	Action       LocalDrainIterationStepAction
	StopLoop     bool
	EmptyDrain   bool
	ImportBatch  bool
	AppliedSteps []LocalDrainIterationStepAction
	UnknownSteps []LocalDrainIterationStepAction
}

// LocalDrainEntryStep is one downloader-owned local drain entry operation.
type LocalDrainEntryStep struct {
	Action LocalDrainEntryStepAction
}

// SessionResetStep is one downloader-owned cleanup step for a sync session.
type SessionResetStep struct {
	Action SessionResetStepAction
}

// LocalDrainEntryApplyResult records whether one drain iteration should stop
// or read staged-body rows, as selected from the downloader-owned entry steps.
type LocalDrainEntryApplyResult struct {
	Action           LocalDrainEntryStepAction
	StopLoop         bool
	ReadStagedBodies bool
	AppliedSteps     []LocalDrainEntryStepAction
	UnknownSteps     []LocalDrainEntryStepAction
}

// SessionResetApplyResult records the cleanup steps dispatched by the
// downloader reset schedule.
type SessionResetApplyResult struct {
	AppliedSteps []SessionResetStepAction
	UnknownSteps []SessionResetStepAction
}

// FetchRefillDispatchStep is one downloader-owned dispatch operation after a
// fetch-slot refill.
type FetchRefillDispatchStep struct {
	Action FetchRefillDispatchStepAction
}

// FetchRefillRunStep is one lock-held operation for a timer/manual peer-ready
// fetch-refill run.
type FetchRefillRunStep struct {
	Action FetchRefillRunStepAction
}

// FetchRefillDispatchApplyResult records fetch-refill dispatch steps
// dispatched by the downloader planner.
type FetchRefillDispatchApplyResult struct {
	AppliedSteps []FetchRefillDispatchStepAction
	UnknownSteps []FetchRefillDispatchStepAction
}

// FetchRefillRunLockedApplyResult records lock-held fetch-refill run steps
// dispatched by the downloader planner.
type FetchRefillRunLockedApplyResult struct {
	AppliedSteps []FetchRefillRunStepAction
	UnknownSteps []FetchRefillRunStepAction
}

// IdleDrainPlan describes the session-level action after a local drain found no
// buffered batch and fetch slots have been refilled.
type IdleDrainPlan struct {
	Finish             bool
	JoinAvailablePeers bool
	Steps              []IdleDrainStep
}

// LocalDrainIterationPlan decides whether one drain-loop iteration should stop,
// settle an empty drain, or import a popped staged-body batch.
type LocalDrainIterationPlan struct {
	Action      LocalDrainIterationStepAction
	StopLoop    bool
	EmptyDrain  bool
	ImportBatch bool
	Steps       []LocalDrainIterationStep
}

// LocalDrainEntryPlan decides whether a local drain iteration should read
// staged-body rows or stop before touching the staged-body table.
type LocalDrainEntryPlan struct {
	StopLoop         bool
	ReadStagedBodies bool
	Steps            []LocalDrainEntryStep
}

// SessionResetPlan describes the ordered cleanup needed to end or abandon a
// sync session.
type SessionResetPlan struct {
	Steps []SessionResetStep
}

// LocalDrainRunPlan groups the staged-body drain result with the downloader
// branch decision for the current local drain iteration.
type LocalDrainRunPlan struct {
	Drain     StagedBodyDrainRunResult
	Batch     BufferedBatch
	Iteration LocalDrainIterationPlan
}

// LocalDrainRunApplyResult groups a staged-body drain result with the applied
// local iteration branch selected by the downloader planner.
type LocalDrainRunApplyResult struct {
	Plan      LocalDrainRunPlan
	Drain     StagedBodyDrainRunResult
	Batch     BufferedBatch
	Iteration LocalDrainIterationApplyResult
}

// LocalDrainSessionRunPlan groups the entry branch with the staged-body drain
// target and the derived iteration plan for one lock-held local drain pass.
type LocalDrainSessionRunPlan struct {
	Entry LocalDrainEntryPlan
	Next  uint64
	Max   int
	Run   LocalDrainRunPlan
}

// LocalDrainSessionRunApplyResult records one downloader-owned local drain
// pass. Entry is resolved before staged bodies are touched; Run is resolved
// only after the staged-body drain has run and fresh progress has been read.
type LocalDrainSessionRunApplyResult struct {
	Plan             LocalDrainSessionRunPlan
	Entry            LocalDrainEntryApplyResult
	Drain            StagedBodyDrainRunResult
	RunProgress      SessionProgress
	Run              LocalDrainRunApplyResult
	Batch            BufferedBatch
	ReadStagedBodies bool
	StopLoop         bool
	EmptyDrain       bool
	ImportBatch      bool
}

// FetchRefillDispatchPlan describes whether refilled outbound requests should
// be sent.
type FetchRefillDispatchPlan struct {
	SendOutboundRequests bool
	Steps                []FetchRefillDispatchStep
}

// FetchRefillRunPlan groups the downloader-owned lock-held mirror and
// post-lock dispatch decisions after a timer or peer-ready refill.
type FetchRefillRunPlan struct {
	MirrorLegacy bool
	LockedSteps  []FetchRefillRunStep
	Dispatch     FetchRefillDispatchPlan
}

// FetchRefillRunApplyResult groups the lock-held and post-lock phases for a
// timer/manual peer-ready fetch-refill run.
type FetchRefillRunApplyResult struct {
	Plan     FetchRefillRunPlan
	Locked   FetchRefillRunLockedApplyResult
	Dispatch FetchRefillDispatchApplyResult
}

// EmptyDrainRefillPlan groups the two downloader-owned decisions after an
// empty local drain: session settlement and network dispatch.
type EmptyDrainRefillPlan struct {
	Idle     IdleDrainPlan
	Dispatch FetchRefillDispatchPlan
}

// EmptyDrainJoinProbePlan tells SyncService whether it should evaluate the
// peer-join throttle/availability gate before settling an empty local drain.
type EmptyDrainJoinProbePlan struct {
	CheckJoinAvailablePeers bool
}

// EmptyDrainRunStepAction names one lock-held operation after an empty local
// drain has refilled peers and planned session settlement.
type EmptyDrainRunStepAction uint8

const (
	EmptyDrainMirrorLegacy EmptyDrainRunStepAction = iota
)

// EmptyDrainPreparationStepAction names one lock-held preparation operation
// before an empty local drain can be settled.
type EmptyDrainPreparationStepAction uint8

const (
	EmptyDrainPrepareBeginBufferWait EmptyDrainPreparationStepAction = iota
	EmptyDrainPrepareRefillFetchSlots
)

// EmptyDrainRunStep is one lock-held operation for an empty local drain run.
type EmptyDrainRunStep struct {
	Action EmptyDrainRunStepAction
}

// EmptyDrainPreparationStep is one lock-held preparation operation for an
// empty local drain.
type EmptyDrainPreparationStep struct {
	Action EmptyDrainPreparationStepAction
	Next   uint64
}

// EmptyDrainRunLockedApplyResult records the lock-held empty-drain steps
// dispatched by the downloader planner.
type EmptyDrainRunLockedApplyResult struct {
	AppliedSteps []EmptyDrainRunStepAction
	UnknownSteps []EmptyDrainRunStepAction
}

// EmptyDrainPreparationApplyResult records the preparation steps applied before
// settling an empty local drain.
type EmptyDrainPreparationApplyResult struct {
	AppliedSteps     []EmptyDrainPreparationStepAction
	UnknownSteps     []EmptyDrainPreparationStepAction
	OutboundRequests int
}

// EmptyDrainPreparationRunApplyResult groups the lock-held preparation result
// with the empty-drain run plan derived from the post-preparation progress.
type EmptyDrainPreparationRunApplyResult struct {
	Preparation EmptyDrainPreparationApplyResult
	Run         EmptyDrainRunPlan
}

// EmptyDrainPreparationLockedRunApplyResult groups empty-drain preparation,
// run planning, and lock-held run application into one downloader-owned unit.
type EmptyDrainPreparationLockedRunApplyResult struct {
	Preparation EmptyDrainPreparationApplyResult
	Run         EmptyDrainRunPlan
	Locked      EmptyDrainRunApplyResult
}

// EmptyDrainRunApplyResult groups the lock-held, post-lock idle settlement,
// and final dispatch phases for one empty local drain run.
type EmptyDrainRunApplyResult struct {
	Locked   EmptyDrainRunLockedApplyResult
	Idle     IdleDrainApplyResult
	Dispatch FetchRefillDispatchApplyResult
}

// EmptyDrainRunPlan groups the full downloader decision for one empty local
// drain iteration.
type EmptyDrainRunPlan struct {
	JoinProbe                 EmptyDrainJoinProbePlan
	JoinAvailablePeersAllowed bool
	Refill                    EmptyDrainRefillPlan
	MirrorLegacy              bool
	LockedSteps               []EmptyDrainRunStep
}

// EmptyDrainPreparationPlan describes the lock-held work that should happen as
// soon as a local drain discovers there is no importable buffered body.
type EmptyDrainPreparationPlan struct {
	BeginBufferWait  bool
	BufferWaitNext   uint64
	RefillFetchSlots bool
	Steps            []EmptyDrainPreparationStep
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

// FetchRefillRunPlanApplier performs lock-held runtime actions named by a
// timer/manual peer-ready fetch-refill run plan.
type FetchRefillRunPlanApplier interface {
	MirrorLegacyUnderLock()
}

// EmptyDrainJoinGate performs the caller-owned peer join throttle and
// availability check when the downloader decides a join probe is useful.
type EmptyDrainJoinGate interface {
	CheckJoinAvailablePeers(progress SessionProgress) bool
}

// EmptyDrainRunPlanApplier performs lock-held runtime actions named by an
// empty-drain run plan.
type EmptyDrainRunPlanApplier interface {
	MirrorLegacyUnderLock()
}

// EmptyDrainPreparationPlanApplier performs lock-held empty-drain preparation
// operations. RefillFetchSlots returns the number of outbound requests created.
type EmptyDrainPreparationPlanApplier interface {
	BeginBufferWait(next uint64)
	RefillFetchSlots() int
}

// EmptyDrainPreparationRunPlanApplier extends preparation with a fresh
// progress read after lock-held side effects, preserving the ordering used by
// SyncService before the downloader planner builds the final empty-drain run.
type EmptyDrainPreparationRunPlanApplier interface {
	EmptyDrainPreparationPlanApplier
	EmptyDrainRunProgress() SessionProgress
}

// LocalDrainSessionRunPlanApplier performs the runtime pieces required between
// the entry decision and the local drain iteration branch.
type LocalDrainSessionRunPlanApplier interface {
	ReadAndApplyStagedBodyDrain(next uint64, max int) StagedBodyDrainRunResult
	LocalDrainRunProgress() SessionProgress
}

// SessionResetPlanApplier performs the runtime operations named by a reset
// plan. SyncService owns the mutable fields; downloader owns the ordering.
type SessionResetPlanApplier interface {
	StopPeerTimers()
	DeactivateSession()
	ClearLegacyFetchState()
	AdvanceFetchSequence()
	StopLegacyFetchTimer()
	ClearPeerState()
	ClearBlockTracking()
	ClearTarget()
	ResetBufferWait()
	DeleteStagedBodies()
}

// PostInventorySettlementInput is the lock-free state needed after an
// inventory message refilled fetch slots.
type PostInventorySettlementInput struct {
	OutboundRequests int
	Progress         SessionProgress
}

// PostInventoryRunInput is the lock-free state after an inventory response has
// been accepted and peer fetch slots have been refilled.
type PostInventoryRunInput struct {
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

// PostInventorySettlementApplyResult records the settlement steps applied after
// accepting a peer inventory response.
type PostInventorySettlementApplyResult struct {
	AppliedSteps []PostInventorySettlementStepAction
	UnknownSteps []PostInventorySettlementStepAction
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

// PostInventoryRunPlan groups the downloader-owned settlement and dispatch
// decisions after accepting a peer inventory response.
type PostInventoryRunPlan struct {
	Settlement PostInventorySettlementPlan
	Dispatch   FetchRefillDispatchPlan
}

// PostInventoryRunApplyResult groups the applied phases for one post-inventory
// run. LockedSettlement runs while SyncService holds its state lock; Dispatch
// and AfterDispatchSettlement run after unlock.
type PostInventoryRunApplyResult struct {
	Plan                    PostInventoryRunPlan
	LockedSettlement        PostInventorySettlementApplyResult
	Dispatch                FetchRefillDispatchApplyResult
	AfterDispatchSettlement PostInventorySettlementApplyResult
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
		SendOutboundRequests: in.OutboundRequests > 0 && in.Progress.Syncing && !in.Progress.Paused,
	}.withSteps()
}

// PlanFetchRefillRun derives the full downloader decision for a timer/manual
// peer-ready fetch refill.
func PlanFetchRefillRun(in FetchRefillRunInput) FetchRefillRunPlan {
	return FetchRefillRunPlan{
		MirrorLegacy: true,
		Dispatch: PlanFetchRefillDispatch(FetchRefillDispatchInput{
			OutboundRequests: in.OutboundRequests,
			Progress:         in.Progress,
		}),
	}.withLockedSteps()
}

// PlanLocalDrainIteration derives the high-level local drain loop branch. The
// service still owns locking, DB handles, and network dispatch; downloader owns
// the branch semantics so import and empty-drain behavior share one planner.
func PlanLocalDrainIteration(in LocalDrainIterationInput) LocalDrainIterationPlan {
	if !in.Progress.Syncing || in.Progress.Paused {
		return LocalDrainIterationPlan{StopLoop: true}.withSteps()
	}
	if in.BufferedBatchLen == 0 {
		return LocalDrainIterationPlan{EmptyDrain: true}.withSteps()
	}
	return LocalDrainIterationPlan{ImportBatch: true}.withSteps()
}

// PlanLocalDrainEntry derives the local drain-loop entry branch before the
// caller reads staged-body rows.
func PlanLocalDrainEntry(in LocalDrainEntryInput) LocalDrainEntryPlan {
	if !in.Progress.Syncing || in.Progress.Paused {
		return LocalDrainEntryPlan{StopLoop: true}.withSteps()
	}
	return LocalDrainEntryPlan{ReadStagedBodies: true}.withSteps()
}

// PlanLocalDrainRun derives the local drain branch from the full staged-body
// drain result. This keeps the "failed vs empty vs importable" decision next
// to the staged-body drain planner instead of re-deriving it in SyncService.
func PlanLocalDrainRun(in LocalDrainRunInput) LocalDrainRunPlan {
	batch := in.Drain.Batch
	if in.Drain.Failed() {
		return LocalDrainRunPlan{
			Drain: in.Drain,
			Batch: batch,
			Iteration: LocalDrainIterationPlan{
				Action:   LocalDrainIterationStop,
				StopLoop: true,
				Steps:    []LocalDrainIterationStep{{Action: LocalDrainIterationStop}},
			},
		}
	}
	return LocalDrainRunPlan{
		Drain: in.Drain,
		Batch: batch,
		Iteration: PlanLocalDrainIteration(LocalDrainIterationInput{
			Progress:         in.Progress,
			BufferedBatchLen: len(batch.Buffered),
		}),
	}
}

// PlanLocalDrainSessionRun creates the entry plan for one lock-held local
// drain pass and records the staged-body drain target that apply will use if
// the entry branch allows reading.
func PlanLocalDrainSessionRun(in LocalDrainSessionRunInput) LocalDrainSessionRunPlan {
	return LocalDrainSessionRunPlan{
		Entry: PlanLocalDrainEntry(LocalDrainEntryInput{Progress: in.Progress}),
		Next:  in.Next,
		Max:   in.Max,
	}
}

// PlanSessionReset returns the canonical session cleanup schedule used by
// finish, failover restart, and explicit reset paths.
func PlanSessionReset() SessionResetPlan {
	return SessionResetPlan{}.withSteps()
}

// ApplyLocalDrainRunPlan resolves the downloader-owned local drain run plan
// into the caller's loop branch while preserving the staged-body drain batch.
func ApplyLocalDrainRunPlan(plan LocalDrainRunPlan) LocalDrainRunApplyResult {
	return LocalDrainRunApplyResult{
		Plan:      plan,
		Drain:     plan.Drain,
		Batch:     plan.Batch,
		Iteration: ApplyLocalDrainIterationPlan(plan.Iteration),
	}
}

// ApplyLocalDrainRun creates and applies the downloader-owned local drain run
// plan from the current staged-body drain result.
func ApplyLocalDrainRun(in LocalDrainRunInput) LocalDrainRunApplyResult {
	return ApplyLocalDrainRunPlan(PlanLocalDrainRun(in))
}

// ApplyLocalDrainSessionRun creates and applies one downloader-owned local
// drain pass. Staged bodies are read only if the entry plan selects that
// branch; the import/empty/stop branch is derived from progress refreshed after
// the staged-body drain side effects have run.
func ApplyLocalDrainSessionRun(in LocalDrainSessionRunInput, applier LocalDrainSessionRunPlanApplier) LocalDrainSessionRunApplyResult {
	return ApplyLocalDrainSessionRunPlan(PlanLocalDrainSessionRun(in), applier)
}

// ApplyLocalDrainSessionRunPlan applies one local drain pass from a prebuilt
// entry plan. The returned Plan.Run is populated with the derived drain
// iteration plan after staged bodies are read.
func ApplyLocalDrainSessionRunPlan(plan LocalDrainSessionRunPlan, applier LocalDrainSessionRunPlanApplier) LocalDrainSessionRunApplyResult {
	result := LocalDrainSessionRunApplyResult{Plan: plan}
	result.Entry = ApplyLocalDrainEntryPlan(plan.Entry)
	if result.Entry.StopLoop {
		result.StopLoop = true
		return result
	}
	if !result.Entry.ReadStagedBodies {
		return result
	}
	result.ReadStagedBodies = true
	if applier == nil {
		return result
	}
	result.Drain = applier.ReadAndApplyStagedBodyDrain(plan.Next, plan.Max)
	result.RunProgress = applier.LocalDrainRunProgress()
	runPlan := PlanLocalDrainRun(LocalDrainRunInput{
		Progress: result.RunProgress,
		Drain:    result.Drain,
	})
	result.Run = ApplyLocalDrainRunPlan(runPlan)
	result.Plan.Run = result.Run.Plan
	result.Batch = result.Run.Batch
	result.StopLoop = result.Run.Iteration.StopLoop
	result.EmptyDrain = result.Run.Iteration.EmptyDrain
	result.ImportBatch = result.Run.Iteration.ImportBatch
	return result
}

// ApplyLocalDrainEntryPlan resolves the downloader-owned local drain entry
// steps into the caller's lock-held branch.
func ApplyLocalDrainEntryPlan(plan LocalDrainEntryPlan) LocalDrainEntryApplyResult {
	var result LocalDrainEntryApplyResult
	if len(plan.Steps) == 0 {
		plan = plan.withSteps()
	}
	for _, step := range plan.Steps {
		result.Action = step.Action
		switch step.Action {
		case LocalDrainEntryStop:
			result.StopLoop = true
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case LocalDrainEntryReadStagedBodies:
			result.ReadStagedBodies = true
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		default:
			result.UnknownSteps = append(result.UnknownSteps, step.Action)
		}
	}
	return result
}

// ApplyLocalDrainEntry creates and applies the downloader-owned local drain
// entry plan from the current session progress.
func ApplyLocalDrainEntry(in LocalDrainEntryInput) LocalDrainEntryApplyResult {
	return ApplyLocalDrainEntryPlan(PlanLocalDrainEntry(in))
}

// ApplySessionResetPlan executes the downloader-owned session reset schedule.
func ApplySessionResetPlan(plan SessionResetPlan, applier SessionResetPlanApplier) SessionResetApplyResult {
	var result SessionResetApplyResult
	if applier == nil {
		return result
	}
	if len(plan.Steps) == 0 {
		plan = plan.withSteps()
	}
	for _, step := range plan.Steps {
		switch step.Action {
		case SessionResetStopPeerTimers:
			applier.StopPeerTimers()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case SessionResetDeactivateSession:
			applier.DeactivateSession()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case SessionResetClearLegacyFetchState:
			applier.ClearLegacyFetchState()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case SessionResetAdvanceFetchSequence:
			applier.AdvanceFetchSequence()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case SessionResetStopLegacyFetchTimer:
			applier.StopLegacyFetchTimer()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case SessionResetClearPeerState:
			applier.ClearPeerState()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case SessionResetClearBlockTracking:
			applier.ClearBlockTracking()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case SessionResetClearTarget:
			applier.ClearTarget()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case SessionResetResetBufferWait:
			applier.ResetBufferWait()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case SessionResetDeleteStagedBodies:
			applier.DeleteStagedBodies()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		default:
			result.UnknownSteps = append(result.UnknownSteps, step.Action)
		}
	}
	return result
}

// ApplyLocalDrainIterationPlan resolves the downloader-owned local drain
// iteration steps into the caller's loop branch.
func ApplyLocalDrainIterationPlan(plan LocalDrainIterationPlan) LocalDrainIterationApplyResult {
	var result LocalDrainIterationApplyResult
	if len(plan.Steps) == 0 {
		plan = plan.withSteps()
	}
	for _, step := range plan.Steps {
		result.Action = step.Action
		switch step.Action {
		case LocalDrainIterationStop:
			result.StopLoop = true
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case LocalDrainIterationEmpty:
			result.EmptyDrain = true
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case LocalDrainIterationImport:
			result.ImportBatch = true
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		default:
			result.UnknownSteps = append(result.UnknownSteps, step.Action)
		}
	}
	return result
}

// PlanEmptyDrainJoinProbe decides whether the caller should evaluate
// peer-join availability before constructing the final empty-drain plan.
// Paused/inactive sessions do nothing; finished sessions should go straight to
// Finish without touching the join-throttle clock.
func PlanEmptyDrainJoinProbe(in EmptyDrainJoinProbeInput) EmptyDrainJoinProbePlan {
	if !in.Progress.Syncing || in.Progress.Paused {
		return EmptyDrainJoinProbePlan{}
	}
	return EmptyDrainJoinProbePlan{CheckJoinAvailablePeers: !in.Progress.ShouldFinish()}
}

// PlanEmptyDrainRefill derives the full downloader decision after a local
// staged-body drain found no importable batch and fetch slots were refilled.
func PlanEmptyDrainRefill(in EmptyDrainRefillInput) EmptyDrainRefillPlan {
	return EmptyDrainRefillPlan{
		Idle: PlanIdleDrainAfterRefill(IdleDrainAfterRefillInput{
			Progress:                  in.Progress,
			JoinAvailablePeersAllowed: in.JoinAvailablePeersAllowed,
		}),
		Dispatch: PlanFetchRefillDispatch(FetchRefillDispatchInput{
			OutboundRequests: in.OutboundRequests,
			Progress:         in.Progress,
		}),
	}
}

// PlanEmptyDrainRun derives the full empty-drain settlement plan, including the
// optional caller-owned peer-join gate. The gate is consulted only for active,
// unfinished sessions.
func PlanEmptyDrainRun(in EmptyDrainRunInput, gate EmptyDrainJoinGate) EmptyDrainRunPlan {
	joinProbe := PlanEmptyDrainJoinProbe(EmptyDrainJoinProbeInput{Progress: in.Progress})
	if !in.Progress.Syncing || in.Progress.Paused {
		return EmptyDrainRunPlan{}
	}
	joinAllowed := false
	if joinProbe.CheckJoinAvailablePeers && gate != nil {
		joinAllowed = gate.CheckJoinAvailablePeers(in.Progress)
	}
	return EmptyDrainRunPlan{
		JoinProbe:                 joinProbe,
		JoinAvailablePeersAllowed: joinAllowed,
		MirrorLegacy:              true,
		Refill: PlanEmptyDrainRefill(EmptyDrainRefillInput{
			OutboundRequests:          in.OutboundRequests,
			Progress:                  in.Progress,
			JoinAvailablePeersAllowed: joinAllowed,
		}),
	}.withLockedSteps()
}

// PlanEmptyDrainPreparation derives the lock-held setup for an empty local
// drain. The caller still owns the clock and peer maps; downloader owns the
// ordering and the next block number used by the buffer-wait tracker.
func PlanEmptyDrainPreparation(in EmptyDrainPreparationInput) EmptyDrainPreparationPlan {
	if !in.Progress.Syncing || in.Progress.Paused {
		return EmptyDrainPreparationPlan{}
	}
	return EmptyDrainPreparationPlan{
		BeginBufferWait:  true,
		BufferWaitNext:   in.Progress.CurrentHead + 1,
		RefillFetchSlots: true,
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

func (p LocalDrainIterationPlan) withSteps() LocalDrainIterationPlan {
	switch {
	case p.StopLoop:
		p.Action = LocalDrainIterationStop
		p.Steps = []LocalDrainIterationStep{{Action: LocalDrainIterationStop}}
	case p.EmptyDrain:
		p.Action = LocalDrainIterationEmpty
		p.Steps = []LocalDrainIterationStep{{Action: LocalDrainIterationEmpty}}
	case p.ImportBatch:
		p.Action = LocalDrainIterationImport
		p.Steps = []LocalDrainIterationStep{{Action: LocalDrainIterationImport}}
	}
	return p
}

func (p LocalDrainEntryPlan) withSteps() LocalDrainEntryPlan {
	if p.StopLoop {
		p.Steps = []LocalDrainEntryStep{{Action: LocalDrainEntryStop}}
	} else if p.ReadStagedBodies {
		p.Steps = []LocalDrainEntryStep{{Action: LocalDrainEntryReadStagedBodies}}
	}
	return p
}

func (p SessionResetPlan) withSteps() SessionResetPlan {
	p.Steps = []SessionResetStep{
		{Action: SessionResetStopPeerTimers},
		{Action: SessionResetDeactivateSession},
		{Action: SessionResetClearLegacyFetchState},
		{Action: SessionResetAdvanceFetchSequence},
		{Action: SessionResetStopLegacyFetchTimer},
		{Action: SessionResetClearPeerState},
		{Action: SessionResetClearBlockTracking},
		{Action: SessionResetClearTarget},
		{Action: SessionResetResetBufferWait},
		{Action: SessionResetDeleteStagedBodies},
	}
	return p
}

func (p FetchRefillDispatchPlan) withSteps() FetchRefillDispatchPlan {
	if p.SendOutboundRequests {
		p.Steps = []FetchRefillDispatchStep{{Action: FetchRefillDispatchSendOutbound}}
	}
	return p
}

func (p FetchRefillRunPlan) withLockedSteps() FetchRefillRunPlan {
	if p.MirrorLegacy {
		p.LockedSteps = []FetchRefillRunStep{{Action: FetchRefillRunMirrorLegacy}}
	}
	return p
}

func (p EmptyDrainRunPlan) withLockedSteps() EmptyDrainRunPlan {
	if p.MirrorLegacy {
		p.LockedSteps = []EmptyDrainRunStep{{Action: EmptyDrainMirrorLegacy}}
	}
	return p
}

func (p EmptyDrainPreparationPlan) withSteps() EmptyDrainPreparationPlan {
	if p.BeginBufferWait {
		p.Steps = append(p.Steps, EmptyDrainPreparationStep{
			Action: EmptyDrainPrepareBeginBufferWait,
			Next:   p.BufferWaitNext,
		})
	}
	if p.RefillFetchSlots {
		p.Steps = append(p.Steps, EmptyDrainPreparationStep{Action: EmptyDrainPrepareRefillFetchSlots})
	}
	return p
}

// ApplyFetchRefillRunLockedPlan executes the lock-held portion of a full
// timer/manual peer-ready fetch-refill run.
func ApplyFetchRefillRunLockedPlan(plan FetchRefillRunPlan, applier FetchRefillRunPlanApplier) FetchRefillRunApplyResult {
	var result FetchRefillRunApplyResult
	if len(plan.LockedSteps) == 0 {
		plan = plan.withLockedSteps()
	}
	result.Plan = plan
	if applier == nil {
		return result
	}
	for _, step := range plan.LockedSteps {
		switch step.Action {
		case FetchRefillRunMirrorLegacy:
			applier.MirrorLegacyUnderLock()
			result.Locked.AppliedSteps = append(result.Locked.AppliedSteps, step.Action)
		default:
			result.Locked.UnknownSteps = append(result.Locked.UnknownSteps, step.Action)
		}
	}
	return result
}

// ApplyFetchRefillRun creates and applies the lock-held portion of the
// downloader-owned fetch-refill run from current session progress. The returned
// plan is reused by the caller for the post-lock dispatch phase.
func ApplyFetchRefillRun(in FetchRefillRunInput, applier FetchRefillRunPlanApplier) FetchRefillRunApplyResult {
	return ApplyFetchRefillRunLockedPlan(PlanFetchRefillRun(in), applier)
}

// ApplyEmptyDrainPreparationPlan executes the lock-held preparation steps for
// an empty local drain.
func ApplyEmptyDrainPreparationPlan(plan EmptyDrainPreparationPlan, applier EmptyDrainPreparationPlanApplier) EmptyDrainPreparationApplyResult {
	var result EmptyDrainPreparationApplyResult
	if applier == nil {
		return result
	}
	if len(plan.Steps) == 0 {
		plan = plan.withSteps()
	}
	for _, step := range plan.Steps {
		switch step.Action {
		case EmptyDrainPrepareBeginBufferWait:
			applier.BeginBufferWait(step.Next)
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case EmptyDrainPrepareRefillFetchSlots:
			result.OutboundRequests += applier.RefillFetchSlots()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		default:
			result.UnknownSteps = append(result.UnknownSteps, step.Action)
		}
	}
	return result
}

// ApplyEmptyDrainPreparationRunPlan applies lock-held empty-drain preparation,
// then builds the final empty-drain run from the refreshed session progress.
func ApplyEmptyDrainPreparationRunPlan(in EmptyDrainPreparationInput, applier EmptyDrainPreparationRunPlanApplier, gate EmptyDrainJoinGate) EmptyDrainPreparationRunApplyResult {
	var result EmptyDrainPreparationRunApplyResult
	if applier == nil {
		return result
	}
	result.Preparation = ApplyEmptyDrainPreparationPlan(PlanEmptyDrainPreparation(in), applier)
	result.Run = PlanEmptyDrainRun(EmptyDrainRunInput{
		OutboundRequests: result.Preparation.OutboundRequests,
		Progress:         applier.EmptyDrainRunProgress(),
	}, gate)
	return result
}

// ApplyEmptyDrainPreparationLockedRunPlan applies lock-held empty-drain
// preparation and lock-held run actions while returning the run plan for
// post-lock idle settlement and dispatch.
func ApplyEmptyDrainPreparationLockedRunPlan(in EmptyDrainPreparationInput, prepApplier EmptyDrainPreparationRunPlanApplier, gate EmptyDrainJoinGate, runApplier EmptyDrainRunPlanApplier) EmptyDrainPreparationLockedRunApplyResult {
	var result EmptyDrainPreparationLockedRunApplyResult
	prepared := ApplyEmptyDrainPreparationRunPlan(in, prepApplier, gate)
	result.Preparation = prepared.Preparation
	result.Run = prepared.Run
	result.Locked = ApplyEmptyDrainRunLockedPlan(prepared.Run, runApplier)
	return result
}

// ApplyFetchRefillRunPostLockPlan executes the post-lock dispatch portion of a
// full timer/manual peer-ready fetch-refill run.
func ApplyFetchRefillRunPostLockPlan(plan FetchRefillRunPlan, applier FetchRefillDispatchPlanApplier) FetchRefillRunApplyResult {
	return FetchRefillRunApplyResult{
		Plan:     plan,
		Dispatch: ApplyFetchRefillDispatchPlan(plan.Dispatch, applier),
	}
}

// ApplyEmptyDrainRunLockedPlan executes the lock-held portion of an empty
// local drain run.
func ApplyEmptyDrainRunLockedPlan(plan EmptyDrainRunPlan, applier EmptyDrainRunPlanApplier) EmptyDrainRunApplyResult {
	var result EmptyDrainRunApplyResult
	if applier == nil {
		return result
	}
	if len(plan.LockedSteps) == 0 {
		plan = plan.withLockedSteps()
	}
	for _, step := range plan.LockedSteps {
		switch step.Action {
		case EmptyDrainMirrorLegacy:
			applier.MirrorLegacyUnderLock()
			result.Locked.AppliedSteps = append(result.Locked.AppliedSteps, step.Action)
		default:
			result.Locked.UnknownSteps = append(result.Locked.UnknownSteps, step.Action)
		}
	}
	return result
}

// ApplyEmptyDrainRunPostLockPlan executes post-lock idle settlement for one
// empty local drain run.
func ApplyEmptyDrainRunPostLockPlan(plan EmptyDrainRunPlan, applier IdleDrainPlanApplier) EmptyDrainRunApplyResult {
	return EmptyDrainRunApplyResult{
		Idle: ApplyIdleDrainAfterRefillPlan(plan.Refill.Idle, applier),
	}
}

// ApplyEmptyDrainRunDispatchPlan executes the final dispatch phase for one
// empty local drain run.
func ApplyEmptyDrainRunDispatchPlan(plan EmptyDrainRunPlan, applier FetchRefillDispatchPlanApplier) EmptyDrainRunApplyResult {
	return EmptyDrainRunApplyResult{
		Dispatch: ApplyFetchRefillDispatchPlan(plan.Refill.Dispatch, applier),
	}
}

// ApplyEmptyDrainRunAfterUnlockPlan executes the post-lock phases for one
// empty local drain run: session idle settlement first, then network dispatch.
func ApplyEmptyDrainRunAfterUnlockPlan(plan EmptyDrainRunPlan, idleApplier IdleDrainPlanApplier, dispatchApplier FetchRefillDispatchPlanApplier) EmptyDrainRunApplyResult {
	return EmptyDrainRunApplyResult{
		Idle:     ApplyIdleDrainAfterRefillPlan(plan.Refill.Idle, idleApplier),
		Dispatch: ApplyFetchRefillDispatchPlan(plan.Refill.Dispatch, dispatchApplier),
	}
}

// ApplyIdleDrainAfterRefillPlan executes the downloader-owned empty-drain
// settlement schedule.
func ApplyIdleDrainAfterRefillPlan(plan IdleDrainPlan, applier IdleDrainPlanApplier) IdleDrainApplyResult {
	var result IdleDrainApplyResult
	if applier == nil {
		return result
	}
	if len(plan.Steps) == 0 {
		plan = plan.withSteps()
	}
	for _, step := range plan.Steps {
		switch step.Action {
		case IdleDrainFinish:
			applier.FinishSync()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case IdleDrainJoinAvailablePeers:
			applier.JoinAvailablePeers()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		default:
			result.UnknownSteps = append(result.UnknownSteps, step.Action)
		}
	}
	return result
}

// ApplyFetchRefillDispatchPlan executes downloader-owned refill dispatch
// operations.
func ApplyFetchRefillDispatchPlan(plan FetchRefillDispatchPlan, applier FetchRefillDispatchPlanApplier) FetchRefillDispatchApplyResult {
	var result FetchRefillDispatchApplyResult
	if applier == nil {
		return result
	}
	if len(plan.Steps) == 0 {
		plan = plan.withSteps()
	}
	for _, step := range plan.Steps {
		switch step.Action {
		case FetchRefillDispatchSendOutbound:
			applier.SendOutboundRequests()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		default:
			result.UnknownSteps = append(result.UnknownSteps, step.Action)
		}
	}
	return result
}

// PlanPostInventorySettlement decides how the service should settle a sync
// session after accepting an inventory response and refilling fetch slots.
func PlanPostInventorySettlement(in PostInventorySettlementInput) PostInventorySettlementPlan {
	if !in.Progress.Syncing || in.Progress.Paused {
		return PostInventorySettlementPlan{Reset: true, TryFindPeer: true}.withSteps()
	}
	if in.OutboundRequests == 0 && in.Progress.ShouldRestartForStalledRetries() {
		return PostInventorySettlementPlan{Reset: true, TryFindPeer: true}.withSteps()
	}
	if in.Progress.ShouldFinish() {
		return PostInventorySettlementPlan{Mirror: true, Finish: true}.withSteps()
	}
	return PostInventorySettlementPlan{Mirror: true}.withSteps()
}

// PlanPostInventoryRun derives the full downloader decision after a peer
// inventory response has refilled fetch slots.
func PlanPostInventoryRun(in PostInventoryRunInput) PostInventoryRunPlan {
	return PostInventoryRunPlan{
		Settlement: PlanPostInventorySettlement(PostInventorySettlementInput{
			OutboundRequests: in.OutboundRequests,
			Progress:         in.Progress,
		}),
		Dispatch: PlanFetchRefillDispatch(FetchRefillDispatchInput{
			OutboundRequests: in.OutboundRequests,
			Progress:         in.Progress,
		}),
	}
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

// ApplyPostInventoryRunLockedPlan executes the lock-held portion of a full
// post-inventory run.
func ApplyPostInventoryRunLockedPlan(plan PostInventoryRunPlan, applier PostInventorySettlementPlanApplier) PostInventoryRunApplyResult {
	return PostInventoryRunApplyResult{
		Plan:             plan,
		LockedSettlement: ApplyPostInventorySettlementLockedPlan(plan.Settlement, applier),
	}
}

// ApplyPostInventoryRun creates and applies the lock-held portion of the
// downloader-owned post-inventory run from current session progress. The
// returned plan is reused by the caller for post-lock dispatch and settlement.
func ApplyPostInventoryRun(in PostInventoryRunInput, applier PostInventorySettlementPlanApplier) PostInventoryRunApplyResult {
	return ApplyPostInventoryRunLockedPlan(PlanPostInventoryRun(in), applier)
}

// ApplyPostInventoryRunPostLockPlan executes post-lock dispatch followed by
// post-dispatch settlement for a full post-inventory run.
func ApplyPostInventoryRunPostLockPlan(plan PostInventoryRunPlan, dispatchApplier FetchRefillDispatchPlanApplier, settlementApplier PostInventorySettlementPlanApplier) PostInventoryRunApplyResult {
	return PostInventoryRunApplyResult{
		Plan:                    plan,
		Dispatch:                ApplyFetchRefillDispatchPlan(plan.Dispatch, dispatchApplier),
		AfterDispatchSettlement: ApplyPostInventorySettlementAfterDispatchPlan(plan.Settlement, settlementApplier),
	}
}

// ApplyPostInventorySettlementLockedPlan executes the lock-held settlement
// steps for a post-inventory plan.
func ApplyPostInventorySettlementLockedPlan(plan PostInventorySettlementPlan, applier PostInventorySettlementPlanApplier) PostInventorySettlementApplyResult {
	var result PostInventorySettlementApplyResult
	if applier == nil {
		return result
	}
	if len(plan.LockedSteps) == 0 {
		plan = plan.withSteps()
	}
	for _, step := range plan.LockedSteps {
		switch step.Action {
		case PostInventoryReset:
			applier.ResetSyncUnderLock()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case PostInventoryMirror:
			applier.MirrorLegacyUnderLock()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		default:
			result.UnknownSteps = append(result.UnknownSteps, step.Action)
		}
	}
	return result
}

// ApplyPostInventorySettlementAfterDispatchPlan executes the post-dispatch
// settlement steps for a post-inventory plan.
func ApplyPostInventorySettlementAfterDispatchPlan(plan PostInventorySettlementPlan, applier PostInventorySettlementPlanApplier) PostInventorySettlementApplyResult {
	var result PostInventorySettlementApplyResult
	if applier == nil {
		return result
	}
	if len(plan.AfterDispatchSteps) == 0 {
		plan = plan.withSteps()
	}
	for _, step := range plan.AfterDispatchSteps {
		switch step.Action {
		case PostInventoryTryFindPeer:
			applier.TryFindSyncPeer()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case PostInventoryFinish:
			applier.FinishSync()
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		default:
			result.UnknownSteps = append(result.UnknownSteps, step.Action)
		}
	}
	return result
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
