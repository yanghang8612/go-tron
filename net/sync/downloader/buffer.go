package downloader

import (
	"errors"
	"fmt"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	tsync "github.com/tronprotocol/go-tron/net/sync"
	"github.com/tronprotocol/go-tron/p2p"
)

// BufferedBlock holds an out-of-order sync block awaiting contiguous drain.
// It stores the raw wire bytes plus light metadata rather than the decoded
// *types.Block: a decoded block pins its full proto tree and can balloon the
// GC mark set when many blocks are waiting on a gap.
type BufferedBlock struct {
	Raw  []byte
	Hash tcommon.Hash
	Num  uint64
	Peer *p2p.Peer
}

// NewBufferedBlock returns a buffered sync block with self-owned wire bytes.
func NewBufferedBlock(peer *p2p.Peer, block *types.Block, raw []byte) BufferedBlock {
	return BufferedBlock{
		Raw:  RawBlockBytes(block, raw),
		Hash: block.Hash(),
		Num:  block.Number(),
		Peer: peer,
	}
}

// RawBlockBytes returns a self-owned copy of the block's wire bytes for the
// sync buffer. raw is the exact payload received off the wire; callers without
// it pass nil and the bytes are re-marshaled from the decoded block.
func RawBlockBytes(block *types.Block, raw []byte) []byte {
	if len(raw) == 0 {
		b, _ := block.Marshal()
		return b
	}
	out := make([]byte, len(raw))
	copy(out, raw)
	return out
}

// BufferedBatch is the contiguous raw block run popped for local import.
type BufferedBatch struct {
	Blocks      []*types.Block
	Buffered    []BufferedBlock
	BufferWaits []time.Duration
}

// BufferedBatchDecodeAction tells the local drain loop what to do after
// decoding a raw buffered batch.
type BufferedBatchDecodeAction uint8

const (
	BufferedBatchDecodeContinue BufferedBatchDecodeAction = iota
	BufferedBatchDecodeImport
)

// BufferedBatchDecodeResult records the decoded-prefix decision for one local
// import chunk.
type BufferedBatchDecodeResult struct {
	Action  BufferedBatchDecodeAction
	Dropped BufferedBlock
	Err     error
}

// BufferedBlockMetadataMismatchError reports a decoded staged-body entry whose
// raw payload does not match the number/hash stored in the sync buffer.
type BufferedBlockMetadataMismatchError struct {
	ExpectedNum  uint64
	ExpectedHash tcommon.Hash
	GotNum       uint64
	GotHash      tcommon.Hash
}

func (e *BufferedBlockMetadataMismatchError) Error() string {
	if e == nil {
		return "sync buffered block metadata mismatch"
	}
	return fmt.Sprintf("sync buffered block metadata mismatch: expected #%d %x got #%d %x",
		e.ExpectedNum, e.ExpectedHash, e.GotNum, e.GotHash)
}

// AppliedBatchSummary is the service-neutral accounting for the prefix of a
// buffered batch that canonical insertion accepted.
type AppliedBatchSummary struct {
	OK       bool
	Applied  int
	TxCount  int
	TxKinds  map[string]int
	Last     BufferedBlock
	HasStage bool
}

// SummarizeAppliedBatch derives stats and the stage-progress boundary from an
// applied prefix of a buffered batch. Callers still own DB deletes, stage
// writes, logging, and sync stats emission.
func SummarizeAppliedBatch(batch BufferedBatch, applied int) AppliedBatchSummary {
	if applied <= 0 || applied > len(batch.Buffered) {
		return AppliedBatchSummary{}
	}
	summary := AppliedBatchSummary{
		OK:      true,
		Applied: applied,
		Last:    batch.Buffered[applied-1],
	}
	for i := 0; i < applied && i < len(batch.Blocks); i++ {
		if block := batch.Blocks[i]; block != nil {
			txs := block.Transactions()
			summary.TxCount += len(txs)
			for _, tx := range txs {
				if tx == nil {
					continue
				}
				if summary.TxKinds == nil {
					summary.TxKinds = make(map[string]int)
				}
				summary.TxKinds[tx.ContractType().String()]++
			}
		}
	}
	summary.HasStage = summary.Last.Num > 0
	return summary
}

// AppliedStagedBlockDeletes returns the staged body rows covered by an applied
// prefix of a buffered batch.
func AppliedStagedBlockDeletes(batch BufferedBatch, applied int) []rawdb.SyncStagedBlockDelete {
	if applied <= 0 || len(batch.Buffered) == 0 {
		return nil
	}
	if applied > len(batch.Buffered) {
		applied = len(batch.Buffered)
	}
	deletes := make([]rawdb.SyncStagedBlockDelete, 0, applied)
	for i := 0; i < applied; i++ {
		deletes = append(deletes, rawdb.SyncStagedBlockDelete{
			Number: batch.Buffered[i].Num,
			Hash:   batch.Buffered[i].Hash,
		})
	}
	return deletes
}

// ImportFailureResolution describes which prefix was applied before a staged
// import failed and which buffered block should be used to pause sync.
type ImportFailureResolution struct {
	OK          bool
	Applied     int
	FailedIndex int
	Failed      BufferedBlock
	FailedNum   uint64
}

// ImportOutcome describes how the local drain loop should settle a canonical
// insert attempt for one decoded buffered batch.
type ImportOutcome struct {
	Applied       int
	RecordApplied bool
	Pause         bool
	PausePeer     *p2p.Peer
	PauseNum      uint64
	StopDrain     bool
}

// ImportBatchExecutionPlan is the downloader-owned execution target for one
// decoded staged-body chunk. SyncService executes Blocks, while StagePlan names
// the expected bodies/execution/commitment/finish task graph for every decoded
// block. Schedule is the final boundary retained for concise diagnostics.
type ImportBatchExecutionPlan struct {
	Blocks           []*types.Block
	Schedules        []ImportStageSchedule
	StagePlan        ImportBatchStagePlan
	StagePhases      ImportBatchStagePhaseSchedule
	Schedule         ImportStageSchedule
	HasStageSchedule bool
	Diagnostics      ImportBatchExecutionPlanDiagnostics
}

// ImportBatchExecutionAttempt is the runnable form of an execution plan. It
// binds the explicit bodies/execution/commitment/finish phase schedule to the
// collector that accepts only planner-owned canonical stage observations.
type ImportBatchExecutionAttempt struct {
	Execution          ImportBatchExecutionPlan
	StagePhaseSchedule ImportBatchStagePhaseSchedule
	ExecutionPhases    []ImportStagePhasePlan
	PostBodyPhases     []ImportStagePhasePlan
	PostBodyTasks      []ImportStageTask
	Collector          *StageProgressCollector
	Diagnostics        ImportBatchExecutionPlanDiagnostics
}

// ImportBatchExecutionAttemptExecutor applies an execution attempt's decoded
// blocks with the attempt-owned stage observer.
type ImportBatchExecutionAttemptExecutor func(blocks []*types.Block, observe StageProgressWriter) error

// ImportBatchStageHookExecutor applies an execution attempt through the
// canonical importer that accepts core stage hooks.
type ImportBatchStageHookExecutor interface {
	InsertBlocksWithStageHook(blocks []*types.Block, hook core.StageProgressHook) error
}

// ImportBatchExecutionAttemptResult records the runtime outcome of executing a
// downloader-owned import attempt.
type ImportBatchExecutionAttemptResult struct {
	Elapsed time.Duration
	Err     error
}

// ImportBatchExecutionPlanDiagnostics is the compact, log-safe view of the
// downloader-owned execution/commitment/finish schedule for a decoded batch.
type ImportBatchExecutionPlanDiagnostics struct {
	PlannedBlocks           int
	PlannedStages           int
	PlannedBodyStages       int
	PlannedPostBodyStages   int
	PlannedExecutionStages  int
	PlannedCommitmentStages int
	PlannedFinishStages     int
	FirstBlockNum           uint64
	FirstBlockHash          tcommon.Hash
	LastBlockNum            uint64
	LastBlockHash           tcommon.Hash
}

// AppliedSchedule returns the stage schedule for the last block in an applied
// prefix of this execution plan.
func (p ImportBatchExecutionPlan) AppliedSchedule(applied int) (ImportStageSchedule, bool) {
	if applied <= 0 || applied > len(p.Schedules) {
		return ImportStageSchedule{}, false
	}
	return p.Schedules[applied-1], true
}

// AppliedStagePlan returns the explicit bodies/execution/commitment/finish
// batch plan for the canonical prefix that was actually accepted.
func (p ImportBatchExecutionPlan) AppliedStagePlan(applied int) (ImportBatchStagePlan, bool) {
	if applied <= 0 || applied > len(p.Schedules) {
		return ImportBatchStagePlan{}, false
	}
	return NewImportBatchStagePlan(p.Schedules[:applied]), true
}

// AppliedPhaseSchedule returns the explicit bodies/execution/commitment/finish
// phase schedule for the canonical prefix that was actually accepted.
func (p ImportBatchExecutionPlan) AppliedPhaseSchedule(applied int) (ImportBatchStagePhaseSchedule, bool) {
	stagePlan, ok := p.AppliedStagePlan(applied)
	if !ok {
		return ImportBatchStagePhaseSchedule{}, false
	}
	return NewImportBatchStagePhaseSchedule(stagePlan), true
}

// AppliedPhaseCursor maps a planned/observed import-stage result onto the
// execution plan's explicit phase schedule for the accepted prefix. This keeps
// restart/report cursors tied to the same bodies/execution/commitment/finish
// graph that was created before canonical insertion ran.
func (p ImportBatchExecutionPlan) AppliedPhaseCursor(applied int, stagePlan ImportStagePlan) (ImportStagePhaseCursor, bool) {
	phaseSchedule, ok := p.AppliedPhaseSchedule(applied)
	if !ok {
		return ImportStagePhaseCursor{}, false
	}
	return PlanImportStagePhaseCursor(phaseSchedule, stagePlan), true
}

// PhaseSchedule returns the explicit phase schedule for this execution plan.
func (p ImportBatchExecutionPlan) PhaseSchedule() ImportBatchStagePhaseSchedule {
	if !p.StagePhases.Empty() {
		return p.StagePhases
	}
	return NewImportBatchStagePhaseSchedule(p.StagePlan)
}

// ProgressPlan derives the DB-side progress/cleanup plan for an applied prefix
// of this execution plan. This keeps the applied boundary tied to the explicit
// bodies/execution/commitment/finish schedule created before canonical import.
func (p ImportBatchExecutionPlan) ProgressPlan(batch BufferedBatch, applied int, collector *StageProgressCollector) ImportedBatchProgressPlan {
	return PlanImportedBatchProgressForExecution(batch, applied, p, collector)
}

// NewImportBatchExecutionAttempt prepares the runnable execution attempt for a
// decoded batch. SyncService executes this object; downloader owns the stage
// phase schedule and the collector used to build restart progress.
func NewImportBatchExecutionAttempt(execution ImportBatchExecutionPlan) ImportBatchExecutionAttempt {
	phases := execution.PhaseSchedule()
	return ImportBatchExecutionAttempt{
		Execution:          execution,
		StagePhaseSchedule: phases,
		ExecutionPhases:    phases.PhasePlans(),
		PostBodyPhases:     phases.PostBodyPhasePlans(),
		PostBodyTasks:      phases.PostBodyStageTasks(),
		Collector:          NewStageProgressCollector(),
		Diagnostics:        execution.Diagnostics,
	}
}

// StageProgressObserver returns the hook passed to canonical insertion for
// this attempt. Only observations owned by the execution plan are recorded.
func (a ImportBatchExecutionAttempt) StageProgressObserver() StageProgressWriter {
	return a.Execution.StageProgressObserver(a.Collector)
}

// ProgressPlan derives the DB-side progress/cleanup plan from the observations
// recorded by this execution attempt.
func (a ImportBatchExecutionAttempt) ProgressPlan(batch BufferedBatch, applied int) ImportedBatchProgressPlan {
	return a.Execution.ProgressPlan(batch, applied, a.Collector)
}

// RunImportBatchExecutionAttempt executes a runnable attempt through the
// supplied canonical inserter. The helper keeps elapsed timing and the
// attempt-owned stage observer in downloader instead of re-deriving them in
// SyncService.
func RunImportBatchExecutionAttempt(attempt ImportBatchExecutionAttempt, execute ImportBatchExecutionAttemptExecutor, now func() time.Time) ImportBatchExecutionAttemptResult {
	if now == nil {
		now = time.Now
	}
	start := now()
	var err error
	if execute != nil {
		err = execute(attempt.Execution.Blocks, attempt.StageProgressObserver())
	}
	elapsed := now().Sub(start)
	if elapsed < 0 {
		elapsed = 0
	}
	return ImportBatchExecutionAttemptResult{Elapsed: elapsed, Err: err}
}

// RunImportBatchExecutionAttemptWithStageHook executes a runnable attempt
// through a canonical importer that accepts core stage hooks. This keeps the
// planned stage observer bound to the downloader-owned attempt rather than
// rebuilding the hook adapter in SyncService.
func RunImportBatchExecutionAttemptWithStageHook(attempt ImportBatchExecutionAttempt, execute ImportBatchStageHookExecutor, now func() time.Time) ImportBatchExecutionAttemptResult {
	return RunImportBatchExecutionAttempt(attempt, func(blocks []*types.Block, observe StageProgressWriter) error {
		if execute == nil {
			return nil
		}
		return execute.InsertBlocksWithStageHook(blocks, core.StageProgressHook(observe))
	}, now)
}

// PlansStageObservation reports whether a canonical insertion hook observation
// belongs to one of the execution plan's explicit stage schedules.
func (p ImportBatchExecutionPlan) PlansStageObservation(stage rawdb.StageID, blockNum uint64, blockHash tcommon.Hash) bool {
	_, ok := p.PlannedStageObservation(stage, blockNum, blockHash)
	return ok
}

// PlannedStageObservation returns the phase-owned stage task matching a
// canonical insertion hook observation.
func (p ImportBatchExecutionPlan) PlannedStageObservation(stage rawdb.StageID, blockNum uint64, blockHash tcommon.Hash) (ImportStageObservation, bool) {
	if observation, ok := p.PhaseSchedule().MatchPhaseObservation(stage, blockNum, blockHash); ok {
		return observation, true
	}
	if !p.StagePlan.Empty() {
		return ImportStageObservation{}, false
	}
	for _, schedule := range p.Schedules {
		task, ok := schedule.MatchCanonicalObservation(stage, blockNum, blockHash)
		if !ok {
			continue
		}
		phase, ok := newImportStagePhasePlan(task.Phase, task.CanonicalStage, task.SyncStage, []ImportStageTask{task})
		if !ok {
			return ImportStageObservation{}, false
		}
		return ImportStageObservation{Phase: phase, Task: task}, true
	}
	return ImportStageObservation{}, false
}

// StageObservationObserver filters canonical stage hook observations through
// the execution plan and emits phase-owned observations.
func (p ImportBatchExecutionPlan) StageObservationObserver(observe ImportStageObservationWriter) StageProgressWriter {
	if observe == nil {
		return nil
	}
	if p.StagePlan.Empty() && len(p.Schedules) == 0 {
		return func(rawdb.StageID, uint64, tcommon.Hash) {}
	}
	return func(stage rawdb.StageID, blockNum uint64, blockHash tcommon.Hash) {
		if observation, ok := p.PlannedStageObservation(stage, blockNum, blockHash); ok {
			observe(observation)
		}
	}
}

// StageProgressObserver filters canonical stage hook observations and records
// phase-owned progress rows in collector.
func (p ImportBatchExecutionPlan) StageProgressObserver(collector *StageProgressCollector) StageProgressWriter {
	if collector == nil {
		return nil
	}
	return p.StageObservationObserver(collector.ObservePlanned)
}

// StageObserver filters canonical stage hook observations through the execution
// plan before they reach a legacy canonical-stage writer.
func (p ImportBatchExecutionPlan) StageObserver(observe StageProgressWriter) StageProgressWriter {
	if observe == nil {
		return nil
	}
	return p.StageObservationObserver(func(observation ImportStageObservation) {
		task := observation.Task
		observe(task.CanonicalStage, task.BlockNum, task.BlockHash)
	})
}

// ImportBatchRunStepAction names one ordered local operation for executing a
// decoded staged-body chunk through the canonical chain importer.
type ImportBatchRunStepAction uint8

const (
	ImportBatchRunDecode ImportBatchRunStepAction = iota
	ImportBatchRunRecordBufferWaits
	ImportBatchRunPlanExecution
	ImportBatchRunPlanStagePhases
	ImportBatchRunPrepareAttempt
	ImportBatchRunExecute
	ImportBatchRunSettle
)

// ImportBatchRunStep is one downloader-owned local import operation. The
// service supplies side effects; downloader owns the ordering.
type ImportBatchRunStep struct {
	Action ImportBatchRunStepAction
}

// ImportBatchRunPlan is the explicit local execution schedule for one popped
// staged-body batch.
type ImportBatchRunPlan struct {
	Batch BufferedBatch
	Steps []ImportBatchRunStep
}

// ImportBatchRunPlanApplier performs side effects for an import-batch run.
// SyncService owns logging, stats, canonical execution, and pause handling.
type ImportBatchRunPlanApplier interface {
	LogDecodeBatchResult(BufferedBatchDecodeResult)
	RecordBufferWait(time.Duration)
	ExecuteImportBatch(attempt ImportBatchExecutionAttempt) (time.Duration, error)
	ApplyImportedBatchRecord(plan ImportedBatchRecordPlan) ImportedBatchRecordApplyResult
	PauseImport(peer *p2p.Peer, blockNum uint64, err error)
}

// ImportBatchRunResult reports the outcome of applying an import-batch run
// plan. ContinueDrain means decode produced no importable prefix; StopDrain
// means canonical import failed, imported-prefix progress could not be durably
// recorded, or a scheduler-owned resume phase is pending, so the caller should
// leave the drain loop.
type ImportBatchRunResult struct {
	Plan                 ImportBatchRunPlan
	Decode               BufferedBatchDecodeResult
	Execution            ImportBatchExecutionPlan
	ExecutionAttempt     ImportBatchExecutionAttempt
	StagePhaseSchedule   ImportBatchStagePhaseSchedule
	ExecutionPhases      []ImportStagePhasePlan
	PostBodyPhases       []ImportStagePhasePlan
	PostBodyTasks        []ImportStageTask
	Outcome              ImportOutcome
	Progress             ImportedBatchProgressPlan
	RecordPlan           ImportedBatchRecordPlan
	RecordApply          ImportedBatchRecordApplyResult
	HasRecord            bool
	ExecutionDiagnostics ImportBatchExecutionPlanDiagnostics
	StageDiagnostics     ImportStagePlanDiagnostics
	ResumePhasePlan      ImportStagePhasePlan
	HasResumePhasePlan   bool
	Steps                []ImportBatchRunStepAction
	ContinueDrain        bool
	StopDrain            bool
}

// ImportBatchRunApplyResult groups an applied import-batch run with the
// downloader-owned drain-loop settlement decision derived from it.
type ImportBatchRunApplyResult struct {
	Plan           ImportBatchRunPlan
	Run            ImportBatchRunResult
	Settlement     ImportBatchRunSettlementPlan
	DrainLoop      ImportBatchDrainLoopPlan
	DrainLoopApply ImportBatchDrainLoopApplyResult
}

// RecordWriteFailed reports whether the accepted prefix reached canonical
// insertion but failed to persist the corresponding sync-stage boundary.
func (r ImportBatchRunResult) RecordWriteFailed() bool {
	return r.HasRecord && r.RecordApply.ProgressApply.WriteFailed()
}

// RecordProgressFailed reports whether imported-prefix storage/runtime
// progress failed after canonical insertion accepted the prefix.
func (r ImportBatchRunResult) RecordProgressFailed() bool {
	return r.HasRecord && r.RecordApply.ProgressApply.Failed()
}

// ImportBatchRunSettlementAction names the local drain-loop branch selected
// after one import-batch run.
type ImportBatchRunSettlementAction uint8

const (
	ImportBatchRunSettlementContinueDrain ImportBatchRunSettlementAction = iota + 1
	ImportBatchRunSettlementYieldResumePhase
	ImportBatchRunSettlementStopDrain
)

// ImportBatchRunSettlementPlan maps a completed import-batch run back to the
// caller's local drain loop.
type ImportBatchRunSettlementPlan struct {
	Action           ImportBatchRunSettlementAction
	ContinueDrain    bool
	StopDrain        bool
	YieldResumePhase bool
	ResumePhasePlan  ImportStagePhasePlan
}

// ImportBatchDrainLoopStepAction names the caller's next drain-loop branch
// after an import-batch settlement.
type ImportBatchDrainLoopStepAction uint8

const (
	ImportBatchDrainLoopContinue ImportBatchDrainLoopStepAction = iota
	ImportBatchDrainLoopStop
	ImportBatchDrainLoopYieldResumePhase
)

// ImportBatchDrainLoopStep is one downloader-owned drain-loop operation after
// a local import batch settles.
type ImportBatchDrainLoopStep struct {
	Action          ImportBatchDrainLoopStepAction
	ResumePhasePlan ImportStagePhasePlan
}

// ImportBatchDrainLoopPlan maps an import-batch settlement into the local
// drain loop's next action. SyncService owns the loop mechanics; downloader
// owns the settlement semantics.
type ImportBatchDrainLoopPlan struct {
	ContinueLoop     bool
	StopLoop         bool
	YieldResumePhase bool
	ResumePhasePlan  ImportStagePhasePlan
	Steps            []ImportBatchDrainLoopStep
}

// ImportBatchDrainLoopApplyResult records the local drain-loop branch selected
// from the downloader-owned step list.
type ImportBatchDrainLoopApplyResult struct {
	Action           ImportBatchDrainLoopStepAction
	ContinueLoop     bool
	StopLoop         bool
	YieldResumePhase bool
	ResumePhasePlan  ImportStagePhasePlan
	AppliedSteps     []ImportBatchDrainLoopStepAction
	UnknownSteps     []ImportBatchDrainLoopStepAction
}

// NewImportBatchRunPlan returns the local staged-body execution schedule for
// one popped batch: decode raw bodies, account wait time, plan canonical
// stages, prepare a runnable attempt with its observer, execute it, then settle
// progress/pause decisions.
func NewImportBatchRunPlan(batch BufferedBatch) ImportBatchRunPlan {
	return ImportBatchRunPlan{
		Batch: batch,
		Steps: []ImportBatchRunStep{
			{Action: ImportBatchRunDecode},
			{Action: ImportBatchRunRecordBufferWaits},
			{Action: ImportBatchRunPlanExecution},
			{Action: ImportBatchRunPlanStagePhases},
			{Action: ImportBatchRunPrepareAttempt},
			{Action: ImportBatchRunExecute},
			{Action: ImportBatchRunSettle},
		},
	}
}

// ApplyImportBatchRun creates and applies the downloader-owned local import
// run plan, then derives the drain-loop settlement from the run result.
func ApplyImportBatchRun(batch BufferedBatch, applier ImportBatchRunPlanApplier) ImportBatchRunApplyResult {
	plan := NewImportBatchRunPlan(batch)
	run := ApplyImportBatchRunPlan(plan, applier)
	settlement := PlanImportBatchRunSettlement(run)
	drainLoop := PlanImportBatchDrainLoop(settlement)
	return ImportBatchRunApplyResult{
		Plan:           plan,
		Run:            run,
		Settlement:     settlement,
		DrainLoop:      drainLoop,
		DrainLoopApply: ApplyImportBatchDrainLoopPlan(drainLoop),
	}
}

// ApplyImportBatchRunPlan executes the downloader-owned local import schedule.
func ApplyImportBatchRunPlan(plan ImportBatchRunPlan, applier ImportBatchRunPlanApplier) ImportBatchRunResult {
	if applier == nil {
		return ImportBatchRunResult{}
	}
	var (
		result    = ImportBatchRunResult{Plan: plan}
		attempt   ImportBatchExecutionAttempt
		insertErr error
		elapsed   time.Duration
		planned   bool
		prepared  bool
		executed  bool
	)
	ensureExecutionPlanned := func() {
		if planned {
			return
		}
		result.Execution = PlanImportBatchExecution(plan.Batch)
		result.ExecutionDiagnostics = result.Execution.Diagnostics
		planned = true
	}
	ensureStagePhasesPlanned := func() {
		ensureExecutionPlanned()
		if result.StagePhaseSchedule.Empty() {
			result.StagePhaseSchedule = result.Execution.PhaseSchedule()
		}
		if result.ExecutionPhases == nil {
			result.ExecutionPhases = result.StagePhaseSchedule.PhasePlans()
		}
		if result.PostBodyPhases == nil {
			result.PostBodyPhases = result.StagePhaseSchedule.PostBodyPhasePlans()
		}
		if result.PostBodyTasks == nil {
			result.PostBodyTasks = result.StagePhaseSchedule.PostBodyStageTasks()
		}
	}
	prepareAttempt := func() {
		if prepared {
			return
		}
		ensureStagePhasesPlanned()
		attempt = NewImportBatchExecutionAttempt(result.Execution)
		if result.StagePhaseSchedule.Empty() {
			result.StagePhaseSchedule = attempt.StagePhaseSchedule
		}
		if result.ExecutionPhases == nil {
			result.ExecutionPhases = cloneImportStagePhasePlanList(attempt.ExecutionPhases)
		}
		if result.PostBodyPhases == nil {
			result.PostBodyPhases = cloneImportStagePhasePlanList(attempt.PostBodyPhases)
		}
		if result.PostBodyTasks == nil {
			result.PostBodyTasks = append([]ImportStageTask(nil), attempt.PostBodyTasks...)
		}
		result.ExecutionAttempt = attempt
		prepared = true
	}
	for _, step := range plan.Steps {
		result.Steps = append(result.Steps, step.Action)
		switch step.Action {
		case ImportBatchRunDecode:
			result.Decode = DecodeBufferedBatch(&plan.Batch)
			applier.LogDecodeBatchResult(result.Decode)
			if result.Decode.Action != BufferedBatchDecodeImport {
				result.ContinueDrain = true
				return result
			}
		case ImportBatchRunRecordBufferWaits:
			for _, wait := range plan.Batch.BufferWaits {
				applier.RecordBufferWait(wait)
			}
		case ImportBatchRunPlanExecution:
			ensureExecutionPlanned()
		case ImportBatchRunPlanStagePhases:
			ensureStagePhasesPlanned()
		case ImportBatchRunPrepareAttempt:
			prepareAttempt()
		case ImportBatchRunExecute:
			prepareAttempt()
			elapsed, insertErr = applier.ExecuteImportBatch(attempt)
			executed = true
		case ImportBatchRunSettle:
			if !executed {
				continue
			}
			result.Outcome = PlanImportOutcome(plan.Batch, insertErr)
			if result.Outcome.RecordApplied {
				result.Progress = attempt.ProgressPlan(plan.Batch, result.Outcome.Applied)
				result.StageDiagnostics = result.Progress.StageDiagnostics
				if resume, ok := result.Progress.RemainingCurrentPhasePlan(); ok {
					result.ResumePhasePlan = resume
					result.HasResumePhasePlan = true
				}
				if result.Progress.OK {
					result.RecordPlan = PlanImportedBatchRecord(result.Progress, elapsed)
					result.RecordApply = applier.ApplyImportedBatchRecord(result.RecordPlan)
					result.HasRecord = true
				}
			}
			if result.Outcome.Pause {
				applier.PauseImport(result.Outcome.PausePeer, result.Outcome.PauseNum, insertErr)
			}
			result.StopDrain = result.Outcome.StopDrain || result.RecordProgressFailed() || result.HasResumePhasePlan
		}
	}
	return result
}

// PlanImportBatchRunSettlement derives the drain-loop branch after applying an
// import-batch run plan. Decode-only drops and successful imports both keep the
// local drain loop moving; canonical import failures, persisted progress
// failures, and pending scheduler resume phases stop. Resume phases use a
// distinct yield action so the staged scheduler can distinguish intentional
// handoff from sticky pause/storage failure stops before another chunk advances.
func PlanImportBatchRunSettlement(result ImportBatchRunResult) ImportBatchRunSettlementPlan {
	if result.Outcome.StopDrain || result.RecordProgressFailed() {
		return ImportBatchRunSettlementPlan{
			Action:    ImportBatchRunSettlementStopDrain,
			StopDrain: true,
		}
	}
	if result.HasResumePhasePlan {
		return ImportBatchRunSettlementPlan{
			Action:           ImportBatchRunSettlementYieldResumePhase,
			StopDrain:        true,
			YieldResumePhase: true,
			ResumePhasePlan:  cloneImportStagePhasePlan(result.ResumePhasePlan),
		}
	}
	if result.StopDrain {
		return ImportBatchRunSettlementPlan{
			Action:    ImportBatchRunSettlementStopDrain,
			StopDrain: true,
		}
	}
	return ImportBatchRunSettlementPlan{
		Action:        ImportBatchRunSettlementContinueDrain,
		ContinueDrain: true,
	}
}

// PlanImportBatchDrainLoop derives the caller's next local drain-loop branch
// from the downloader-owned import settlement.
func PlanImportBatchDrainLoop(settlement ImportBatchRunSettlementPlan) ImportBatchDrainLoopPlan {
	if settlement.Action == ImportBatchRunSettlementYieldResumePhase || settlement.YieldResumePhase {
		return ImportBatchDrainLoopPlan{
			StopLoop:         true,
			YieldResumePhase: true,
			ResumePhasePlan:  cloneImportStagePhasePlan(settlement.ResumePhasePlan),
		}.withSteps()
	}
	if settlement.Action == ImportBatchRunSettlementStopDrain || settlement.StopDrain {
		return ImportBatchDrainLoopPlan{StopLoop: true}.withSteps()
	}
	return ImportBatchDrainLoopPlan{ContinueLoop: true}.withSteps()
}

// ApplyImportBatchDrainLoopPlan resolves the downloader-owned drain-loop step
// list into the caller's loop branch. The caller still owns the concrete loop
// mechanics, but it no longer needs to interpret plan booleans directly.
func ApplyImportBatchDrainLoopPlan(plan ImportBatchDrainLoopPlan) ImportBatchDrainLoopApplyResult {
	var result ImportBatchDrainLoopApplyResult
	if len(plan.Steps) == 0 {
		plan = plan.withSteps()
	}
	for _, step := range plan.Steps {
		switch step.Action {
		case ImportBatchDrainLoopContinue:
			result.Action = step.Action
			result.ContinueLoop = true
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case ImportBatchDrainLoopStop:
			result.Action = step.Action
			result.StopLoop = true
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case ImportBatchDrainLoopYieldResumePhase:
			result.Action = step.Action
			result.StopLoop = true
			result.YieldResumePhase = true
			result.ResumePhasePlan = cloneImportStagePhasePlan(step.ResumePhasePlan)
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		default:
			result.UnknownSteps = append(result.UnknownSteps, step.Action)
		}
	}
	return result
}

func (p ImportBatchDrainLoopPlan) withSteps() ImportBatchDrainLoopPlan {
	if p.YieldResumePhase {
		p.StopLoop = true
		p.Steps = []ImportBatchDrainLoopStep{{
			Action:          ImportBatchDrainLoopYieldResumePhase,
			ResumePhasePlan: cloneImportStagePhasePlan(p.ResumePhasePlan),
		}}
	} else if p.StopLoop {
		p.Steps = []ImportBatchDrainLoopStep{{Action: ImportBatchDrainLoopStop}}
	} else if p.ContinueLoop {
		p.Steps = []ImportBatchDrainLoopStep{{Action: ImportBatchDrainLoopContinue}}
	}
	return p
}

// PlanImportBatchExecution returns the explicit canonical import target for a
// decoded staged-body chunk.
func PlanImportBatchExecution(batch BufferedBatch) ImportBatchExecutionPlan {
	execution := ImportBatchExecutionPlan{
		Blocks: append([]*types.Block(nil), batch.Blocks...),
	}
	for i := 0; i < len(batch.Blocks) && i < len(batch.Buffered); i++ {
		buffered := batch.Buffered[i]
		if buffered.Num == 0 {
			continue
		}
		execution.Schedules = append(execution.Schedules, NewImportStageSchedule(buffered.Num, buffered.Hash))
	}
	if len(execution.Schedules) > 0 {
		execution.StagePlan = NewImportBatchStagePlan(execution.Schedules)
		execution.StagePhases = NewImportBatchStagePhaseSchedule(execution.StagePlan)
		execution.Schedule = execution.Schedules[len(execution.Schedules)-1]
		execution.HasStageSchedule = true
		execution.Diagnostics = NewImportBatchExecutionPlanDiagnostics(execution.Schedules, execution.StagePlan)
	}
	return execution
}

// NewImportBatchExecutionPlanDiagnostics summarizes an explicit batch stage
// schedule without depending on canonical stage hook observations.
func NewImportBatchExecutionPlanDiagnostics(schedules []ImportStageSchedule, stagePlan ImportBatchStagePlan) ImportBatchExecutionPlanDiagnostics {
	var diag ImportBatchExecutionPlanDiagnostics
	if len(schedules) == 0 {
		return diag
	}
	diag.PlannedBlocks = len(schedules)
	diag.PlannedStages = len(stagePlan.Tasks)
	diag.PlannedBodyStages = importStagePhaseTaskCount(stagePlan, ImportStagePhaseBodies)
	diag.PlannedPostBodyStages = len(stagePlan.PostBody)
	diag.PlannedExecutionStages = importStagePhaseTaskCount(stagePlan, ImportStagePhaseExecution)
	diag.PlannedCommitmentStages = importStagePhaseTaskCount(stagePlan, ImportStagePhaseCommitment)
	diag.PlannedFinishStages = importStagePhaseTaskCount(stagePlan, ImportStagePhaseFinish)
	diag.FirstBlockNum = schedules[0].BlockNum
	diag.FirstBlockHash = schedules[0].BlockHash
	last := schedules[len(schedules)-1]
	diag.LastBlockNum = last.BlockNum
	diag.LastBlockHash = last.BlockHash
	return diag
}

func importStagePhaseTaskCount(stagePlan ImportBatchStagePlan, phase ImportStagePhase) int {
	phasePlan, ok := stagePlan.PhasePlan(phase)
	if !ok {
		return 0
	}
	return len(phasePlan.Tasks)
}

// PlanImportOutcome maps a canonical insert result into service actions:
// record the applied prefix, pause on failure, and decide whether the local
// drain loop should stop.
func PlanImportOutcome(batch BufferedBatch, insertErr error) ImportOutcome {
	if insertErr == nil {
		applied := len(batch.Blocks)
		return ImportOutcome{
			Applied:       applied,
			RecordApplied: applied > 0,
		}
	}
	outcome := ImportOutcome{
		Pause:     true,
		StopDrain: true,
	}
	failure := ResolveImportFailure(batch, insertErr)
	if !failure.OK {
		return outcome
	}
	outcome.Applied = failure.Applied
	outcome.RecordApplied = failure.Applied > 0
	outcome.PausePeer = failure.Failed.Peer
	outcome.PauseNum = failure.FailedNum
	return outcome
}

// ResolveImportFailure maps a canonical range-insert error back onto the
// buffered downloader batch. InsertBlocksError.Index names the first failed
// block; generic errors conservatively pause at the first buffered block.
func ResolveImportFailure(batch BufferedBatch, insertErr error) ImportFailureResolution {
	if insertErr == nil || len(batch.Buffered) == 0 {
		return ImportFailureResolution{}
	}
	failed := 0
	var rangeErr *core.InsertBlocksError
	if errors.As(insertErr, &rangeErr) && rangeErr.Index >= 0 && rangeErr.Index < len(batch.Buffered) {
		failed = rangeErr.Index
	}
	failedBlock := batch.Buffered[failed]
	failedNum := failedBlock.Num
	if failedNum == 0 && rangeErr != nil {
		failedNum = rangeErr.BlockNumber
	}
	return ImportFailureResolution{
		OK:          true,
		Applied:     failed,
		FailedIndex: failed,
		Failed:      failedBlock,
		FailedNum:   failedNum,
	}
}

// ImportBatchLimit resolves the local staged-body import chunk from the
// operator setting and sync package-wide bounds.
func ImportBatchLimit(configured int) int {
	if configured <= 0 {
		return tsync.MaxImportBatch
	}
	if configured > tsync.MaxFetchBatch {
		return tsync.MaxFetchBatch
	}
	return configured
}

// StagedBodyDrainLimit clamps one local staged-body drain chunk to the
// hash-verified SyncBodiesReady frontier. The returned bool is false when the
// ready frontier is behind the next needed block, so callers should not drain
// from the local buffer.
func StagedBodyDrainLimit(next uint64, max int, readyLimit uint64, hasReadyLimit bool) (int, bool) {
	if max <= 0 {
		return 0, false
	}
	if !hasReadyLimit {
		return max, true
	}
	if readyLimit < next {
		return 0, false
	}
	if span := readyLimit - next + 1; span < uint64(max) {
		return int(span), true
	}
	return max, true
}

// StagedBodyDrainPlan is the local import chunk decision derived from the
// persisted ready frontier and the operator's import-batch limit.
type StagedBodyDrainPlan struct {
	RestoreLimit  int
	CanDrain      bool
	ReadyLimit    uint64
	HasReadyLimit bool
	RefreshReady  bool
	Steps         []StagedBodyDrainStep
}

// StagedBodyDrainStepAction names one ordered local operation required to drain
// a staged-body prefix into canonical import.
type StagedBodyDrainStepAction uint8

const (
	StagedBodyDrainRefreshReady StagedBodyDrainStepAction = iota
	StagedBodyDrainRestoreBodies
	StagedBodyDrainPopBuffer
)

// StagedBodyDrainStep is one downloader-owned step in a local staged-body drain.
type StagedBodyDrainStep struct {
	Action         StagedBodyDrainStepAction
	From           uint64
	Next           uint64
	Limit          int
	PruneStaleTail bool
}

// StagedBodyDrainApplyResult records the drain preparation steps that were
// actually dispatched plus the restored batch popped for canonical import.
type StagedBodyDrainApplyResult struct {
	AppliedSteps         []StagedBodyDrainStepAction
	UnknownSteps         []StagedBodyDrainStepAction
	ReadyRefresh         StagedBodyReadyProgressRefresh
	HasReadyRefresh      bool
	StagedBodyRestore    StagedBodyRestoreResult
	HasStagedBodyRestore bool
	Batch                BufferedBatch
}

// Failed reports whether the pre-import staged-body drain preparation failed.
func (r StagedBodyDrainApplyResult) Failed() bool {
	return r.HasReadyRefresh && r.ReadyRefresh.Failed()
}

// StagedBodyDrainPlanApplier performs the persistence/runtime operations named
// by a staged-body drain plan. SyncService owns DB handles and in-memory maps;
// downloader owns the ordered local import preparation.
type StagedBodyDrainPlanApplier interface {
	RefreshSyncBodiesReady() StagedBodyReadyProgressRefresh
	RestoreStagedBodies(from uint64, limit int, pruneStaleTail bool) StagedBodyRestoreResult
	PopBufferedBatch(next uint64, limit int) BufferedBatch
}

// PlanStagedBodyDrain decides how many staged bodies the local importer may
// restore and pop. Only a valid SyncBodiesReady row clamps the chunk; invalid
// rows first refresh the ready frontier; if that repair succeeds, the caller
// may fall back to an uncapped local drain so any already-contiguous in-memory
// prefix can still import.
func PlanStagedBodyDrain(next uint64, max int, ready StagedBodyReadyLimit) StagedBodyDrainPlan {
	var plan StagedBodyDrainPlan
	if ready.Valid() {
		plan.ReadyLimit = ready.Limit
		plan.HasReadyLimit = true
	}
	if ShouldRefreshStagedBodyReadyBeforeDrain(ready.Status) {
		plan.RefreshReady = true
	}
	plan.RestoreLimit, plan.CanDrain = StagedBodyDrainLimit(next, max, plan.ReadyLimit, plan.HasReadyLimit)
	return plan.withSteps(next)
}

// ShouldRefreshStagedBodyReadyBeforeDrain reports whether a persisted
// SyncBodiesReady row is unusable enough that the downloader should repair the
// ready frontier before planning a local import chunk.
func ShouldRefreshStagedBodyReadyBeforeDrain(status StagedBodyReadyLimitStatus) bool {
	switch status {
	case StagedBodyReadyLimitMissing, StagedBodyReadyLimitValid:
		return false
	default:
		return true
	}
}

func (p StagedBodyDrainPlan) withSteps(next uint64) StagedBodyDrainPlan {
	if p.RefreshReady {
		p.Steps = append(p.Steps, StagedBodyDrainStep{Action: StagedBodyDrainRefreshReady})
	}
	if p.CanDrain {
		p.Steps = append(p.Steps,
			StagedBodyDrainStep{Action: StagedBodyDrainRestoreBodies, From: next, Limit: p.RestoreLimit},
			StagedBodyDrainStep{Action: StagedBodyDrainPopBuffer, Next: next, Limit: p.RestoreLimit},
		)
	}
	return p
}

// ApplyStagedBodyDrainPlan executes the downloader-owned local drain schedule.
func ApplyStagedBodyDrainPlan(plan StagedBodyDrainPlan, applier StagedBodyDrainPlanApplier) StagedBodyDrainApplyResult {
	var result StagedBodyDrainApplyResult
	if applier == nil {
		return result
	}
	for _, step := range plan.Steps {
		switch step.Action {
		case StagedBodyDrainRefreshReady:
			result.ReadyRefresh = applier.RefreshSyncBodiesReady()
			result.HasReadyRefresh = true
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
			if result.ReadyRefresh.Failed() {
				return result
			}
		case StagedBodyDrainRestoreBodies:
			result.StagedBodyRestore = applier.RestoreStagedBodies(step.From, step.Limit, step.PruneStaleTail)
			result.HasStagedBodyRestore = true
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case StagedBodyDrainPopBuffer:
			result.Batch = applier.PopBufferedBatch(step.Next, step.Limit)
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		default:
			result.UnknownSteps = append(result.UnknownSteps, step.Action)
		}
	}
	return result
}

// PopBufferedBatch removes the contiguous run starting at next from the local
// raw block buffer. Popping also releases the session path reservation and hash
// de-dup entry because canonical import, or a sticky pause on failure, owns the
// block number after this point.
func PopBufferedBatch(buffer map[uint64]BufferedBlock, bufferedHashes map[tcommon.Hash]struct{}, path BlockPath, wait *BufferWaitTracker, next uint64, limit int, now time.Time) BufferedBatch {
	var batch BufferedBatch
	if limit <= 0 {
		return batch
	}
	for len(batch.Buffered) < limit {
		buffered, ok := buffer[next]
		if !ok {
			break
		}
		batch.BufferWaits = append(batch.BufferWaits, wait.End(next, now))
		delete(buffer, next)
		path.Release(next)
		delete(bufferedHashes, buffered.Hash)
		batch.Buffered = append(batch.Buffered, buffered)
		next++
	}
	return batch
}

// DecodeBlocks decodes raw buffered entries into Blocks. It preserves the
// successfully decoded prefix and reports the first undecodable entry; callers
// can import the prefix and refetch the dropped suffix.
func (b *BufferedBatch) DecodeBlocks() (BufferedBlock, error) {
	if b == nil {
		return BufferedBlock{}, nil
	}
	b.Blocks = make([]*types.Block, 0, len(b.Buffered))
	for _, buffered := range b.Buffered {
		block, err := types.UnmarshalBlock(buffered.Raw)
		if err != nil {
			return buffered, err
		}
		if err := ValidateBufferedBlockMetadata(buffered, block); err != nil {
			return buffered, err
		}
		b.Blocks = append(b.Blocks, block)
	}
	return BufferedBlock{}, nil
}

// ValidateBufferedBlockMetadata verifies that a decoded staged-body payload
// still matches the number/hash used to schedule and de-duplicate it.
func ValidateBufferedBlockMetadata(buffered BufferedBlock, block *types.Block) error {
	if block == nil {
		return errors.New("sync buffered block decoded to nil")
	}
	gotNum := block.Number()
	gotHash := block.Hash()
	var zeroHash tcommon.Hash
	numMismatch := buffered.Num != 0 && buffered.Num != gotNum
	hashMismatch := buffered.Hash != zeroHash && buffered.Hash != gotHash
	if !numMismatch && !hashMismatch {
		return nil
	}
	return &BufferedBlockMetadataMismatchError{
		ExpectedNum:  buffered.Num,
		ExpectedHash: buffered.Hash,
		GotNum:       gotNum,
		GotHash:      gotHash,
	}
}

// DecodeBufferedBatch decodes a local import chunk and returns the next drain
// action. A decode error after a non-empty prefix still imports that prefix;
// an error on the first entry asks the caller to continue the drain loop.
func DecodeBufferedBatch(batch *BufferedBatch) BufferedBatchDecodeResult {
	dropped, err := batch.DecodeBlocks()
	result := BufferedBatchDecodeResult{
		Dropped: dropped,
		Err:     err,
	}
	if batch != nil && len(batch.Blocks) > 0 {
		result.Action = BufferedBatchDecodeImport
	}
	return result
}
