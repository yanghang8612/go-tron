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
			summary.TxCount += len(block.Transactions())
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
	ExecuteImportBatch(execution ImportBatchExecutionPlan, observe StageProgressWriter) (time.Duration, error)
	ApplyImportedBatchRecord(plan ImportedBatchRecordPlan) ImportedBatchRecordApplyResult
	PauseImport(peer *p2p.Peer, blockNum uint64, err error)
}

// ImportBatchRunResult reports the outcome of applying an import-batch run
// plan. ContinueDrain means decode produced no importable prefix; StopDrain
// means canonical import failed and the caller should leave the drain loop.
type ImportBatchRunResult struct {
	Decode               BufferedBatchDecodeResult
	Execution            ImportBatchExecutionPlan
	StagePhaseSchedule   ImportBatchStagePhaseSchedule
	ExecutionPhases      []ImportStagePhasePlan
	Outcome              ImportOutcome
	Progress             ImportedBatchProgressPlan
	RecordPlan           ImportedBatchRecordPlan
	RecordApply          ImportedBatchRecordApplyResult
	HasRecord            bool
	ExecutionDiagnostics ImportBatchExecutionPlanDiagnostics
	StageDiagnostics     ImportStagePlanDiagnostics
	Steps                []ImportBatchRunStepAction
	ContinueDrain        bool
	StopDrain            bool
}

// ImportBatchRunApplyResult groups an applied import-batch run with the
// downloader-owned drain-loop settlement decision derived from it.
type ImportBatchRunApplyResult struct {
	Run        ImportBatchRunResult
	Settlement ImportBatchRunSettlementPlan
}

// ImportBatchRunSettlementAction names the local drain-loop branch selected
// after one import-batch run.
type ImportBatchRunSettlementAction uint8

const (
	ImportBatchRunSettlementContinueDrain ImportBatchRunSettlementAction = iota + 1
	ImportBatchRunSettlementStopDrain
)

// ImportBatchRunSettlementPlan maps a completed import-batch run back to the
// caller's local drain loop.
type ImportBatchRunSettlementPlan struct {
	Action        ImportBatchRunSettlementAction
	ContinueDrain bool
	StopDrain     bool
}

// ImportBatchDrainLoopStepAction names the caller's next drain-loop branch
// after an import-batch settlement.
type ImportBatchDrainLoopStepAction uint8

const (
	ImportBatchDrainLoopContinue ImportBatchDrainLoopStepAction = iota
	ImportBatchDrainLoopStop
)

// ImportBatchDrainLoopStep is one downloader-owned drain-loop operation after
// a local import batch settles.
type ImportBatchDrainLoopStep struct {
	Action ImportBatchDrainLoopStepAction
}

// ImportBatchDrainLoopPlan maps an import-batch settlement into the local
// drain loop's next action. SyncService owns the loop mechanics; downloader
// owns the settlement semantics.
type ImportBatchDrainLoopPlan struct {
	ContinueLoop bool
	StopLoop     bool
	Steps        []ImportBatchDrainLoopStep
}

// NewImportBatchRunPlan returns the local staged-body execution schedule for
// one popped batch: decode raw bodies, account wait time, execute canonical
// stages with an explicit observer, then settle progress/pause decisions.
func NewImportBatchRunPlan(batch BufferedBatch) ImportBatchRunPlan {
	return ImportBatchRunPlan{
		Batch: batch,
		Steps: []ImportBatchRunStep{
			{Action: ImportBatchRunDecode},
			{Action: ImportBatchRunRecordBufferWaits},
			{Action: ImportBatchRunPlanExecution},
			{Action: ImportBatchRunPlanStagePhases},
			{Action: ImportBatchRunExecute},
			{Action: ImportBatchRunSettle},
		},
	}
}

// ApplyImportBatchRun creates and applies the downloader-owned local import
// run plan, then derives the drain-loop settlement from the run result.
func ApplyImportBatchRun(batch BufferedBatch, applier ImportBatchRunPlanApplier) ImportBatchRunApplyResult {
	run := ApplyImportBatchRunPlan(NewImportBatchRunPlan(batch), applier)
	return ImportBatchRunApplyResult{
		Run:        run,
		Settlement: PlanImportBatchRunSettlement(run),
	}
}

// ApplyImportBatchRunPlan executes the downloader-owned local import schedule.
func ApplyImportBatchRunPlan(plan ImportBatchRunPlan, applier ImportBatchRunPlanApplier) ImportBatchRunResult {
	if applier == nil {
		return ImportBatchRunResult{}
	}
	var (
		result    ImportBatchRunResult
		collector *StageProgressCollector
		insertErr error
		elapsed   time.Duration
		planned   bool
		executed  bool
	)
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
			result.Execution = PlanImportBatchExecution(plan.Batch)
			result.ExecutionDiagnostics = result.Execution.Diagnostics
			planned = true
		case ImportBatchRunPlanStagePhases:
			if !planned {
				result.Execution = PlanImportBatchExecution(plan.Batch)
				result.ExecutionDiagnostics = result.Execution.Diagnostics
				planned = true
			}
			result.StagePhaseSchedule = result.Execution.PhaseSchedule()
			result.ExecutionPhases = result.StagePhaseSchedule.PhasePlans()
		case ImportBatchRunExecute:
			if !planned {
				result.Execution = PlanImportBatchExecution(plan.Batch)
				result.ExecutionDiagnostics = result.Execution.Diagnostics
				planned = true
			}
			if result.StagePhaseSchedule.Empty() {
				result.StagePhaseSchedule = result.Execution.PhaseSchedule()
			}
			if result.ExecutionPhases == nil {
				result.ExecutionPhases = result.StagePhaseSchedule.PhasePlans()
			}
			collector = NewStageProgressCollector()
			elapsed, insertErr = applier.ExecuteImportBatch(result.Execution, result.Execution.StageProgressObserver(collector))
			executed = true
		case ImportBatchRunSettle:
			if !executed {
				continue
			}
			result.Outcome = PlanImportOutcome(plan.Batch, insertErr)
			if result.Outcome.RecordApplied {
				result.Progress = result.Execution.ProgressPlan(plan.Batch, result.Outcome.Applied, collector)
				result.StageDiagnostics = result.Progress.StageDiagnostics
				if result.Progress.OK {
					result.RecordPlan = PlanImportedBatchRecord(result.Progress, elapsed)
					result.RecordApply = applier.ApplyImportedBatchRecord(result.RecordPlan)
					result.HasRecord = true
				}
			}
			if result.Outcome.Pause {
				applier.PauseImport(result.Outcome.PausePeer, result.Outcome.PauseNum, insertErr)
			}
			result.StopDrain = result.Outcome.StopDrain
		}
	}
	return result
}

// PlanImportBatchRunSettlement derives the drain-loop branch after applying an
// import-batch run plan. Decode-only drops and successful imports both keep the
// local drain loop moving; canonical import failures stop so the sticky pause
// can be observed by the session.
func PlanImportBatchRunSettlement(result ImportBatchRunResult) ImportBatchRunSettlementPlan {
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
	if settlement.Action == ImportBatchRunSettlementStopDrain || settlement.StopDrain {
		return ImportBatchDrainLoopPlan{StopLoop: true}.withSteps()
	}
	return ImportBatchDrainLoopPlan{ContinueLoop: true}.withSteps()
}

func (p ImportBatchDrainLoopPlan) withSteps() ImportBatchDrainLoopPlan {
	if p.StopLoop {
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
// rows first refresh the ready frontier, then fall back to an uncapped local
// drain so the caller can still import any already-contiguous in-memory prefix.
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
