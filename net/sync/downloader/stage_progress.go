package downloader

import (
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/p2p"
)

// StageProgressRow is one observed hash-bound progress row for a sync pipeline
// stage.
type StageProgressRow struct {
	BlockNum uint64
	Hash     tcommon.Hash
}

// StageProgressWriter persists hash-bound sync pipeline progress.
type StageProgressWriter func(stage rawdb.StageID, blockNum uint64, blockHash tcommon.Hash)

// StageProgressReader reads one hash-bound or legacy stage progress row.
type StageProgressReader func(stage rawdb.StageID) (rawdb.StageProgress, bool, error)

// StageProgressErrorWriter persists hash-bound sync pipeline progress and
// reports write failures to the planner apply result.
type StageProgressErrorWriter func(stage rawdb.StageID, blockNum uint64, blockHash tcommon.Hash) error

// ImportStageObservationWriter records one phase-owned import-stage
// observation accepted by an execution plan.
type ImportStageObservationWriter func(ImportStageObservation)

// CanonicalHashReader returns the canonical block hash for number.
type CanonicalHashReader func(number uint64) (tcommon.Hash, bool)

// SyncStageProgressRepairStatus describes startup repair for one sync
// pipeline diagnostic stage row.
type SyncStageProgressRepairStatus uint8

const (
	SyncStageProgressMissing SyncStageProgressRepairStatus = iota
	SyncStageProgressKept
	SyncStageProgressDeleted
	SyncStageProgressReadError
	SyncStageProgressDeleteError
)

// SyncStageProgressRepair is the result of validating and possibly deleting
// one sync pipeline stage row at session startup.
type SyncStageProgressRepair struct {
	Stage         rawdb.StageID
	Status        SyncStageProgressRepairStatus
	Row           rawdb.StageProgress
	CanonicalHash tcommon.Hash
	ReadError     error
	DeleteError   error
}

// SyncPipelineProgressRepairResult is the structured restart-repair outcome
// for the hash-bound sync import pipeline.
type SyncPipelineProgressRepairResult struct {
	Repairs           []SyncStageProgressRepair
	Kept              int
	Missing           int
	Deleted           int
	FirstBlockedStage rawdb.StageID
	HasBlocked        bool
	Interrupted       bool
	ErrorStage        rawdb.StageID
	Complete          bool
}

// SyncPipelineProgressOrderOptions controls how strictly sync-stage order is
// validated. Startup repair is strict; operator diagnostics may allow missing
// upstream rows while still rejecting downstream rows that are ahead.
type SyncPipelineProgressOrderOptions struct {
	RequireUpstream bool
}

// SyncPipelineProgressOrderPair is one downstream <= upstream invariant in the
// full downloader sync pipeline.
type SyncPipelineProgressOrderPair struct {
	Downstream rawdb.StageID
	Upstream   rawdb.StageID
}

// SyncPipelineProgressOrderIssue describes one sync-stage ordering violation.
type SyncPipelineProgressOrderIssue struct {
	Downstream      rawdb.StageID
	DownstreamBlock uint64
	DownstreamHash  tcommon.Hash
	Upstream        rawdb.StageID
	UpstreamBlock   uint64
	UpstreamHash    tcommon.Hash
	MissingUpstream bool
	HashMismatch    bool
}

// SyncPipelineProgressOrderReadError records a stage row that could not be
// read while checking full sync pipeline ordering.
type SyncPipelineProgressOrderReadError struct {
	Stage rawdb.StageID
	Err   error
}

// SyncPipelineProgressOrderRepair records one stage row updated or deleted
// while repairing full sync-stage ordering.
type SyncPipelineProgressOrderRepair struct {
	Stage       rawdb.StageID
	Row         rawdb.StageProgress
	Issue       SyncPipelineProgressOrderIssue
	Updated     bool
	DeleteError error
	WriteError  error
}

// SyncPipelineProgressOrderCheckResult is the DB-backed full sync pipeline
// order check result used by startup diagnostics.
type SyncPipelineProgressOrderCheckResult struct {
	Rows       []rawdb.StageProgress
	Issues     []SyncPipelineProgressOrderIssue
	ReadErrors []SyncPipelineProgressOrderReadError
}

// SyncPipelineProgressCursor is the restart cursor derived from the repaired
// full sync pipeline rows. It names the downstream-most persisted stage and
// the next stage the downloader should expect to advance.
type SyncPipelineProgressCursor struct {
	StageRows    int
	HasLast      bool
	LastStage    rawdb.StageID
	LastBlock    uint64
	LastHash     tcommon.Hash
	LastHasHash  bool
	HasNext      bool
	NextStage    rawdb.StageID
	Complete     bool
	HasBlocked   bool
	BlockedIssue SyncPipelineProgressOrderIssue
	Interrupted  bool
	ErrorStage   rawdb.StageID
}

// SyncPipelineProgressHeadCompletionPlan names downstream sync-stage rows that
// can be completed at startup because the canonical head already proves the
// full block import boundary exists.
type SyncPipelineProgressHeadCompletionPlan struct {
	Head          uint64
	HeadHash      tcommon.Hash
	HasHeadPrefix bool
	LastStage     rawdb.StageID
	LastBlock     uint64
	FillStages    []rawdb.StageID
	Complete      bool
}

// SyncPipelineProgressHeadCompletion records the result of applying a
// head-completion plan. The caller owns the actual stage-progress writes.
type SyncPipelineProgressHeadCompletion struct {
	Plan       SyncPipelineProgressHeadCompletionPlan
	Written    int
	WriteError error
	ErrorStage rawdb.StageID
	Complete   bool
}

// SyncPipelineProgressOrderRepairResult is the DB-backed startup repair result
// for full sync-stage ordering.
type SyncPipelineProgressOrderRepairResult struct {
	Before      SyncPipelineProgressOrderCheckResult
	After       SyncPipelineProgressOrderCheckResult
	Repairs     []SyncPipelineProgressOrderRepair
	Deleted     int
	Updated     int
	Interrupted bool
	ErrorStage  rawdb.StageID
	Complete    bool
}

func (i SyncPipelineProgressOrderIssue) String() string {
	if i.MissingUpstream {
		return fmt.Sprintf("%s requires %s", i.Downstream, i.Upstream)
	}
	if i.HashMismatch {
		return fmt.Sprintf("%s=%d/%x hash does not match %s=%d/%x", i.Downstream, i.DownstreamBlock, i.DownstreamHash, i.Upstream, i.UpstreamBlock, i.UpstreamHash)
	}
	return fmt.Sprintf("%s=%d ahead of %s=%d", i.Downstream, i.DownstreamBlock, i.Upstream, i.UpstreamBlock)
}

func (r *SyncPipelineProgressRepairResult) add(repair SyncStageProgressRepair) {
	r.Repairs = append(r.Repairs, repair)
	switch repair.Status {
	case SyncStageProgressKept:
		r.Kept++
	case SyncStageProgressMissing:
		r.Missing++
	case SyncStageProgressDeleted:
		r.Deleted++
	case SyncStageProgressReadError, SyncStageProgressDeleteError:
		r.Interrupted = true
		r.ErrorStage = repair.Stage
	}
}

func (r *SyncPipelineProgressRepairResult) blockAt(stage rawdb.StageID) {
	if r.HasBlocked {
		return
	}
	r.HasBlocked = true
	r.FirstBlockedStage = stage
}

// ImportStageProgressStatus records how one sync pipeline stage was handled
// when planning imported-batch progress.
type ImportStageProgressStatus uint8

const (
	ImportStageProgressPlanned ImportStageProgressStatus = iota
	ImportStageProgressMissing
	ImportStageProgressMismatch
	ImportStageProgressBlocked
)

func (s ImportStageProgressStatus) String() string {
	switch s {
	case ImportStageProgressPlanned:
		return "planned"
	case ImportStageProgressMissing:
		return "missing"
	case ImportStageProgressMismatch:
		return "mismatch"
	case ImportStageProgressBlocked:
		return "blocked"
	default:
		return "unknown"
	}
}

// ImportStagePhase names the sync import subphase a task belongs to. The
// schedule keeps execution/commitment/finish as explicit tasks rather than
// treating canonical hooks as the source of ordering.
type ImportStagePhase string

const (
	ImportStagePhaseBodies     ImportStagePhase = "bodies"
	ImportStagePhaseExecution  ImportStagePhase = "execution"
	ImportStagePhaseCommitment ImportStagePhase = "commitment"
	ImportStagePhaseFinish     ImportStagePhase = "finish"
)

// ImportStageTask is one explicit canonical import stage target. The
// downloader publishes the matching sync diagnostic stage only after the
// canonical stage hook observes this exact block/hash boundary.
type ImportStageTask struct {
	Phase          ImportStagePhase
	CanonicalStage rawdb.StageID
	SyncStage      rawdb.StageID
	BlockNum       uint64
	BlockHash      tcommon.Hash
}

// ImportStageSpec is one phase in the downloader-owned import stage planner.
// The order returned by ImportStageSpecs is the only place that defines the
// bodies -> execution -> commitment -> finish schedule.
type ImportStageSpec struct {
	Phase          ImportStagePhase
	CanonicalStage rawdb.StageID
	SyncStage      rawdb.StageID
}

var importStageSpecs = []ImportStageSpec{
	{Phase: ImportStagePhaseBodies, CanonicalStage: rawdb.StageBodies, SyncStage: rawdb.StageSyncImport},
	{Phase: ImportStagePhaseExecution, CanonicalStage: rawdb.StageExecution, SyncStage: rawdb.StageSyncExecution},
	{Phase: ImportStagePhaseCommitment, CanonicalStage: rawdb.StageCommitment, SyncStage: rawdb.StageSyncCommitment},
	{Phase: ImportStagePhaseFinish, CanonicalStage: rawdb.StageFinish, SyncStage: rawdb.StageSyncFinish},
}

// ImportStageSpecs returns the downloader import stage planner phases in
// execution order.
func ImportStageSpecs() []ImportStageSpec {
	return append([]ImportStageSpec(nil), importStageSpecs...)
}

// Task returns this phase's concrete stage target for one import boundary.
func (s ImportStageSpec) Task(blockNum uint64, blockHash tcommon.Hash) ImportStageTask {
	return ImportStageTask{
		Phase:          s.Phase,
		CanonicalStage: s.CanonicalStage,
		SyncStage:      s.SyncStage,
		BlockNum:       blockNum,
		BlockHash:      blockHash,
	}
}

func importStageSpecForPhase(phase ImportStagePhase) (ImportStageSpec, bool) {
	for _, spec := range importStageSpecs {
		if spec.Phase == phase {
			return spec, true
		}
	}
	return ImportStageSpec{}, false
}

func importStageSpecForCanonicalStage(stage rawdb.StageID) (ImportStageSpec, bool) {
	for _, spec := range importStageSpecs {
		if spec.CanonicalStage == stage {
			return spec, true
		}
	}
	return ImportStageSpec{}, false
}

func importStageTaskForPhase(phase ImportStagePhase, blockNum uint64, blockHash tcommon.Hash) ImportStageTask {
	spec, ok := importStageSpecForPhase(phase)
	if !ok {
		return ImportStageTask{}
	}
	return spec.Task(blockNum, blockHash)
}

// ImportStageObservation is one canonical stage hook observation accepted by
// the downloader-owned phase plan.
type ImportStageObservation struct {
	Phase ImportStagePhasePlan
	Task  ImportStageTask
}

// Valid reports whether the observation is owned by its phase plan. This keeps
// planned hook observations tied to the downloader stage schedule instead of
// accepting an arbitrary task-shaped record.
func (o ImportStageObservation) Valid() bool {
	if o.Task.Phase == "" || o.Task.CanonicalStage == "" || o.Task.SyncStage == "" {
		return false
	}
	if o.Phase.Phase != o.Task.Phase ||
		o.Phase.CanonicalStage != o.Task.CanonicalStage ||
		o.Phase.SyncStage != o.Task.SyncStage {
		return false
	}
	task, ok := o.Phase.MatchCanonicalObservation(o.Task.CanonicalStage, o.Task.BlockNum, o.Task.BlockHash)
	return ok && task == o.Task
}

// ImportStagePhasePlan is the batch-level unit the downloader stage planner
// owns: one phase, its canonical/sync stage pair, and every block boundary that
// phase must complete.
type ImportStagePhasePlan struct {
	Phase          ImportStagePhase
	CanonicalStage rawdb.StageID
	SyncStage      rawdb.StageID
	Tasks          []ImportStageTask
}

// ImportStageSchedule is the explicit stage schedule for one sync import
// boundary. The canonical insert hook only supplies observations; this
// schedule owns the required bodies/execution/commitment/finish targets.
type ImportStageSchedule struct {
	BlockNum   uint64
	BlockHash  tcommon.Hash
	Body       ImportStageTask
	Execution  ImportStageTask
	Commitment ImportStageTask
	Finish     ImportStageTask
	PostBody   []ImportStageTask
	Tasks      []ImportStageTask
}

// ImportBatchStagePlan groups every canonical import task in a decoded batch
// by phase. It is the batch-level schedule; canonical insert hooks only confirm
// observations against these targets.
type ImportBatchStagePlan struct {
	Schedules  []ImportStageSchedule
	Phases     []ImportStagePhasePlan
	Bodies     []ImportStageTask
	Execution  []ImportStageTask
	Commitment []ImportStageTask
	Finish     []ImportStageTask
	// PostBody is phase-ordered: all execution tasks, then commitment tasks,
	// then finish tasks. This is the batch-level stage planner order even
	// though current canonical insertion still emits observations per block.
	PostBody []ImportStageTask
	// Tasks is phase-ordered: bodies first, then PostBody.
	Tasks []ImportStageTask
}

// ImportBatchStagePhaseSchedule is the phase-level execution schedule for a
// decoded import batch. It makes the post-body execution/commitment/finish
// phases explicit before canonical insertion emits any stage hooks.
type ImportBatchStagePhaseSchedule struct {
	Phases        []ImportStagePhasePlan
	Body          ImportStagePhasePlan
	HasBody       bool
	Execution     ImportStagePhasePlan
	HasExecution  bool
	Commitment    ImportStagePhasePlan
	HasCommitment bool
	Finish        ImportStagePhasePlan
	HasFinish     bool
	PostBody      []ImportStagePhasePlan
	Tasks         []ImportStageTask
	PostBodyTasks []ImportStageTask
}

// ImportStagePhaseCursor is the phase-level restart cursor for an applied
// import batch. It names the first incomplete phase/task in the explicit
// bodies/execution/commitment/finish schedule.
type ImportStagePhaseCursor struct {
	ScheduledPhases       int
	CompletedPhases       int
	ScheduledTasks        int
	CompletedTasks        int
	Complete              bool
	HasCurrent            bool
	CurrentPhase          ImportStagePhase
	CurrentCanonicalStage rawdb.StageID
	CurrentSyncStage      rawdb.StageID
	CurrentTasks          []ImportStageTask
	CurrentTaskIndex      int
	HasNextTask           bool
	NextTask              ImportStageTask
	HasBlocked            bool
	BlockedStatus         ImportStageProgressStatus
}

// NewImportBatchStagePlan returns the phase-indexed stage schedule for a batch
// of decoded import boundaries.
func NewImportBatchStagePlan(schedules []ImportStageSchedule) ImportBatchStagePlan {
	if len(schedules) == 0 {
		return ImportBatchStagePlan{}
	}
	plan := ImportBatchStagePlan{
		Schedules: append([]ImportStageSchedule(nil), schedules...),
	}
	for _, schedule := range schedules {
		if len(schedule.Tasks) == 0 {
			continue
		}
		plan.Bodies = append(plan.Bodies, schedule.Body)
		plan.Execution = append(plan.Execution, schedule.Execution)
		plan.Commitment = append(plan.Commitment, schedule.Commitment)
		plan.Finish = append(plan.Finish, schedule.Finish)
	}
	plan.Phases = newImportStagePhasePlans(plan)
	for _, phase := range plan.Phases {
		if phase.Phase != ImportStagePhaseBodies {
			plan.PostBody = append(plan.PostBody, phase.Tasks...)
		}
		plan.Tasks = append(plan.Tasks, phase.Tasks...)
	}
	return plan
}

// NewImportBatchStagePhaseSchedule returns the phase-level plan for an import
// batch. Bodies are scheduled first; execution, commitment, and finish are
// retained as separate post-body phases in canonical planner order.
func NewImportBatchStagePhaseSchedule(stagePlan ImportBatchStagePlan) ImportBatchStagePhaseSchedule {
	var schedule ImportBatchStagePhaseSchedule
	for _, phase := range stagePlan.PhasePlans() {
		phase = cloneImportStagePhasePlan(phase)
		schedule.Phases = append(schedule.Phases, phase)
		schedule.Tasks = append(schedule.Tasks, phase.Tasks...)
		switch phase.Phase {
		case ImportStagePhaseBodies:
			schedule.Body = phase
			schedule.HasBody = true
		case ImportStagePhaseExecution:
			schedule.Execution = phase
			schedule.HasExecution = true
			schedule.PostBody = append(schedule.PostBody, phase)
			schedule.PostBodyTasks = append(schedule.PostBodyTasks, phase.Tasks...)
		case ImportStagePhaseCommitment:
			schedule.Commitment = phase
			schedule.HasCommitment = true
			schedule.PostBody = append(schedule.PostBody, phase)
			schedule.PostBodyTasks = append(schedule.PostBodyTasks, phase.Tasks...)
		case ImportStagePhaseFinish:
			schedule.Finish = phase
			schedule.HasFinish = true
			schedule.PostBody = append(schedule.PostBody, phase)
			schedule.PostBodyTasks = append(schedule.PostBodyTasks, phase.Tasks...)
		}
	}
	return schedule
}

// Empty reports whether the batch has any scheduled canonical import tasks.
func (p ImportBatchStagePlan) Empty() bool {
	return len(p.Tasks) == 0
}

// Empty reports whether the phase schedule has any planned stage phases.
func (s ImportBatchStagePhaseSchedule) Empty() bool {
	return len(s.Phases) == 0
}

// PhasePlans returns a defensive copy of the phase-level schedule.
func (s ImportBatchStagePhaseSchedule) PhasePlans() []ImportStagePhasePlan {
	return cloneImportStagePhasePlanList(s.Phases)
}

// PostBodyPhasePlans returns a defensive copy of the execution/commitment/
// finish phase schedule. These phases are the post-body work that canonical
// insertion must complete before a batch is fully published.
func (s ImportBatchStagePhaseSchedule) PostBodyPhasePlans() []ImportStagePhasePlan {
	return cloneImportStagePhasePlanList(s.PostBody)
}

// PostBodyStageTasks returns a defensive copy of all execution/commitment/
// finish tasks in planner order.
func (s ImportBatchStagePhaseSchedule) PostBodyStageTasks() []ImportStageTask {
	return append([]ImportStageTask(nil), s.PostBodyTasks...)
}

// PhasePlan returns the scheduled phase by name.
func (s ImportBatchStagePhaseSchedule) PhasePlan(phase ImportStagePhase) (ImportStagePhasePlan, bool) {
	switch phase {
	case ImportStagePhaseBodies:
		if s.HasBody {
			return cloneImportStagePhasePlan(s.Body), true
		}
	case ImportStagePhaseExecution:
		if s.HasExecution {
			return cloneImportStagePhasePlan(s.Execution), true
		}
	case ImportStagePhaseCommitment:
		if s.HasCommitment {
			return cloneImportStagePhasePlan(s.Commitment), true
		}
	case ImportStagePhaseFinish:
		if s.HasFinish {
			return cloneImportStagePhasePlan(s.Finish), true
		}
	}
	return ImportStagePhasePlan{}, false
}

// MatchPhaseObservation returns the phase-owned stage task matching a
// canonical insertion hook observation.
func (s ImportBatchStagePhaseSchedule) MatchPhaseObservation(stage rawdb.StageID, blockNum uint64, blockHash tcommon.Hash) (ImportStageObservation, bool) {
	for _, phase := range s.Phases {
		task, ok := phase.MatchCanonicalObservation(stage, blockNum, blockHash)
		if !ok {
			continue
		}
		return ImportStageObservation{
			Phase: cloneImportStagePhasePlan(phase),
			Task:  task,
		}, true
	}
	return ImportStageObservation{}, false
}

// PlanImportStagePhaseCursor maps a task-level stage plan back to the
// explicit phase schedule. CompletedPhases counts only fully completed phases;
// a partially observed execution/commitment/finish phase remains current.
func PlanImportStagePhaseCursor(schedule ImportBatchStagePhaseSchedule, stagePlan ImportStagePlan) ImportStagePhaseCursor {
	cursor := ImportStagePhaseCursor{
		ScheduledPhases: len(schedule.Phases),
		ScheduledTasks:  len(schedule.Tasks),
		CompletedTasks:  len(stagePlan.Completed),
		Complete:        stagePlan.Complete,
	}
	if cursor.ScheduledTasks == 0 {
		cursor.ScheduledTasks = len(stagePlan.Tasks)
	}
	if len(schedule.Phases) == 0 {
		return cursor
	}
	if stagePlan.Complete {
		cursor.CompletedPhases = len(schedule.Phases)
		return cursor
	}
	if !stagePlan.HasNext {
		return cursor
	}
	cursor.HasNextTask = true
	cursor.NextTask = stagePlan.Next
	if stagePlan.HasBlocked {
		cursor.HasBlocked = true
		cursor.BlockedStatus = stagePlan.Blocked.Status
	}
	for phaseIndex, phase := range schedule.Phases {
		if phase.Phase != stagePlan.Next.Phase ||
			phase.CanonicalStage != stagePlan.Next.CanonicalStage ||
			phase.SyncStage != stagePlan.Next.SyncStage {
			continue
		}
		cursor.CompletedPhases = phaseIndex
		cursor.HasCurrent = true
		cursor.CurrentPhase = phase.Phase
		cursor.CurrentCanonicalStage = phase.CanonicalStage
		cursor.CurrentSyncStage = phase.SyncStage
		cursor.CurrentTasks = append([]ImportStageTask(nil), phase.Tasks...)
		for taskIndex, task := range phase.Tasks {
			if task == stagePlan.Next {
				cursor.CurrentTaskIndex = taskIndex
				break
			}
		}
		break
	}
	return cursor
}

// CurrentTaskRemaining returns the number of tasks left in the cursor's
// current phase. It is zero for complete cursors and for malformed cursors
// whose current task index does not point inside the current phase.
func (c ImportStagePhaseCursor) CurrentTaskRemaining() int {
	if !c.HasCurrent || c.CurrentTaskIndex < 0 || c.CurrentTaskIndex >= len(c.CurrentTasks) {
		return 0
	}
	return len(c.CurrentTasks) - c.CurrentTaskIndex
}

// RemainingCurrentPhasePlan returns the runnable suffix of the current phase.
// This is the scheduler-facing form of the restart cursor: a future staged
// import loop can resume at the first incomplete execution/commitment/finish
// task without re-deriving the batch phase graph from log diagnostics.
func (c ImportStagePhaseCursor) RemainingCurrentPhasePlan() (ImportStagePhasePlan, bool) {
	if c.CurrentTaskRemaining() == 0 {
		return ImportStagePhasePlan{}, false
	}
	return ImportStagePhasePlan{
		Phase:          c.CurrentPhase,
		CanonicalStage: c.CurrentCanonicalStage,
		SyncStage:      c.CurrentSyncStage,
		Tasks:          append([]ImportStageTask(nil), c.CurrentTasks[c.CurrentTaskIndex:]...),
	}, true
}

// PhasePlans returns a defensive copy of the batch-level stage phase plan.
func (p ImportBatchStagePlan) PhasePlans() []ImportStagePhasePlan {
	if len(p.Phases) == 0 {
		return nil
	}
	out := make([]ImportStagePhasePlan, 0, len(p.Phases))
	for _, phase := range p.Phases {
		out = append(out, cloneImportStagePhasePlan(phase))
	}
	return out
}

// PhasePlan returns the explicit batch-level plan for phase.
func (p ImportBatchStagePlan) PhasePlan(phase ImportStagePhase) (ImportStagePhasePlan, bool) {
	for _, phasePlan := range p.Phases {
		if phasePlan.Phase == phase {
			return cloneImportStagePhasePlan(phasePlan), true
		}
	}
	spec, ok := importStageSpecForPhase(phase)
	if !ok {
		return ImportStagePhasePlan{}, false
	}
	return newImportStagePhasePlan(spec.Phase, spec.CanonicalStage, spec.SyncStage, p.tasksForPhase(phase))
}

// TasksForPhase returns the batch-level stage tasks for phase in planner order.
func (p ImportBatchStagePlan) TasksForPhase(phase ImportStagePhase) []ImportStageTask {
	phasePlan, ok := p.PhasePlan(phase)
	if !ok {
		return nil
	}
	return phasePlan.Tasks
}

func newImportStagePhasePlans(plan ImportBatchStagePlan) []ImportStagePhasePlan {
	phases := make([]ImportStagePhasePlan, 0, 4)
	for _, spec := range importStageSpecs {
		phasePlan, ok := newImportStagePhasePlan(spec.Phase, spec.CanonicalStage, spec.SyncStage, plan.tasksForPhase(spec.Phase))
		if ok {
			phases = append(phases, phasePlan)
		}
	}
	return phases
}

func (p ImportBatchStagePlan) tasksForPhase(phase ImportStagePhase) []ImportStageTask {
	switch phase {
	case ImportStagePhaseBodies:
		return p.Bodies
	case ImportStagePhaseExecution:
		return p.Execution
	case ImportStagePhaseCommitment:
		return p.Commitment
	case ImportStagePhaseFinish:
		return p.Finish
	default:
		return nil
	}
}

func newImportStagePhasePlan(phase ImportStagePhase, canonical rawdb.StageID, syncStage rawdb.StageID, tasks []ImportStageTask) (ImportStagePhasePlan, bool) {
	if len(tasks) == 0 {
		return ImportStagePhasePlan{}, false
	}
	return ImportStagePhasePlan{
		Phase:          phase,
		CanonicalStage: canonical,
		SyncStage:      syncStage,
		Tasks:          append([]ImportStageTask(nil), tasks...),
	}, true
}

func cloneImportStagePhasePlan(plan ImportStagePhasePlan) ImportStagePhasePlan {
	plan.Tasks = append([]ImportStageTask(nil), plan.Tasks...)
	return plan
}

func cloneImportStagePhasePlanList(source []ImportStagePhasePlan) []ImportStagePhasePlan {
	if len(source) == 0 {
		return nil
	}
	out := make([]ImportStagePhasePlan, 0, len(source))
	for _, phase := range source {
		out = append(out, cloneImportStagePhasePlan(phase))
	}
	return out
}

// MatchCanonicalObservation reports whether this phase owns a canonical stage
// hook observation.
func (p ImportStagePhasePlan) MatchCanonicalObservation(stage rawdb.StageID, blockNum uint64, blockHash tcommon.Hash) (ImportStageTask, bool) {
	if p.CanonicalStage != stage {
		return ImportStageTask{}, false
	}
	for _, task := range p.Tasks {
		if task.BlockNum == blockNum && task.BlockHash == blockHash {
			return task, true
		}
	}
	return ImportStageTask{}, false
}

// MatchPhaseObservation returns the explicit phase and task that own a
// canonical stage hook observation.
func (p ImportBatchStagePlan) MatchPhaseObservation(stage rawdb.StageID, blockNum uint64, blockHash tcommon.Hash) (ImportStageObservation, bool) {
	for _, phase := range p.Phases {
		task, ok := phase.MatchCanonicalObservation(stage, blockNum, blockHash)
		if !ok {
			continue
		}
		return ImportStageObservation{
			Phase: cloneImportStagePhasePlan(phase),
			Task:  task,
		}, true
	}
	return ImportStageObservation{}, false
}

// MatchCanonicalObservation reports whether a canonical stage hook observation
// belongs to one of this batch plan's explicit stage tasks.
func (p ImportBatchStagePlan) MatchCanonicalObservation(stage rawdb.StageID, blockNum uint64, blockHash tcommon.Hash) (ImportStageTask, bool) {
	observation, ok := p.MatchPhaseObservation(stage, blockNum, blockHash)
	if ok {
		return observation.Task, true
	}
	return ImportStageTask{}, false
}

// MatchCanonicalObservation reports whether a canonical stage hook observation
// is one of this schedule's explicit bodies/execution/commitment/finish tasks.
func (s ImportStageSchedule) MatchCanonicalObservation(stage rawdb.StageID, blockNum uint64, blockHash tcommon.Hash) (ImportStageTask, bool) {
	for _, task := range s.Tasks {
		if task.CanonicalStage == stage && task.BlockNum == blockNum && task.BlockHash == blockHash {
			return task, true
		}
	}
	return ImportStageTask{}, false
}

// ImportStageProgressDecision is one stage planner decision for an applied
// sync import prefix. Later stages are blocked after the first missing stage so
// persisted sync progress remains a contiguous pipeline prefix.
type ImportStageProgressDecision struct {
	Task   ImportStageTask
	Stage  rawdb.StageID
	Status ImportStageProgressStatus
	Row    rawdb.StageProgress
}

// ImportStagePhaseProgress is the planner result for one explicit import
// phase. It keeps execution/commitment/finish progress visible as phase-owned
// state instead of only a flattened stage-row prefix.
type ImportStagePhaseProgress struct {
	Phase          ImportStagePhase
	CanonicalStage rawdb.StageID
	SyncStage      rawdb.StageID
	Tasks          []ImportStageTask
	Progress       rawdb.StageProgress
	HasProgress    bool
	Decisions      []ImportStageProgressDecision
	Completed      []ImportStageTask
	Next           ImportStageTask
	HasNext        bool
	Blocked        ImportStageProgressDecision
	HasBlocked     bool
	Complete       bool
}

// ImportStagePlan is the stage planner view for one local import boundary.
// It names the completed contiguous prefix and the first stage that still
// prevents the boundary from being fully published.
type ImportStagePlan struct {
	Schedule   ImportStageSchedule
	Tasks      []ImportStageTask
	Phases     []ImportStagePhaseProgress
	Progress   []rawdb.StageProgress
	Decisions  []ImportStageProgressDecision
	Completed  []ImportStageTask
	Next       ImportStageTask
	HasNext    bool
	Blocked    ImportStageProgressDecision
	HasBlocked bool
	Complete   bool
}

// ImportStagePlanDiagnostics is the compact, log-safe view of an import stage
// plan. It lets SyncService report stage planner state without re-deriving it.
type ImportStagePlanDiagnostics struct {
	Scheduled          int
	Completed          int
	Complete           bool
	NextPhase          ImportStagePhase
	NextCanonicalStage rawdb.StageID
	NextStage          rawdb.StageID
	NextBlockNum       uint64
	NextBlockHash      tcommon.Hash
	BlockedStatus      ImportStageProgressStatus
	HasBlocked         bool
}

// ImportStagePlanner owns the schedule-to-observation mapping for one import
// run. StageProgressCollector records canonical hooks; the planner turns those
// observations into an explicit bodies/execution/commitment/finish stage plan.
type ImportStagePlanner struct {
	observed map[rawdb.StageID][]StageProgressRow
}

// ImportedBatchProgressStepAction names one ordered side effect of committing an
// imported staged-body prefix.
type ImportedBatchProgressStepAction uint8

const (
	ImportedBatchWriteProgress ImportedBatchProgressStepAction = iota
	ImportedBatchRefreshBodiesReady
)

// ImportedBatchProgressStep is one downloader-owned persistence/runtime step
// after a local staged-body prefix was imported.
type ImportedBatchProgressStep struct {
	Action   ImportedBatchProgressStepAction
	Deletes  []rawdb.SyncStagedBlockDelete
	Progress []rawdb.StageProgress
}

// ImportResumePhasePublishStatus describes whether a scheduler-yielded import
// phase can be safely published after the caller has crossed its commit
// barrier.
type ImportResumePhasePublishStatus uint8

const (
	ImportResumePhasePublishReady ImportResumePhasePublishStatus = iota + 1
	ImportResumePhasePublishEmpty
	ImportResumePhasePublishReadError
	ImportResumePhasePublishCanonicalMissing
	ImportResumePhasePublishCanonicalUnbound
	ImportResumePhasePublishCanonicalMismatch
	ImportResumePhasePublishSyncAhead
	ImportResumePhasePublishSyncHashMismatch
	ImportResumePhasePublishUpstreamMissing
	ImportResumePhasePublishUpstreamUnbound
	ImportResumePhasePublishUpstreamBehind
	ImportResumePhasePublishUpstreamHashMismatch
)

func (s ImportResumePhasePublishStatus) String() string {
	switch s {
	case ImportResumePhasePublishReady:
		return "ready"
	case ImportResumePhasePublishEmpty:
		return "empty"
	case ImportResumePhasePublishReadError:
		return "read-error"
	case ImportResumePhasePublishCanonicalMissing:
		return "canonical-missing"
	case ImportResumePhasePublishCanonicalUnbound:
		return "canonical-unbound"
	case ImportResumePhasePublishCanonicalMismatch:
		return "canonical-mismatch"
	case ImportResumePhasePublishSyncAhead:
		return "sync-ahead"
	case ImportResumePhasePublishSyncHashMismatch:
		return "sync-hash-mismatch"
	case ImportResumePhasePublishUpstreamMissing:
		return "upstream-missing"
	case ImportResumePhasePublishUpstreamUnbound:
		return "upstream-unbound"
	case ImportResumePhasePublishUpstreamBehind:
		return "upstream-behind"
	case ImportResumePhasePublishUpstreamHashMismatch:
		return "upstream-hash-mismatch"
	default:
		return "unknown"
	}
}

// ImportResumePhasePublishFinalizationSkipReason explains why the post-drain
// resume-phase publish gate did not run.
type ImportResumePhasePublishFinalizationSkipReason uint8

const (
	ImportResumePhasePublishFinalizationNone ImportResumePhasePublishFinalizationSkipReason = iota
	ImportResumePhasePublishFinalizationNoPhases
	ImportResumePhasePublishFinalizationCommitBarrierFailed
	ImportResumePhasePublishFinalizationPaused
)

func (r ImportResumePhasePublishFinalizationSkipReason) String() string {
	switch r {
	case ImportResumePhasePublishFinalizationNone:
		return "none"
	case ImportResumePhasePublishFinalizationNoPhases:
		return "no-phases"
	case ImportResumePhasePublishFinalizationCommitBarrierFailed:
		return "commit-barrier-failed"
	case ImportResumePhasePublishFinalizationPaused:
		return "paused"
	default:
		return "unknown"
	}
}

// ImportResumePhasePublishDecision records one phase-level publish decision.
type ImportResumePhasePublishDecision struct {
	Phase           ImportStagePhase
	CanonicalStage  rawdb.StageID
	SyncStage       rawdb.StageID
	TargetBlock     uint64
	TargetHash      tcommon.Hash
	CanonicalRow    rawdb.StageProgress
	HasCanonicalRow bool
	SyncRow         rawdb.StageProgress
	HasSyncRow      bool
	UpstreamStage   rawdb.StageID
	UpstreamRow     rawdb.StageProgress
	HasUpstreamRow  bool
	Status          ImportResumePhasePublishStatus
	Err             error
}

// ImportResumePhasePublishPlan is the storage plan for a scheduler-yielded
// import phase suffix after the caller has crossed the canonical commit
// barrier. It publishes only when every phase in the suffix has an exact
// hash-bound canonical stage row, preventing diagnostic sync rows from moving
// past canonical execution.
type ImportResumePhasePublishPlan struct {
	Phases    []ImportStagePhasePlan
	Progress  []rawdb.StageProgress
	Decisions []ImportResumePhasePublishDecision
	Complete  bool
	OK        bool
}

// ImportResumePhasePublishPlanApplier performs the storage write selected by a
// resume-phase publish plan.
type ImportResumePhasePublishPlanApplier interface {
	WriteResumePhaseProgress(rows []rawdb.StageProgress) error
}

// ImportResumePhasePublishRunApplier performs the read/validate/write runtime
// operations for a scheduler-yielded phase suffix.
type ImportResumePhasePublishRunApplier interface {
	ReadStageProgress(stage rawdb.StageID) (rawdb.StageProgress, bool, error)
	WriteResumePhaseProgress(rows []rawdb.StageProgress) error
}

// ImportResumePhasePublishApplyResult records the write outcome for a
// scheduler-yielded phase suffix.
type ImportResumePhasePublishApplyResult struct {
	Plan       ImportResumePhasePublishPlan
	Applied    bool
	Rows       int
	WriteError error
}

// ImportResumePhasePublishRunPlan is the downloader-owned schedule for
// publishing a scheduler-yielded phase suffix after the caller's commit
// barrier has completed.
type ImportResumePhasePublishRunPlan struct {
	Phases []ImportStagePhasePlan
}

// ImportResumePhasePublishFinalizationInput is the post-drain gate for a
// scheduler-yielded phase suffix. The caller supplies commit-barrier state;
// downloader owns the publish/no-op decision.
type ImportResumePhasePublishFinalizationInput struct {
	Phases   []ImportStagePhasePlan
	FinishOK bool
	Paused   bool
}

// ImportResumePhasePublishFinalizationPlan is the downloader-owned decision
// for whether the yielded phase suffix should be read/verified/written.
type ImportResumePhasePublishFinalizationPlan struct {
	Phases     []ImportStagePhasePlan
	Publish    bool
	SkipReason ImportResumePhasePublishFinalizationSkipReason
}

// ImportResumePhasePublishFinalizationRunApplyResult groups the final
// post-drain gate and the optional read/verify/write publish run.
type ImportResumePhasePublishFinalizationRunApplyResult struct {
	Finalization ImportResumePhasePublishFinalizationPlan
	Publish      ImportResumePhasePublishRunApplyResult
}

// ImportResumePhaseDrainContinuationPlan is the downloader-owned post-publish
// local drain decision after a scheduler-yielded phase suffix settles.
type ImportResumePhaseDrainContinuationPlan struct {
	DrainAgain bool
	Pause      bool
	PauseBlock uint64
	Err        error
}

// ImportResumePhasePublishRunApplyResult groups the read/plan/write phases for
// one resume-phase publish run.
type ImportResumePhasePublishRunApplyResult struct {
	Plan        ImportResumePhasePublishRunPlan
	PublishPlan ImportResumePhasePublishPlan
	Publish     ImportResumePhasePublishApplyResult
}

// ImportedBatchProgressPlanApplier performs the persistence/runtime operations
// named by an imported-batch progress plan. SyncService owns DB handles and
// logging; downloader owns the ordered stage side effects.
type ImportedBatchProgressPlanApplier interface {
	WriteImportedSyncProgress(deletes []rawdb.SyncStagedBlockDelete, rows []rawdb.StageProgress) rawdb.SyncImportProgressWriteResult
	RefreshSyncBodiesReady() StagedBodyReadyProgressRefresh
}

// ImportedBatchProgressPlan is the downloader-owned storage plan for the
// successfully imported prefix of one local staged-body batch.
type ImportedBatchProgressPlan struct {
	OK                   bool
	Summary              AppliedBatchSummary
	Schedule             ImportStageSchedule
	AppliedStagePlan     ImportBatchStagePlan
	AppliedStagePhases   ImportBatchStagePhaseSchedule
	AppliedPhases        []ImportStagePhasePlan
	StagePhaseCursor     ImportStagePhaseCursor
	ResumePhasePlan      ImportStagePhasePlan
	HasResumePhasePlan   bool
	StagePlan            ImportStagePlan
	Stages               []ImportStageTask
	Deletes              []rawdb.SyncStagedBlockDelete
	Progress             []rawdb.StageProgress
	Decisions            []ImportStageProgressDecision
	ExecutionDiagnostics ImportBatchExecutionPlanDiagnostics
	AppliedDiagnostics   ImportBatchExecutionPlanDiagnostics
	StageDiagnostics     ImportStagePlanDiagnostics
	RefreshReady         bool
	Steps                []ImportedBatchProgressStep
	StatsBlocks          int
	StatsTransactions    int
	StatsTxKinds         map[string]int
	ReportHead           uint64
	ReportPeer           *p2p.Peer
}

// ImportedBatchProgressApplyResult records the storage/runtime side effects
// dispatched for an imported staged-body prefix.
type ImportedBatchProgressApplyResult struct {
	AppliedSteps      []ImportedBatchProgressStepAction
	UnknownSteps      []ImportedBatchProgressStepAction
	WriteResult       rawdb.SyncImportProgressWriteResult
	WriteDeletes      int
	WriteProgressRows int
	HasWriteResult    bool
	ReadyRefresh      StagedBodyReadyProgressRefresh
	HasReadyRefresh   bool
}

// WriteFailed reports whether the imported-batch progress write failed after
// canonical insertion accepted the staged-body prefix.
func (r ImportedBatchProgressApplyResult) WriteFailed() bool {
	return r.HasWriteResult && (r.WriteResult.ProgressError != nil || len(r.WriteResult.DeleteErrors) > 0)
}

// ReadyRefreshFailed reports whether the downstream SyncBodiesReady frontier
// could not be recomputed or persisted after the import-stage boundary write.
func (r ImportedBatchProgressApplyResult) ReadyRefreshFailed() bool {
	return r.HasReadyRefresh && r.ReadyRefresh.Failed()
}

// Failed reports whether any imported-batch progress side effect failed after
// canonical insertion accepted the staged-body prefix.
func (r ImportedBatchProgressApplyResult) Failed() bool {
	return r.WriteFailed() || r.ReadyRefreshFailed()
}

// StageProgressCollector observes canonical block insertion stages and records
// the matching downloader/import diagnostic stages.
type StageProgressCollector struct {
	mu   sync.Mutex
	rows map[rawdb.StageID][]StageProgressRow
}

// NewStageProgressCollector returns an empty stage progress collector.
func NewStageProgressCollector() *StageProgressCollector {
	return &StageProgressCollector{rows: make(map[rawdb.StageID][]StageProgressRow)}
}

// Observe records one canonical stage observation when it maps to a sync
// pipeline diagnostic stage.
func (c *StageProgressCollector) Observe(stage rawdb.StageID, blockNum uint64, hash tcommon.Hash) {
	syncStage, ok := StageForCanonicalStage(stage)
	if c == nil || !ok {
		return
	}
	c.observeSyncStage(syncStage, blockNum, hash)
}

// ObservePlanned records one observation already matched by the downloader's
// phase plan.
func (c *StageProgressCollector) ObservePlanned(observation ImportStageObservation) {
	if c == nil || !observation.Valid() {
		return
	}
	c.observeSyncStage(observation.Task.SyncStage, observation.Task.BlockNum, observation.Task.BlockHash)
}

func (c *StageProgressCollector) observeSyncStage(syncStage rawdb.StageID, blockNum uint64, hash tcommon.Hash) {
	c.mu.Lock()
	if c.rows == nil {
		c.rows = make(map[rawdb.StageID][]StageProgressRow)
	}
	c.rows[syncStage] = append(c.rows[syncStage], StageProgressRow{BlockNum: blockNum, Hash: hash})
	c.mu.Unlock()
}

// Write emits the hook-derived sync pipeline prefix for through, in operator
// stage-status order. Production import paths should prefer WriteSchedule so
// the execution plan, not hook observations, owns the target boundary.
func (c *StageProgressCollector) Write(through uint64, write StageProgressWriter) {
	if c == nil || write == nil {
		return
	}
	for _, row := range c.Rows(through) {
		write(row.Stage, row.BlockNum, row.BlockHash)
	}
}

// WriteSchedule emits the explicit schedule-owned sync pipeline prefix in
// operator stage-status order.
func (c *StageProgressCollector) WriteSchedule(schedule ImportStageSchedule, write StageProgressWriter) {
	if c == nil || write == nil {
		return
	}
	for _, row := range c.RowsForSchedule(schedule) {
		write(row.Stage, row.BlockNum, row.BlockHash)
	}
}

// Rows returns the hook-derived sync pipeline prefix for through, in operator
// stage-status order. A boundary is publishable only when the import/bodies
// stage was observed at exactly through; later stages must match the same
// block/hash or they are skipped with the rest of the downstream pipeline.
func (c *StageProgressCollector) Rows(through uint64) []rawdb.StageProgress {
	if c == nil {
		return nil
	}
	observed := c.snapshotRows()
	tasks, ok := observedImportStageTasks(observed, through)
	if !ok {
		return nil
	}
	schedule := NewImportStageSchedule(through, tasks[0].BlockHash)
	return newImportStagePlannerFromObserved(observed).Plan(schedule).Progress
}

// RowsForSchedule returns the explicit schedule-owned sync pipeline prefix in
// operator stage-status order.
func (c *StageProgressCollector) RowsForSchedule(schedule ImportStageSchedule) []rawdb.StageProgress {
	return c.PlanSchedule(schedule).Progress
}

// PlanImportedBatchProgress derives the DB-side progress plan for an applied
// import prefix. It keeps sync import/execution/commitment/finish rows as a
// contiguous stage prefix: if a stage is missing, later observed rows are not
// published for this batch.
func PlanImportedBatchProgress(batch BufferedBatch, applied int, collector *StageProgressCollector) ImportedBatchProgressPlan {
	return planImportedBatchProgress(batch, applied, ImportStageSchedule{}, false, collector)
}

// PlanImportedBatchProgressForExecution derives the DB-side progress plan for
// an applied import prefix using the schedule already owned by the execution
// plan. This keeps execution/commitment/finish planning on one path while
// preserving PlanImportedBatchProgress as the legacy batch-derived entry point.
func PlanImportedBatchProgressForExecution(batch BufferedBatch, applied int, execution ImportBatchExecutionPlan, collector *StageProgressCollector) ImportedBatchProgressPlan {
	schedule, hasSchedule := execution.AppliedSchedule(applied)
	if !hasSchedule {
		plan := planImportedBatchProgress(batch, applied, ImportStageSchedule{}, true, collector)
		if plan.OK {
			plan.ExecutionDiagnostics = execution.Diagnostics
		}
		return plan
	}
	plan := planImportedBatchProgress(batch, applied, schedule, hasSchedule, collector)
	if plan.OK {
		plan.ExecutionDiagnostics = execution.Diagnostics
		if appliedStagePlan, ok := execution.AppliedStagePlan(applied); ok {
			plan.AppliedStagePlan = appliedStagePlan
			if appliedStagePhases, ok := execution.AppliedPhaseSchedule(applied); ok {
				plan.AppliedStagePhases = appliedStagePhases
				plan.AppliedPhases = plan.AppliedStagePhases.PhasePlans()
			}
			plan.AppliedDiagnostics = NewImportBatchExecutionPlanDiagnostics(appliedStagePlan.Schedules, appliedStagePlan)
			plan.Stages = append([]ImportStageTask(nil), appliedStagePlan.Tasks...)
			plan.StagePlan = collector.PlanBatch(appliedStagePlan)
			plan.Progress = plan.StagePlan.Progress
			plan.Decisions = plan.StagePlan.Decisions
			plan.StageDiagnostics = plan.StagePlan.Diagnostics()
			if cursor, ok := execution.AppliedPhaseCursor(applied, plan.StagePlan); ok {
				plan.StagePhaseCursor = cursor
				if resume, ok := cursor.RemainingCurrentPhasePlan(); ok {
					plan.ResumePhasePlan = resume
					plan.HasResumePhasePlan = true
				}
			}
			plan = plan.withSteps()
		}
	}
	return plan
}

func planImportedBatchProgress(batch BufferedBatch, applied int, schedule ImportStageSchedule, hasSchedule bool, collector *StageProgressCollector) ImportedBatchProgressPlan {
	summary := SummarizeAppliedBatch(batch, applied)
	if !summary.OK {
		return ImportedBatchProgressPlan{}
	}
	plan := ImportedBatchProgressPlan{
		OK:                true,
		Summary:           summary,
		Deletes:           AppliedStagedBlockDeletes(batch, summary.Applied),
		RefreshReady:      true,
		StatsBlocks:       summary.Applied,
		StatsTransactions: summary.TxCount,
		StatsTxKinds:      cloneTxKindCounts(summary.TxKinds),
		ReportHead:        summary.Last.Num,
		ReportPeer:        summary.Last.Peer,
	}
	if !summary.HasStage {
		return plan.withSteps()
	}
	if !hasSchedule {
		schedule = NewImportStageSchedule(summary.Last.Num, summary.Last.Hash)
	}
	plan.Schedule = schedule
	plan.Stages = plan.Schedule.Tasks
	plan.StagePlan = collector.PlanSchedule(plan.Schedule)
	plan.Progress = plan.StagePlan.Progress
	plan.Decisions = plan.StagePlan.Decisions
	plan.StageDiagnostics = plan.StagePlan.Diagnostics()
	return plan.withSteps()
}

func (p ImportedBatchProgressPlan) withSteps() ImportedBatchProgressPlan {
	if !p.OK {
		return p
	}
	p.Steps = []ImportedBatchProgressStep{
		{Action: ImportedBatchWriteProgress, Deletes: p.Deletes, Progress: p.Progress},
	}
	if p.RefreshReady {
		p.Steps = append(p.Steps, ImportedBatchProgressStep{Action: ImportedBatchRefreshBodiesReady})
	}
	return p
}

// RemainingCurrentPhasePlan returns the runnable suffix of the current import
// phase for a future staged scheduler. The returned plan owns its task slice.
func (p ImportedBatchProgressPlan) RemainingCurrentPhasePlan() (ImportStagePhasePlan, bool) {
	if !p.HasResumePhasePlan || len(p.ResumePhasePlan.Tasks) == 0 {
		return ImportStagePhasePlan{}, false
	}
	return cloneImportStagePhasePlan(p.ResumePhasePlan), true
}

// PlanImportResumePhaseSuffix returns the phase suffix that can be revisited
// after a local import yields at resume. The current phase uses the runnable
// suffix retained by the import result; following phases use the original
// execution schedule.
func PlanImportResumePhaseSuffix(schedule ImportBatchStagePhaseSchedule, resume ImportStagePhasePlan) []ImportStagePhasePlan {
	if len(resume.Tasks) == 0 {
		return nil
	}
	resume = cloneImportStagePhasePlan(resume)
	if len(schedule.Phases) == 0 {
		return []ImportStagePhasePlan{resume}
	}
	for i, phase := range schedule.Phases {
		if !sameImportStagePhase(phase, resume) {
			continue
		}
		out := []ImportStagePhasePlan{resume}
		for _, following := range schedule.Phases[i+1:] {
			out = append(out, cloneImportStagePhasePlan(following))
		}
		return out
	}
	return []ImportStagePhasePlan{resume}
}

// PlanImportResumePhasePublishFinalization gates a yielded phase suffix after
// the caller's commit barrier. Resume-phase progress may be published only
// when there is real work, the commit barrier completed, and the sync loop has
// not already paused.
func PlanImportResumePhasePublishFinalization(input ImportResumePhasePublishFinalizationInput) ImportResumePhasePublishFinalizationPlan {
	plan := ImportResumePhasePublishFinalizationPlan{
		Phases: cloneImportStagePhasePlanList(input.Phases),
	}
	if len(input.Phases) == 0 {
		plan.SkipReason = ImportResumePhasePublishFinalizationNoPhases
		return plan
	}
	if !input.FinishOK {
		plan.SkipReason = ImportResumePhasePublishFinalizationCommitBarrierFailed
		return plan
	}
	if input.Paused {
		plan.SkipReason = ImportResumePhasePublishFinalizationPaused
		return plan
	}
	plan.Publish = true
	return plan
}

// ApplyImportResumePhasePublishFinalizationRun applies the full post-drain
// finalization path. If the gate does not publish, no stage rows are read or
// written.
func ApplyImportResumePhasePublishFinalizationRun(input ImportResumePhasePublishFinalizationInput, applier ImportResumePhasePublishRunApplier) ImportResumePhasePublishFinalizationRunApplyResult {
	result := ImportResumePhasePublishFinalizationRunApplyResult{
		Finalization: PlanImportResumePhasePublishFinalization(input),
	}
	if !result.Finalization.Publish {
		return result
	}
	result.Publish = ApplyImportResumePhasePublishRunPlan(NewImportResumePhasePublishRunPlan(result.Finalization.Phases), applier)
	return result
}

// PlanImportResumePhaseDrainContinuation decides whether the local drain loop
// should immediately revisit staged bodies after resume-phase progress is
// published. A rerun is safe only after the post-barrier publish path ran and
// durably applied its rows; skipped publishes leave the current drain stopped
// at the scheduler boundary, while attempted-but-blocked/failed publishes
// sticky-pause sync so later bodies cannot cross the undurable boundary.
func PlanImportResumePhaseDrainContinuation(run ImportResumePhasePublishFinalizationRunApplyResult) ImportResumePhaseDrainContinuationPlan {
	if !run.Finalization.Publish {
		return ImportResumePhaseDrainContinuationPlan{}
	}
	if run.Publish.Publish.Applied {
		return ImportResumePhaseDrainContinuationPlan{DrainAgain: true}
	}
	return ImportResumePhaseDrainContinuationPlan{
		Pause:      true,
		PauseBlock: importResumePhasePublishFailureBlock(run),
		Err:        importResumePhasePublishFailureError(run),
	}
}

func importResumePhasePublishFailureBlock(run ImportResumePhasePublishFinalizationRunApplyResult) uint64 {
	for _, decision := range run.Publish.PublishPlan.Decisions {
		if decision.Status != ImportResumePhasePublishReady {
			return decision.TargetBlock
		}
	}
	if len(run.Publish.PublishPlan.Decisions) > 0 {
		return run.Publish.PublishPlan.Decisions[len(run.Publish.PublishPlan.Decisions)-1].TargetBlock
	}
	if len(run.Publish.PublishPlan.Progress) > 0 {
		return run.Publish.PublishPlan.Progress[len(run.Publish.PublishPlan.Progress)-1].BlockNum
	}
	for _, phase := range run.Finalization.Phases {
		if len(phase.Tasks) > 0 {
			return phase.Tasks[len(phase.Tasks)-1].BlockNum
		}
	}
	return 0
}

func importResumePhasePublishFailureError(run ImportResumePhasePublishFinalizationRunApplyResult) error {
	if run.Publish.Publish.WriteError != nil {
		return fmt.Errorf("downloader: sync import resume phase publish failed: %w", run.Publish.Publish.WriteError)
	}
	for _, decision := range run.Publish.PublishPlan.Decisions {
		if decision.Status == ImportResumePhasePublishReady {
			continue
		}
		if decision.Err != nil {
			return fmt.Errorf("downloader: sync import resume phase not publishable at %s block %d: %s: %w",
				decision.SyncStage, decision.TargetBlock, decision.Status.String(), decision.Err)
		}
		return fmt.Errorf("downloader: sync import resume phase not publishable at %s block %d: %s",
			decision.SyncStage, decision.TargetBlock, decision.Status.String())
	}
	return fmt.Errorf("downloader: sync import resume phase publish did not apply")
}

// PlanImportResumePhasePublish verifies a yielded phase suffix against
// canonical stage progress and builds the sync-stage rows that can be safely
// published after the caller's commit barrier. The returned plan is all-or-none:
// a missing or mismatched phase leaves Progress empty so callers do not publish
// a partial suffix that could hide an async commit failure.
func PlanImportResumePhasePublish(phases []ImportStagePhasePlan, read StageProgressReader) ImportResumePhasePublishPlan {
	plan := ImportResumePhasePublishPlan{
		Phases: cloneImportStagePhasePlanList(phases),
	}
	if len(phases) == 0 {
		return plan
	}
	progress := make([]rawdb.StageProgress, 0, len(phases))
	planned := make(map[rawdb.StageID]rawdb.StageProgress, len(phases))
	for _, phase := range phases {
		decision, row, ok := planImportResumePhasePublishDecision(phase, read, planned)
		plan.Decisions = append(plan.Decisions, decision)
		if !ok {
			return plan
		}
		progress = append(progress, row)
		planned[row.Stage] = row
	}
	plan.Progress = progress
	plan.Complete = true
	plan.OK = len(progress) > 0
	return plan
}

func planImportResumePhasePublishDecision(phase ImportStagePhasePlan, read StageProgressReader, planned map[rawdb.StageID]rawdb.StageProgress) (ImportResumePhasePublishDecision, rawdb.StageProgress, bool) {
	decision := ImportResumePhasePublishDecision{
		Phase:          phase.Phase,
		CanonicalStage: phase.CanonicalStage,
		SyncStage:      phase.SyncStage,
	}
	if len(phase.Tasks) == 0 {
		decision.Status = ImportResumePhasePublishEmpty
		return decision, rawdb.StageProgress{}, false
	}
	target := phase.Tasks[len(phase.Tasks)-1]
	decision.TargetBlock = target.BlockNum
	decision.TargetHash = target.BlockHash
	if read == nil {
		decision.Status = ImportResumePhasePublishReadError
		decision.Err = fmt.Errorf("downloader: nil stage progress reader")
		return decision, rawdb.StageProgress{}, false
	}
	canonical, ok, err := read(phase.CanonicalStage)
	if err != nil {
		decision.Status = ImportResumePhasePublishReadError
		decision.Err = err
		return decision, rawdb.StageProgress{}, false
	}
	decision.CanonicalRow = canonical
	decision.HasCanonicalRow = ok
	if !ok {
		decision.Status = ImportResumePhasePublishCanonicalMissing
		return decision, rawdb.StageProgress{}, false
	}
	if !canonical.HasBlockHash {
		decision.Status = ImportResumePhasePublishCanonicalUnbound
		return decision, rawdb.StageProgress{}, false
	}
	if canonical.BlockNum != target.BlockNum || canonical.BlockHash != target.BlockHash {
		decision.Status = ImportResumePhasePublishCanonicalMismatch
		return decision, rawdb.StageProgress{}, false
	}
	syncRow, syncOK, err := read(phase.SyncStage)
	if err != nil {
		decision.Status = ImportResumePhasePublishReadError
		decision.Err = err
		return decision, rawdb.StageProgress{}, false
	}
	decision.SyncRow = syncRow
	decision.HasSyncRow = syncOK
	if syncOK {
		if syncRow.BlockNum > target.BlockNum {
			decision.Status = ImportResumePhasePublishSyncAhead
			return decision, rawdb.StageProgress{}, false
		}
		if syncRow.BlockNum == target.BlockNum && syncRow.HasBlockHash && syncRow.BlockHash != target.BlockHash {
			decision.Status = ImportResumePhasePublishSyncHashMismatch
			return decision, rawdb.StageProgress{}, false
		}
	}
	if upstreamStage, hasUpstream := importResumePhasePublishUpstreamStage(phase.SyncStage); hasUpstream {
		decision.UpstreamStage = upstreamStage
		upstream, upstreamOK, err := readImportResumePhasePublishUpstream(upstreamStage, read, planned)
		if err != nil {
			decision.Status = ImportResumePhasePublishReadError
			decision.Err = err
			return decision, rawdb.StageProgress{}, false
		}
		decision.UpstreamRow = upstream
		decision.HasUpstreamRow = upstreamOK
		if !upstreamOK {
			decision.Status = ImportResumePhasePublishUpstreamMissing
			return decision, rawdb.StageProgress{}, false
		}
		if !upstream.HasBlockHash {
			decision.Status = ImportResumePhasePublishUpstreamUnbound
			return decision, rawdb.StageProgress{}, false
		}
		if upstream.BlockNum < target.BlockNum {
			decision.Status = ImportResumePhasePublishUpstreamBehind
			return decision, rawdb.StageProgress{}, false
		}
		if upstream.BlockNum == target.BlockNum && upstream.BlockHash != target.BlockHash {
			decision.Status = ImportResumePhasePublishUpstreamHashMismatch
			return decision, rawdb.StageProgress{}, false
		}
	}
	row := rawdb.StageProgress{
		Stage:        phase.SyncStage,
		BlockNum:     target.BlockNum,
		BlockHash:    target.BlockHash,
		HasBlockHash: true,
	}
	decision.Status = ImportResumePhasePublishReady
	return decision, row, true
}

func importResumePhasePublishUpstreamStage(stage rawdb.StageID) (rawdb.StageID, bool) {
	for _, pair := range SyncPipelineProgressOrderPairs() {
		if pair.Downstream == stage {
			return pair.Upstream, true
		}
	}
	return "", false
}

func readImportResumePhasePublishUpstream(stage rawdb.StageID, read StageProgressReader, planned map[rawdb.StageID]rawdb.StageProgress) (rawdb.StageProgress, bool, error) {
	if row, ok := planned[stage]; ok {
		return row, true, nil
	}
	return read(stage)
}

// ApplyImportResumePhasePublishPlan writes the verified sync-stage suffix for
// a scheduler-yielded import phase.
func ApplyImportResumePhasePublishPlan(plan ImportResumePhasePublishPlan, applier ImportResumePhasePublishPlanApplier) ImportResumePhasePublishApplyResult {
	result := ImportResumePhasePublishApplyResult{Plan: plan}
	if !plan.OK || applier == nil {
		return result
	}
	result.Rows = len(plan.Progress)
	result.WriteError = applier.WriteResumePhaseProgress(plan.Progress)
	result.Applied = result.WriteError == nil
	return result
}

// NewImportResumePhasePublishRunPlan returns the downloader-owned publish
// schedule for a yielded phase suffix.
func NewImportResumePhasePublishRunPlan(phases []ImportStagePhasePlan) ImportResumePhasePublishRunPlan {
	return ImportResumePhasePublishRunPlan{Phases: cloneImportStagePhasePlanList(phases)}
}

// ApplyImportResumePhasePublishRun creates and applies the full downloader
// publish run for a scheduler-yielded phase suffix.
func ApplyImportResumePhasePublishRun(phases []ImportStagePhasePlan, applier ImportResumePhasePublishRunApplier) ImportResumePhasePublishRunApplyResult {
	return ApplyImportResumePhasePublishRunPlan(NewImportResumePhasePublishRunPlan(phases), applier)
}

// ApplyImportResumePhasePublishRunPlan verifies and writes a yielded phase
// suffix through the supplied runtime applier.
func ApplyImportResumePhasePublishRunPlan(plan ImportResumePhasePublishRunPlan, applier ImportResumePhasePublishRunApplier) ImportResumePhasePublishRunApplyResult {
	result := ImportResumePhasePublishRunApplyResult{Plan: plan}
	var read StageProgressReader
	if applier != nil {
		read = applier.ReadStageProgress
	}
	result.PublishPlan = PlanImportResumePhasePublish(plan.Phases, read)
	result.Publish = ApplyImportResumePhasePublishPlan(result.PublishPlan, applier)
	return result
}

func sameImportStagePhase(a, b ImportStagePhasePlan) bool {
	return a.Phase == b.Phase && a.CanonicalStage == b.CanonicalStage && a.SyncStage == b.SyncStage
}

func cloneTxKindCounts(source map[string]int) map[string]int {
	if len(source) == 0 {
		return nil
	}
	out := make(map[string]int, len(source))
	for kind, count := range source {
		out[kind] = count
	}
	return out
}

// ApplyImportedBatchProgressPlan executes the downloader-owned side-effect
// schedule for an imported staged-body prefix.
func ApplyImportedBatchProgressPlan(plan ImportedBatchProgressPlan, applier ImportedBatchProgressPlanApplier) ImportedBatchProgressApplyResult {
	var result ImportedBatchProgressApplyResult
	if !plan.OK || applier == nil {
		return result
	}
	for _, step := range plan.Steps {
		switch step.Action {
		case ImportedBatchWriteProgress:
			result.WriteDeletes = len(step.Deletes)
			result.WriteProgressRows = len(step.Progress)
			result.WriteResult = applier.WriteImportedSyncProgress(step.Deletes, step.Progress)
			result.HasWriteResult = true
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
			if result.WriteFailed() {
				return result
			}
		case ImportedBatchRefreshBodiesReady:
			result.ReadyRefresh = applier.RefreshSyncBodiesReady()
			result.HasReadyRefresh = true
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
			if result.ReadyRefreshFailed() {
				return result
			}
		default:
			result.UnknownSteps = append(result.UnknownSteps, step.Action)
		}
	}
	return result
}

// NewImportStageSchedule returns the bodies/execution/commitment/finish targets
// required before sync progress can be published at one import boundary.
func NewImportStageSchedule(blockNum uint64, blockHash tcommon.Hash) ImportStageSchedule {
	schedule := ImportStageSchedule{
		BlockNum:  blockNum,
		BlockHash: blockHash,
	}
	for _, spec := range importStageSpecs {
		task := spec.Task(blockNum, blockHash)
		schedule.Tasks = append(schedule.Tasks, task)
		switch spec.Phase {
		case ImportStagePhaseBodies:
			schedule.Body = task
		case ImportStagePhaseExecution:
			schedule.Execution = task
			schedule.PostBody = append(schedule.PostBody, task)
		case ImportStagePhaseCommitment:
			schedule.Commitment = task
			schedule.PostBody = append(schedule.PostBody, task)
		case ImportStagePhaseFinish:
			schedule.Finish = task
			schedule.PostBody = append(schedule.PostBody, task)
		}
	}
	return schedule
}

// PlanSchedule returns the explicit stage planner result for schedule.
func (c *StageProgressCollector) PlanSchedule(schedule ImportStageSchedule) ImportStagePlan {
	return NewImportStagePlanner(c).Plan(schedule)
}

// PlanBatch returns the explicit batch-phase planner result for all applied
// bodies/execution/commitment/finish tasks.
func (c *StageProgressCollector) PlanBatch(stagePlan ImportBatchStagePlan) ImportStagePlan {
	return NewImportStagePlanner(c).PlanBatch(stagePlan)
}

// Plan maps observed canonical stage hooks onto a schedule-owned contiguous
// sync progress prefix.
func (c *StageProgressCollector) Plan(schedule ImportStageSchedule) ([]rawdb.StageProgress, []ImportStageProgressDecision) {
	plan := c.PlanSchedule(schedule)
	return plan.Progress, plan.Decisions
}

// NewImportStagePlanner snapshots collector observations for deterministic
// import-stage planning.
func NewImportStagePlanner(collector *StageProgressCollector) ImportStagePlanner {
	if collector == nil {
		return ImportStagePlanner{}
	}
	return newImportStagePlannerFromObserved(collector.snapshotRows())
}

func newImportStagePlannerFromObserved(observed map[rawdb.StageID][]StageProgressRow) ImportStagePlanner {
	return ImportStagePlanner{observed: cloneStageProgressRows(observed)}
}

// Plan maps observed canonical stage hooks onto a schedule-owned contiguous
// sync progress prefix and records the first incomplete stage.
func (p ImportStagePlanner) Plan(schedule ImportStageSchedule) ImportStagePlan {
	if len(schedule.Tasks) == 0 {
		return ImportStagePlan{}
	}
	progress, decisions := planImportStageRows(p.observed, schedule.Tasks)
	return newImportStagePlan(schedule, progress, decisions)
}

// PlanBatch maps observed canonical stage hooks onto a batch phase plan. Unlike
// Plan, which checks a single import boundary, this verifies each applied
// bodies/execution/commitment/finish phase task before publishing that phase's
// latest sync progress row.
func (p ImportStagePlanner) PlanBatch(stagePlan ImportBatchStagePlan) ImportStagePlan {
	if stagePlan.Empty() {
		return ImportStagePlan{}
	}
	progress, decisions, phases := planImportBatchStageRows(p.observed, stagePlan)
	plan := newImportStagePlanFromTasks(stagePlan.Tasks, progress, decisions)
	plan.Phases = cloneImportStagePhaseProgressList(phases)
	return plan
}

func newImportStagePlan(schedule ImportStageSchedule, progress []rawdb.StageProgress, decisions []ImportStageProgressDecision) ImportStagePlan {
	plan := newImportStagePlanFromTasks(schedule.Tasks, progress, decisions)
	plan.Schedule = schedule
	return plan
}

func newImportStagePlanFromTasks(tasks []ImportStageTask, progress []rawdb.StageProgress, decisions []ImportStageProgressDecision) ImportStagePlan {
	plan := ImportStagePlan{
		Tasks:     append([]ImportStageTask(nil), tasks...),
		Progress:  append([]rawdb.StageProgress(nil), progress...),
		Decisions: append([]ImportStageProgressDecision(nil), decisions...),
	}
	for _, decision := range decisions {
		if decision.Status == ImportStageProgressPlanned {
			plan.Completed = append(plan.Completed, decision.Task)
			continue
		}
		if !plan.HasNext {
			plan.Next = decision.Task
			plan.HasNext = true
			plan.Blocked = decision
			plan.HasBlocked = true
		}
	}
	plan.Complete = len(tasks) > 0 && len(plan.Completed) == len(tasks)
	return plan
}

// Diagnostics returns the stable diagnostic view of this import stage plan.
func (p ImportStagePlan) Diagnostics() ImportStagePlanDiagnostics {
	diag := ImportStagePlanDiagnostics{
		Scheduled: len(p.Tasks),
		Completed: len(p.Completed),
		Complete:  p.Complete,
	}
	if diag.Scheduled == 0 {
		diag.Scheduled = len(p.Schedule.Tasks)
	}
	if p.HasBlocked {
		diag.NextPhase = p.Next.Phase
		diag.NextCanonicalStage = p.Next.CanonicalStage
		diag.NextStage = p.Next.SyncStage
		diag.NextBlockNum = p.Next.BlockNum
		diag.NextBlockHash = p.Next.BlockHash
		diag.BlockedStatus = p.Blocked.Status
		diag.HasBlocked = true
	}
	return diag
}

func planImportStageRows(observed map[rawdb.StageID][]StageProgressRow, stages []ImportStageTask) ([]rawdb.StageProgress, []ImportStageProgressDecision) {
	progress := make([]rawdb.StageProgress, 0, len(stages))
	decisions := make([]ImportStageProgressDecision, 0, len(stages))
	blocked := false
	for _, task := range stages {
		if blocked {
			decisions = append(decisions, ImportStageProgressDecision{
				Task:   task,
				Stage:  task.SyncStage,
				Status: ImportStageProgressBlocked,
			})
			continue
		}
		row, status := matchImportStageTask(observed[task.SyncStage], task)
		if status != ImportStageProgressPlanned {
			blocked = true
			decisions = append(decisions, ImportStageProgressDecision{
				Task:   task,
				Stage:  task.SyncStage,
				Status: status,
				Row:    row,
			})
			continue
		}
		progress = append(progress, row)
		decisions = append(decisions, ImportStageProgressDecision{
			Task:   task,
			Stage:  task.SyncStage,
			Status: ImportStageProgressPlanned,
			Row:    row,
		})
	}
	return progress, decisions
}

func planImportBatchStageRows(observed map[rawdb.StageID][]StageProgressRow, stagePlan ImportBatchStagePlan) ([]rawdb.StageProgress, []ImportStageProgressDecision, []ImportStagePhaseProgress) {
	progress := make([]rawdb.StageProgress, 0, len(stagePlan.Phases))
	decisions := make([]ImportStageProgressDecision, 0, len(stagePlan.Tasks))
	phases := make([]ImportStagePhaseProgress, 0, len(stagePlan.Phases))
	blocked := false
	for _, phase := range stagePlan.Phases {
		var (
			phaseProgress = newImportStagePhaseProgress(phase)
			phaseRow      rawdb.StageProgress
			havePhaseRow  bool
		)
		for _, task := range phase.Tasks {
			if blocked {
				decision := ImportStageProgressDecision{
					Task:   task,
					Stage:  task.SyncStage,
					Status: ImportStageProgressBlocked,
				}
				phaseProgress.addDecision(decision)
				decisions = append(decisions, decision)
				continue
			}
			row, status := matchImportStageTask(observed[task.SyncStage], task)
			if status != ImportStageProgressPlanned {
				blocked = true
				decision := ImportStageProgressDecision{
					Task:   task,
					Stage:  task.SyncStage,
					Status: status,
					Row:    row,
				}
				phaseProgress.addDecision(decision)
				decisions = append(decisions, decision)
				continue
			}
			phaseRow = row
			havePhaseRow = true
			decision := ImportStageProgressDecision{
				Task:   task,
				Stage:  task.SyncStage,
				Status: ImportStageProgressPlanned,
				Row:    row,
			}
			phaseProgress.addDecision(decision)
			decisions = append(decisions, decision)
		}
		if havePhaseRow {
			progress = append(progress, phaseRow)
			phaseProgress.Progress = phaseRow
			phaseProgress.HasProgress = true
		}
		phaseProgress.Complete = len(phaseProgress.Tasks) > 0 && len(phaseProgress.Completed) == len(phaseProgress.Tasks)
		phases = append(phases, phaseProgress)
	}
	return progress, decisions, phases
}

func newImportStagePhaseProgress(phase ImportStagePhasePlan) ImportStagePhaseProgress {
	return ImportStagePhaseProgress{
		Phase:          phase.Phase,
		CanonicalStage: phase.CanonicalStage,
		SyncStage:      phase.SyncStage,
		Tasks:          append([]ImportStageTask(nil), phase.Tasks...),
	}
}

func (p *ImportStagePhaseProgress) addDecision(decision ImportStageProgressDecision) {
	p.Decisions = append(p.Decisions, decision)
	if decision.Status == ImportStageProgressPlanned {
		p.Completed = append(p.Completed, decision.Task)
		return
	}
	if !p.HasNext {
		p.Next = decision.Task
		p.HasNext = true
		p.Blocked = decision
		p.HasBlocked = true
	}
}

func cloneImportStagePhaseProgressList(source []ImportStagePhaseProgress) []ImportStagePhaseProgress {
	if len(source) == 0 {
		return nil
	}
	out := make([]ImportStagePhaseProgress, 0, len(source))
	for _, phase := range source {
		phase.Tasks = append([]ImportStageTask(nil), phase.Tasks...)
		phase.Decisions = append([]ImportStageProgressDecision(nil), phase.Decisions...)
		phase.Completed = append([]ImportStageTask(nil), phase.Completed...)
		out = append(out, phase)
	}
	return out
}

func observedImportStageTasks(observed map[rawdb.StageID][]StageProgressRow, through uint64) ([]ImportStageTask, bool) {
	for _, row := range observed[rawdb.StageSyncImport] {
		if row.BlockNum == through {
			return ImportPipelineStageTasks(row.BlockNum, row.Hash), true
		}
	}
	return nil, false
}

func matchImportStageTask(rows []StageProgressRow, task ImportStageTask) (rawdb.StageProgress, ImportStageProgressStatus) {
	var (
		latest StageProgressRow
		have   bool
	)
	for _, row := range rows {
		if row.BlockNum == task.BlockNum && row.Hash == task.BlockHash {
			return rawdb.StageProgress{
				Stage:        task.SyncStage,
				BlockNum:     row.BlockNum,
				BlockHash:    row.Hash,
				HasBlockHash: true,
			}, ImportStageProgressPlanned
		}
		if !have || row.BlockNum > latest.BlockNum {
			latest = row
			have = true
		}
	}
	if !have {
		return rawdb.StageProgress{}, ImportStageProgressMissing
	}
	return rawdb.StageProgress{
		Stage:        task.SyncStage,
		BlockNum:     latest.BlockNum,
		BlockHash:    latest.Hash,
		HasBlockHash: true,
	}, ImportStageProgressMismatch
}

func (c *StageProgressCollector) snapshotRows() map[rawdb.StageID][]StageProgressRow {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return cloneStageProgressRows(c.rows)
}

func cloneStageProgressRows(source map[rawdb.StageID][]StageProgressRow) map[rawdb.StageID][]StageProgressRow {
	if len(source) == 0 {
		return nil
	}
	rows := make(map[rawdb.StageID][]StageProgressRow, len(source))
	for stage, stageRows := range source {
		rows[stage] = append([]StageProgressRow(nil), stageRows...)
	}
	return rows
}

// SyncPipelineProgressStages returns the hash-bound sync import stages in the
// order expected by db stage-status output.
func SyncPipelineProgressStages() []rawdb.StageID {
	return []rawdb.StageID{
		rawdb.StageSyncImport,
		rawdb.StageSyncExecution,
		rawdb.StageSyncCommitment,
		rawdb.StageSyncFinish,
	}
}

// FullSyncPipelineProgressStages returns the ordered body-download and import
// stage rows that must remain monotonic during full staged sync.
func FullSyncPipelineProgressStages() []rawdb.StageID {
	return []rawdb.StageID{
		rawdb.StageSyncBodies,
		rawdb.StageSyncBodiesReady,
		rawdb.StageSyncImport,
		rawdb.StageSyncExecution,
		rawdb.StageSyncCommitment,
		rawdb.StageSyncFinish,
	}
}

// SyncPipelineProgressOrderPairs returns downstream/upstream stage pairs for
// the full sync pipeline. A present downstream row cannot be ahead of its
// upstream row.
func SyncPipelineProgressOrderPairs() []SyncPipelineProgressOrderPair {
	return []SyncPipelineProgressOrderPair{
		{Downstream: rawdb.StageSyncBodiesReady, Upstream: rawdb.StageSyncBodies},
		{Downstream: rawdb.StageSyncImport, Upstream: rawdb.StageSyncBodiesReady},
		{Downstream: rawdb.StageSyncExecution, Upstream: rawdb.StageSyncImport},
		{Downstream: rawdb.StageSyncCommitment, Upstream: rawdb.StageSyncExecution},
		{Downstream: rawdb.StageSyncFinish, Upstream: rawdb.StageSyncCommitment},
	}
}

// CheckSyncPipelineProgressOrder validates full-sync stage progress ordering
// from already-read stage rows. The input map is read-only.
func CheckSyncPipelineProgressOrder(rows map[rawdb.StageID]rawdb.StageProgress, opts SyncPipelineProgressOrderOptions) []SyncPipelineProgressOrderIssue {
	if len(rows) == 0 {
		return nil
	}
	var issues []SyncPipelineProgressOrderIssue
	for _, pair := range SyncPipelineProgressOrderPairs() {
		downstream, downstreamOK := rows[pair.Downstream]
		if !downstreamOK {
			continue
		}
		upstream, upstreamOK := rows[pair.Upstream]
		if !upstreamOK {
			if opts.RequireUpstream {
				issues = append(issues, SyncPipelineProgressOrderIssue{
					Downstream:      pair.Downstream,
					DownstreamBlock: downstream.BlockNum,
					Upstream:        pair.Upstream,
					MissingUpstream: true,
				})
			}
			continue
		}
		if downstream.BlockNum <= upstream.BlockNum {
			if downstream.BlockNum == upstream.BlockNum &&
				downstream.HasBlockHash && upstream.HasBlockHash &&
				downstream.BlockHash != upstream.BlockHash {
				issues = append(issues, SyncPipelineProgressOrderIssue{
					Downstream:      pair.Downstream,
					DownstreamBlock: downstream.BlockNum,
					DownstreamHash:  downstream.BlockHash,
					Upstream:        pair.Upstream,
					UpstreamBlock:   upstream.BlockNum,
					UpstreamHash:    upstream.BlockHash,
					HashMismatch:    true,
				})
			}
			continue
		}
		issues = append(issues, SyncPipelineProgressOrderIssue{
			Downstream:      pair.Downstream,
			DownstreamBlock: downstream.BlockNum,
			Upstream:        pair.Upstream,
			UpstreamBlock:   upstream.BlockNum,
		})
	}
	return issues
}

// CheckSyncPipelineProgressOrderFromDB reads full sync pipeline stage rows and
// validates their ordering. The downloader owns the stage set and ordering
// rules; callers own any logging for row read failures.
func CheckSyncPipelineProgressOrderFromDB(db ethdb.KeyValueReader, opts SyncPipelineProgressOrderOptions) SyncPipelineProgressOrderCheckResult {
	var result SyncPipelineProgressOrderCheckResult
	if db == nil {
		return result
	}
	rows := make(map[rawdb.StageID]rawdb.StageProgress)
	for _, stage := range FullSyncPipelineProgressStages() {
		row, ok, err := rawdb.ReadStageProgressRow(db, stage)
		if err != nil {
			result.ReadErrors = append(result.ReadErrors, SyncPipelineProgressOrderReadError{
				Stage: stage,
				Err:   err,
			})
			continue
		}
		if !ok {
			continue
		}
		rows[stage] = row
		result.Rows = append(result.Rows, row)
	}
	result.Issues = CheckSyncPipelineProgressOrder(rows, opts)
	return result
}

// PlanSyncPipelineProgressCursor derives the startup continuation cursor from
// already-checked full sync pipeline rows. Missing body-ready rows are tolerated
// the same way the order check tolerates them: once a downstream stage exists,
// it becomes the cursor even if an earlier optional diagnostic row is absent.
func PlanSyncPipelineProgressCursor(check SyncPipelineProgressOrderCheckResult) SyncPipelineProgressCursor {
	cursor := SyncPipelineProgressCursor{StageRows: len(check.Rows)}
	if len(check.ReadErrors) > 0 {
		cursor.Interrupted = true
		cursor.ErrorStage = check.ReadErrors[0].Stage
		return cursor
	}
	rows := make(map[rawdb.StageID]rawdb.StageProgress, len(check.Rows))
	for _, row := range check.Rows {
		rows[row.Stage] = row
	}
	stages := FullSyncPipelineProgressStages()
	if len(check.Issues) > 0 {
		cursor.HasBlocked = true
		cursor.BlockedIssue = check.Issues[0]
		cursor.HasNext = true
		cursor.NextStage = check.Issues[0].Downstream
		cursor.setLastBefore(stages, rows, check.Issues[0].Downstream)
		return cursor
	}
	lastIndex := -1
	for i, stage := range stages {
		row, ok := rows[stage]
		if !ok {
			continue
		}
		lastIndex = i
		cursor.setLast(row)
	}
	if lastIndex == len(stages)-1 && lastIndex >= 0 {
		cursor.Complete = true
		return cursor
	}
	cursor.HasNext = true
	if lastIndex+1 >= 0 && lastIndex+1 < len(stages) {
		cursor.NextStage = stages[lastIndex+1]
	}
	return cursor
}

func (c *SyncPipelineProgressCursor) setLastBefore(stages []rawdb.StageID, rows map[rawdb.StageID]rawdb.StageProgress, before rawdb.StageID) {
	for _, stage := range stages {
		if stage == before {
			return
		}
		row, ok := rows[stage]
		if !ok {
			continue
		}
		c.setLast(row)
	}
}

func (c *SyncPipelineProgressCursor) setLast(row rawdb.StageProgress) {
	c.HasLast = true
	c.LastStage = row.Stage
	c.LastBlock = row.BlockNum
	c.LastHash = row.BlockHash
	c.LastHasHash = row.HasBlockHash
}

// PlanSyncPipelineProgressHeadCompletion completes missing downstream sync
// diagnostic stages only when the repaired prefix is hash-bound to the current
// canonical head. It never advances progress beyond head, and it does nothing
// when the prefix belongs to an older imported block.
func PlanSyncPipelineProgressHeadCompletion(repair SyncPipelineProgressRepairResult, head uint64, headHash tcommon.Hash) SyncPipelineProgressHeadCompletionPlan {
	plan := SyncPipelineProgressHeadCompletionPlan{
		Head:     head,
		HeadHash: headHash,
	}
	if repair.Interrupted || len(repair.Repairs) == 0 {
		return plan
	}
	stages := SyncPipelineProgressStages()
	indexByStage := make(map[rawdb.StageID]int, len(stages))
	for i, stage := range stages {
		indexByStage[stage] = i
	}
	lastIndex := -1
	var last SyncStageProgressRepair
	for _, candidate := range repair.Repairs {
		if candidate.Status != SyncStageProgressKept {
			continue
		}
		index, ok := indexByStage[candidate.Stage]
		if !ok || index < lastIndex {
			continue
		}
		lastIndex = index
		last = candidate
	}
	if lastIndex < 0 || !last.Row.HasBlockHash || last.Row.BlockNum != head || last.Row.BlockHash != headHash {
		return plan
	}
	plan.HasHeadPrefix = true
	plan.LastStage = last.Stage
	plan.LastBlock = last.Row.BlockNum
	if lastIndex == len(stages)-1 {
		plan.Complete = true
		return plan
	}
	plan.FillStages = append(plan.FillStages, stages[lastIndex+1:]...)
	return plan
}

// ApplySyncPipelineProgressHeadCompletionPlan writes the downstream sync-stage
// rows named by a current-head completion plan. The downloader owns the ordered
// fill semantics; callers own the concrete DB writer and logging.
func ApplySyncPipelineProgressHeadCompletionPlan(plan SyncPipelineProgressHeadCompletionPlan, write StageProgressErrorWriter) SyncPipelineProgressHeadCompletion {
	result := SyncPipelineProgressHeadCompletion{Plan: plan}
	if !plan.HasHeadPrefix {
		return result
	}
	if len(plan.FillStages) == 0 {
		result.Complete = plan.Complete
		return result
	}
	if write == nil {
		return result
	}
	for _, stage := range plan.FillStages {
		if err := write(stage, plan.Head, plan.HeadHash); err != nil {
			result.WriteError = err
			result.ErrorStage = stage
			return result
		}
		result.Written++
	}
	result.Complete = true
	return result
}

// RepairSyncPipelineProgressOrderFromDB repairs detected full-pipeline ordering
// violations and then rechecks the stage rows.
// Missing upstream rows are controlled by opts; the default non-strict mode is
// intentionally tolerant of SyncBodiesReady being absent after imported staged
// bodies were drained.
func RepairSyncPipelineProgressOrderFromDB(db ethdb.KeyValueStore, opts SyncPipelineProgressOrderOptions) SyncPipelineProgressOrderRepairResult {
	var result SyncPipelineProgressOrderRepairResult
	if db == nil {
		result.Complete = true
		return result
	}
	result.Before = CheckSyncPipelineProgressOrderFromDB(db, opts)
	if len(result.Before.ReadErrors) > 0 {
		result.Interrupted = true
		result.ErrorStage = result.Before.ReadErrors[0].Stage
		return result
	}
	rows := make(map[rawdb.StageID]rawdb.StageProgress, len(result.Before.Rows))
	for _, row := range result.Before.Rows {
		rows[row.Stage] = row
	}
	deleted := make(map[rawdb.StageID]struct{})
	for _, stage := range FullSyncPipelineProgressStages() {
		row, ok := rows[stage]
		if !ok || row.HasBlockHash {
			continue
		}
		repair := SyncPipelineProgressOrderRepair{
			Stage: stage,
			Row:   row,
		}
		if err := rawdb.DeleteStageProgress(db, stage); err != nil {
			repair.DeleteError = err
			result.Repairs = append(result.Repairs, repair)
			result.Interrupted = true
			result.ErrorStage = stage
			return result
		}
		result.Repairs = append(result.Repairs, repair)
		result.Deleted++
		deleted[stage] = struct{}{}
		delete(rows, stage)
	}
	if len(result.Before.Issues) == 0 {
		if len(result.Repairs) == 0 {
			result.After = result.Before
			result.Complete = true
			return result
		}
		result.After = CheckSyncPipelineProgressOrderFromDB(db, opts)
		if len(result.After.ReadErrors) > 0 {
			result.Interrupted = true
			result.ErrorStage = result.After.ReadErrors[0].Stage
			return result
		}
		result.Complete = len(result.After.Issues) == 0 && !result.Interrupted
		return result
	}
	for _, issue := range result.Before.Issues {
		if _, ok := deleted[issue.Downstream]; ok {
			continue
		}
		if issue.Downstream == rawdb.StageSyncBodiesReady && issue.Upstream == rawdb.StageSyncBodies {
			if repair, ok := repairSyncBodiesProgressFromReady(db, rows[rawdb.StageSyncBodiesReady], issue); ok {
				result.Repairs = append(result.Repairs, repair)
				if repair.WriteError != nil {
					result.Interrupted = true
					result.ErrorStage = repair.Stage
					return result
				}
				result.Updated++
				rows[rawdb.StageSyncBodies] = repair.Row
				continue
			}
		}
		for _, stage := range fullSyncPipelineStagesFrom(issue.Downstream) {
			if _, ok := deleted[stage]; ok {
				continue
			}
			row, ok := rows[stage]
			if !ok {
				continue
			}
			repair := SyncPipelineProgressOrderRepair{
				Stage: stage,
				Row:   row,
				Issue: issue,
			}
			if err := rawdb.DeleteStageProgress(db, stage); err != nil {
				repair.DeleteError = err
				result.Repairs = append(result.Repairs, repair)
				result.Interrupted = true
				result.ErrorStage = stage
				return result
			}
			result.Repairs = append(result.Repairs, repair)
			result.Deleted++
			deleted[stage] = struct{}{}
			delete(rows, stage)
		}
	}
	result.After = CheckSyncPipelineProgressOrderFromDB(db, opts)
	if len(result.After.ReadErrors) > 0 {
		result.Interrupted = true
		result.ErrorStage = result.After.ReadErrors[0].Stage
		return result
	}
	result.Complete = len(result.After.Issues) == 0 && !result.Interrupted
	return result
}

func repairSyncBodiesProgressFromReady(db ethdb.KeyValueStore, ready rawdb.StageProgress, issue SyncPipelineProgressOrderIssue) (SyncPipelineProgressOrderRepair, bool) {
	limit := ReadStagedBodyReadyDrainLimit(db, ready.BlockNum)
	if !limit.Valid() {
		return SyncPipelineProgressOrderRepair{}, false
	}
	if limit.StageRow.BlockNum != ready.BlockNum || limit.StageRow.BlockHash != ready.BlockHash {
		return SyncPipelineProgressOrderRepair{}, false
	}
	repair := SyncPipelineProgressOrderRepair{
		Stage:   rawdb.StageSyncBodies,
		Row:     limit.StageRow,
		Issue:   issue,
		Updated: true,
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodies, limit.Limit, limit.StageRow.BlockHash); err != nil {
		repair.WriteError = err
	}
	return repair, true
}

func fullSyncPipelineStagesFrom(stage rawdb.StageID) []rawdb.StageID {
	stages := FullSyncPipelineProgressStages()
	for i, candidate := range stages {
		if candidate == stage {
			return stages[i:]
		}
	}
	return nil
}

// ImportPipelineStageTasks returns the canonical-to-sync stage schedule for
// one applied import boundary.
func ImportPipelineStageTasks(blockNum uint64, blockHash tcommon.Hash) []ImportStageTask {
	schedule := NewImportStageSchedule(blockNum, blockHash)
	return append([]ImportStageTask(nil), schedule.Tasks...)
}

// ImportBodyStageTask returns the local body-import task for one applied
// boundary. It must precede execution tasks before progress can be published.
func ImportBodyStageTask(blockNum uint64, blockHash tcommon.Hash) ImportStageTask {
	return importStageTaskForPhase(ImportStagePhaseBodies, blockNum, blockHash)
}

// ImportExecutionStageTask returns the execution task for one applied import
// boundary.
func ImportExecutionStageTask(blockNum uint64, blockHash tcommon.Hash) ImportStageTask {
	return importStageTaskForPhase(ImportStagePhaseExecution, blockNum, blockHash)
}

// ImportCommitmentStageTask returns the commitment task for one applied import
// boundary.
func ImportCommitmentStageTask(blockNum uint64, blockHash tcommon.Hash) ImportStageTask {
	return importStageTaskForPhase(ImportStagePhaseCommitment, blockNum, blockHash)
}

// ImportFinishStageTask returns the finish task for one applied import
// boundary.
func ImportFinishStageTask(blockNum uint64, blockHash tcommon.Hash) ImportStageTask {
	return importStageTaskForPhase(ImportStagePhaseFinish, blockNum, blockHash)
}

// ImportExecutionStageTasks returns the explicit execution/commitment/finish
// task chain for one applied import boundary.
func ImportExecutionStageTasks(blockNum uint64, blockHash tcommon.Hash) []ImportStageTask {
	tasks := make([]ImportStageTask, 0, len(importStageSpecs)-1)
	for _, spec := range importStageSpecs {
		if spec.Phase == ImportStagePhaseBodies {
			continue
		}
		tasks = append(tasks, spec.Task(blockNum, blockHash))
	}
	return tasks
}

// StageForCanonicalStage maps canonical block insertion stages to their
// downloader/import diagnostic counterparts.
func StageForCanonicalStage(stage rawdb.StageID) (rawdb.StageID, bool) {
	spec, ok := importStageSpecForCanonicalStage(stage)
	if !ok {
		return "", false
	}
	return spec.SyncStage, true
}

// RepairSyncStageProgress keeps a hash-bound sync diagnostic stage row only
// when it still resolves to canonical chain state at or below the current head.
// Rows without hashes, rows ahead of head, forked rows, and rows whose
// canonical block is unavailable are deleted.
func RepairSyncStageProgress(db ethdb.KeyValueStore, stage rawdb.StageID, head uint64, canonicalHash CanonicalHashReader) SyncStageProgressRepair {
	result := SyncStageProgressRepair{Stage: stage}
	row, ok, err := rawdb.ReadStageProgressRow(db, stage)
	if err != nil {
		result.Status = SyncStageProgressReadError
		result.ReadError = err
		return result
	}
	if !ok {
		result.Status = SyncStageProgressMissing
		return result
	}
	result.Row = row
	if row.HasBlockHash && row.BlockNum <= head && canonicalHash != nil {
		if hash, ok := canonicalHash(row.BlockNum); ok {
			result.CanonicalHash = hash
			if hash == row.BlockHash {
				result.Status = SyncStageProgressKept
				return result
			}
		}
	}
	if err := rawdb.DeleteStageProgress(db, stage); err != nil {
		result.Status = SyncStageProgressDeleteError
		result.DeleteError = err
		return result
	}
	result.Status = SyncStageProgressDeleted
	return result
}

// RepairSyncPipelineProgress validates sync import pipeline rows as one
// ordered pipeline. Individual rows must be hash-bound canonical rows at or
// below head, and downstream stages cannot be ahead of their upstream stage.
// Rows after the first missing/invalid stage are deleted so restart diagnostics
// remain a contiguous sync-stage prefix.
func RepairSyncPipelineProgress(db ethdb.KeyValueStore, head uint64, canonicalHash CanonicalHashReader) []SyncStageProgressRepair {
	return RepairSyncPipelineProgressWithResult(db, head, canonicalHash).Repairs
}

// RepairSyncPipelineProgressWithResult validates and repairs sync import
// pipeline rows while returning a summary that startup diagnostics can expose
// without re-deriving kept/deleted/missing boundaries.
func RepairSyncPipelineProgressWithResult(db ethdb.KeyValueStore, head uint64, canonicalHash CanonicalHashReader) SyncPipelineProgressRepairResult {
	stages := SyncPipelineProgressStages()
	result := SyncPipelineProgressRepairResult{Repairs: make([]SyncStageProgressRepair, 0, len(stages))}
	var (
		upstream rawdb.StageProgress
		haveUp   bool
		blocked  bool
	)
	for _, stage := range stages {
		repair := RepairSyncStageProgress(db, stage, head, canonicalHash)
		switch repair.Status {
		case SyncStageProgressKept:
			if blocked || (haveUp && repair.Row.BlockNum > upstream.BlockNum) {
				result.blockAt(repair.Stage)
				if err := rawdb.DeleteStageProgress(db, repair.Stage); err != nil {
					repair.Status = SyncStageProgressDeleteError
					repair.DeleteError = err
					result.add(repair)
					return result
				}
				repair.Status = SyncStageProgressDeleted
				blocked = true
				result.add(repair)
				continue
			}
			upstream = repair.Row
			haveUp = true
		case SyncStageProgressMissing, SyncStageProgressDeleted:
			result.blockAt(repair.Stage)
			blocked = true
		case SyncStageProgressReadError, SyncStageProgressDeleteError:
			result.add(repair)
			return result
		}
		result.add(repair)
	}
	result.Complete = len(result.Repairs) == len(stages) && result.Kept == len(stages) && !result.Interrupted
	return result
}
