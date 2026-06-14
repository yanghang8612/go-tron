package downloader

import (
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

// ImportStageObservation is one canonical stage hook observation accepted by
// the downloader-owned phase plan.
type ImportStageObservation struct {
	Phase ImportStagePhasePlan
	Task  ImportStageTask
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

// Empty reports whether the batch has any scheduled canonical import tasks.
func (p ImportBatchStagePlan) Empty() bool {
	return len(p.Tasks) == 0
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
	var (
		tasks     []ImportStageTask
		canonical rawdb.StageID
		syncStage rawdb.StageID
	)
	switch phase {
	case ImportStagePhaseBodies:
		tasks, canonical, syncStage = p.Bodies, rawdb.StageBodies, rawdb.StageSyncImport
	case ImportStagePhaseExecution:
		tasks, canonical, syncStage = p.Execution, rawdb.StageExecution, rawdb.StageSyncExecution
	case ImportStagePhaseCommitment:
		tasks, canonical, syncStage = p.Commitment, rawdb.StageCommitment, rawdb.StageSyncCommitment
	case ImportStagePhaseFinish:
		tasks, canonical, syncStage = p.Finish, rawdb.StageFinish, rawdb.StageSyncFinish
	default:
		return ImportStagePhasePlan{}, false
	}
	return newImportStagePhasePlan(phase, canonical, syncStage, tasks)
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
	for _, spec := range []struct {
		phase     ImportStagePhase
		canonical rawdb.StageID
		sync      rawdb.StageID
		tasks     []ImportStageTask
	}{
		{ImportStagePhaseBodies, rawdb.StageBodies, rawdb.StageSyncImport, plan.Bodies},
		{ImportStagePhaseExecution, rawdb.StageExecution, rawdb.StageSyncExecution, plan.Execution},
		{ImportStagePhaseCommitment, rawdb.StageCommitment, rawdb.StageSyncCommitment, plan.Commitment},
		{ImportStagePhaseFinish, rawdb.StageFinish, rawdb.StageSyncFinish, plan.Finish},
	} {
		phasePlan, ok := newImportStagePhasePlan(spec.phase, spec.canonical, spec.sync, spec.tasks)
		if ok {
			phases = append(phases, phasePlan)
		}
	}
	return phases
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

// ImportStagePlan is the stage planner view for one local import boundary.
// It names the completed contiguous prefix and the first stage that still
// prevents the boundary from being fully published.
type ImportStagePlan struct {
	Schedule   ImportStageSchedule
	Tasks      []ImportStageTask
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

// ImportedBatchProgressPlanApplier performs the persistence/runtime operations
// named by an imported-batch progress plan. SyncService owns DB handles and
// logging; downloader owns the ordered stage side effects.
type ImportedBatchProgressPlanApplier interface {
	WriteImportedSyncProgress(deletes []rawdb.SyncStagedBlockDelete, rows []rawdb.StageProgress)
	RefreshSyncBodiesReady()
}

// ImportedBatchProgressPlan is the downloader-owned storage plan for the
// successfully imported prefix of one local staged-body batch.
type ImportedBatchProgressPlan struct {
	OK                   bool
	Summary              AppliedBatchSummary
	Schedule             ImportStageSchedule
	AppliedStagePlan     ImportBatchStagePlan
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
	ReportHead           uint64
	ReportPeer           *p2p.Peer
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
	if c == nil || observation.Task.SyncStage == "" {
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
			plan.AppliedDiagnostics = NewImportBatchExecutionPlanDiagnostics(appliedStagePlan.Schedules, appliedStagePlan)
			plan.Stages = append([]ImportStageTask(nil), appliedStagePlan.Tasks...)
			plan.StagePlan = collector.PlanBatch(appliedStagePlan)
			plan.Progress = plan.StagePlan.Progress
			plan.Decisions = plan.StagePlan.Decisions
			plan.StageDiagnostics = plan.StagePlan.Diagnostics()
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

// ApplyImportedBatchProgressPlan executes the downloader-owned side-effect
// schedule for an imported staged-body prefix.
func ApplyImportedBatchProgressPlan(plan ImportedBatchProgressPlan, applier ImportedBatchProgressPlanApplier) {
	if !plan.OK || applier == nil {
		return
	}
	for _, step := range plan.Steps {
		switch step.Action {
		case ImportedBatchWriteProgress:
			applier.WriteImportedSyncProgress(step.Deletes, step.Progress)
		case ImportedBatchRefreshBodiesReady:
			applier.RefreshSyncBodiesReady()
		}
	}
}

// NewImportStageSchedule returns the bodies/execution/commitment/finish targets
// required before sync progress can be published at one import boundary.
func NewImportStageSchedule(blockNum uint64, blockHash tcommon.Hash) ImportStageSchedule {
	body := ImportBodyStageTask(blockNum, blockHash)
	execution := ImportExecutionStageTask(blockNum, blockHash)
	commitment := ImportCommitmentStageTask(blockNum, blockHash)
	finish := ImportFinishStageTask(blockNum, blockHash)
	postBody := []ImportStageTask{execution, commitment, finish}
	return ImportStageSchedule{
		BlockNum:   blockNum,
		BlockHash:  blockHash,
		Body:       body,
		Execution:  execution,
		Commitment: commitment,
		Finish:     finish,
		PostBody:   postBody,
		Tasks:      append([]ImportStageTask{body}, postBody...),
	}
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
	progress, decisions := planImportBatchStageRows(p.observed, stagePlan)
	return newImportStagePlanFromTasks(stagePlan.Tasks, progress, decisions)
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

func planImportBatchStageRows(observed map[rawdb.StageID][]StageProgressRow, stagePlan ImportBatchStagePlan) ([]rawdb.StageProgress, []ImportStageProgressDecision) {
	progress := make([]rawdb.StageProgress, 0, len(stagePlan.Phases))
	decisions := make([]ImportStageProgressDecision, 0, len(stagePlan.Tasks))
	blocked := false
	for _, phase := range stagePlan.Phases {
		var (
			phaseRow     rawdb.StageProgress
			havePhaseRow bool
		)
		for _, task := range phase.Tasks {
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
			phaseRow = row
			havePhaseRow = true
			decisions = append(decisions, ImportStageProgressDecision{
				Task:   task,
				Stage:  task.SyncStage,
				Status: ImportStageProgressPlanned,
				Row:    row,
			})
		}
		if havePhaseRow {
			progress = append(progress, phaseRow)
		}
	}
	return progress, decisions
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

// ImportPipelineStageTasks returns the canonical-to-sync stage schedule for
// one applied import boundary.
func ImportPipelineStageTasks(blockNum uint64, blockHash tcommon.Hash) []ImportStageTask {
	schedule := NewImportStageSchedule(blockNum, blockHash)
	return append([]ImportStageTask(nil), schedule.Tasks...)
}

// ImportBodyStageTask returns the local body-import task for one applied
// boundary. It must precede execution tasks before progress can be published.
func ImportBodyStageTask(blockNum uint64, blockHash tcommon.Hash) ImportStageTask {
	return ImportStageTask{
		Phase:          ImportStagePhaseBodies,
		CanonicalStage: rawdb.StageBodies,
		SyncStage:      rawdb.StageSyncImport,
		BlockNum:       blockNum,
		BlockHash:      blockHash,
	}
}

// ImportExecutionStageTask returns the execution task for one applied import
// boundary.
func ImportExecutionStageTask(blockNum uint64, blockHash tcommon.Hash) ImportStageTask {
	return ImportStageTask{Phase: ImportStagePhaseExecution, CanonicalStage: rawdb.StageExecution, SyncStage: rawdb.StageSyncExecution, BlockNum: blockNum, BlockHash: blockHash}
}

// ImportCommitmentStageTask returns the commitment task for one applied import
// boundary.
func ImportCommitmentStageTask(blockNum uint64, blockHash tcommon.Hash) ImportStageTask {
	return ImportStageTask{Phase: ImportStagePhaseCommitment, CanonicalStage: rawdb.StageCommitment, SyncStage: rawdb.StageSyncCommitment, BlockNum: blockNum, BlockHash: blockHash}
}

// ImportFinishStageTask returns the finish task for one applied import
// boundary.
func ImportFinishStageTask(blockNum uint64, blockHash tcommon.Hash) ImportStageTask {
	return ImportStageTask{Phase: ImportStagePhaseFinish, CanonicalStage: rawdb.StageFinish, SyncStage: rawdb.StageSyncFinish, BlockNum: blockNum, BlockHash: blockHash}
}

// ImportExecutionStageTasks returns the explicit execution/commitment/finish
// task chain for one applied import boundary.
func ImportExecutionStageTasks(blockNum uint64, blockHash tcommon.Hash) []ImportStageTask {
	return []ImportStageTask{
		ImportExecutionStageTask(blockNum, blockHash),
		ImportCommitmentStageTask(blockNum, blockHash),
		ImportFinishStageTask(blockNum, blockHash),
	}
}

// StageForCanonicalStage maps canonical block insertion stages to their
// downloader/import diagnostic counterparts.
func StageForCanonicalStage(stage rawdb.StageID) (rawdb.StageID, bool) {
	switch stage {
	case rawdb.StageBodies:
		return rawdb.StageSyncImport, true
	case rawdb.StageExecution:
		return rawdb.StageSyncExecution, true
	case rawdb.StageCommitment:
		return rawdb.StageSyncCommitment, true
	case rawdb.StageFinish:
		return rawdb.StageSyncFinish, true
	default:
		return "", false
	}
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
