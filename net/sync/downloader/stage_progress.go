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

// ImportStageSchedule is the explicit stage schedule for one sync import
// boundary. The canonical insert hook only supplies observations; this
// schedule owns the required bodies/execution/commitment/finish targets.
type ImportStageSchedule struct {
	BlockNum  uint64
	BlockHash tcommon.Hash
	Body      ImportStageTask
	Execution []ImportStageTask
	Tasks     []ImportStageTask
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
	Scheduled     int
	Completed     int
	Complete      bool
	NextPhase     ImportStagePhase
	NextStage     rawdb.StageID
	BlockedStatus ImportStageProgressStatus
	HasBlocked    bool
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
	OK                bool
	Summary           AppliedBatchSummary
	Schedule          ImportStageSchedule
	StagePlan         ImportStagePlan
	Stages            []ImportStageTask
	Deletes           []rawdb.SyncStagedBlockDelete
	Progress          []rawdb.StageProgress
	Decisions         []ImportStageProgressDecision
	RefreshReady      bool
	Steps             []ImportedBatchProgressStep
	StatsBlocks       int
	StatsTransactions int
	ReportHead        uint64
	ReportPeer        *p2p.Peer
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
	c.mu.Lock()
	if c.rows == nil {
		c.rows = make(map[rawdb.StageID][]StageProgressRow)
	}
	c.rows[syncStage] = append(c.rows[syncStage], StageProgressRow{BlockNum: blockNum, Hash: hash})
	c.mu.Unlock()
}

// Write emits the planned sync pipeline prefix for through, in operator
// stage-status order.
func (c *StageProgressCollector) Write(through uint64, write StageProgressWriter) {
	if c == nil || write == nil {
		return
	}
	for _, row := range c.Rows(through) {
		write(row.Stage, row.BlockNum, row.BlockHash)
	}
}

// Rows returns the planned sync pipeline prefix for through, in operator
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

// PlanImportedBatchProgress derives the DB-side progress plan for an applied
// import prefix. It keeps sync import/execution/commitment/finish rows as a
// contiguous stage prefix: if a stage is missing, later observed rows are not
// published for this batch.
func PlanImportedBatchProgress(batch BufferedBatch, applied int, collector *StageProgressCollector) ImportedBatchProgressPlan {
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
	plan.Schedule = NewImportStageSchedule(summary.Last.Num, summary.Last.Hash)
	plan.Stages = plan.Schedule.Tasks
	plan.StagePlan = collector.PlanSchedule(plan.Schedule)
	plan.Progress = plan.StagePlan.Progress
	plan.Decisions = plan.StagePlan.Decisions
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
	execution := ImportExecutionStageTasks(blockNum, blockHash)
	return ImportStageSchedule{
		BlockNum:  blockNum,
		BlockHash: blockHash,
		Body:      body,
		Execution: execution,
		Tasks:     append([]ImportStageTask{body}, execution...),
	}
}

// PlanSchedule returns the explicit stage planner result for schedule.
func (c *StageProgressCollector) PlanSchedule(schedule ImportStageSchedule) ImportStagePlan {
	return NewImportStagePlanner(c).Plan(schedule)
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

func newImportStagePlan(schedule ImportStageSchedule, progress []rawdb.StageProgress, decisions []ImportStageProgressDecision) ImportStagePlan {
	plan := ImportStagePlan{
		Schedule:  schedule,
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
	plan.Complete = len(decisions) > 0 && len(plan.Completed) == len(schedule.Tasks)
	return plan
}

// Diagnostics returns the stable diagnostic view of this import stage plan.
func (p ImportStagePlan) Diagnostics() ImportStagePlanDiagnostics {
	diag := ImportStagePlanDiagnostics{
		Scheduled: len(p.Schedule.Tasks),
		Completed: len(p.Completed),
		Complete:  p.Complete,
	}
	if p.HasBlocked {
		diag.NextPhase = p.Next.Phase
		diag.NextStage = p.Next.SyncStage
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

// ImportExecutionStageTasks returns the explicit execution/commitment/finish
// task chain for one applied import boundary.
func ImportExecutionStageTasks(blockNum uint64, blockHash tcommon.Hash) []ImportStageTask {
	return []ImportStageTask{
		{Phase: ImportStagePhaseExecution, CanonicalStage: rawdb.StageExecution, SyncStage: rawdb.StageSyncExecution, BlockNum: blockNum, BlockHash: blockHash},
		{Phase: ImportStagePhaseCommitment, CanonicalStage: rawdb.StageCommitment, SyncStage: rawdb.StageSyncCommitment, BlockNum: blockNum, BlockHash: blockHash},
		{Phase: ImportStagePhaseFinish, CanonicalStage: rawdb.StageFinish, SyncStage: rawdb.StageSyncFinish, BlockNum: blockNum, BlockHash: blockHash},
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
	stages := SyncPipelineProgressStages()
	repairs := make([]SyncStageProgressRepair, 0, len(stages))
	for _, stage := range stages {
		repairs = append(repairs, RepairSyncStageProgress(db, stage, head, canonicalHash))
	}
	var (
		upstream rawdb.StageProgress
		haveUp   bool
		blocked  bool
	)
	for i := range repairs {
		repair := &repairs[i]
		switch repair.Status {
		case SyncStageProgressKept:
			if blocked || (haveUp && repair.Row.BlockNum > upstream.BlockNum) {
				if err := rawdb.DeleteStageProgress(db, repair.Stage); err != nil {
					repair.Status = SyncStageProgressDeleteError
					repair.DeleteError = err
					blocked = true
					continue
				}
				repair.Status = SyncStageProgressDeleted
				blocked = true
				continue
			}
			upstream = repair.Row
			haveUp = true
		case SyncStageProgressMissing, SyncStageProgressDeleted:
			blocked = true
		case SyncStageProgressReadError, SyncStageProgressDeleteError:
			return repairs
		}
	}
	return repairs
}
