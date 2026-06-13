package downloader

import (
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
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

// ImportStageTask is one explicit canonical import stage target. The
// downloader publishes the matching sync diagnostic stage only after the
// canonical stage hook observes this exact block/hash boundary.
type ImportStageTask struct {
	CanonicalStage rawdb.StageID
	SyncStage      rawdb.StageID
	BlockNum       uint64
	BlockHash      tcommon.Hash
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

// ImportedBatchProgressPlan is the downloader-owned storage plan for the
// successfully imported prefix of one local staged-body batch.
type ImportedBatchProgressPlan struct {
	OK        bool
	Summary   AppliedBatchSummary
	Stages    []ImportStageTask
	Deletes   []rawdb.SyncStagedBlockDelete
	Progress  []rawdb.StageProgress
	Decisions []ImportStageProgressDecision
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
	rows, _ := planImportStageRows(observed, tasks)
	return rows
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
		OK:      true,
		Summary: summary,
		Deletes: AppliedStagedBlockDeletes(batch, summary.Applied),
	}
	if !summary.HasStage {
		return plan
	}
	plan.Stages = ImportPipelineStageTasks(summary.Last.Num, summary.Last.Hash)
	plan.Progress, plan.Decisions = collector.plannedRows(plan.Stages)
	return plan
}

func (c *StageProgressCollector) plannedRows(stages []ImportStageTask) ([]rawdb.StageProgress, []ImportStageProgressDecision) {
	observed := c.snapshotRows()
	return planImportStageRows(observed, stages)
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
	rows := make(map[rawdb.StageID][]StageProgressRow, len(c.rows))
	for stage, stageRows := range c.rows {
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
	return []ImportStageTask{
		{CanonicalStage: rawdb.StageBodies, SyncStage: rawdb.StageSyncImport, BlockNum: blockNum, BlockHash: blockHash},
		{CanonicalStage: rawdb.StageExecution, SyncStage: rawdb.StageSyncExecution, BlockNum: blockNum, BlockHash: blockHash},
		{CanonicalStage: rawdb.StageCommitment, SyncStage: rawdb.StageSyncCommitment, BlockNum: blockNum, BlockHash: blockHash},
		{CanonicalStage: rawdb.StageFinish, SyncStage: rawdb.StageSyncFinish, BlockNum: blockNum, BlockHash: blockHash},
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
