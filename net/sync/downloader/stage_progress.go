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
	ImportStageProgressBlocked
)

// ImportStageProgressDecision is one stage planner decision for an applied
// sync import prefix. Later stages are blocked after the first missing stage so
// persisted sync progress remains a contiguous pipeline prefix.
type ImportStageProgressDecision struct {
	Stage  rawdb.StageID
	Status ImportStageProgressStatus
	Row    rawdb.StageProgress
}

// ImportedBatchProgressPlan is the downloader-owned storage plan for the
// successfully imported prefix of one local staged-body batch.
type ImportedBatchProgressPlan struct {
	OK        bool
	Summary   AppliedBatchSummary
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

// Write emits the newest observed row at or below through for each sync
// pipeline stage, in operator stage-status order.
func (c *StageProgressCollector) Write(through uint64, write StageProgressWriter) {
	if c == nil || write == nil {
		return
	}
	for _, row := range c.Rows(through) {
		write(row.Stage, row.BlockNum, row.BlockHash)
	}
}

// Rows returns the newest observed hash-bound row at or below through for each
// sync pipeline stage, in operator stage-status order.
func (c *StageProgressCollector) Rows(through uint64) []rawdb.StageProgress {
	if c == nil {
		return nil
	}
	observed := c.snapshotRows()
	rows := make([]rawdb.StageProgress, 0, len(SyncPipelineProgressStages()))
	for _, stage := range SyncPipelineProgressStages() {
		latest, ok := latestStageProgress(observed[stage], through)
		if !ok {
			continue
		}
		rows = append(rows, rawdb.StageProgress{
			Stage:        stage,
			BlockNum:     latest.BlockNum,
			BlockHash:    latest.Hash,
			HasBlockHash: true,
		})
	}
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
	plan.Progress, plan.Decisions = collector.contiguousRows(summary.Last.Num)
	return plan
}

func (c *StageProgressCollector) contiguousRows(through uint64) ([]rawdb.StageProgress, []ImportStageProgressDecision) {
	observed := make(map[rawdb.StageID]rawdb.StageProgress)
	for _, row := range c.Rows(through) {
		observed[row.Stage] = row
	}
	stages := SyncPipelineProgressStages()
	progress := make([]rawdb.StageProgress, 0, len(stages))
	decisions := make([]ImportStageProgressDecision, 0, len(stages))
	blocked := false
	for _, stage := range stages {
		if blocked {
			decisions = append(decisions, ImportStageProgressDecision{
				Stage:  stage,
				Status: ImportStageProgressBlocked,
			})
			continue
		}
		row, ok := observed[stage]
		if !ok {
			blocked = true
			decisions = append(decisions, ImportStageProgressDecision{
				Stage:  stage,
				Status: ImportStageProgressMissing,
			})
			continue
		}
		progress = append(progress, row)
		decisions = append(decisions, ImportStageProgressDecision{
			Stage:  stage,
			Status: ImportStageProgressPlanned,
			Row:    row,
		})
	}
	return progress, decisions
}

func (c *StageProgressCollector) snapshotRows() map[rawdb.StageID][]StageProgressRow {
	c.mu.Lock()
	defer c.mu.Unlock()
	rows := make(map[rawdb.StageID][]StageProgressRow, len(c.rows))
	for stage, stageRows := range c.rows {
		rows[stage] = append([]StageProgressRow(nil), stageRows...)
	}
	return rows
}

func latestStageProgress(rows []StageProgressRow, through uint64) (StageProgressRow, bool) {
	var (
		latest StageProgressRow
		have   bool
	)
	for _, row := range rows {
		if row.BlockNum > through {
			continue
		}
		if !have || row.BlockNum > latest.BlockNum {
			latest = row
			have = true
		}
	}
	return latest, have
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
