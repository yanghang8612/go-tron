package downloader

import (
	"sync"

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
	rows := c.snapshotRows()
	for _, stage := range SyncPipelineProgressStages() {
		latest, ok := latestStageProgress(rows[stage], through)
		if !ok {
			continue
		}
		write(stage, latest.BlockNum, latest.Hash)
	}
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
