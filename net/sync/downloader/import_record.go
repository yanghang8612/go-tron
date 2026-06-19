package downloader

import (
	"time"

	tsync "github.com/tronprotocol/go-tron/net/sync"
	"github.com/tronprotocol/go-tron/p2p"
)

// ImportedBatchRecordStepAction names one ordered side effect after an imported
// staged-body prefix has a downloader-owned progress plan.
type ImportedBatchRecordStepAction uint8

const (
	ImportedBatchRecordApplyProgress ImportedBatchRecordStepAction = iota
	ImportedBatchRecordStats
	ImportedBatchRecordPrepareReport
	ImportedBatchRecordReportSegment
)

// ImportedBatchRecordStep is one downloader-owned record/report operation.
type ImportedBatchRecordStep struct {
	Action ImportedBatchRecordStepAction
}

// ImportedBatchStatsRecordResult records the rolling sync-stats outcome for an
// imported prefix.
type ImportedBatchStatsRecordResult struct {
	Snapshot tsync.Snapshot
	Emit     bool
}

// ImportedBatchReportPreparation carries the lock-held diagnostics snapshot
// needed before emitting an imported-segment report.
type ImportedBatchReportPreparation struct {
	Diagnostics Diagnostics
	Remaining   int64
}

// ImportedBatchRecordReport is the final imported-segment report payload.
type ImportedBatchRecordReport struct {
	Snapshot    tsync.Snapshot
	Diagnostics Diagnostics
	Head        uint64
	Remaining   int64
	Peer        *p2p.Peer
}

// ImportedBatchRecordPlan is the downloader-owned record/report schedule after
// a local import prefix has been accepted.
type ImportedBatchRecordPlan struct {
	Progress ImportedBatchProgressPlan
	Elapsed  time.Duration
	Steps    []ImportedBatchRecordStep
}

// ImportedBatchRecordPlanApplier performs service-owned side effects for an
// imported-batch record plan.
type ImportedBatchRecordPlanApplier interface {
	ApplyImportedBatchProgress(ImportedBatchProgressPlan) ImportedBatchProgressApplyResult
	RecordImportedBatchStats(blocks int, txs int, txKinds map[string]int, elapsed time.Duration) ImportedBatchStatsRecordResult
	PrepareImportedBatchReport(ImportedBatchProgressPlan, bool) ImportedBatchReportPreparation
	ReportImportedBatchSegment(ImportedBatchRecordReport)
}

// ImportedBatchRecordApplyResult records the side effects dispatched by an
// imported-batch record plan.
type ImportedBatchRecordApplyResult struct {
	AppliedSteps   []ImportedBatchRecordStepAction
	UnknownSteps   []ImportedBatchRecordStepAction
	ProgressApply  ImportedBatchProgressApplyResult
	HasProgress    bool
	Stats          ImportedBatchStatsRecordResult
	HasStats       bool
	Preparation    ImportedBatchReportPreparation
	HasPreparation bool
	Report         ImportedBatchRecordReport
	HasReport      bool
}

// PlanImportedBatchRecord returns the ordered record/report plan for an
// accepted local import prefix.
func PlanImportedBatchRecord(progress ImportedBatchProgressPlan, elapsed time.Duration) ImportedBatchRecordPlan {
	if !progress.OK {
		return ImportedBatchRecordPlan{}
	}
	return ImportedBatchRecordPlan{
		Progress: progress,
		Elapsed:  elapsed,
		Steps: []ImportedBatchRecordStep{
			{Action: ImportedBatchRecordApplyProgress},
			{Action: ImportedBatchRecordStats},
			{Action: ImportedBatchRecordPrepareReport},
			{Action: ImportedBatchRecordReportSegment},
		},
	}
}

// ApplyImportedBatchRecordPlan executes the downloader-owned record/report
// schedule for an imported staged-body prefix.
func ApplyImportedBatchRecordPlan(plan ImportedBatchRecordPlan, applier ImportedBatchRecordPlanApplier) ImportedBatchRecordApplyResult {
	var result ImportedBatchRecordApplyResult
	if !plan.Progress.OK || applier == nil {
		return result
	}
	for _, step := range plan.Steps {
		switch step.Action {
		case ImportedBatchRecordApplyProgress:
			result.ProgressApply = applier.ApplyImportedBatchProgress(plan.Progress)
			result.HasProgress = true
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
			if result.ProgressApply.Failed() {
				return result
			}
		case ImportedBatchRecordStats:
			result.Stats = applier.RecordImportedBatchStats(plan.Progress.StatsBlocks, plan.Progress.StatsTransactions, plan.Progress.StatsTxKinds, plan.Elapsed)
			result.HasStats = true
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case ImportedBatchRecordPrepareReport:
			result.Preparation = applier.PrepareImportedBatchReport(plan.Progress, result.Stats.Emit)
			result.HasPreparation = true
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		case ImportedBatchRecordReportSegment:
			if result.Stats.Emit {
				result.Report = ImportedBatchRecordReport{
					Snapshot:    result.Stats.Snapshot,
					Diagnostics: result.Preparation.Diagnostics,
					Head:        plan.Progress.ReportHead,
					Remaining:   result.Preparation.Remaining,
					Peer:        plan.Progress.ReportPeer,
				}
				applier.ReportImportedBatchSegment(result.Report)
				result.HasReport = true
			}
			result.AppliedSteps = append(result.AppliedSteps, step.Action)
		default:
			result.UnknownSteps = append(result.UnknownSteps, step.Action)
		}
	}
	return result
}
