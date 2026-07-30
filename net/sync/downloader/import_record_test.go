package downloader

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
	tsync "github.com/tronprotocol/go-tron/net/sync"
)

func TestPlanImportedBatchRecord(t *testing.T) {
	progress := ImportedBatchProgressPlan{OK: true}
	elapsed := 12 * time.Millisecond

	got := PlanImportedBatchRecord(progress, elapsed)
	wantSteps := []ImportedBatchRecordStep{
		{Action: ImportedBatchRecordApplyProgress},
		{Action: ImportedBatchRecordStats},
		{Action: ImportedBatchRecordPrepareReport},
		{Action: ImportedBatchRecordReportSegment},
	}
	if !reflect.DeepEqual(got.Progress, progress) || got.Elapsed != elapsed || !reflect.DeepEqual(got.Steps, wantSteps) {
		t.Fatalf("record plan = %+v, want progress/elapsed and ordered record steps %+v", got, wantSteps)
	}

	if empty := PlanImportedBatchRecord(ImportedBatchProgressPlan{}, elapsed); empty.Progress.OK || len(empty.Steps) != 0 {
		t.Fatalf("empty record plan = %+v, want no-op for invalid progress plan", empty)
	}
}

func TestApplyImportedBatchRecordPlanEmitsReport(t *testing.T) {
	progress := ImportedBatchProgressPlan{
		OK:                true,
		StatsBlocks:       2,
		StatsTransactions: 3,
		StatsTxKinds:      map[string]int{"TransferContract": 2, "TriggerSmartContract": 1},
		ReportHead:        9,
	}
	elapsed := 27 * time.Millisecond
	plan := PlanImportedBatchRecord(progress, elapsed)
	applier := &recordingImportedBatchRecordApplier{
		progressApply: ImportedBatchProgressApplyResult{HasReadyRefresh: true},
		stats: ImportedBatchStatsRecordResult{
			Emit:     true,
			Snapshot: tsync.Snapshot{Blocks: 2, Txs: 3},
		},
		preparation: ImportedBatchReportPreparation{
			Diagnostics: Diagnostics{BlockBufferLen: 7},
			Remaining:   42,
		},
	}

	got := ApplyImportedBatchRecordPlan(plan, applier)
	wantSteps := []ImportedBatchRecordStepAction{
		ImportedBatchRecordApplyProgress,
		ImportedBatchRecordStats,
		ImportedBatchRecordPrepareReport,
		ImportedBatchRecordReportSegment,
	}
	if !reflect.DeepEqual(got.AppliedSteps, wantSteps) || len(got.UnknownSteps) != 0 {
		t.Fatalf("record applied steps = %+v unknown=%+v, want %+v and no unknown", got.AppliedSteps, got.UnknownSteps, wantSteps)
	}
	if !reflect.DeepEqual(applier.calls, wantSteps) {
		t.Fatalf("applier calls = %+v, want %+v", applier.calls, wantSteps)
	}
	if !reflect.DeepEqual(applier.progress, progress) || applier.statsBlocks != 2 || applier.statsTxs != 3 || applier.statsElapsed != elapsed {
		t.Fatalf("applier progress/stats = progress:%+v blocks:%d txs:%d elapsed:%s, want plan stats",
			applier.progress, applier.statsBlocks, applier.statsTxs, applier.statsElapsed)
	}
	if !reflect.DeepEqual(applier.statsTxKinds, progress.StatsTxKinds) {
		t.Fatalf("applier statsTxKinds = %v, want %v", applier.statsTxKinds, progress.StatsTxKinds)
	}
	if !got.HasProgress || !reflect.DeepEqual(got.ProgressApply, applier.progressApply) ||
		!got.HasStats || !reflect.DeepEqual(got.Stats, applier.stats) ||
		!got.HasPreparation || !reflect.DeepEqual(got.Preparation, applier.preparation) {
		t.Fatalf("record result = %+v, want progress/stats/preparation results preserved", got)
	}
	if !applier.prepareEmit || !reflect.DeepEqual(applier.prepareProgress, progress) {
		t.Fatalf("prepare args = emit:%v progress:%+v, want emit=true and progress plan", applier.prepareEmit, applier.prepareProgress)
	}
	wantReport := ImportedBatchRecordReport{
		Snapshot:    applier.stats.Snapshot,
		Diagnostics: applier.preparation.Diagnostics,
		Head:        progress.ReportHead,
		Remaining:   applier.preparation.Remaining,
	}
	if !got.HasReport || !reflect.DeepEqual(got.Report, wantReport) || !reflect.DeepEqual(applier.report, wantReport) {
		t.Fatalf("report = result:%+v applier:%+v, want %+v", got.Report, applier.report, wantReport)
	}
}

func TestApplyImportedBatchRecordPlanSkipsReportWithoutStatsEmit(t *testing.T) {
	progress := ImportedBatchProgressPlan{OK: true, StatsBlocks: 1, ReportHead: 4}
	applier := &recordingImportedBatchRecordApplier{
		stats:       ImportedBatchStatsRecordResult{Emit: false},
		preparation: ImportedBatchReportPreparation{Remaining: 11},
	}

	got := ApplyImportedBatchRecordPlan(PlanImportedBatchRecord(progress, time.Millisecond), applier)
	if !got.HasPreparation || got.Preparation.Remaining != 11 {
		t.Fatalf("preparation = %+v set=%v, want preparation even without report emit", got.Preparation, got.HasPreparation)
	}
	if applier.prepareEmit {
		t.Fatal("prepare saw emit=true, want false")
	}
	if got.HasReport || applier.reported {
		t.Fatalf("report emitted = result:%v applier:%v, want no report without stats emit", got.HasReport, applier.reported)
	}

	if empty := ApplyImportedBatchRecordPlan(PlanImportedBatchRecord(progress, time.Millisecond), nil); len(empty.AppliedSteps) != 0 || empty.HasStats || empty.HasReport {
		t.Fatalf("nil applier result = %+v, want empty", empty)
	}
	if empty := ApplyImportedBatchRecordPlan(ImportedBatchRecordPlan{}, applier); len(empty.AppliedSteps) != 0 || empty.HasStats || empty.HasReport {
		t.Fatalf("empty plan result = %+v, want empty", empty)
	}
}

func TestApplyImportedBatchRecordPlanStopsAfterProgressWriteFailure(t *testing.T) {
	progressErr := errors.New("progress write failed")
	progress := ImportedBatchProgressPlan{OK: true, StatsBlocks: 2, StatsTransactions: 3, ReportHead: 9}
	applier := &recordingImportedBatchRecordApplier{
		progressApply: ImportedBatchProgressApplyResult{
			HasWriteResult: true,
			WriteResult:    rawdb.SyncImportProgressWriteResult{ProgressError: progressErr},
		},
		stats: ImportedBatchStatsRecordResult{
			Emit:     true,
			Snapshot: tsync.Snapshot{Blocks: 2, Txs: 3},
		},
		preparation: ImportedBatchReportPreparation{Remaining: 42},
	}

	got := ApplyImportedBatchRecordPlan(PlanImportedBatchRecord(progress, time.Millisecond), applier)
	if !got.HasProgress || !got.ProgressApply.WriteFailed() || got.ProgressApply.WriteResult.ProgressError != progressErr {
		t.Fatalf("progress apply = %+v, want write failure preserved", got.ProgressApply)
	}
	if !reflect.DeepEqual(got.AppliedSteps, []ImportedBatchRecordStepAction{ImportedBatchRecordApplyProgress}) {
		t.Fatalf("applied steps = %+v, want progress only", got.AppliedSteps)
	}
	if !reflect.DeepEqual(applier.calls, []ImportedBatchRecordStepAction{ImportedBatchRecordApplyProgress}) {
		t.Fatalf("applier calls = %+v, want progress only", applier.calls)
	}
	if got.HasStats || got.HasPreparation || got.HasReport || applier.reported {
		t.Fatalf("record result after write failure = %+v reported=%v, want no stats/prep/report", got, applier.reported)
	}
}

func TestApplyImportedBatchRecordPlanStopsAfterReadyRefreshFailure(t *testing.T) {
	readyErr := errors.New("ready refresh failed")
	progress := ImportedBatchProgressPlan{OK: true, StatsBlocks: 2, StatsTransactions: 3, ReportHead: 9}
	applier := &recordingImportedBatchRecordApplier{
		progressApply: ImportedBatchProgressApplyResult{
			HasWriteResult:  true,
			WriteResult:     rawdb.SyncImportProgressWriteResult{Deleted: 1, ProgressRows: 4},
			HasReadyRefresh: true,
			ReadyRefresh:    StagedBodyReadyProgressRefresh{Updated: true, WriteError: readyErr},
		},
		stats: ImportedBatchStatsRecordResult{
			Emit:     true,
			Snapshot: tsync.Snapshot{Blocks: 2, Txs: 3},
		},
		preparation: ImportedBatchReportPreparation{Remaining: 42},
	}

	got := ApplyImportedBatchRecordPlan(PlanImportedBatchRecord(progress, time.Millisecond), applier)
	if !got.HasProgress || !got.ProgressApply.ReadyRefreshFailed() || !got.ProgressApply.Failed() || got.ProgressApply.ReadyRefresh.WriteError != readyErr {
		t.Fatalf("progress apply = %+v, want ready refresh failure preserved", got.ProgressApply)
	}
	if !reflect.DeepEqual(got.AppliedSteps, []ImportedBatchRecordStepAction{ImportedBatchRecordApplyProgress}) {
		t.Fatalf("applied steps = %+v, want progress only", got.AppliedSteps)
	}
	if !reflect.DeepEqual(applier.calls, []ImportedBatchRecordStepAction{ImportedBatchRecordApplyProgress}) {
		t.Fatalf("applier calls = %+v, want progress only", applier.calls)
	}
	if got.HasStats || got.HasPreparation || got.HasReport || applier.reported {
		t.Fatalf("record result after ready refresh failure = %+v reported=%v, want no stats/prep/report", got, applier.reported)
	}
}

type recordingImportedBatchRecordApplier struct {
	calls           []ImportedBatchRecordStepAction
	progress        ImportedBatchProgressPlan
	progressApply   ImportedBatchProgressApplyResult
	statsBlocks     int
	statsTxs        int
	statsTxKinds    map[string]int
	statsElapsed    time.Duration
	stats           ImportedBatchStatsRecordResult
	prepareProgress ImportedBatchProgressPlan
	prepareEmit     bool
	preparation     ImportedBatchReportPreparation
	report          ImportedBatchRecordReport
	reported        bool
}

func (a *recordingImportedBatchRecordApplier) ApplyImportedBatchProgress(plan ImportedBatchProgressPlan) ImportedBatchProgressApplyResult {
	a.calls = append(a.calls, ImportedBatchRecordApplyProgress)
	a.progress = plan
	return a.progressApply
}

func (a *recordingImportedBatchRecordApplier) RecordImportedBatchStats(blocks int, txs int, txKinds map[string]int, elapsed time.Duration) ImportedBatchStatsRecordResult {
	a.calls = append(a.calls, ImportedBatchRecordStats)
	a.statsBlocks = blocks
	a.statsTxs = txs
	a.statsTxKinds = txKinds
	a.statsElapsed = elapsed
	return a.stats
}

func (a *recordingImportedBatchRecordApplier) PrepareImportedBatchReport(plan ImportedBatchProgressPlan, emit bool) ImportedBatchReportPreparation {
	a.calls = append(a.calls, ImportedBatchRecordPrepareReport)
	a.prepareProgress = plan
	a.prepareEmit = emit
	return a.preparation
}

func (a *recordingImportedBatchRecordApplier) ReportImportedBatchSegment(report ImportedBatchRecordReport) {
	a.calls = append(a.calls, ImportedBatchRecordReportSegment)
	a.report = report
	a.reported = true
}
