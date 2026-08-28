package snapshots

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/metrics"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
)

func TestHistoryCompactionProgressPublishesLiveMetrics(t *testing.T) {
	var logBuf bytes.Buffer
	previousLogger := gtronlog.Root()
	defer gtronlog.SetDefault(previousLogger)
	gtronlog.SetDefault(gtronlog.NewLogger(gtronlog.LogfmtHandlerWithLevel(&logBuf, gtronlog.LevelInfo)))

	live := historyCompactionLiveMetrics{
		active:           metrics.NewGauge(),
		phase:            metrics.NewGauge(),
		recordsProcessed: metrics.NewGauge(),
		recordsTotal:     metrics.NewGauge(),
		sourcesProcessed: metrics.NewGauge(),
		sourcesTotal:     metrics.NewGauge(),
		elapsed:          metrics.NewGauge(),
		remapRows:        metrics.NewGauge(),
	}
	progress := newHistoryCompactionProgressWithMetrics(SegmentDatasetStateDomainChange, 10, 20, 3, live, historyCompactionProgressInterval)
	progress.setRecordTotal(1_000)
	progress.setPhase(historyCompactionPhaseCopyRecords)
	progress.setRecordsProcessed(400)
	progress.setSourcesProcessed(2)
	progress.addRemapRows(128)
	progress.logProgress()

	assertGaugeValue(t, live.active, 1)
	assertGaugeValue(t, live.phase, historyCompactionPhaseCopyRecords)
	assertGaugeValue(t, live.recordsProcessed, 400)
	assertGaugeValue(t, live.recordsTotal, 1_000)
	assertGaugeValue(t, live.sourcesProcessed, 2)
	assertGaugeValue(t, live.sourcesTotal, 3)
	assertGaugeValue(t, live.remapRows, 128)
	if live.elapsed.Snapshot().Value() <= 0 {
		t.Fatal("live compaction elapsed metric was not updated")
	}
	for _, field := range []string{`msg="History cold snapshot compaction progress"`, "progressPct=40", "recordsPerSecond=", "elapsed="} {
		if !strings.Contains(logBuf.String(), field) {
			t.Errorf("missing progress field %q:\n%s", field, logBuf.String())
		}
	}

	progress.finish(nil)
	assertGaugeValue(t, live.active, 0)
	assertGaugeValue(t, live.phase, historyCompactionPhaseIdle)
}

func TestHistoryCompactionPercentIsNumericAndRounded(t *testing.T) {
	if got := historyCompactionPercent(1, 3); got != 33.33 {
		t.Fatalf("progress = %v, want 33.33", got)
	}
	if got := historyCompactionPercent(0, 0); got != 0 {
		t.Fatalf("zero-total progress = %v, want 0", got)
	}
}

func TestHistoryCompactionCancellationIsNotLoggedAsFailure(t *testing.T) {
	var buf bytes.Buffer
	previous := gtronlog.Root()
	defer gtronlog.SetDefault(previous)
	gtronlog.SetDefault(gtronlog.NewLogger(gtronlog.LogfmtHandlerWithLevel(&buf, gtronlog.LevelInfo)))
	progress := newHistoryCompactionProgress(SegmentDatasetStateDomainChange, 1, 2, 2)
	progress.setPhase(historyCompactionPhaseBuildAccessor)
	progress.finish(context.Canceled)
	if !strings.Contains(buf.String(), `msg="History cold snapshot compaction canceled"`) || strings.Contains(buf.String(), "compaction failed") {
		t.Fatalf("unexpected cancellation log: %s", buf.String())
	}
}

func assertGaugeValue(t *testing.T, gauge *metrics.Gauge, want int64) {
	t.Helper()
	if got := gauge.Snapshot().Value(); got != want {
		t.Fatalf("gauge = %d, want %d", got, want)
	}
}
