package snapshots

import (
	"testing"

	"github.com/ethereum/go-ethereum/metrics"
)

func TestHistoryCompactionProgressPublishesLiveMetrics(t *testing.T) {
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

	progress.finish(nil)
	assertGaugeValue(t, live.active, 0)
	assertGaugeValue(t, live.phase, historyCompactionPhaseIdle)
}

func assertGaugeValue(t *testing.T, gauge *metrics.Gauge, want int64) {
	t.Helper()
	if got := gauge.Snapshot().Value(); got != want {
		t.Fatalf("gauge = %d, want %d", got, want)
	}
}
