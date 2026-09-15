package snapshots

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestHistoryIndependentGCDoesNotShrinkRowDensity(t *testing.T) {
	for _, level := range []int{0, 1, 2, 3} {
		for _, hard := range []bool{false, true} {
			r := historyLoadRegressionRunner(t)
			r.historyLoad.level, r.historyLoad.hard = level, hard
			base := PassResult{Built: true, HistoryBuildAttempted: true, HistoryBatchBlocks: 100, HistoryBatchTxNums: 1000,
				BuildDuration: 3 * time.Second, HistoryMetadataDuration: 2 * time.Second,
				BeforeMergeDuration: 2 * time.Second, BeforeMergeMetadataDuration: time.Second,
				Segments: []SegmentRef{{Size: 1 << 20}}}
			r.recordHistoryWork(&base)
			wantBlocks, wantTxs := r.adaptiveHistoryBatchLimits(5000, 390625)
			for _, gc := range []time.Duration{0, 8 * time.Second, 2 * time.Minute} {
				result := base
				result.BeforeMergeDuration += gc
				result.BeforeMergeHistoryGCDuration = gc
				r.recordHistoryWork(&result)
				blocks, txs := r.adaptiveHistoryBatchLimits(5000, 390625)
				if r.historyLoad.history.work != 2*time.Second || blocks != wantBlocks || txs != wantTxs {
					t.Fatalf("independent GC changed row capacity level=%d hard=%v gc=%s: work=%s limits=%d/%d want=%d/%d", level, hard, gc, r.historyLoad.history.work, blocks, txs, wantBlocks, wantTxs)
				}
				if got := r.historyLoad.metrics["density_gc_work"].Snapshot().Value(); got != int64(gc) {
					t.Fatalf("GC metric=%d want=%d", got, gc)
				}
			}
			// Current-batch authentication/deletes remain row work, and output bytes
			// still restrict capacity. Neither is exempted merely because GC ran.
			base.BuildDuration += 400 * time.Second
			r.recordHistoryWork(&base)
			blocks, _ := r.adaptiveHistoryBatchLimits(5000, 390625)
			if blocks >= wantBlocks {
				t.Fatal("current batch row cost escaped limits")
			}
		}
	}
}

func TestHistoryIndependentGCTimingBoundsAndLegacy(t *testing.T) {
	for _, tc := range []struct {
		name           string
		result         PassResult
		work, metadata time.Duration
		measurement    int64
	}{
		{"legacy zero", PassResult{BuildDuration: time.Second, BeforeMergeDuration: 5 * time.Second}, 6 * time.Second, 0, 0},
		{"only GC timed", PassResult{BuildDuration: time.Second, BeforeMergeDuration: 5 * time.Second, BeforeMergeHistoryGCDuration: 5 * time.Second}, time.Second, 0, 1},
		{"disjoint regions", PassResult{BuildDuration: time.Second, BeforeMergeDuration: 6 * time.Second, BeforeMergeMetadataDuration: 2 * time.Second, BeforeMergeHistoryGCDuration: 3 * time.Second}, 2 * time.Second, 2 * time.Second, 1},
		{"negative GC", PassResult{BuildDuration: time.Second, BeforeMergeDuration: 5 * time.Second, BeforeMergeHistoryGCDuration: -1}, 6 * time.Second, 0, 2},
		{"overlapping exclusions", PassResult{BuildDuration: time.Second, BeforeMergeDuration: 5 * time.Second, BeforeMergeMetadataDuration: 3 * time.Second, BeforeMergeHistoryGCDuration: 3 * time.Second}, 6 * time.Second, 0, 2},
		{"GC cannot exclude build", PassResult{BuildDuration: 9 * time.Second, BeforeMergeDuration: time.Second, BeforeMergeHistoryGCDuration: 2 * time.Second}, 10 * time.Second, 0, 2},
		{"invalid zero", PassResult{BeforeMergeHistoryGCDuration: time.Second}, time.Duration(math.MaxInt64), 0, 2},
		{"timer resolution", PassResult{BeforeMergeDuration: time.Nanosecond, BeforeMergeHistoryGCDuration: time.Nanosecond}, time.Nanosecond, 0, 1},
		{"subtract before saturation", PassResult{BuildDuration: time.Duration(math.MaxInt64), HistoryMetadataDuration: time.Duration(math.MaxInt64) - 1, BeforeMergeDuration: time.Duration(math.MaxInt64), BeforeMergeHistoryGCDuration: time.Duration(math.MaxInt64) - 1}, 2 * time.Nanosecond, time.Duration(math.MaxInt64) - 1, 1},
		{"sum exclusion overflow", PassResult{BeforeMergeDuration: time.Duration(math.MaxInt64), BeforeMergeMetadataDuration: time.Duration(math.MaxInt64) - 1, BeforeMergeHistoryGCDuration: time.Duration(math.MaxInt64) - 1}, time.Duration(math.MaxInt64), 0, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, m, measurement := historyDensityWork(&tc.result)
			if w != tc.work || m != tc.metadata || measurement != tc.measurement {
				t.Fatalf("got %s/%s/%d want %s/%s/%d", w, m, measurement, tc.work, tc.metadata, tc.measurement)
			}
			r := historyLoadRegressionRunner(t)
			tc.result.Built, tc.result.HistoryBuildAttempted = true, true
			r.recordHistoryWork(&tc.result)
			wantGC := time.Duration(0)
			if measurement == 1 {
				wantGC = tc.result.BeforeMergeHistoryGCDuration
			}
			if r.historyLoad.metrics["density_gc_work"].Snapshot().Value() != int64(wantGC) {
				t.Fatal("invalid measurement leaked GC exclusion metric")
			}
		})
	}
}

func TestHistoryIndependentGCCompleteMaintenanceStillCharged(t *testing.T) {
	r := throughputFixture(t)
	result, err := r.OnePassWithDeferredMaintenanceContext(context.Background(), nil)
	if err != nil || !result.Built || !result.historyCompletionPending {
		t.Fatalf("real builder handoff: %+v %v", result, err)
	}
	// Replay an independently timed outer prune: 1s metadata, 8s GC, 1s hot rows.
	// Exact outer accounting still comes from lifecycle wall time, not this sample.
	result.BeforeMergeDuration = 10 * time.Second
	result.BeforeMergeMetadataDuration = time.Second
	result.BeforeMergeHistoryGCDuration = 8 * time.Second
	rowWork, _, measurement := historyDensityWork(&result)
	if measurement != 1 {
		t.Fatal("invalid replay fixture")
	}
	prior := r.historyNotBefore.Load()
	r.CompleteHistoryMaintenance(&result, time.Now().Add(-20*time.Second), nil)
	if result.HistoryMaintenanceDuration < 20*time.Second || result.HistoryRecoveryCost < 20*time.Second-result.CompactionDuration || result.HistoryMinRecovery < r.throughputRecovery(result.HistoryRecoveryCost, false) || r.historyNotBefore.Load() < prior {
		t.Fatalf("independent GC escaped complete recovery: %+v", result)
	}
	if r.historyLoad.history.work != rowWork {
		t.Fatal("completion did not record independent row density")
	}
	deadline := r.historyNotBefore.Load()
	r.CompleteHistoryMaintenance(&result, time.Now().Add(-time.Hour), nil)
	if r.historyNotBefore.Load() != deadline {
		t.Fatal("duplicate completion charged recovery twice")
	}
	r.cfg.SyncBuildReady = func() bool { return true }
	r.chain.(*coldBuilderChain).syncRemainingOK = false
	next, err := r.OnePass()
	if err != nil || next.HistoryBuildAttempted || !next.HistoryRateLimited || r.historyNotBefore.Load() != deadline {
		t.Fatalf("ready/idle bypassed full recovery: %+v %v", next, err)
	}
}
