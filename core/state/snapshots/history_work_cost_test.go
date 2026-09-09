package snapshots

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestHistoryDensityFixedMetadataDoesNotShrinkRows(t *testing.T) {
	for _, level := range []int{1, 2, 3} {
		t.Run(string(rune('0'+level)), func(t *testing.T) {
			r := historyLoadRegressionRunner(t)
			r.historyLoad.level = level
			result := PassResult{Built: true, HistoryBuildAttempted: true,
				HistoryBatchBlocks: 100, HistoryBatchTxNums: 1000,
				BuildDuration: 40*time.Second + 500*time.Millisecond, HistoryMetadataDuration: 40 * time.Second,
				BeforeMergeDuration: 20*time.Second + 500*time.Millisecond, BeforeMergeMetadataDuration: 20 * time.Second,
				Segments: []SegmentRef{{Size: 1 << 20}}}
			r.recordHistoryWork(&result)
			if r.historyLoad.history.work != time.Second {
				t.Fatalf("metadata poisoned row density: %+v", r.historyLoad.history)
			}
			blocks, txs := r.adaptiveHistoryBatchLimits(5000, 390625)
			if blocks != 125 || txs != 1250 {
				t.Fatalf("fixed work above target shrank rows: %d/%d", blocks, txs)
			}
			// Sixty seconds of metadata has not become free: full recovery is
			// still based on sixty-one seconds, not the one-second row estimate.
			result.HistoryForcedBusy = true
			start := time.Now()
			r.applyThroughputRecovery(&result, start, start.Add(61*time.Second), nil)
			if result.HistoryRecoveryCost != 61*time.Second || result.HistoryMinRecovery != r.throughputRecovery(61*time.Second, false) {
				t.Fatalf("metadata removed from maintenance duty: %+v", result)
			}
		})
	}
}

func TestHistoryDensityKeepsRealWorkAndBytesLimits(t *testing.T) {
	r := historyLoadRegressionRunner(t)
	for _, tc := range []struct {
		name                string
		data                time.Duration
		bytes               uint64
		wantBlocks, wantTxs uint64
	}{
		{"row cost jump", 400 * time.Second, 64 << 20, 20, 2000},
		{"byte cost jump", time.Second, 6400 << 20, 20, 2000},
		{"both jump", 800 * time.Second, 12800 << 20, 10, 1000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := PassResult{Built: true, HistoryBuildAttempted: true,
				HistoryBatchBlocks: 1000, HistoryBatchTxNums: 100_000,
				BuildDuration: tc.data + time.Minute, HistoryMetadataDuration: time.Minute,
				Segments: []SegmentRef{{Size: tc.bytes}}}
			r.recordHistoryWork(&result)
			b, tx := r.adaptiveHistoryBatchLimits(5000, 390625)
			if b != tc.wantBlocks || tx != tc.wantTxs {
				t.Fatalf("actual density/bytes were discounted: %d/%d", b, tx)
			}
		})
	}
}

func TestHistoryDensityFallbackAndDurationBounds(t *testing.T) {
	for _, tc := range []struct {
		name           string
		result         PassResult
		work, metadata time.Duration
		measurement    int64
	}{
		{"unmeasured callback", PassResult{BuildDuration: 6 * time.Second, HistoryMetadataDuration: 5 * time.Second, BeforeMergeDuration: 30 * time.Second}, 31 * time.Second, 5 * time.Second, 1},
		{"legacy caller", PassResult{BuildDuration: 6 * time.Second, BeforeMergeDuration: time.Second}, 7 * time.Second, 0, 0},
		{"negative metadata", PassResult{BuildDuration: time.Second, HistoryMetadataDuration: -time.Second}, time.Second, 0, 2},
		{"impossible builder metadata", PassResult{BuildDuration: time.Second, HistoryMetadataDuration: 2 * time.Second}, time.Second, 0, 2},
		{"impossible prune metadata", PassResult{BuildDuration: 6 * time.Second, HistoryMetadataDuration: 5 * time.Second, BeforeMergeMetadataDuration: time.Second}, 6 * time.Second, 0, 2},
		{"impossible zero timing", PassResult{HistoryMetadataDuration: time.Second}, time.Duration(math.MaxInt64), 0, 2},
		{"timer resolution", PassResult{BuildDuration: time.Nanosecond, HistoryMetadataDuration: time.Nanosecond}, time.Nanosecond, time.Nanosecond, 1},
		{"saturated totals", PassResult{BuildDuration: time.Duration(math.MaxInt64), HistoryMetadataDuration: time.Duration(math.MaxInt64) - 1, BeforeMergeDuration: time.Duration(math.MaxInt64), BeforeMergeMetadataDuration: time.Duration(math.MaxInt64) - 1}, 2 * time.Nanosecond, time.Duration(math.MaxInt64), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, m, known := historyDensityWork(&tc.result)
			if w != tc.work || m != tc.metadata || known != tc.measurement {
				t.Fatalf("cost=%s/%s/%d want %s/%s/%d", w, m, known, tc.work, tc.metadata, tc.measurement)
			}
		})
	}
}

func TestHistoryDensitySmallUnknownAndFailedBatch(t *testing.T) {
	r := historyLoadRegressionRunner(t)
	for _, blocks := range []uint64{1, 2, 3} {
		result := PassResult{Built: true, HistoryBuildAttempted: true,
			HistoryBatchBlocks: blocks, HistoryBatchTxNums: blocks,
			BuildDuration: time.Minute + time.Millisecond, HistoryMetadataDuration: time.Minute}
		r.recordHistoryWork(&result)
		r.historyLoad.level = 3
		b, tx := r.adaptiveHistoryBatchLimits(5000, 390625)
		if b != blocks+1 || tx != blocks+1 {
			t.Fatalf("small range cannot recover: %d/%d", b, tx)
		}
		r.historyLoad.level = 0
		b, tx = r.adaptiveHistoryBatchLimits(5000, 390625)
		if b != blocks || tx != blocks {
			t.Fatalf("unknown storage grew range: %d/%d", b, tx)
		}
		r.historyLoad.level, r.historyLoad.hard = 3, true
		b, tx = r.adaptiveHistoryBatchLimits(5000, 390625)
		if b != blocks || tx != blocks {
			t.Fatalf("hard pressure grew range: %d/%d", b, tx)
		}
		r.historyLoad.hard = false
	}
	previous := r.historyLoad.history
	failed := PassResult{HistoryBuildAttempted: true, HistoryForcedBusy: true,
		HistoryBatchBlocks: 4, HistoryBatchTxNums: 40,
		BuildDuration: 10 * time.Second, HistoryMetadataDuration: 9 * time.Second}
	completeHistoryLoadRegression(r, &failed, context.Canceled)
	if r.historyLoad.history != previous || r.historyLoad.historyFailureBlocks != 2 || r.historyLoad.historyFailureTxNums != 20 || failed.HistoryMinRecovery < time.Minute {
		t.Fatalf("canceled unfinished range invented cheap density or lost backoff: %+v", r.historyLoad)
	}
	initial := historyLoadState{}
	if got := initial.batchLimit(5000, 0, historyWorkSample{}); got != 1250 {
		t.Fatalf("unmeasured initial range increased: %d", got)
	}
}

func TestHistoryDensityTimerResolutionCannotResetBatch(t *testing.T) {
	r := historyLoadRegressionRunner(t)
	for _, previous := range []uint64{1, 2, 3, 100} {
		result := PassResult{Built: true, HistoryBuildAttempted: true,
			HistoryBatchBlocks: previous, HistoryBatchTxNums: previous,
			BuildDuration: time.Nanosecond, HistoryMetadataDuration: time.Nanosecond}
		r.recordHistoryWork(&result)
		want := previous + max(uint64(1), previous/4)
		blocks, txs := r.adaptiveHistoryBatchLimits(5000, 390625)
		if blocks != want || txs != want {
			t.Fatalf("timer-sized observation reset bounded growth: previous=%d got=%d/%d", previous, blocks, txs)
		}
	}
	invalid := PassResult{Built: true, HistoryBuildAttempted: true,
		HistoryBatchBlocks: 100, HistoryBatchTxNums: 1000, HistoryMetadataDuration: time.Second}
	r.recordHistoryWork(&invalid)
	blocks, txs := r.adaptiveHistoryBatchLimits(5000, 390625)
	if blocks != 1 || txs != 1 {
		t.Fatalf("impossible timing grew a batch: %d/%d", blocks, txs)
	}
}

func TestHistoryDensityRealRunnerMeasuresMetadata(t *testing.T) {
	r := throughputFixture(t)
	result, err := r.OnePass()
	if err != nil || !result.Built || result.HistoryMetadataDuration <= 0 || result.HistoryMetadataDuration >= result.BuildDuration {
		t.Fatalf("real metadata timing absent/invalid: %+v err=%v", result, err)
	}
	w, m, measurement := historyDensityWork(&result)
	if measurement != 1 || m != result.HistoryMetadataDuration || r.historyLoad.history.work != w || w >= result.BuildDuration {
		t.Fatalf("Runner did not feed measured density: work=%s metadata=%s measurement=%d sample=%+v", w, m, measurement, r.historyLoad.history)
	}
	if result.HistoryRecoveryCost < result.BuildDuration || result.HistoryMinRecovery < r.throughputRecovery(result.HistoryRecoveryCost, false) {
		t.Fatalf("real metadata escaped recovery budget: %+v", result)
	}
}
