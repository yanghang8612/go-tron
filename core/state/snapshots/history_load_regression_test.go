package snapshots

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

func TestHistoryLoadSmallBatchGrowthRecovers(t *testing.T) {
	s := historyLoadState{level: 3}
	for _, previous := range []uint64{1, 2, 3, 4} {
		sample := historyWorkSample{work: time.Second, bytes: 1}
		if got := s.batchLimit(100, previous, sample); got != previous+1 {
			t.Fatalf("small batch %d cannot recover: got %d", previous, got)
		}
	}
	// A minimum integer increment is allowed only when the observed workload
	// fits that extra complete unit. Spare time does not override a byte bound.
	for _, tc := range []struct {
		name     string
		previous uint64
		work     time.Duration
		bytes    uint64
	}{
		{"one time limited", 1, 6 * time.Second, 1},
		{"two time limited", 2, 6 * time.Second, 1},
		{"three time limited", 3, 7 * time.Second, 1},
		{"one byte limited", 1, time.Second, 80 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := s.batchLimit(100, tc.previous, historyWorkSample{work: tc.work, bytes: tc.bytes}); got != tc.previous {
				t.Fatalf("growth exceeded observed capacity: previous=%d got=%d", tc.previous, got)
			}
		})
	}
	if got := s.batchLimit(100, 1, historyWorkSample{work: 4 * time.Second, bytes: 1}); got != 2 {
		t.Fatalf("exact capacity boundary did not grow: %d", got)
	}
}

func TestHistoryLoadGrowthHonorsConfigAndUintBounds(t *testing.T) {
	s := historyLoadState{level: 3}
	sample := historyWorkSample{work: time.Nanosecond, bytes: 1}
	for _, tc := range []struct {
		configured, previous, want uint64
	}{
		{0, 1, 0}, {1, 1, 1}, {2, 1, 2}, {3, 3, 3}, {50, 100, 50},
		{math.MaxUint64, math.MaxUint64, math.MaxUint64},
		{math.MaxUint64, math.MaxUint64 - 1, math.MaxUint64},
		{math.MaxUint64 - 1, math.MaxUint64, math.MaxUint64 - 1},
	} {
		if got := s.batchLimit(tc.configured, tc.previous, sample); got != tc.want {
			t.Fatalf("batchLimit(%d,%d)=%d want %d", tc.configured, tc.previous, got, tc.want)
		}
	}
	if got := s.batchLimit(100, 1, historyWorkSample{work: time.Duration(math.MaxInt64), bytes: math.MaxUint64}); got != 1 {
		t.Fatalf("oversized complete unit lost its lower bound: %d", got)
	}
}

func historyLoadRegressionRunner(t *testing.T) *Runner {
	t.Helper()
	r := throughputFixture(t)
	r.cfg.HistoryLoadProbe = func() maintenance.StoragePressure { return maintenance.StoragePressure{} }
	r.cfg.BatchBlocks = 5000
	r.historyLoad.level = 3
	return r
}

// Exercise the external completion decision with deterministic scheduler
// inputs, without manufacturing successful publication counters or waiting.
func completeHistoryLoadRegression(r *Runner, result *PassResult, passErr error) {
	r.passMu.Lock()
	defer r.passMu.Unlock()
	r.maintenanceSerial++
	result.historyMaintenanceID = r.maintenanceSerial
	result.historyCompletionPending = true
	r.pendingMaintenanceID = result.historyMaintenanceID
	finished := time.Now()
	r.completeHistoryMaintenance(result, finished.Add(-time.Second), finished, passErr)
}

func TestHistoryLoadFailedAttemptShrinksWithoutInventingDensity(t *testing.T) {
	r := historyLoadRegressionRunner(t)
	previous := historyWorkSample{blocks: 80, txnums: 800, bytes: 1 << 20, work: time.Second}
	r.historyLoad.history = previous
	for _, attempted := range []uint64{80, 40, 20, 2, 1} {
		result := PassResult{HistoryBuildAttempted: true, historyWasSyncing: true,
			HistoryBatchBlocks: attempted, HistoryBatchTxNums: attempted * 10, BuildDuration: time.Second}
		completeHistoryLoadRegression(r, &result, errors.New("injected unfinished history build"))
		wantBlocks, wantTxs := max(uint64(1), attempted/2), max(uint64(1), attempted*10/2)
		if r.historyLoad.historyFailureBlocks != wantBlocks || r.historyLoad.historyFailureTxNums != wantTxs {
			t.Fatalf("failed attempt %d kept oversized bounds: %+v", attempted, r.historyLoad)
		}
		blocks, txs := r.adaptiveHistoryBatchLimits(5000, 390625)
		if blocks > wantBlocks || txs > wantTxs || r.historyLoad.history != previous {
			t.Fatalf("failure failed to cap or invented density: %d/%d sample=%+v", blocks, txs, r.historyLoad.history)
		}
		if result.HistoryMinRecovery < time.Minute || r.Snapshot().SegmentsBuilt != 0 || r.lastSuccessfulForcedAt.Load() != 0 {
			t.Fatal("failed attempt bypassed backoff or counted as a publication")
		}
	}
	// No actual attempt means no new failure penalty (e.g. pressure admission).
	beforeBlocks, beforeTxs := r.historyLoad.historyFailureBlocks, r.historyLoad.historyFailureTxNums
	deferred := PassResult{HistoryDeferred: true, HistoryLoadDeferred: true}
	completeHistoryLoadRegression(r, &deferred, errors.New("admission failed"))
	if r.historyLoad.historyFailureBlocks != beforeBlocks || r.historyLoad.historyFailureTxNums != beforeTxs || r.historyLoad.history != previous {
		t.Fatal("unattempted work changed density or failure limits")
	}
}

func TestHistoryLoadBuiltOuterFailureKeepsRealDensity(t *testing.T) {
	r := historyLoadRegressionRunner(t)
	r.historyLoad.historyFailureBlocks, r.historyLoad.historyFailureTxNums = 1, 1
	result := PassResult{Built: true, HistoryBuildAttempted: true, historyWasSyncing: true,
		HistoryBatchBlocks: 4, HistoryBatchTxNums: 40, BuildDuration: 4 * time.Second,
		BeforeMergeDuration: time.Second, Segments: []SegmentRef{{Size: 10 << 20}}}
	completeHistoryLoadRegression(r, &result, errors.New("post-publication catalog failed"))
	want := historyWorkSample{blocks: 4, txnums: 40, bytes: 10 << 20, work: 5 * time.Second}
	if r.historyLoad.history != want || r.historyLoad.historyFailureBlocks != 0 || r.historyLoad.historyFailureTxNums != 0 {
		t.Fatalf("completed construction confused with failed build: %+v", r.historyLoad)
	}
	blocks, txs := r.adaptiveHistoryBatchLimits(5000, 390625)
	if blocks != 5 || txs != 50 || result.HistoryMinRecovery < time.Minute {
		t.Fatalf("real small batch did not recover gradually: %d/%d, recovery=%s", blocks, txs, result.HistoryMinRecovery)
	}
}

func TestHistoryLoadEventFailureRequiresCompletedPublication(t *testing.T) {
	r := historyLoadRegressionRunner(t)
	previous := historyWorkSample{blocks: 80, bytes: 1 << 20, work: time.Second}
	r.historyLoad.event = previous
	// The event writer can finish before manifest integration fails. Its
	// EventLogBuilt flag alone must not pretend the catch-up was published.
	failed := PassResult{HistoryEventAttempted: true, EventLogBuilt: true,
		historyWasSyncing: true, historyEventBatchBlocks: 20, DerivedSidecarDuration: time.Second}
	completeHistoryLoadRegression(r, &failed, errors.New("event manifest failed"))
	if r.historyLoad.eventFailureBlocks != 10 || r.historyLoad.event != previous || r.adaptiveEventBatchLimit(65536) > 10 {
		t.Fatalf("unpublished event did not retain failure cap: %+v", r.historyLoad)
	}
	success := PassResult{HistoryEventAttempted: true, EventLogBuilt: true, DerivedSidecarCatchup: true,
		historyWasSyncing: true, historyEventBatchBlocks: 4, DerivedSidecarDuration: 4 * time.Second,
		Segments: []SegmentRef{{Size: 10 << 20}}}
	completeHistoryLoadRegression(r, &success, errors.New("post-event catalog failed"))
	if r.historyLoad.eventFailureBlocks != 0 || r.historyLoad.event.blocks != 4 || r.historyLoad.event.bytes != 10<<20 || r.adaptiveEventBatchLimit(65536) != 5 {
		t.Fatalf("published event discarded density or failed to recover gradually: %+v", r.historyLoad)
	}
}
