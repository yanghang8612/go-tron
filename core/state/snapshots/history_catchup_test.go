package snapshots

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

func throughputFixture(t *testing.T) *Runner {
	r := pressureFixture(t)
	r.cfg.HistoryCatchupMode = HistoryCatchupThroughput
	r.cfg.MaxDeferredHistoryBlocks = 2
	r.cfg.MaxBusyDeferredHistoryBlocks = 4
	r.cfg.CatchupUnthrottledLagBlocks = 1
	return r
}

func TestThroughputModeDefaultsAndRecoveryBounds(t *testing.T) {
	if got := (Config{}).applyDefaults().HistoryCatchupMode; got != HistoryCatchupBalanced {
		t.Fatal(got)
	}
	for _, value := range []string{"", "balanced", "throughput"} {
		if _, err := ParseHistoryCatchupMode(value); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ParseHistoryCatchupMode("fast"); err == nil {
		t.Fatal("accepted invalid mode")
	}
	r := throughputFixture(t)
	for _, tc := range []struct {
		work   time.Duration
		failed bool
		want   time.Duration
	}{
		{time.Nanosecond, false, 3 * time.Second}, {750 * time.Millisecond, false, 3 * time.Second},
		{time.Second, false, 4 * time.Second}, {20 * time.Second, false, 80 * time.Second},
		{time.Millisecond, true, time.Minute}, {20 * time.Second, true, 80 * time.Second},
		{time.Duration(math.MaxInt64), false, time.Duration(math.MaxInt64)},
	} {
		if got := r.throughputRecovery(tc.work, tc.failed); got != tc.want {
			t.Fatalf("%+v got %s", tc, got)
		}
	}
	for _, n := range []uint64{1, 5000, 390625} {
		if got := r.forcedBusyHistoryBatchLimit(n); got != n {
			t.Fatal(got)
		}
	}
	now := time.Now()
	if got := historyRecoveryDeadline(now, time.Duration(math.MaxInt64)).UnixNano(); got != math.MaxInt64 {
		t.Fatal(got)
	}
	if got := historyRecoveryDeadline(now, time.Second); !got.Equal(now.Add(time.Second)) {
		t.Fatal(got)
	}
}

func TestThroughputFullBatchPressureAndReadyCannotBypassDeadline(t *testing.T) {
	for _, pressured := range []bool{false, true} {
		t.Run(map[bool]string{false: "busy", true: "pressure"}[pressured], func(t *testing.T) {
			r := throughputFixture(t)
			if pressured {
				r.cfg.HistoryPressureProbe = func(context.Context) (HistoryPressure, error) {
					return HistoryPressure{HotHistoryBytes: 100, HotHistoryBytesAvailable: true}, nil
				}
			}
			first, err := r.OnePass()
			if err != nil || !first.Built || !first.HistoryForcedBusy || first.HistoryBatchBlocks != 8 || first.HistoryBatchTxNums != 8 || first.HistoryMinRecovery < 3*time.Second {
				t.Fatalf("full bounded build: %+v %v", first, err)
			}
			if r.lastSuccessfulForcedAt.Load() == 0 || r.Snapshot().ForcedBusyBuilds != 1 {
				t.Fatal("standalone completion missing")
			}
			deadline := r.historyNotBefore.Load()
			r.cfg.SyncBuildReady = func() bool { return true }
			for _, active := range []bool{true, false} {
				r.chain.(*coldBuilderChain).syncRemainingOK = active
				if !active {
					r.cfg.CatchupBuildMinInterval = 0 // zero must not bypass an existing deadline
				}
				next, err := r.OnePass()
				if err != nil || next.Built || next.HistoryBuildAttempted || !next.HistoryRateLimited || r.historyNotBefore.Load() != deadline {
					t.Fatalf("ready/idle bypassed or refreshed recovery: %+v %v", next, err)
				}
			}
			r.historyNotBefore.Store(time.Now().Add(-time.Second).UnixNano())
			next, err := r.OnePass()
			if err != nil || !next.Built || next.FromBlock != 9 || next.ToBlock != 12 {
				t.Fatalf("expiry: %+v %v", next, err)
			}
		})
	}
}

func TestThroughputFailureAfterPublicationKeepsDeadlineAcrossAdmissionChanges(t *testing.T) {
	r := throughputFixture(t)
	want := errors.New("maintenance failed")
	first, err := r.OnePassWithMaintenanceContext(context.Background(), func(context.Context, PassResult) error { return want })
	if !errors.Is(err, want) || !first.Built || first.HistoryMinRecovery < time.Minute || r.lastSuccessfulForcedAt.Load() != 0 {
		t.Fatalf("failure: %+v %v", first, err)
	}
	r.cfg.SyncBuildReady = func() bool { return true }
	r.chain.(*coldBuilderChain).syncRemainingOK = false
	next, err := r.OnePass()
	if err != nil || next.HistoryBuildAttempted || !next.HistoryRateLimited || next.HistoryRetryAfter < 59*time.Second {
		t.Fatalf("failure deadline bypass: %+v %v", next, err)
	}
}

func TestThroughputDeferredCompletionAccountsNonMergeCostOnce(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failed"}[failed], func(t *testing.T) {
			r := throughputFixture(t)
			result, err := r.OnePassWithDeferredMaintenanceContext(context.Background(), nil)
			if err != nil || !result.Built || !result.historyCompletionPending || r.lastSuccessfulForcedAt.Load() != 0 {
				t.Fatalf("provisional: %+v %v", result, err)
			}
			var outerErr error
			// Even if the inner provisional cooldown expires, a second caller
			// cannot begin new work before the outer maintenance is finished.
			r.historyNotBefore.Store(time.Now().Add(-time.Second).UnixNano())
			if next, err := r.OnePass(); !errors.Is(err, ErrHistoryMaintenancePending) || next.HistoryBuildAttempted {
				t.Fatalf("pending outer work admitted another pass: %+v %v", next, err)
			}
			if failed {
				outerErr = errors.New("post-build failed")
			}
			copyBefore := result
			r.CompleteHistoryMaintenance(&result, time.Now().Add(-20*time.Second), outerErr)
			stats := r.Snapshot()
			if result.historyCompletionPending || result.HistoryMaintenanceDuration < 20*time.Second || result.HistoryMinRecovery < 79*time.Second || stats.LastMaintenanceDuration < 20*time.Second || stats.ForcedBusyBuilds != 1 || stats.SegmentsBuilt != 1 {
				t.Fatalf("outer cost/counts: %+v %+v", result, stats)
			}
			if (r.lastSuccessfulForcedAt.Load() == 0) != failed {
				t.Fatal("incorrect successful completion sample")
			}
			deadline := r.historyNotBefore.Load()
			r.CompleteHistoryMaintenance(&copyBefore, time.Now().Add(-time.Hour), errors.New("duplicate"))
			if r.historyNotBefore.Load() != deadline || r.Snapshot() != stats {
				t.Fatal("duplicate completion changed events or deadline")
			}
		})
	}
}

func TestThroughputReserveGateAndCancellationRemainClosed(t *testing.T) {
	r := throughputFixture(t)
	r.cfg.HistoryPressureProbe = func(context.Context) (HistoryPressure, error) {
		return HistoryPressure{}, errors.New("probe failed")
	}
	if result, err := r.OnePass(); err == nil || result.HistoryBuildAttempted || r.historyNotBefore.Load() != 0 {
		t.Fatalf("failed probe changed admission deadline: %+v %v", result, err)
	}
	r.cfg.HistoryPressureProbe = func(context.Context) (HistoryPressure, error) {
		return HistoryPressure{FreeBytes: 39, FreeBytesAvailable: true}, nil
	}
	result, err := r.OnePass()
	if err != nil || result.HistoryBuildAttempted || !result.HistorySpaceDeferred || r.historyNotBefore.Load() != 0 {
		t.Fatalf("reserve: %+v %v", result, err)
	}
	r.cfg.HistoryPressureProbe = nil
	r.cfg.HeavyWorkGate = maintenance.NewHeavyWorkGateWithCooldown(3 * time.Second)
	release, ok := r.cfg.HeavyWorkGate.TryAcquire()
	if !ok {
		t.Fatal("gate")
	}
	result, err = r.OnePass()
	if err != nil || !result.HistoryGateDeferred || result.HistoryBuildAttempted || r.historyNotBefore.Load() != 0 {
		t.Fatalf("gate: %+v %v", result, err)
	}
	release()
	result, err = r.OnePass()
	if err != nil || !result.HistoryGateDeferred || result.HistoryBuildAttempted {
		t.Fatalf("cooldown: %+v %v", result, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = r.OnePassContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	r.cfg.HeavyWorkGate = nil
	ctx, cancel = context.WithCancel(context.Background())
	result, err = r.OnePassWithMaintenanceContext(ctx, func(context.Context, PassResult) error { cancel(); return nil })
	if !errors.Is(err, context.Canceled) || !result.Built || result.HistoryMinRecovery < time.Minute || r.lastSuccessfulForcedAt.Load() != 0 {
		t.Fatalf("cancel: %+v %v", result, err)
	}
}
