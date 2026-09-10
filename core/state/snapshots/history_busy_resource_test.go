package snapshots

import (
	"context"
	"errors"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

func busyResourceFixture(t *testing.T) *Runner {
	t.Helper()
	r := throughputFixture(t)
	// All twelve canonical blocks lie below the busy watermark. The original
	// throughput fixture's busy=4 would exercise only the old liveness fallback.
	r.cfg.MaxBusyDeferredHistoryBlocks = 20
	r.cfg.HeavyWorkGate = maintenance.NewHeavyWorkGate()
	r.cfg.HistoryLoadProbe = func() maintenance.StoragePressure { return healthyHistoryLoad(time.Now()) }
	r.cfg.BusyHistoryBuildReady = func() bool { return true }
	return r
}

func assertBusyResourceUnattempted(t *testing.T, r *Runner, result PassResult) {
	t.Helper()
	if result.Built || result.HistoryBuildAttempted || r.Snapshot().BusyResourceAttempts != 0 || r.Snapshot().BusyResourceBuilds != 0 || r.historyNotBefore.Load() != 0 {
		t.Fatalf("unadmitted candidate created work or recovery: %+v stats=%+v", result, r.Snapshot())
	}
	if _, err := os.Stat(r.cfg.Dir + "/" + ManifestFile); !os.IsNotExist(err) {
		t.Fatalf("unadmitted candidate created manifest: %v", err)
	}
}

func TestHistoryBusyResourceWatermarksAndModes(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		lag, soft, busy            uint64
		mode                       HistoryCatchupMode
		resource, ready            bool
		build, opportunity, forced bool
	}{
		{"soft equality", 4, 4, 8, HistoryCatchupThroughput, true, false, false, false, false},
		{"above soft", 5, 4, 8, HistoryCatchupThroughput, true, false, true, true, true},
		{"busy equality", 8, 4, 8, HistoryCatchupThroughput, true, false, true, true, true},
		{"busy equality denied", 8, 4, 8, HistoryCatchupThroughput, false, false, false, false, false},
		{"busy liveness", 9, 4, 8, HistoryCatchupThroughput, false, false, true, false, true},
		{"balanced keeps deferral", 5, 4, 8, HistoryCatchupBalanced, true, false, false, false, false},
		{"balanced keeps liveness", 9, 4, 8, HistoryCatchupBalanced, false, false, true, false, true},
		{"ready keeps acceleration", 5, 4, 8, HistoryCatchupThroughput, true, true, true, false, false},
		{"zero busy normalization", 5, 4, 0, HistoryCatchupThroughput, false, false, true, false, true},
		{"smaller busy normalization", 5, 4, 2, HistoryCatchupThroughput, false, false, true, false, true},
		{"default soft equality", 1, 0, 8, HistoryCatchupThroughput, true, false, false, false, false},
		{"above default soft", 2, 0, 8, HistoryCatchupThroughput, true, false, true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := busyResourceFixture(t)
			r.chain.(*coldBuilderChain).solidified = int64(tc.lag + 1)
			r.cfg.MaxDeferredHistoryBlocks, r.cfg.MaxBusyDeferredHistoryBlocks = tc.soft, tc.busy
			r.cfg.HistoryCatchupMode = tc.mode
			r.cfg.SyncBuildReady = func() bool { return tc.ready }
			calls := 0
			r.cfg.BusyHistoryBuildReady = func() bool { calls++; return tc.resource }
			result, err := r.OnePass()
			if err != nil || result.Built != tc.build || result.HistoryBusyResourceReady != tc.opportunity || result.HistoryForcedBusy != tc.forced {
				t.Fatalf("watermark policy: %+v err=%v", result, err)
			}
			if tc.opportunity {
				if !result.HistoryAdmissionChecked || result.HistoryAdmissionReady || result.HistoryAccelerated || calls != 1 {
					t.Fatalf("busy opportunity became ready or unthrottled: %+v calls=%d", result, calls)
				}
				stats := r.Snapshot()
				if stats.BusyResourceAttempts != 1 || stats.BusyResourceBuilds != 1 || stats.AdmissionBusy != 1 || stats.ForcedBusyBuilds != 1 || stats.HistoryAcceleratedBuilds != 0 {
					t.Fatalf("admission attribution: %+v", stats)
				}
				if r.metrics.busyResourceAttempts.Snapshot().Value() != 1 || r.metrics.busyResourceBuilds.Snapshot().Value() != 1 {
					t.Fatal("resource metrics do not match actual attempt/publication")
				}
			} else if r.Snapshot().BusyResourceAttempts != 0 || r.Snapshot().BusyResourceBuilds != 0 {
				t.Fatal("ordinary or liveness work counted as resource opportunity")
			}
			if !tc.build {
				assertBusyResourceUnattempted(t, r, result)
			}
		})
	}
}

func TestHistoryBusyResourceRequiresIndependentHeadroom(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Runner, *maintenance.StoragePressure)
	}{
		{"nil resource probe", func(r *Runner, _ *maintenance.StoragePressure) { r.cfg.BusyHistoryBuildReady = nil }},
		{"CPU or memory unavailable", func(r *Runner, _ *maintenance.StoragePressure) {
			r.cfg.BusyHistoryBuildReady = func() bool { return false }
		}},
		{"nil storage probe", func(r *Runner, _ *maintenance.StoragePressure) { r.cfg.HistoryLoadProbe = nil }},
		{"nil shared gate", func(r *Runner, _ *maintenance.StoragePressure) { r.cfg.HeavyWorkGate = nil }},
		{"missing engine", func(_ *Runner, p *maintenance.StoragePressure) { p.Available = false }},
		{"stale engine", func(_ *Runner, p *maintenance.StoragePressure) { p.SampledAt = time.Now().Add(-16 * time.Second) }},
		{"future engine", func(_ *Runner, p *maintenance.StoragePressure) { p.SampledAt = time.Now().Add(2 * time.Second) }},
		{"missing device", func(_ *Runner, p *maintenance.StoragePressure) { p.DeviceAvailable = false }},
		{"stale device", func(_ *Runner, p *maintenance.StoragePressure) { p.DeviceSampledAt = time.Now().Add(-16 * time.Second) }},
		{"future device", func(_ *Runner, p *maintenance.StoragePressure) { p.DeviceSampledAt = time.Now().Add(2 * time.Second) }},
		{"slow device", func(_ *Runner, p *maintenance.StoragePressure) { p.DeviceAwait = 2*time.Millisecond + time.Nanosecond }},
		{"invalid device latency", func(_ *Runner, p *maintenance.StoragePressure) { p.DeviceAwait = -time.Nanosecond }},
		{"write stall", func(_ *Runner, p *maintenance.StoragePressure) { p.WriteStalled = true }},
		{"memtable hard limit", func(_ *Runner, p *maintenance.StoragePressure) { p.MemTableCount = 3 }},
		{"L0 hard limit", func(_ *Runner, p *maintenance.StoragePressure) { p.L0Sublevels = 144 }},
		{"L0 soft pressure", func(_ *Runner, p *maintenance.StoragePressure) { p.L0Sublevels = 16 }},
		{"pressure recovery hysteresis", func(r *Runner, _ *maintenance.StoragePressure) { r.historyLoad.level = 1 }},
		{"compaction debt growth", func(r *Runner, p *maintenance.StoragePressure) {
			r.historyLoad.debtRises, r.historyLoad.lastDebt = 1, 2<<30
			r.historyLoad.lastAccepted = p.SampledAt.Add(-6 * time.Second)
			p.CompactionDebt = 3 << 30
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := busyResourceFixture(t)
			p := healthyHistoryLoad(time.Now())
			r.cfg.HistoryLoadProbe = func() maintenance.StoragePressure { return p }
			tc.change(r, &p)
			result, err := r.OnePass()
			if err != nil || !result.HistoryDeferred || result.HistoryBusyResourceReady {
				t.Fatalf("unobserved or pressured capacity admitted: %+v err=%v", result, err)
			}
			assertBusyResourceUnattempted(t, r, result)
		})
	}
}

func TestHistoryBusyResourceKeepsHardAdmissionAndPruneCallback(t *testing.T) {
	for _, kind := range []string{"lease", "cooldown", "gate pressure", "free reserve", "probe error", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			r := busyResourceFixture(t)
			ctx := context.Background()
			var wantErr error
			switch kind {
			case "lease":
				release, ok := r.cfg.HeavyWorkGate.TryAcquire()
				if !ok {
					t.Fatal("setup lease")
				}
				t.Cleanup(release)
			case "cooldown":
				r.cfg.HeavyWorkGate = maintenance.NewHeavyWorkGateWithCooldown(time.Minute)
				release, ok := r.cfg.HeavyWorkGate.TryAcquire()
				if !ok {
					t.Fatal("setup cooldown")
				}
				release()
			case "gate pressure":
				r.cfg.HeavyWorkGate.SetAdmissionCheck(func() bool { return false })
			case "free reserve":
				r.cfg.HistoryPressureProbe = func(context.Context) (HistoryPressure, error) {
					return HistoryPressure{FreeBytesAvailable: true, FreeBytes: 39}, nil
				}
			case "probe error":
				wantErr = errors.New("capacity probe failed")
				r.cfg.HistoryPressureProbe = func(context.Context) (HistoryPressure, error) { return HistoryPressure{}, wantErr }
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				wantErr = context.Canceled
			}
			callbacks := 0
			result, err := r.OnePassWithMaintenanceContext(ctx, func(context.Context, PassResult) error { callbacks++; return nil })
			if !errors.Is(err, wantErr) {
				t.Fatalf("err=%v want=%v", err, wantErr)
			}
			assertBusyResourceUnattempted(t, r, result)
			wantCallbacks := 1
			if kind == "canceled" {
				wantCallbacks = 0
			}
			if callbacks != wantCallbacks {
				t.Fatalf("covered-prune callback count=%d want=%d", callbacks, wantCallbacks)
			}
			if kind == "lease" || kind == "cooldown" || kind == "gate pressure" {
				if !result.HistoryBusyResourceReady || !result.HistoryGateDeferred {
					t.Fatalf("lost resource/gate attribution: %+v", result)
				}
			}
		})
	}
}

func TestHistoryBusyResourceRecoveryCannotBypassOrRestoreFixedInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := busyResourceFixture(t)
		first, err := r.OnePass()
		if err != nil || !first.Built || !first.HistoryBusyResourceReady {
			t.Fatalf("first opportunity: %+v %v", first, err)
		}
		deadline := r.historyNotBefore.Load()
		if first.HistoryMinRecovery != historyCPUBurstRecovery {
			t.Fatalf("bounded healthy recovery=%s", first.HistoryMinRecovery)
		}
		for _, kind := range []string{"busy", "ready", "idle", "zero interval"} {
			r.cfg.SyncBuildReady = func() bool { return kind == "ready" }
			r.chain.(*coldBuilderChain).syncRemainingOK = kind != "idle"
			if kind == "zero interval" {
				r.cfg.CatchupBuildMinInterval = 0
			}
			next, err := r.OnePass()
			if err != nil || next.HistoryBuildAttempted || !next.HistoryRateLimited || r.historyNotBefore.Load() != deadline {
				t.Fatalf("%s bypassed or extended deadline: %+v %v", kind, next, err)
			}
		}
		r.cfg.CatchupBuildMinInterval = time.Minute
		r.cfg.SyncBuildReady = func() bool { return false }
		r.chain.(*coldBuilderChain).syncRemainingOK = true
		// Virtual time reaches the exact measured deadline, well before the old
		// one-minute cadence. No reset of scheduling state manufactures admission.
		time.Sleep(time.Until(time.Unix(0, deadline)))
		next, err := r.OnePass()
		if err != nil || !next.Built || !next.HistoryBusyResourceReady || next.FromBlock != first.ToBlock+1 {
			t.Fatalf("expired dynamic recovery fell back to fixed interval: %+v %v", next, err)
		}
		if got := r.Snapshot(); got.BusyResourceAttempts != 2 || got.BusyResourceBuilds != 2 || got.HistoryAcceleratedBuilds != 0 {
			t.Fatalf("deferred polling counted extra work: %+v", got)
		}
	})
}

func TestHistoryBusyResourceFullCompletionAndFailures(t *testing.T) {
	for _, kind := range []string{"success", "post-publication error", "post-publication cancel"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r := busyResourceFixture(t)
				started := time.Now()
				result, err := r.OnePassWithDeferredMaintenanceContext(context.Background(), func(context.Context, PassResult) error {
					// A metadata/prune callback is real lifecycle wall cost even
					// when density separately excludes measured metadata.
					time.Sleep(2 * time.Second)
					return nil
				})
				if err != nil || !result.Built || !result.HistoryBusyResourceReady {
					t.Fatalf("publication: %+v %v", result, err)
				}
				if _, err := r.OnePass(); !errors.Is(err, ErrHistoryMaintenancePending) {
					t.Fatalf("pending completion allowed reentry: %v", err)
				}
				provisional := r.historyNotBefore.Load()
				time.Sleep(2 * time.Second) // post-builder catalog/prune work
				r.chain.(*coldBuilderChain).syncRemainingOK = false
				var passErr error
				switch kind {
				case "post-publication error":
					passErr = errors.New("catalog failed")
				case "post-publication cancel":
					passErr = context.Canceled
				}
				r.CompleteHistoryMaintenance(&result, started, passErr)
				// Even a no-op merge records the timer-resolution minimum. It is
				// the only phase excluded from the complete history recovery cost.
				wantCost := 4*time.Second - result.CompactionDuration
				if result.HistoryMaintenanceDuration != 4*time.Second || result.HistoryRecoveryCost != wantCost || result.HistoryMinRecovery != r.throughputRecovery(wantCost, passErr != nil) || r.historyNotBefore.Load() <= provisional {
					t.Fatalf("complete work/idle transition lost recovery: %+v", result)
				}
				if passErr != nil && result.HistoryMinRecovery < time.Minute {
					t.Fatal("failure lost minimum backoff")
				}
				deadline := r.historyNotBefore.Load()
				before := r.Snapshot()
				r.CompleteHistoryMaintenance(&result, started.Add(-time.Hour), errors.New("duplicate"))
				if r.historyNotBefore.Load() != deadline || r.Snapshot() != before {
					t.Fatal("duplicate completion changed counters/deadline")
				}
				if before.BusyResourceAttempts != 1 || before.BusyResourceBuilds != 1 || before.SegmentsBuilt != 1 {
					t.Fatalf("outer completion duplicated/lost publication: %+v", before)
				}
				if _, err := LoadProductionManifest(r.cfg.Dir); err != nil {
					t.Fatalf("outer failure lost published files: %v", err)
				}
				if next, err := r.OnePass(); err != nil || next.HistoryBuildAttempted || !next.HistoryRateLimited {
					t.Fatalf("outer completion deadline bypassed: %+v %v", next, err)
				}
			})
		})
	}
}
