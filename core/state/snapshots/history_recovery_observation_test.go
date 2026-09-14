package snapshots

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
)

func pendingRecoveryObservationFixture(t *testing.T) (*Runner, PassResult) {
	t.Helper()
	r := busyResourceFixture(t)
	r.cfg.MaxBusyDeferredHistoryBlocks = 2 // Existing over-busy liveness path.
	r.cfg.HistoryRecoveryLoadProbe = func() maintenance.StoragePressure { return healthyHistoryLoad(time.Now()) }
	r.extendHistoryNotBefore(time.Now().Add(40 * time.Second))
	result, err := r.OnePass()
	if err != nil || !result.HistoryRateLimited || result.HistoryBuildAttempted || !result.HistoryRecoveryObservation || result.HistoryBusyResourceDeferred || r.busyHistoryObservationPending.Load() == 0 {
		t.Fatalf("expected recovery-only opportunity: %+v err=%v", result, err)
	}
	return r, result
}

func TestHistoryRecoveryObservationDoesNotWakeOrRunMaintenance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, _ := pendingRecoveryObservationFixture(t)
		r.chain = observationNoDatabaseChain{r.chain.(*coldBuilderChain)}
		r.cfg.HistoryLoadProbe = func() maintenance.StoragePressure { panic("ordinary admission probe during recovery") }
		r.cfg.HistoryPressureProbe = func(context.Context) (HistoryPressure, error) { panic("history space during recovery") }
		r.cfg.BusyHistoryBuildReady = func() bool { panic("CPU admission during recovery") }
		r.cfg.HeavyWorkGate.SetAdmissionCheck(func() bool { panic("lease admission during recovery") })
		stats, density, event := r.Snapshot(), r.historyLoad.history, r.historyLoad.event
		deadline := r.historyNotBefore.Load()
		sequence := r.historyLoad.acceptedSequence
		busyChecks := r.busyHistoryObservationMetrics.checks.Snapshot().Count()
		recoveryChecks := r.historyRecoveryMetrics.checks.Snapshot().Count()
		accepted := r.historyRecoveryMetrics.accepted.Snapshot().Count()
		for i := 0; i < 20; i++ {
			if wake, after := r.ObserveBusyHistoryResources(context.Background()); wake || after != 5*time.Second {
				t.Fatalf("early=%v/%s", wake, after)
			}
		}
		if r.historyLoad.acceptedSequence != sequence {
			t.Fatal("early calls consumed observations")
		}
		for i := 0; i < 3; i++ {
			time.Sleep(5 * time.Second)
			if wake, after := r.ObserveBusyHistoryResources(context.Background()); wake || after != 5*time.Second {
				t.Fatalf("recovery=%v/%s", wake, after)
			}
		}
		if r.historyLoad.acceptedSequence != sequence+3 || r.historyRecoveryMetrics.checks.Snapshot().Count() != recoveryChecks+3 || r.historyRecoveryMetrics.accepted.Snapshot().Count() != accepted+3 {
			t.Fatal("recovery samples not counted separately exactly once")
		}
		if r.Snapshot() != stats || r.historyLoad.history != density || r.historyLoad.event != event || r.historyNotBefore.Load() != deadline || r.busyHistoryObservationMetrics.checks.Snapshot().Count() != busyChecks {
			t.Fatal("observation changed work, density, deadline, or busy counters")
		}
		if r.historyRecoveryMetrics.duration.Snapshot().Value() <= 0 {
			t.Fatal("missing duration")
		}
	})
}

// Both controllers receive the same immutable five-second producer trace.
// The old full-pass-only schedule observes just its initial and final samples;
// only the candidate observes intermediate samples during the fixed deadline.
func TestHistoryRecoveryObservationDeterministicPressureReplay(t *testing.T) {
	for _, renewed := range []bool{false, true} {
		t.Run(map[bool]string{false: "healthy recovery", true: "renewed pressure"}[renewed], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r, _ := pendingRecoveryObservationFixture(t)
				started := time.Now()
				trace := make([]maintenance.StoragePressure, 9)
				for i := range trace {
					trace[i] = healthyHistoryLoad(started.Add(time.Duration(i) * 5 * time.Second))
				}
				trace[0].L0Sublevels = 16
				if renewed {
					trace[3].StallCount = 1
					for i := 4; i < len(trace); i++ {
						trace[i].StallCount = 1
					}
				}
				r.refreshHistoryLoadFromProbe(started, func() maintenance.StoragePressure { return trace[0] })
				baseline := &Runner{cfg: r.cfg, chain: r.chain, historyLoad: historyLoadState{sample: r.historyLoad.sample, lastAccepted: r.historyLoad.lastAccepted, lastDebt: r.historyLoad.lastDebt, lastStalls: r.historyLoad.lastStalls, level: r.historyLoad.level}}
				baseline.historyLoad.acceptedSequence = r.historyLoad.acceptedSequence
				deadline := r.historyNotBefore.Load()
				for i := 1; i <= 3; i++ {
					sample := trace[i]
					r.cfg.HistoryRecoveryLoadProbe = func() maintenance.StoragePressure { return sample }
					time.Sleep(5 * time.Second)
					if wake, _ := r.ObserveBusyHistoryResources(context.Background()); wake {
						t.Fatal("pressure observation woke maintenance")
					}
					if r.historyNotBefore.Load() != deadline {
						t.Fatal("recovery deadline changed")
					}
				}
				if renewed {
					if r.historyLoad.level != 1 || r.historyLoad.good != 0 || r.historyLoad.reasons&historyLoadReasonNewStall == 0 {
						t.Fatalf("renewed stall missed: %+v", r.historyLoad)
					}
				} else if r.historyLoad.level != 3 || r.historyLoad.good != 3 || baseline.historyLoad.level != 1 {
					t.Fatalf("candidate level/good=%d/%d baseline=%d", r.historyLoad.level, r.historyLoad.good, baseline.historyLoad.level)
				}
				time.Sleep(25 * time.Second)
				sequence := r.historyLoad.acceptedSequence
				if wake, after := r.ObserveBusyHistoryResources(context.Background()); wake || after != 0 || r.busyHistoryObservationPending.Load() != 0 || r.historyLoad.acceptedSequence != sequence {
					t.Fatal("deadline expiry emitted wake or consumed stale sample")
				}
				baseline.refreshHistoryLoadFromProbe(time.Now(), func() maintenance.StoragePressure { return trace[8] })
				if !renewed && (baseline.historyLoad.level != 1 || baseline.historyLoad.good != 1) {
					t.Fatal("old schedule did not preserve observed-sample hysteresis")
				}
			})
		})
	}
}

func TestHistoryRecoveryObservationProbeAndScope(t *testing.T) {
	for _, kind := range []string{"disabled", "balanced", "nil ordinary probe", "nil recovery probe", "no deadline", "not deferred", "idle", "near tip"} {
		t.Run(kind, func(t *testing.T) {
			r := busyResourceFixture(t)
			r.cfg.HistoryRecoveryLoadProbe = func() maintenance.StoragePressure { panic("arming called probe") }
			r.extendHistoryNotBefore(time.Now().Add(time.Minute))
			switch kind {
			case "disabled":
				r.cfg.Enabled = false
			case "balanced":
				r.cfg.HistoryCatchupMode = HistoryCatchupBalanced
			case "nil ordinary probe":
				r.cfg.HistoryLoadProbe = nil
			case "nil recovery probe":
				r.cfg.HistoryRecoveryLoadProbe = nil
			case "no deadline":
				r.historyNotBefore.Store(0)
			case "not deferred":
				r.cfg.DeferHistoryBuildWhileSyncing = false
			case "idle":
				r.chain.(*coldBuilderChain).syncRemainingOK = false
			case "near tip":
				r.chain.(*coldBuilderChain).syncRemaining = 1
			}
			var result PassResult
			r.armHistoryRecoveryObservation(&result, time.Now())
			if result.HistoryRecoveryObservation || r.busyHistoryObservationPending.Load() != 0 {
				t.Fatal("out-of-scope observer armed")
			}
		})
	}
	synctest.Test(t, func(t *testing.T) {
		r, _, result := pendingBusyObservationFixture(t)
		id := result.historyBusyObservationID
		r.cfg.HistoryRecoveryLoadProbe = func() maintenance.StoragePressure { panic("recovery replaced existing busy observation") }
		r.extendHistoryNotBefore(time.Now().Add(time.Minute))
		r.armHistoryRecoveryObservation(&result, time.Now())
		if result.HistoryRecoveryObservation || !result.HistoryBusyResourceDeferred || result.historyBusyObservationID != id || r.historyObservationRecovery {
			t.Fatal("recovery replaced old busy wake mode")
		}
	})
}

func TestHistoryRecoveryObservationPendingOuterAndLatestDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, _ := pendingRecoveryObservationFixture(t)
		time.Sleep(5 * time.Second)
		before := r.historyLoad.acceptedSequence
		r.passMu.Lock()
		wake, after := r.ObserveBusyHistoryResources(context.Background())
		r.passMu.Unlock()
		if wake || after != 5*time.Second {
			t.Fatal("TryLock did not defer")
		}
		r.pendingMaintenanceID = 42
		if wake, after := r.ObserveBusyHistoryResources(context.Background()); wake || after != 5*time.Second || r.historyLoad.acceptedSequence != before {
			t.Fatal("pending outer completion observed")
		}
		r.pendingMaintenanceID = 0
		deadline := time.Now().Add(time.Minute).UnixNano()
		r.extendHistoryNotBefore(time.Unix(0, deadline))
		time.Sleep(35 * time.Second) // The original deadline has expired.
		if wake, after := r.ObserveBusyHistoryResources(context.Background()); wake || after != 5*time.Second || r.historyNotBefore.Load() != deadline || r.historyLoad.acceptedSequence != before+1 {
			t.Fatal("observer did not preserve newer completion deadline")
		}
	})
}

func TestHistoryRecoveryObservationFailuresCancelOnlyTheirGeneration(t *testing.T) {
	for _, kind := range []string{"before merge", "outer completion", "next metadata"} {
		t.Run(kind, func(t *testing.T) {
			r, result := pendingRecoveryObservationFixture(t)
			wantErr := errors.New("injected recovery outer error")
			switch kind {
			case "before merge":
				_, err := r.OnePassWithMaintenanceContext(context.Background(), func(context.Context, PassResult) error { return wantErr })
				if !errors.Is(err, wantErr) {
					t.Fatal(err)
				}
			case "outer completion":
				r.CompleteHistoryMaintenance(&result, time.Now(), wantErr)
			case "next metadata":
				if err := os.WriteFile(filepath.Join(r.cfg.Dir, ManifestFile), []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
				if _, err := r.OnePass(); err == nil {
					t.Fatal("invalid metadata passed")
				}
			}
			if wake, after := r.ObserveBusyHistoryResources(context.Background()); wake || after != 0 || r.busyHistoryObservationPending.Load() != 0 {
				t.Fatal("failed pass retained observation")
			}
		})
	}
	r, old := pendingRecoveryObservationFixture(t)
	next, err := r.OnePass()
	if err != nil || !next.HistoryRecoveryObservation || next.historyBusyObservationID == old.historyBusyObservationID {
		t.Fatal("new pass did not replace generation")
	}
	id := r.busyHistoryObservationPending.Load()
	r.CompleteHistoryMaintenance(&old, time.Now(), errors.New("late old error"))
	if r.busyHistoryObservationPending.Load() != id {
		t.Fatal("old error canceled current observation")
	}
}

func TestHistoryRecoveryObservationCancellationAndConcurrency(t *testing.T) {
	for _, kind := range []string{"cancel generation", "context", "runner stopped"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r, _ := pendingRecoveryObservationFixture(t)
				time.Sleep(5 * time.Second)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				entered, resume := make(chan struct{}), make(chan struct{})
				r.cfg.HistoryRecoveryLoadProbe = func() maintenance.StoragePressure { close(entered); <-resume; return healthyHistoryLoad(time.Now()) }
				sequence := r.historyLoad.acceptedSequence
				done := make(chan bool, 1)
				go func() { wake, _ := r.ObserveBusyHistoryResources(ctx); done <- wake }()
				<-entered
				switch kind {
				case "cancel generation":
					r.CancelBusyHistoryObservation()
				case "context":
					cancel()
				case "runner stopped":
					r.cancel()
				}
				close(resume)
				if <-done || r.busyHistoryObservationPending.Load() != 0 || r.historyLoad.acceptedSequence != sequence {
					t.Fatal("canceled in-flight observation consumed result")
				}
			})
		})
	}
	synctest.Test(t, func(t *testing.T) {
		r, _ := pendingRecoveryObservationFixture(t)
		time.Sleep(5 * time.Second)
		var probes, wakes atomic.Int32
		r.cfg.HistoryRecoveryLoadProbe = func() maintenance.StoragePressure { probes.Add(1); return healthyHistoryLoad(time.Now()) }
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if wake, _ := r.ObserveBusyHistoryResources(context.Background()); wake {
					wakes.Add(1)
				}
			}()
		}
		wg.Wait()
		if probes.Load() != 1 || wakes.Load() != 0 {
			t.Fatalf("concurrent probes=%d wakes=%d", probes.Load(), wakes.Load())
		}
	})
	for _, kind := range []string{"sync ended", "near tip"} {
		t.Run(kind, func(t *testing.T) {
			r, _ := pendingRecoveryObservationFixture(t)
			if kind == "sync ended" {
				r.chain.(*coldBuilderChain).syncRemainingOK = false
			} else {
				r.chain.(*coldBuilderChain).syncRemaining = 1
			}
			if wake, after := r.ObserveBusyHistoryResources(context.Background()); wake || after != 0 || r.busyHistoryObservationPending.Load() != 0 {
				t.Fatal("ended scope retained observation")
			}
		})
	}
}

func TestHistoryRecoveryObservationNextAdmissionRechecksPressure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, _ := pendingRecoveryObservationFixture(t)
		for i := 0; i < 3; i++ {
			time.Sleep(5 * time.Second)
			r.ObserveBusyHistoryResources(context.Background())
		}
		time.Sleep(25 * time.Second)
		calls := 0
		r.cfg.HistoryLoadProbe = func() maintenance.StoragePressure {
			calls++
			p := healthyHistoryLoad(time.Now())
			p.MemTableCount = int64(p.MemTableStopWritesThreshold)
			return p
		}
		result, err := r.OnePass()
		if err != nil || calls == 0 || !result.HistoryLoadDeferred || result.HistoryBuildAttempted || result.Built {
			t.Fatalf("normal admission reused recovery readiness: %+v calls=%d err=%v", result, calls, err)
		}
	})
}

func TestHistoryRecoveryObservationDoesNotInventFreshSamples(t *testing.T) {
	for _, kind := range []string{"repeated", "unknown engine", "stale engine", "future engine", "unknown device", "stale device"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r, _ := pendingRecoveryObservationFixture(t)
				initial := r.historyLoad.sample
				sequence := r.historyLoad.acceptedSequence
				time.Sleep(5 * time.Second)
				p := healthyHistoryLoad(time.Now())
				wantAccepted := uint64(0)
				switch kind {
				case "repeated":
					p = initial
				case "unknown engine":
					p.Available = false
				case "stale engine":
					p.SampledAt = time.Now().Add(-time.Minute)
				case "future engine":
					p.SampledAt = time.Now().Add(time.Minute)
				case "unknown device":
					p.DeviceAvailable = false
					wantAccepted = 1
				case "stale device":
					p.DeviceSampledAt = time.Now().Add(-time.Minute)
					wantAccepted = 1
				}
				r.cfg.HistoryRecoveryLoadProbe = func() maintenance.StoragePressure { return p }
				if wake, after := r.ObserveBusyHistoryResources(context.Background()); wake || after != 5*time.Second || r.historyLoad.acceptedSequence != sequence+wantAccepted {
					t.Fatal("invalid/repeated sample acceptance changed")
				}
				if wantAccepted == 1 && r.historyLoad.level > 2 {
					t.Fatal("missing device data asserted healthy capacity")
				}
			})
		})
	}
}
