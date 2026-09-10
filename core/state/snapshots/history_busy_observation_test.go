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

func pendingBusyObservationFixture(t *testing.T) (*Runner, *bool, PassResult) {
	t.Helper()
	r := busyResourceFixture(t)
	ready := false
	r.cfg.BusyHistoryBuildReady = func() bool { return ready }
	result, err := r.OnePass()
	if err != nil || !result.HistoryBusyResourceDeferred || result.HistoryBuildAttempted || r.busyHistoryObservationPending.Load() == 0 {
		t.Fatalf("expected resource-only observation opportunity: %+v %v", result, err)
	}
	return r, &ready, result
}

type observationNoDatabaseChain struct{ *coldBuilderChain }

func (observationNoDatabaseChain) DB() AggregatorDB {
	panic("resource observation accessed the application database")
}

func TestHistoryBusyObservationIsBoundedAndDoesNoMaintenance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, ready, _ := pendingBusyObservationFixture(t)
		r.chain = observationNoDatabaseChain{r.chain.(*coldBuilderChain)}
		r.cfg.HistoryPressureProbe = func(context.Context) (HistoryPressure, error) { panic("resource observation probed history space") }
		var leaseChecks int
		r.cfg.HeavyWorkGate.SetAdmissionCheck(func() bool { leaseChecks++; return true })
		stats, density, eventDensity := r.Snapshot(), r.historyLoad.history, r.historyLoad.event
		sequence := r.historyLoad.acceptedSequence
		checks := r.busyHistoryObservationMetrics.checks.Snapshot().Count()
		for i := 0; i < 20; i++ {
			wake, after := r.ObserveBusyHistoryResources(context.Background())
			if wake || after != BusyHistoryObservationInterval {
				t.Fatalf("early recheck=%v/%v", wake, after)
			}
		}
		if r.historyLoad.acceptedSequence != sequence || r.busyHistoryObservationMetrics.checks.Snapshot().Count() != checks {
			t.Fatal("repeated early calls consumed observations")
		}
		time.Sleep(BusyHistoryObservationInterval)
		if wake, after := r.ObserveBusyHistoryResources(context.Background()); wake || after != BusyHistoryObservationInterval {
			t.Fatalf("unready observation=%v/%v", wake, after)
		}
		if r.historyLoad.acceptedSequence != sequence+1 || r.busyHistoryObservationMetrics.checks.Snapshot().Count() != checks+1 {
			t.Fatal("eligible observation did not refresh budget exactly once")
		}
		*ready = true
		if wake, _ := r.ObserveBusyHistoryResources(context.Background()); wake {
			t.Fatal("readiness change bypassed observation spacing")
		}
		time.Sleep(BusyHistoryObservationInterval)
		if wake, after := r.ObserveBusyHistoryResources(context.Background()); !wake || after != 0 {
			t.Fatalf("ready observation=%v/%v", wake, after)
		}
		if wake, after := r.ObserveBusyHistoryResources(context.Background()); wake || after != 0 {
			t.Fatalf("consumed wake repeated=%v/%v", wake, after)
		}
		if r.Snapshot() != stats || r.historyLoad.history != density || r.historyLoad.event != eventDensity || r.historyNotBefore.Load() != 0 || leaseChecks != 0 {
			t.Fatal("pure observations performed maintenance or changed its accounting")
		}
		if r.busyHistoryObservationMetrics.duration.Snapshot().Value() <= 0 {
			t.Fatal("observation duration missing")
		}
	})
}

func TestHistoryBusyObservationOnlyArmsEnabledSoftBusyOpportunity(t *testing.T) {
	for _, kind := range []string{"balanced", "nil CPU callback", "nil load", "nil gate", "below soft", "above busy", "idle", "near tip", "hot pressure"} {
		t.Run(kind, func(t *testing.T) {
			r := busyResourceFixture(t)
			r.cfg.BusyHistoryBuildReady = func() bool { return false }
			switch kind {
			case "balanced":
				r.cfg.HistoryCatchupMode = HistoryCatchupBalanced
			case "nil CPU callback":
				r.cfg.BusyHistoryBuildReady = nil
			case "nil load":
				r.cfg.HistoryLoadProbe = nil
			case "nil gate":
				r.cfg.HeavyWorkGate = nil
			case "below soft":
				r.cfg.MaxDeferredHistoryBlocks = 12
			case "above busy":
				r.cfg.MaxBusyDeferredHistoryBlocks = 4
			case "idle":
				r.chain.(*coldBuilderChain).syncRemainingOK = false
			case "near tip":
				r.chain.(*coldBuilderChain).syncRemaining = 1
			case "hot pressure":
				r.cfg.HistoryPressureProbe = func(context.Context) (HistoryPressure, error) {
					return HistoryPressure{HotHistoryBytesAvailable: true, HotHistoryBytes: 100}, nil
				}
			}
			result, err := r.OnePass()
			if err != nil || result.HistoryBusyResourceDeferred || r.busyHistoryObservationPending.Load() != 0 {
				t.Fatalf("unexpected observation arm: %+v %v", result, err)
			}
		})
	}
}

func TestHistoryBusyObservationPreservesLatestDeadlineAndGateCooldown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, ready, _ := pendingBusyObservationFixture(t)
		*ready = true
		start := time.Now()
		r.extendHistoryNotBefore(start.Add(20 * time.Second))
		r.cfg.HeavyWorkGate = maintenance.NewHeavyWorkGateWithCooldown(40 * time.Second)
		release, ok := r.cfg.HeavyWorkGate.TryAcquire()
		if !ok {
			t.Fatal("setup cooldown")
		}
		release()
		for elapsed := 5 * time.Second; elapsed <= 40*time.Second; elapsed += BusyHistoryObservationInterval {
			time.Sleep(BusyHistoryObservationInterval)
			if elapsed == 10*time.Second {
				r.extendHistoryNotBefore(start.Add(30 * time.Second))
			}
			deadline := r.historyNotBefore.Load()
			cooldown := r.cfg.HeavyWorkGate.CooldownRemaining()
			wake, after := r.ObserveBusyHistoryResources(context.Background())
			if wake != (elapsed == 40*time.Second) || wake && after != 0 || !wake && after != BusyHistoryObservationInterval {
				t.Fatalf("at %s wake/retry=%v/%s", elapsed, wake, after)
			}
			if r.historyNotBefore.Load() != deadline || r.cfg.HeavyWorkGate.CooldownRemaining() != cooldown {
				t.Fatal("observation changed an existing deadline or cooldown")
			}
		}
		if r.Snapshot().BusyResourceAttempts != 0 {
			t.Fatal("observation acquired work")
		}
	})
}

func TestHistoryBusyObservationSkipsLockedAndPendingCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, _, _ := pendingBusyObservationFixture(t)
		time.Sleep(BusyHistoryObservationInterval)
		before := r.historyLoad.acceptedSequence
		r.passMu.Lock()
		wake, after := r.ObserveBusyHistoryResources(context.Background())
		r.passMu.Unlock()
		if wake || after != BusyHistoryObservationInterval {
			t.Fatalf("locked recheck=%v/%s", wake, after)
		}
		r.pendingMaintenanceID = 17
		wake, after = r.ObserveBusyHistoryResources(context.Background())
		if wake || after != BusyHistoryObservationInterval || r.historyLoad.acceptedSequence != before || r.pendingMaintenanceID != 17 {
			t.Fatal("pending completion was observed or modified")
		}
		r.pendingMaintenanceID = 0
		if wake, _ := r.ObserveBusyHistoryResources(context.Background()); wake || r.historyLoad.acceptedSequence != before+1 {
			t.Fatal("unlocked observation did not resume")
		}
	})
}

func TestHistoryBusyObservationCancellationAndConcurrentWake(t *testing.T) {
	t.Run("cancel while callback runs", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			r, _, _ := pendingBusyObservationFixture(t)
			time.Sleep(BusyHistoryObservationInterval)
			entered, resume := make(chan struct{}), make(chan struct{})
			r.cfg.BusyHistoryBuildReady = func() bool { close(entered); <-resume; return true }
			result := make(chan bool, 1)
			go func() { wake, _ := r.ObserveBusyHistoryResources(context.Background()); result <- wake }()
			<-entered
			r.CancelBusyHistoryObservation() // production can cancel before catalog preflight
			close(resume)
			if <-result || r.busyHistoryObservationPending.Load() != 0 {
				t.Fatal("canceled generation emitted a stale wake")
			}
		})
	})
	t.Run("one wake under concurrent callers", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			r, ready, _ := pendingBusyObservationFixture(t)
			*ready = true
			time.Sleep(BusyHistoryObservationInterval)
			var wakes atomic.Int32
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
			if wakes.Load() != 1 {
				t.Fatalf("coalesced wakes=%d", wakes.Load())
			}
		})
	})
	for _, kind := range []string{"context", "runner stopped", "sync ended", "no longer deep"} {
		t.Run(kind, func(t *testing.T) {
			r, _, _ := pendingBusyObservationFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch kind {
			case "context":
				cancel()
			case "runner stopped":
				r.cancel()
			case "sync ended":
				r.chain.(*coldBuilderChain).syncRemainingOK = false
			case "no longer deep":
				r.chain.(*coldBuilderChain).syncRemaining = 1
			}
			before := r.historyLoad.acceptedSequence
			if wake, after := r.ObserveBusyHistoryResources(ctx); wake || after != 0 || r.busyHistoryObservationPending.Load() != 0 || r.historyLoad.acceptedSequence != before {
				t.Fatal("canceled scope retained an observation")
			}
		})
	}
}

func TestHistoryBusyObservationFailuresCancelOnlyTheirGeneration(t *testing.T) {
	for _, kind := range []string{"before merge", "outer completion", "next metadata"} {
		t.Run(kind, func(t *testing.T) {
			r, _, result := pendingBusyObservationFixture(t)
			wantErr := errors.New("injected maintenance failure")
			switch kind {
			case "before merge":
				var err error
				result, err = r.OnePassWithMaintenanceContext(context.Background(), func(context.Context, PassResult) error { return wantErr })
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
					t.Fatal("malformed next preflight unexpectedly succeeded")
				}
			}
			if r.busyHistoryObservationPending.Load() != 0 {
				t.Fatal("failed full pass retained fast observation retry")
			}
			if wake, after := r.ObserveBusyHistoryResources(context.Background()); wake || after != 0 {
				t.Fatal("failure created a fast full wake")
			}
		})
	}
	r, _, old := pendingBusyObservationFixture(t)
	next, err := r.OnePass()
	if err != nil || !next.HistoryBusyResourceDeferred || next.historyBusyObservationID == old.historyBusyObservationID {
		t.Fatal("new full pass did not replace the generation")
	}
	id := r.busyHistoryObservationPending.Load()
	r.CompleteHistoryMaintenance(&old, time.Now(), errors.New("late duplicate failure"))
	if r.busyHistoryObservationPending.Load() != id {
		t.Fatal("old completion canceled a newer observation generation")
	}
}

func TestHistoryBusyObservationWaitingFullPassCancelsNewlyArmedGeneration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := busyResourceFixture(t)
		var ready atomic.Bool
		var reads atomic.Int32
		entered, resume := make(chan struct{}), make(chan struct{})
		r.cfg.BusyHistoryBuildReady = ready.Load
		r.cfg.HistoryLoadProbe = func() maintenance.StoragePressure {
			if reads.Add(1) == 1 {
				close(entered)
				<-resume
			}
			return healthyHistoryLoad(time.Now())
		}
		type outcome struct {
			result PassResult
			err    error
		}
		a, b := make(chan outcome, 1), make(chan outcome, 1)
		go func() {
			result, err := r.OnePassWithMaintenanceContext(context.Background(), func(context.Context, PassResult) error { ready.Store(true); return nil })
			a <- outcome{result, err}
		}()
		<-entered // A owns passMu but has not reached the resource decision.
		// Err is called after B's early cancellation but before passMu.Lock.
		// This explicit handshake does not treat mutex waits as synctest-durable.
		bctx := &busyObservationEntryContext{Context: context.Background(), entered: make(chan struct{})}
		go func() { result, err := r.OnePassContext(bctx); b <- outcome{result, err} }()
		<-bctx.entered
		close(resume)
		first, second := <-a, <-b
		if first.err != nil || !first.result.HistoryBusyResourceDeferred || second.err != nil || !second.result.Built {
			t.Fatalf("concurrent full passes: first=%+v second=%+v", first, second)
		}
		if r.busyHistoryObservationPending.Load() != 0 {
			t.Fatal("waiting full pass retained A's later-armed generation")
		}
	})
}

type busyObservationEntryContext struct {
	context.Context
	once    sync.Once
	entered chan struct{}
}

func (c *busyObservationEntryContext) Err() error {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Err()
}

func TestHistoryBusyObservationCoalescesAlreadyDueStandaloneTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		catchup := make(chan struct{}, 1)
		time.Sleep(time.Minute)
		catchup <- struct{}{} // an observation requested a normal full pass
		<-catchup             // force the catchup branch to win while the old tick is ready
		coalesceColdHistoryWakeups(catchup, ticker.C)
		select {
		case <-ticker.C:
			t.Fatal("old interval tick would immediately repeat the full pass")
		default:
		}
		// A genuinely later tick, including one arriving during that full pass,
		// must remain available; observation does not replace the regular cadence.
		time.Sleep(time.Minute)
		select {
		case <-ticker.C:
		default:
			t.Fatal("coalescing removed the next interval tick")
		}
	})
}

func TestHistoryBusyObservationDoesNotRecoverFromRepeatedOrUnknownEngine(t *testing.T) {
	for _, kind := range []string{"repeated engine", "unknown engine", "nil load"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r := busyResourceFixture(t)
				p := healthyHistoryLoad(time.Now())
				r.cfg.HistoryLoadProbe = func() maintenance.StoragePressure { return p }
				r.historyLoad.level = 1
				result, err := r.OnePass()
				if err != nil || !result.HistoryBusyResourceDeferred || r.historyLoad.good != 1 {
					t.Fatalf("pressure recovery setup: %+v %v", result, err)
				}
				sequence := r.historyLoad.acceptedSequence
				switch kind {
				case "unknown engine":
					p.Available = false
				case "nil load":
					r.cfg.HistoryLoadProbe = nil
				}
				for i := 0; i < 4; i++ {
					time.Sleep(BusyHistoryObservationInterval)
					wake, after := r.ObserveBusyHistoryResources(context.Background())
					if wake || kind == "nil load" && after != 0 || kind != "nil load" && after != BusyHistoryObservationInterval {
						t.Fatalf("%s yielded wake/retry=%v/%s", kind, wake, after)
					}
					if r.historyLoad.acceptedSequence != sequence || r.historyLoad.good > 1 || r.historyLoad.level > 1 {
						t.Fatal("repeated/unknown engine invented a recovery observation")
					}
				}
				if r.Snapshot().BusyResourceAttempts != 0 {
					t.Fatal("unknown resources caused maintenance")
				}
			})
		})
	}
}
