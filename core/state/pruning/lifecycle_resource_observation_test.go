package pruning

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

func TestSnapshotLifecycleResourceObservationPreflightFailureCancels(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db := rawdb.NewMemoryDatabase()
		defer func() {
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}()
		dir := t.TempDir()
		for n := uint64(1); n <= 12; n++ {
			writeSnapPruningChange(t, db, n, n, n)
		}
		identity := snapshots.ChainIdentity{ChainID: 1, NetworkID: 1, GenesisHash: strings.Repeat("91", common.HashLength)}
		var probes atomic.Int64
		l := NewSnapshotLifecycle(&fakePruneChain{db: db, solidified: 13, syncRemaining: 1000, syncRemainingOK: true}, SnapshotLifecycleConfig{
			Snapshot: snapshots.Config{Dir: dir, Enabled: true, HistoryWindow: 1,
				HistoryCatchupMode: snapshots.HistoryCatchupThroughput, DeferHistoryBuildWhileSyncing: true,
				MaxDeferredHistoryBlocks: 2, MaxBusyDeferredHistoryBlocks: 20,
				SyncBuildReady: func() bool { return false }, BusyHistoryBuildReady: func() bool { return false },
				HeavyWorkGate: maintenance.NewHeavyWorkGate(), HistoryLoadProbe: func() maintenance.StoragePressure {
					probes.Add(1)
					return maintenance.StoragePressure{}
				},
				CatalogChain: &identity, CatalogSigningKey: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x91}, ed25519.SeedSize))},
			Pruner: PrunerConfig{Policy: SnapPolicy(1, 1), SnapshotDir: dir},
		})
		defer l.cancel()
		result, err := l.OnePass()
		if err != nil || !result.Snapshot.HistoryBusyResourceDeferred {
			t.Fatalf("no initial observation opportunity: %+v err=%v", result.Snapshot, err)
		}
		before := l.builder.Snapshot()
		beforeProbes := probes.Load()
		// Catalog preflight fails before the runner can invalidate the old
		// opportunity itself. Direct callers must also clear that opportunity.
		if err := os.WriteFile(filepath.Join(dir, snapshots.ManifestFile), []byte("invalid manifest"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := l.OnePass(); err == nil {
			t.Fatal("invalid catalog manifest passed preflight")
		}
		time.Sleep(snapshots.BusyHistoryObservationInterval)
		wake, retry := l.builder.ObserveBusyHistoryResources(context.Background())
		if wake || retry != 0 || probes.Load() != beforeProbes || l.builder.Snapshot().PassesCompleted != before.PassesCompleted {
			t.Fatalf("preflight failure retained stale observation: wake=%v retry=%s probes=%d want=%d", wake, retry, probes.Load(), beforeProbes)
		}
	})
}

// These loop tests use the real timer/select/coalescing machinery. The fake
// observer models the runner's short API; its deadline and pending completion
// can change after a pass returned, without changing the lifecycle's timers.
type lifecycleResourceObserver struct {
	pending, ready, completionPending bool
	deadline                          time.Time
	checks                            atomic.Int64
	mu                                sync.Mutex
	observe                           func(context.Context) (bool, time.Duration)
}

func (o *lifecycleResourceObserver) CancelBusyHistoryObservation() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pending = false
}

func (o *lifecycleResourceObserver) isPending() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.pending
}

func (o *lifecycleResourceObserver) setObserve(observe func(context.Context) (bool, time.Duration)) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.observe = observe
}

func (o *lifecycleResourceObserver) ObserveBusyHistoryResources(ctx context.Context) (bool, time.Duration) {
	o.checks.Add(1)
	o.mu.Lock()
	if observe := o.observe; observe != nil {
		o.mu.Unlock()
		return observe(ctx)
	}
	defer o.mu.Unlock()
	if !o.pending || ctx.Err() != nil {
		return false, 0
	}
	if !o.ready || o.completionPending || time.Now().Before(o.deadline) {
		return false, snapshots.BusyHistoryObservationInterval
	}
	o.pending = false
	return true, 0
}

func startResourceLoop(o *lifecycleResourceObserver, pass func() (SnapshotLifecyclePass, error)) *SnapshotLifecycle {
	l := NewSnapshotLifecycle(nil, SnapshotLifecycleConfig{Interval: time.Minute})
	go l.runLoop(func() (SnapshotLifecyclePass, error) {
		// The production OnePass cancels before catalog preflight and the
		// runner only arms a new opportunity for its successful result.
		o.CancelBusyHistoryObservation()
		result, err := pass()
		o.mu.Lock()
		o.pending = result.Snapshot.HistoryBusyResourceDeferred
		o.mu.Unlock()
		return result, err
	}, o)
	synctest.Wait()
	return l
}

func stopResourceLoop(t *testing.T, l *SnapshotLifecycle) {
	t.Helper()
	if err := l.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotLifecycleResourceObservationOnlyWhilePending(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "no-opportunity", true: "resources-unavailable"}[pending], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				o := new(lifecycleResourceObserver)
				var passes atomic.Int64
				l := startResourceLoop(o, func() (SnapshotLifecyclePass, error) {
					passes.Add(1)
					return SnapshotLifecyclePass{Snapshot: snapshots.PassResult{HistoryBusyResourceDeferred: pending}}, nil
				})
				defer stopResourceLoop(t, l)
				time.Sleep(55 * time.Second)
				synctest.Wait()
				wantChecks := int64(0)
				if pending {
					wantChecks = 11
				}
				if passes.Load() != 1 || o.checks.Load() != wantChecks {
					t.Fatalf("short observations became full maintenance: passes=%d checks=%d want=%d", passes.Load(), o.checks.Load(), wantChecks)
				}
				time.Sleep(5 * time.Second)
				synctest.Wait()
				if passes.Load() != 2 {
					t.Fatalf("original minute ticker changed: passes=%d", passes.Load())
				}
			})
		})
	}
}

func TestSnapshotLifecycleResourceObservationUsesLatestCompletionDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		o := &lifecycleResourceObserver{ready: true, completionPending: true, deadline: time.Now().Add(10 * time.Second)}
		var passes atomic.Int64
		l := startResourceLoop(o, func() (SnapshotLifecyclePass, error) {
			passes.Add(1)
			return SnapshotLifecyclePass{Snapshot: snapshots.PassResult{HistoryBusyResourceDeferred: passes.Load() == 1}}, nil
		})
		defer stopResourceLoop(t, l)
		time.Sleep(10 * time.Second)
		synctest.Wait()
		if passes.Load() != 1 || o.checks.Load() != 2 {
			t.Fatalf("observation bypassed pending outer completion: passes=%d checks=%d", passes.Load(), o.checks.Load())
		}
		// Full outer completion extends the old deadline after it would have
		// expired. Each observation must consult the current state anew.
		o.mu.Lock()
		o.completionPending = false
		o.deadline = time.Now().Add(15 * time.Second)
		o.mu.Unlock()
		time.Sleep(14 * time.Second)
		synctest.Wait()
		if passes.Load() != 1 {
			t.Fatalf("observation reused the obsolete deadline: passes=%d", passes.Load())
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if passes.Load() != 2 || o.checks.Load() != 5 {
			t.Fatalf("ready observation failed to request ordinary pass: passes=%d checks=%d", passes.Load(), o.checks.Load())
		}
		time.Sleep(20 * time.Second)
		synctest.Wait()
		if passes.Load() != 2 || o.checks.Load() != 5 {
			t.Fatalf("consumed opportunity kept polling: passes=%d checks=%d", passes.Load(), o.checks.Load())
		}
	})
}

func TestSnapshotLifecycleResourceObservationFailureKeepsAbsoluteRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		o := &lifecycleResourceObserver{ready: true}
		var passes atomic.Int64
		deadline := time.Now().Add(20 * time.Second)
		l := startResourceLoop(o, func() (SnapshotLifecyclePass, error) {
			passes.Add(1)
			if passes.Load() == 1 {
				return SnapshotLifecyclePass{Snapshot: snapshots.PassResult{HistoryBusyResourceDeferred: true, HistoryRetryDeadline: deadline}}, errors.New("outer maintenance failed")
			}
			return SnapshotLifecyclePass{}, nil
		})
		defer stopResourceLoop(t, l)
		time.Sleep(19 * time.Second)
		synctest.Wait()
		if passes.Load() != 1 || o.checks.Load() != 0 || o.isPending() {
			t.Fatalf("failed pass armed an observation: passes=%d checks=%d pending=%v", passes.Load(), o.checks.Load(), o.isPending())
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if passes.Load() != 2 || o.checks.Load() != 0 {
			t.Fatalf("absolute failure retry lost: passes=%d checks=%d", passes.Load(), o.checks.Load())
		}
	})
}

func TestSnapshotLifecycleResourceObservationWakeCoalesces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		o := new(lifecycleResourceObserver)
		var passes atomic.Int64
		l := startResourceLoop(o, func() (SnapshotLifecyclePass, error) {
			passes.Add(1)
			return SnapshotLifecyclePass{Snapshot: snapshots.PassResult{HistoryBusyResourceDeferred: passes.Load() == 1}}, nil
		})
		defer stopResourceLoop(t, l)
		o.setObserve(func(context.Context) (bool, time.Duration) {
			l.RequestPass()
			l.RequestPass()
			return true, 0
		})
		time.Sleep(10 * time.Second)
		synctest.Wait()
		if passes.Load() != 2 || o.checks.Load() != 1 {
			t.Fatalf("ready wake duplicated pending request: passes=%d checks=%d", passes.Load(), o.checks.Load())
		}
	})
}

func TestSnapshotLifecycleResourceObservationCoalescesDueTimers(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(map[bool]string{false: "interval", true: "retry"}[retry], func(t *testing.T) {
			// Both select orders must produce one pass. Repeating this small
			// virtual-time case exercises the randomized ready-channel order.
			for range 32 {
				synctest.Test(t, func(t *testing.T) {
					due := time.Minute
					if retry {
						due = 20 * time.Second
					}
					deadline := time.Now().Add(due)
					o := new(lifecycleResourceObserver)
					var passes atomic.Int64
					l := startResourceLoop(o, func() (SnapshotLifecyclePass, error) {
						passes.Add(1)
						result := SnapshotLifecyclePass{}
						if passes.Load() == 1 {
							result.Snapshot.HistoryBusyResourceDeferred = true
							if retry {
								result.Snapshot.HistoryRetryDeadline = deadline
							}
						}
						return result, nil
					})
					defer stopResourceLoop(t, l)
					o.setObserve(func(context.Context) (bool, time.Duration) {
						// Queue a normal request first, then hold the short API
						// until the independent old timer is also ready.
						l.RequestPass()
						time.Sleep(time.Until(deadline))
						return true, 0
					})
					time.Sleep(due + time.Second)
					synctest.Wait()
					if passes.Load() != 2 || o.checks.Load() != 1 {
						t.Fatalf("ready wake and due timer duplicated maintenance: passes=%d checks=%d", passes.Load(), o.checks.Load())
					}
				})
			}
		})
	}
}

func TestSnapshotLifecycleResourceObservationStop(t *testing.T) {
	for _, inFlight := range []bool{false, true} {
		t.Run(map[bool]string{false: "armed", true: "in-flight"}[inFlight], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				o := new(lifecycleResourceObserver)
				var passes atomic.Int64
				l := startResourceLoop(o, func() (SnapshotLifecyclePass, error) {
					passes.Add(1)
					return SnapshotLifecyclePass{Snapshot: snapshots.PassResult{HistoryBusyResourceDeferred: true}}, nil
				})
				if inFlight {
					o.setObserve(func(ctx context.Context) (bool, time.Duration) {
						<-ctx.Done()
						return true, 0
					})
					time.Sleep(5 * time.Second)
					synctest.Wait()
				}
				stopResourceLoop(t, l)
				checks := o.checks.Load()
				l.RequestPass()
				time.Sleep(time.Minute)
				synctest.Wait()
				if passes.Load() != 1 || o.checks.Load() != checks || o.isPending() || len(l.wake) != 0 {
					t.Fatalf("Stop left an observation or wake: passes=%d checks=%d pending=%v queued=%d", passes.Load(), o.checks.Load(), o.isPending(), len(l.wake))
				}
			})
		})
	}
}

// Exercise the production OnePass and Runner together: the five-second path
// refreshes resources without repeating any ordered maintenance callback.
func TestSnapshotLifecycleResourceObservationDoesNotRepeatMaintenance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db := rawdb.NewMemoryDatabase()
		defer func() {
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}()
		dir := t.TempDir()
		for n := uint64(1); n <= 12; n++ {
			writeSnapPruningChange(t, db, n, n, n)
		}
		var ready atomic.Bool
		var probes, freezers, lookups, hooks atomic.Int64
		l := NewSnapshotLifecycle(&fakePruneChain{db: db, solidified: 13, syncRemaining: 1000, syncRemainingOK: true}, SnapshotLifecycleConfig{
			Snapshot: snapshots.Config{Dir: dir, Enabled: true, HistoryWindow: 1, BatchBlocks: 8, BatchTxNums: 8,
				HistoryCatchupMode: snapshots.HistoryCatchupThroughput, CatchupBuildMinInterval: time.Minute,
				CatchupHeavyWorkCooldown: 3 * time.Second, CatchupUnthrottledLagBlocks: 1,
				DeferHistoryBuildWhileSyncing: true, MaxDeferredHistoryBlocks: 2, MaxBusyDeferredHistoryBlocks: 20,
				SyncBuildReady: func() bool { return false }, BusyHistoryBuildReady: func() bool { return ready.Load() },
				HeavyWorkGate: maintenance.NewHeavyWorkGate(), HistoryLoadProbe: func() maintenance.StoragePressure {
					probes.Add(1)
					return maintenance.StoragePressure{Available: true, SampledAt: time.Now(),
						L0Sublevels: 2, L0CompactionThreshold: 8, L0StopWritesThreshold: 192,
						MemTableCount: 1, MemTableStopWritesThreshold: 4,
						DeviceAvailable: true, DeviceSampledAt: time.Now(), DeviceBusyPPM: 500_000,
						DeviceQueueMilli: 500, DeviceAwait: time.Millisecond}
				}},
			Pruner: PrunerConfig{Policy: SnapPolicy(1, 1), SnapshotDir: dir}, Interval: time.Minute,
			ChainFreezerBuild: func() (snapshots.ChainFreezerSnapshotPassResult, error) {
				freezers.Add(1)
				return snapshots.ChainFreezerSnapshotPassResult{}, nil
			},
			ChainLookupPrune: func() (*snapshots.PruneHotChainLookupResult, error) { lookups.Add(1); return nil, nil },
		})
		l.AddPassCompleteHook(func() { hooks.Add(1) })
		if err := l.Start(); err != nil {
			t.Fatal(err)
		}
		defer stopResourceLoop(t, l)
		synctest.Wait()
		initial := l.builder.Snapshot()
		time.Sleep(20 * time.Second)
		synctest.Wait()
		stats := l.builder.Snapshot()
		if initial.HistoryDeferredSync != 1 || stats.PassesCompleted != initial.PassesCompleted || probes.Load() != 5 || freezers.Load() != 1 || lookups.Load() != 1 || hooks.Load() != 1 || stats.SegmentsBuilt != 0 {
			t.Fatalf("resource polling ran ordered work: initial=%+v stats=%+v probes=%d freezer=%d lookup=%d hooks=%d", initial, stats, probes.Load(), freezers.Load(), lookups.Load(), hooks.Load())
		}
		ready.Store(true)
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if stats := l.builder.Snapshot(); stats.SegmentsBuilt == 0 || stats.BusyResourceBuilds == 0 || hooks.Load() <= 1 {
			t.Fatalf("ready observation did not wake production pass before minute ticker: %+v hooks=%d", stats, hooks.Load())
		}
	})
}
