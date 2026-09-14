package pruning

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

func TestSnapshotLifecycleRecoveryObservationKeepsOriginalScheduling(t *testing.T) {
	for _, kind := range []string{"deadline", "minute", "outer error"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				o := new(lifecycleResourceObserver)
				var passes atomic.Int64
				deadline := time.Now().Add(20 * time.Second)
				l := NewSnapshotLifecycle(nil, SnapshotLifecycleConfig{Interval: time.Minute})
				o.setObserve(func(context.Context) (bool, time.Duration) { return false, snapshots.BusyHistoryObservationInterval })
				go l.runLoop(func() (SnapshotLifecyclePass, error) {
					o.CancelBusyHistoryObservation()
					n := passes.Add(1)
					result := SnapshotLifecyclePass{}
					if n == 1 {
						result.Snapshot.HistoryRecoveryObservation = true
						o.mu.Lock()
						o.pending = true
						o.mu.Unlock()
						if kind != "minute" {
							result.Snapshot.HistoryRetryDeadline = deadline
						}
						if kind == "outer error" {
							return result, errors.New("outer failure")
						}
					}
					return result, nil
				}, o)
				synctest.Wait()
				time.Sleep(15 * time.Second)
				synctest.Wait()
				wantChecks := int64(3)
				if kind == "outer error" {
					wantChecks = 0
				}
				if passes.Load() != 1 || o.checks.Load() != wantChecks {
					t.Fatalf("five-second observations ran maintenance: passes=%d checks=%d want=%d", passes.Load(), o.checks.Load(), wantChecks)
				}
				if kind == "minute" {
					time.Sleep(45 * time.Second)
				} else {
					time.Sleep(5 * time.Second)
				}
				synctest.Wait()
				if passes.Load() != 2 {
					t.Fatalf("original retry/ticker changed: passes=%d", passes.Load())
				}
				checks := o.checks.Load()
				time.Sleep(10 * time.Second)
				synctest.Wait()
				if o.checks.Load() != checks || o.isPending() {
					t.Fatal("completed opportunity kept observing")
				}
				stopResourceLoop(t, l)
			})
		})
	}
}

func TestSnapshotLifecycleRecoveryObservationDoesNotRepeatMaintenance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db := rawdb.NewMemoryDatabase()
		defer db.Close()
		dir := t.TempDir()
		for n := uint64(1); n <= 12; n++ {
			writeSnapPruningChange(t, db, n, n, n)
		}
		var probes, recovery, freezers, lookups, hooks atomic.Int64
		healthy := func() maintenance.StoragePressure {
			return maintenance.StoragePressure{Available: true, SampledAt: time.Now(), L0Sublevels: 2, L0CompactionThreshold: 8, L0StopWritesThreshold: 192, MemTableCount: 1, MemTableStopWritesThreshold: 4, DeviceAvailable: true, DeviceSampledAt: time.Now(), DeviceBusyPPM: 500_000, DeviceQueueMilli: 500, DeviceAwait: time.Millisecond}
		}
		l := NewSnapshotLifecycle(&fakePruneChain{db: db, solidified: 13, syncRemaining: 1000, syncRemainingOK: true}, SnapshotLifecycleConfig{
			Snapshot: snapshots.Config{Dir: dir, Enabled: true, HistoryWindow: 1, BatchBlocks: 8, BatchTxNums: 8,
				HistoryCatchupMode: snapshots.HistoryCatchupThroughput, CatchupBuildMinInterval: time.Minute,
				CatchupHeavyWorkCooldown: 30 * time.Second, CatchupUnthrottledLagBlocks: 1,
				DeferHistoryBuildWhileSyncing: true, MaxDeferredHistoryBlocks: 2, MaxBusyDeferredHistoryBlocks: 2,
				SyncBuildReady: func() bool { return false }, BusyHistoryBuildReady: func() bool { panic("over-busy should not request resource wake") },
				HeavyWorkGate: maintenance.NewHeavyWorkGate(), HistoryLoadProbe: func() maintenance.StoragePressure { probes.Add(1); return healthy() },
				HistoryRecoveryLoadProbe: func() maintenance.StoragePressure { recovery.Add(1); return healthy() }},
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
		initialProbes := probes.Load()
		initialFreezers, initialLookups, initialHooks := freezers.Load(), lookups.Load(), hooks.Load()
		time.Sleep(20 * time.Second)
		synctest.Wait()
		current := l.builder.Snapshot()
		if initial.SegmentsBuilt == 0 || current != initial || probes.Load() != initialProbes || recovery.Load() != 4 || freezers.Load() != initialFreezers || lookups.Load() != initialLookups || hooks.Load() != initialHooks {
			t.Fatalf("recovery observation repeated work: initial=%+v current=%+v ordinary=%d/%d recovery=%d freezer=%d lookup=%d hooks=%d", initial, current, probes.Load(), initialProbes, recovery.Load(), freezers.Load(), lookups.Load(), hooks.Load())
		}
		time.Sleep(11 * time.Second)
		synctest.Wait()
		if hooks.Load() <= initialHooks || probes.Load() <= initialProbes {
			t.Fatal("original full-pass retry did not recheck ordinary admission")
		}
	})
}
