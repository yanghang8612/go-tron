package pruning

import (
	"errors"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

func TestSnapshotLifecycleThroughputFinalizesPostBuildWorkAndFailure(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "slow-success", true: "post-build-failure"}[failed], func(t *testing.T) {
			db := rawdb.NewMemoryDatabase()
			defer db.Close()
			dir := t.TempDir()
			for n := uint64(1); n <= 12; n++ {
				writeSnapPruningChange(t, db, n, n, n)
			}
			chain := &fakePruneChain{db: db, solidified: 13, syncRemaining: 1000, syncRemainingOK: true}
			wantErr := errors.New("injected post-build freezer failure")
			called := false
			l := NewSnapshotLifecycle(chain, SnapshotLifecycleConfig{
				Snapshot: snapshots.Config{Dir: dir, Enabled: true, HistoryWindow: 1, BatchBlocks: 8, BatchTxNums: 8,
					HistoryCatchupMode:      snapshots.HistoryCatchupThroughput,
					CatchupBuildMinInterval: time.Minute, CatchupHeavyWorkCooldown: 3 * time.Second,
					CatchupUnthrottledLagBlocks: 1, DeferHistoryBuildWhileSyncing: true,
					MaxDeferredHistoryBlocks: 2, MaxBusyDeferredHistoryBlocks: 4, SyncBuildReady: func() bool { return false }},
				Pruner: PrunerConfig{Policy: SnapPolicy(1, 1), SnapshotDir: dir},
				ChainFreezerBuild: func() (snapshots.ChainFreezerSnapshotPassResult, error) {
					called = true
					if failed {
						time.Sleep(20 * time.Millisecond)
						return snapshots.ChainFreezerSnapshotPassResult{}, wantErr
					}
					// Above the 750 ms threshold, outer work must extend the
					// 3 second provisional recovery from the cheap inner build.
					time.Sleep(800 * time.Millisecond)
					return snapshots.ChainFreezerSnapshotPassResult{}, nil
				},
			})
			var hookStats snapshots.Stats
			l.AddPassCompleteHook(func() { hookStats = l.builder.Snapshot() })
			result, err := l.OnePass()
			if !called || !result.Snapshot.Built || result.Snapshot.HistoryBatchBlocks != 8 {
				t.Fatalf("no full batch: %+v %v", result, err)
			}
			stats := l.builder.Snapshot()
			if stats.ForcedBusyBuilds != 1 || stats.SegmentsBuilt != 1 || result.Prune.DeletedDomainChangeBlocks != 8 {
				t.Fatalf("duplicate/lost publication or prune: %+v %+v", stats, result.Prune)
			}
			if failed {
				if !errors.Is(err, wantErr) || result.Snapshot.HistoryMaintenanceDuration < 20*time.Millisecond || result.Snapshot.HistoryMinRecovery < time.Minute || result.Snapshot.HistoryRetryRemaining(time.Now()) < 59*time.Second {
					t.Fatalf("late failure recovery: %+v %v", result.Snapshot, err)
				}
				if stats.LastForcedCompletionInterval != 0 || stats.LastForcedGrossRateMilli != 0 {
					t.Fatalf("failure recorded successful rate: %+v", stats)
				}
			} else {
				if err != nil || result.Snapshot.HistoryMaintenanceDuration < 800*time.Millisecond || result.Snapshot.HistoryMinRecovery < 3200*time.Millisecond || hookStats.LastMaintenanceDuration < 800*time.Millisecond {
					t.Fatalf("outer duration missing: %+v %+v %v", result.Snapshot, hookStats, err)
				}
			}
			// Going idle after either full success or an outer failure cannot
			// bypass the independent deadline or re-enter the freezer callback.
			chain.syncRemainingOK = false
			l.chainFreezerBuild = nil
			next, err := l.OnePass()
			if err != nil || next.Snapshot.HistoryBuildAttempted || !next.Snapshot.HistoryRateLimited {
				t.Fatalf("outer deadline bypassed: %+v %v", next.Snapshot, err)
			}
		})
	}
}
