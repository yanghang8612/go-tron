package pruning

import (
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

func TestSnapshotLifecycleRetriesActiveLeaseWithoutCoarseTick(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	writeSnapPruningChange(t, db, 1, 1, 2)
	gate := maintenance.NewHeavyWorkGate()
	release, _ := gate.TryAcquire()
	defer release()
	lifecycle := NewSnapshotLifecycle(&fakePruneChain{db: db, solidified: 2}, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{Dir: dir, Enabled: true, HistoryWindow: 1, HeavyWorkGate: gate},
		Pruner:   PrunerConfig{Policy: SnapPolicy(1, 1), SnapshotDir: dir, Interval: time.Hour},
		Interval: time.Hour,
	})
	if err := lifecycle.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lifecycle.Stop() })
	deadline := time.Now().Add(5 * time.Second)
	for lifecycle.builder.Snapshot().HistoryGateDeferred == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if stats := lifecycle.builder.Snapshot(); stats.HistoryGateDeferred == 0 || stats.SegmentsBuilt != 0 {
		t.Fatalf("initial active lease was not deferred: %+v", stats)
	}
	release()
	// No RequestPass or freezer hook fires here, and the ticker is one hour.
	// Successful publication proves that the positive fallback armed the timer.
	deadline = time.Now().Add(5 * time.Second)
	for lifecycle.builder.Snapshot().SegmentsBuilt == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	progress, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotBuild)
	if err != nil || !ok || progress != 1 || lifecycle.builder.Snapshot().SegmentsBuilt != 1 {
		t.Fatalf("active lease lost retry until coarse tick: progress=%d valid=%v err=%v stats=%+v", progress, ok, err, lifecycle.builder.Snapshot())
	}
}
