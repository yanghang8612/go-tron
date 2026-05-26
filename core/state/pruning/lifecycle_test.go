package pruning

import (
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

func TestSnapshotLifecycleBuildsVisibleHistoryBeforePruningHotRows(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	writeSnapPruningChange(t, db, 1, 10, 12)

	chain := &fakePruneChain{db: db, solidified: 2}
	lifecycle := NewSnapshotLifecycle(chain, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{
			Dir:           dir,
			Enabled:       true,
			Interval:      time.Hour,
			HistoryWindow: 1,
		},
		Pruner: PrunerConfig{
			Policy:      SnapPolicy(1, 1),
			Interval:    time.Hour,
			SnapshotDir: dir,
		},
	})

	result, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("lifecycle pass: %v", err)
	}
	if !result.Snapshot.Built || result.Snapshot.FromTxNum != 1 || result.Snapshot.ToTxNum != 12 {
		t.Fatalf("snapshot result = %+v, want visible history [1,12]", result.Snapshot)
	}
	if result.Prune.DeletedDomainChangeBlocks != 1 || result.Prune.DeletedTxRanges != 0 {
		t.Fatalf("prune result = %+v, want one covered hot change block pruned and tx range retained", result.Prune)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, 1, 1); err != nil || ok {
		t.Fatalf("hot domain change survived ok=%v err=%v", ok, err)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 1); err != nil || !ok {
		t.Fatalf("state tx range should remain hot in snap mode ok=%v err=%v", ok, err)
	}
	manifest, err := snapshots.LoadManifest(dir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if manifest.Progress == nil || manifest.Progress.HistoryBuildTxNum != 12 || manifest.Progress.HotPruneTxNum != 12 {
		t.Fatalf("manifest progress = %+v, want history/hot-prune at 12", manifest.Progress)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotHistory); err != nil || !ok || got != 12 {
		t.Fatalf("snapshot history stage = %d ok=%v err=%v, want 12", got, ok, err)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotHotPrune); err != nil || !ok || got != 12 {
		t.Fatalf("snapshot hot-prune stage = %d ok=%v err=%v, want 12", got, ok, err)
	}
}
