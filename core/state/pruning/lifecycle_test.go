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

func TestSnapshotLifecycleRunsChainLookupPruneAfterHotPrune(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	writeSnapPruningChange(t, db, 1, 10, 12)
	chain := &fakePruneChain{db: db, solidified: 5}
	sawHotPruneStage := false
	chainLookupRan := false
	sawChainLookupBeforeSectionBloom := false
	sectionBloomRan := false
	sawSectionBloomBeforeBalanceTrace := false
	lifecycle := NewSnapshotLifecycle(chain, SnapshotLifecycleConfig{
		Pruner: PrunerConfig{
			Policy:    FullPolicy(2, 1),
			Interval:  time.Hour,
			BatchSize: 10,
		},
		ChainLookupPrune: func() (*snapshots.PruneHotChainLookupResult, error) {
			got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotPrune)
			if err != nil {
				return nil, err
			}
			sawHotPruneStage = ok && got == 5
			chainLookupRan = true
			return &snapshots.PruneHotChainLookupResult{
				HasRange:          true,
				FromBlock:         0,
				ToBlock:           1,
				ColdIndexSegments: 1,
			}, nil
		},
		SectionBloomPrune: func() (*snapshots.PruneHotSectionBloomResult, error) {
			sawChainLookupBeforeSectionBloom = chainLookupRan
			sectionBloomRan = true
			return &snapshots.PruneHotSectionBloomResult{
				HasRange:          true,
				FromSection:       0,
				ToSection:         1,
				ColdBloomSegments: 1,
				RowsDeleted:       2,
			}, nil
		},
		BalanceTracePrune: func() (*snapshots.PruneHotBalanceTraceResult, error) {
			sawSectionBloomBeforeBalanceTrace = sectionBloomRan
			return &snapshots.PruneHotBalanceTraceResult{
				HasRange:             true,
				FromBlock:            10,
				ToBlock:              12,
				ColdTraceSegments:    1,
				BlockTracesDeleted:   2,
				AccountTracesDeleted: 2,
			}, nil
		},
	})

	result, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("lifecycle pass: %v", err)
	}
	if !sawHotPruneStage {
		t.Fatal("chain lookup prune hook ran before state hot-prune stage advanced")
	}
	if result.ChainLookupPrune == nil || !result.ChainLookupPrune.HasRange || result.ChainLookupPrune.ToBlock != 1 {
		t.Fatalf("chain lookup prune result = %+v, want hook result", result.ChainLookupPrune)
	}
	if !sawChainLookupBeforeSectionBloom {
		t.Fatal("section bloom prune hook ran before chain lookup prune hook")
	}
	if result.SectionBloomPrune == nil || !result.SectionBloomPrune.HasRange || result.SectionBloomPrune.RowsDeleted != 2 {
		t.Fatalf("section bloom prune result = %+v, want hook result", result.SectionBloomPrune)
	}
	if !sawSectionBloomBeforeBalanceTrace {
		t.Fatal("balance trace prune hook ran before section bloom prune hook")
	}
	if result.BalanceTracePrune == nil || !result.BalanceTracePrune.HasRange || result.BalanceTracePrune.ToBlock != 12 {
		t.Fatalf("balance trace prune result = %+v, want hook result", result.BalanceTracePrune)
	}
}
