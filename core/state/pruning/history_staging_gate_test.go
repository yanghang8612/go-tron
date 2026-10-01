package pruning

import (
	"context"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestWorkerHistoryStagingDefersLegacyHistoryDeletion(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { _ = db.Close() })
	writeSnapPruningChange(t, db, 1, 10, 12)
	w := Worker{
		DB: db, Policy: SnapPolicy(2, 1), SnapshotDir: t.TempDir(),
		HistoryStaging: &rawdb.HistoryStagingManager{},
	}
	stats, err := w.PruneToContext(context.Background(), 4)
	if err != nil {
		t.Fatal(err)
	}
	if stats.DeletedDomainChangeBlocks != 0 || stats.DeletedTxRanges != 0 || stats.HistoryChunkGC.Retired != 0 {
		t.Fatalf("legacy history retirement ran under staging: %+v", stats)
	}
	seen := 0
	if err := rawdb.IterateStateDomainChangesByBlockRange(db, 1, 1, func(_ *rawdb.StateDomainChange) (bool, error) {
		seen++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if seen == 0 {
		t.Fatal("source history was removed before staging adopted it")
	}
}
