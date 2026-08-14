package pruning

import (
	"context"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

func TestStateChangeIndexPrunerSweepsOnceAndPersistsProgress(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	change := &rawdb.StateDomainChange{
		BlockNum: 10, TxNum: 10, Seq: 1,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      common.Address{common.AddressPrefixMainnet, 0x70},
		Domain:     kvdomains.ContractStorage,
		Key:        []byte("slot"),
	}
	if err := rawdb.WriteStateDomainChange(db, change); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageStateHistoryIndex, 10, common.Hash{10}); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.DeleteStateDomainChanges(db, 10); err != nil {
		t.Fatal(err)
	}
	manifest := snapshots.NewManifest(1, 10, nil)
	manifest.Progress = &snapshots.Progress{HotPruneBlockNum: 10, HotPruneTxNum: 10}
	if err := snapshots.PublishManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}
	pruner := StateChangeIndexPruner{DB: db, SnapshotDir: dir, MinAdvanceBlocks: 4}
	result, err := pruner.OnePass(context.Background())
	if err != nil || result == nil || result.PostingRowsDeleted != 1 || result.DirectoryRowsDeleted != 1 {
		t.Fatalf("first sweep = (%+v,%v)", result, err)
	}
	manifest, err = snapshots.LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Progress == nil || manifest.Progress.StateChangeIndexPruneBlockNum != 10 {
		t.Fatalf("persisted sweep progress = %+v, want block 10", manifest.Progress)
	}
	result, err = pruner.OnePass(context.Background())
	if err != nil || result != nil {
		t.Fatalf("unchanged sweep = (%+v,%v), want no work", result, err)
	}
	if err := snapshots.UpdateHotPruneProgress(dir, 12, 12); err != nil {
		t.Fatal(err)
	}
	result, err = pruner.OnePass(context.Background())
	if err != nil || result != nil {
		t.Fatalf("sub-threshold sweep = (%+v,%v), want no work", result, err)
	}
}

func TestStateChangeIndexPrunerWaitsForPostingIndexCoverage(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	manifest := snapshots.NewManifest(1, 10, nil)
	manifest.Progress = &snapshots.Progress{HotPruneBlockNum: 10, HotPruneTxNum: 10}
	if err := snapshots.PublishManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}
	pruner := StateChangeIndexPruner{DB: db, SnapshotDir: dir, MinAdvanceBlocks: 1}
	if result, err := pruner.OnePass(context.Background()); err != nil || result != nil {
		t.Fatalf("missing index stage = (%+v,%v), want deferred", result, err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageStateHistoryIndex, 9, common.Hash{9}); err != nil {
		t.Fatal(err)
	}
	if result, err := pruner.OnePass(context.Background()); err != nil || result != nil {
		t.Fatalf("lagging index stage = (%+v,%v), want deferred", result, err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageStateHistoryIndex, 10); err != nil {
		t.Fatal(err)
	}
	if result, err := pruner.OnePass(context.Background()); err != nil || result != nil {
		t.Fatalf("unbound index stage = (%+v,%v), want deferred", result, err)
	}
}

func TestStateChangeIndexPrunerDefersWhenHeavyWorkIsBusy(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	manifest := snapshots.NewManifest(1, 10, nil)
	manifest.Progress = &snapshots.Progress{HotPruneBlockNum: 10, HotPruneTxNum: 10}
	if err := snapshots.PublishManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageStateHistoryIndex, 10, common.Hash{10}); err != nil {
		t.Fatal(err)
	}
	gate := maintenance.NewHeavyWorkGate()
	release, ok := gate.TryAcquire()
	if !ok {
		t.Fatal("failed to occupy heavy-work gate")
	}
	defer release()
	pruner := StateChangeIndexPruner{DB: db, SnapshotDir: dir, MinAdvanceBlocks: 1, HeavyWorkGate: gate}
	if result, err := pruner.OnePass(context.Background()); err != nil || result != nil {
		t.Fatalf("busy-gate sweep = (%+v,%v), want deferred", result, err)
	}
	manifest, err := snapshots.LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Progress == nil || manifest.Progress.StateChangeIndexPruneBlockNum != 0 {
		t.Fatalf("busy gate advanced progress: %+v", manifest.Progress)
	}
}
