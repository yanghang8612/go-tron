package domains

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// buildManagerWithBranchSnapshot builds a CommitmentRoot snapshot PLUS a
// branch-row snapshot for db's current staged state, publishes BOTH into the
// manifest, and returns an opened bare *snapshots.Manager whose
// IterateCommitmentBranches will serve the branch rows. This is the production
// wiring: the bare Manager passed to the orchestrator, rather than the older
// CommitmentBranchSource wrapper that bypasses the manifest.
func buildManagerWithBranchSnapshot(t *testing.T, db CommitmentDB, dir string, txNum uint64) *snapshots.Manager {
	t.Helper()
	rootRef, rootAccessorRef, rootBTreeRef, err := snapshots.BuildCommitmentRootSegmentFilesFromDB(db, dir, txNum, txNum, "commitment/root-snap.seg")
	if err != nil {
		t.Fatalf("build commitment root snapshot: %v", err)
	}
	branchRef, err := snapshots.BuildCommitmentBranchSegmentFromDB(db, dir, "commitment/branches-snap.json", txNum, txNum)
	if err != nil {
		t.Fatalf("build commitment branch snapshot: %v", err)
	}
	// Include branchRef in the manifest so Manager.IterateCommitmentBranches
	// finds it via coveringCommitmentBranchRef.
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(txNum, txNum, []snapshots.SegmentRef{
		rootRef, rootAccessorRef, rootBTreeRef, branchRef,
	})); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	mgr, err := snapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("open snapshot manager: %v", err)
	}
	return mgr
}

// buildStagedCommitmentRootSnapshot builds and publishes a CommitmentRoot
// snapshot plus a branch-row snapshot for db's current staged state, returning an
// opened CommitmentBranchSource over both. txNum is the visible tx of the
// segments.
func buildStagedCommitmentRootSnapshot(t *testing.T, db CommitmentDB, dir string, txNum uint64) *snapshots.CommitmentBranchSource {
	t.Helper()
	rootRef, rootAccessorRef, rootBTreeRef, err := snapshots.BuildCommitmentRootSegmentFilesFromDB(db, dir, txNum, txNum, "commitment/root-snap.seg")
	if err != nil {
		t.Fatalf("build commitment root snapshot: %v", err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(txNum, txNum, []snapshots.SegmentRef{
		rootRef, rootAccessorRef, rootBTreeRef,
	})); err != nil {
		t.Fatalf("publish commitment root manifest: %v", err)
	}
	branchRef, err := snapshots.BuildCommitmentBranchSegmentFromDB(db, dir, "commitment/branches-snap.json", txNum, txNum)
	if err != nil {
		t.Fatalf("build commitment branch snapshot: %v", err)
	}
	mgr, err := snapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("open snapshot manager: %v", err)
	}
	return snapshots.NewCommitmentBranchSource(mgr, dir, branchRef)
}

// TestStagedApplyRestoresPrunedBranchFromColdSnapshot is the headline acceptance
// test: a staged store commits branch state, has a cold snapshot taken, then its
// hot branch rows are pruned. A subsequent commit through the orchestrator with a
// CommitmentSnapshotRepair source restores the branch rows from the snapshot and
// re-derives the correct root WITHOUT a bootstrap Rebuild scan.
func TestStagedApplyRestoresPrunedBranchFromColdSnapshot(t *testing.T) {
	owner := common.Address{0x41, 0x60}
	key := []byte("slot/cold")
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStateKVGeneration(db, owner, 0); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner, 0, kvdomains.ContractStorage, key, []byte("v1")); err != nil {
		t.Fatal(err)
	}

	store := newStagedCommitmentStore(db)
	// Establish branch state + root row over {v1}.
	if _, err := store.Rebuild(); err != nil {
		t.Fatalf("initial rebuild: %v", err)
	}

	// Take the cold snapshot of the current (v1) staged branch rows + root.
	dir := t.TempDir()
	src := buildStagedCommitmentRootSnapshot(t, db, dir, 10)

	// Prune the hot branch rows. The root row survives (only branches are cold).
	deleteStagedBranchRows(t, db)
	if _, ok, err := store.store.GetBranch(nil); err != nil || ok {
		t.Fatalf("precondition: root branch still present after prune (ok=%v err=%v)", ok, err)
	}

	// Mutate the source row and commit incrementally through the orchestrator with
	// the snapshot repair source.
	if err := rawdb.WriteStateKVLatest(db, owner, 0, kvdomains.ContractStorage, key, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	commitmentKey := rawdb.StateKVLatestCommitmentKey(owner, 0, kvdomains.ContractStorage, key)
	commitmentValue := rawdb.EncodeStateKVLatestValue([]byte("v2"))

	bootstrapBefore := store.bootstrapCount
	root, err := applyLatestCommitmentWithRepair(store, []rawdb.StateCommitmentUpdate{
		rawdb.NewStateCommitmentPut(commitmentKey, commitmentValue),
	}, CommitmentSnapshotRepair{Source: src, TxNum: 10})
	if err != nil {
		t.Fatalf("apply with cold-snapshot branch repair: %v", err)
	}

	// The headline assertion: the restore path must NOT have run a bootstrap scan.
	if store.bootstrapCount != bootstrapBefore {
		t.Fatalf("cold-snapshot repair ran bootstrap (count %d -> %d); branch rows must be restored from snapshot, not rebuilt",
			bootstrapBefore, store.bootstrapCount)
	}

	// The re-derived root must equal a from-scratch staged build over {v2}.
	wantDB := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStateKVGeneration(wantDB, owner, 0); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(wantDB, owner, 0, kvdomains.ContractStorage, key, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	want, err := newStagedCommitmentStore(wantDB).Rebuild()
	if err != nil {
		t.Fatalf("rebuild expected commitment: %v", err)
	}
	if root != want {
		t.Fatalf("root after cold-snapshot repair = %x, want from-scratch staged build %x", root, want)
	}
	if folded, err := store.trie.Fold(nil); err != nil {
		t.Fatalf("Fold(nil) after repair: %v", err)
	} else if folded != want {
		t.Fatalf("Fold(nil) after repair = %x, want %x", folded, want)
	}
	if stored, ok, err := rawdb.ReadLatestDomainCommitmentRoot(db); err != nil || !ok || stored != want {
		t.Fatalf("stored root row = %x ok=%v err=%v, want %x", stored, ok, err, want)
	}
}

// TestStagedApplyFallsBackToRebuildOnAbsentSnapshot is the negative test: when
// the snapshot does not match (here, the repair tx is outside the snapshot's
// range so no branch rows are restorable), RestoreNodesFromSnapshot returns
// (false, nil) and the orchestrator falls through to Rebuild, still producing the
// correct root.
func TestStagedApplyFallsBackToRebuildOnAbsentSnapshot(t *testing.T) {
	owner := common.Address{0x41, 0x61}
	key := []byte("slot/fallback")
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStateKVGeneration(db, owner, 0); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner, 0, kvdomains.ContractStorage, key, []byte("v1")); err != nil {
		t.Fatal(err)
	}

	store := newStagedCommitmentStore(db)
	if _, err := store.Rebuild(); err != nil {
		t.Fatalf("initial rebuild: %v", err)
	}

	// Build the snapshot at tx 10, but drive the repair with tx 99 — out of range,
	// so the branch source yields nothing and the restore declines.
	dir := t.TempDir()
	src := buildStagedCommitmentRootSnapshot(t, db, dir, 10)

	deleteStagedBranchRows(t, db)

	if err := rawdb.WriteStateKVLatest(db, owner, 0, kvdomains.ContractStorage, key, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	commitmentKey := rawdb.StateKVLatestCommitmentKey(owner, 0, kvdomains.ContractStorage, key)
	commitmentValue := rawdb.EncodeStateKVLatestValue([]byte("v2"))

	bootstrapBefore := store.bootstrapCount
	root, err := applyLatestCommitmentWithRepair(store, []rawdb.StateCommitmentUpdate{
		rawdb.NewStateCommitmentPut(commitmentKey, commitmentValue),
	}, CommitmentSnapshotRepair{Source: src, TxNum: 99})
	if err != nil {
		t.Fatalf("apply with absent-snapshot repair: %v", err)
	}

	// Rebuild MUST have run exactly once for the fallback.
	if store.bootstrapCount != bootstrapBefore+1 {
		t.Fatalf("absent-snapshot fallback bootstrap count %d -> %d, want exactly one rebuild",
			bootstrapBefore, store.bootstrapCount)
	}

	wantDB := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStateKVGeneration(wantDB, owner, 0); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(wantDB, owner, 0, kvdomains.ContractStorage, key, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	want, err := newStagedCommitmentStore(wantDB).Rebuild()
	if err != nil {
		t.Fatalf("rebuild expected commitment: %v", err)
	}
	if root != want {
		t.Fatalf("root after Rebuild fallback = %x, want %x", root, want)
	}
}

// TestStagedColdRestoreUsesSnapshotNotRebuild is the end-to-end proof that the
// pipeline restores staged commitment branches from a cold snapshot rather than
// running a full Rebuild scan. The test has three parts:
//
//  1. Snapshot path: the bare *snapshots.Manager is passed as the repair source.
//     The spy hook must fire zero times (RestoreNodesFromSnapshot succeeds; Rebuild
//     is never entered).
//
//  2. Negative control: repair.Source == nil, so the orchestrator falls through to
//     Rebuild.  The spy hook must fire exactly once.
//
//  3. Equivalence oracle: R1 (snapshot path) == R2 (rebuild path), proving that
//     the snapshot restore produces a correct root, not just a fast one.
func TestStagedColdRestoreUsesSnapshotNotRebuild(t *testing.T) {
	const txNum = uint64(20)

	owner := common.Address{0x41, 0x70}
	key := []byte("slot/cold-restore")

	// ---- helpers -----------------------------------------------------------

	// seedDB seeds a DB with a known latest-domain row and returns the commit-
	// ment update that represents it.
	seedDB := func(t *testing.T, db CommitmentDB, value string) rawdb.StateCommitmentUpdate {
		t.Helper()
		if err := rawdb.WriteStateKVGeneration(db, owner, 0); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateKVLatest(db, owner, 0, kvdomains.ContractStorage, key, []byte(value)); err != nil {
			t.Fatal(err)
		}
		commitKey := rawdb.StateKVLatestCommitmentKey(owner, 0, kvdomains.ContractStorage, key)
		commitVal := rawdb.EncodeStateKVLatestValue([]byte(value))
		return rawdb.NewStateCommitmentPut(commitKey, commitVal)
	}

	// buildAndPruneStore seeds a DB with "v1", builds branch state via Rebuild,
	// takes a full snapshot (root + branches, both in manifest), prunes hot branch
	// rows, and returns the store + update-for-v2 ready for the orchestrator run.
	buildAndPruneStore := func(t *testing.T, snapshotDir string) (*stagedCommitmentStore, rawdb.StateCommitmentUpdate, *snapshots.Manager) {
		t.Helper()
		db := rawdb.NewMemoryDatabase()
		seedDB(t, db, "v1")

		store := newStagedCommitmentStore(db)
		if _, err := store.Rebuild(); err != nil {
			t.Fatalf("initial Rebuild: %v", err)
		}

		// Take the snapshot BEFORE pruning: captures the v1 branch rows.
		mgr := buildManagerWithBranchSnapshot(t, db, snapshotDir, txNum)

		// Confirm the snapshot was built with branch rows.
		var branchCount int
		if err := mgr.IterateCommitmentBranches(txNum, func(_, _ []byte) (bool, error) {
			branchCount++
			return true, nil
		}); err != nil {
			t.Fatalf("IterateCommitmentBranches: %v", err)
		}
		if branchCount == 0 {
			t.Fatal("precondition: snapshot contains no branch rows")
		}

		// Prune hot branch rows. Root row must survive.
		deleteStagedBranchRows(t, db)
		if _, ok, err := store.store.GetBranch(nil); err != nil || ok {
			t.Fatalf("precondition: root branch still present after prune (ok=%v err=%v)", ok, err)
		}

		// Advance the source row to v2 so the update slice is non-empty.
		update := seedDB(t, db, "v2")
		return store, update, mgr
	}

	// ---- install spy hook --------------------------------------------------
	var calls int
	rebuildSpyHook = func() { calls++ }
	t.Cleanup(func() { rebuildSpyHook = nil })

	// ======================================================================
	// Part 1: snapshot path — bare *Manager as repair source.
	// ======================================================================
	dir1 := t.TempDir()
	store1, update1, mgr1 := buildAndPruneStore(t, dir1)

	calls = 0
	r1, err := applyLatestCommitmentWithRepair(store1, []rawdb.StateCommitmentUpdate{update1},
		CommitmentSnapshotRepair{Source: mgr1, TxNum: txNum})
	if err != nil {
		t.Fatalf("snapshot path: applyLatestCommitmentWithRepair: %v", err)
	}
	if calls != 0 {
		t.Fatalf("snapshot path: Rebuild fired %d time(s), want 0 (restore must use snapshot, not full scan)", calls)
	}
	if r1 == (common.Hash{}) {
		t.Fatal("snapshot path: root is zero")
	}
	// Hot branch keyspace must be repopulated after restore.
	var branchRows int
	if err := rawdb.IterateCommitmentBranches(store1.db, func(_, _ []byte) (bool, error) {
		branchRows++
		return true, nil
	}); err != nil {
		t.Fatalf("snapshot path: iterate branches: %v", err)
	}
	if branchRows == 0 {
		t.Fatal("snapshot path: branch keyspace empty after restore — restore did not repopulate")
	}

	// ======================================================================
	// Part 2: negative control — nil repair source forces Rebuild.
	// ======================================================================
	dir2 := t.TempDir()
	store2, update2, _ := buildAndPruneStore(t, dir2)

	calls = 0
	r2, err := applyLatestCommitmentWithRepair(store2, []rawdb.StateCommitmentUpdate{update2},
		CommitmentSnapshotRepair{Source: nil, TxNum: txNum})
	if err != nil {
		t.Fatalf("negative control: applyLatestCommitmentWithRepair: %v", err)
	}
	if calls != 1 {
		t.Fatalf("negative control: Rebuild fired %d time(s), want exactly 1 (no snapshot → fallback must rebuild)", calls)
	}

	// ======================================================================
	// Part 3: equivalence oracle — both paths must produce the same root.
	// ======================================================================
	if r1 != r2 {
		t.Fatalf("snapshot-restore root %x != rebuild root %x — restore path is incorrect", r1, r2)
	}
}
