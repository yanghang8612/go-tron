package domains

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// TestRawdbBranchStoreRoundTrip pins the rawdb-backed branchStore adapter:
// PutBranch persists an encoded BranchData row, GetBranch decodes it back, and
// DelBranch removes it. Absent prefixes report (_, false, nil).
func TestRawdbBranchStoreRoundTrip(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	store := newRawdbBranchStore(db)

	if _, ok, err := store.GetBranch([]byte{0x01, 0x02}); err != nil || ok {
		t.Fatalf("GetBranch(absent) = ok %v err %v, want false nil", ok, err)
	}

	var b BranchData
	b.SetHashChild(0x3, common.Hash{0xAB})
	b.SetLeafChild(0xc, []byte("leafkey"), common.Hash{0x12, 0x34})

	prefix := []byte{0x0a, 0x0b}
	if err := store.PutBranch(prefix, b); err != nil {
		t.Fatalf("PutBranch: %v", err)
	}

	got, ok, err := store.GetBranch(prefix)
	if err != nil || !ok {
		t.Fatalf("GetBranch(present) = ok %v err %v, want true nil", ok, err)
	}
	if !b.Equal(got) {
		t.Fatalf("round-tripped branch != original")
	}

	// Root prefix (nil) round-trips too.
	var root BranchData
	root.SetHashChild(0x0, common.Hash{0x55})
	if err := store.PutBranch(nil, root); err != nil {
		t.Fatalf("PutBranch(nil): %v", err)
	}
	gotRoot, ok, err := store.GetBranch(nil)
	if err != nil || !ok {
		t.Fatalf("GetBranch(nil) = ok %v err %v, want true nil", ok, err)
	}
	if !root.Equal(gotRoot) {
		t.Fatalf("root branch round-trip mismatch")
	}

	if err := store.DelBranch(prefix); err != nil {
		t.Fatalf("DelBranch: %v", err)
	}
	if _, ok, err := store.GetBranch(prefix); err != nil || ok {
		t.Fatalf("GetBranch(after del) = ok %v err %v, want false nil", ok, err)
	}
}

// seedLatestDomainRows writes a representative set of account / KV-generation /
// KV-latest flat rows and returns the matching commitment updates that an
// incremental Update would carry for the same rows.
func seedLatestDomainRows(t *testing.T, db CommitmentDB) []rawdb.StateCommitmentUpdate {
	t.Helper()
	ownerA := common.Address{0x41, 0x01}
	ownerB := common.Address{0x41, 0x02}

	if err := rawdb.WriteStateAccountLatest(db, ownerA, []byte("acctA")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateAccountLatest(db, ownerB, []byte("acctB")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVGeneration(db, ownerA, 7); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVGeneration(db, ownerB, 0); err != nil {
		t.Fatal(err)
	}
	keyA := []byte("slotA")
	keyB := []byte("slotB")
	if err := rawdb.WriteStateKVLatest(db, ownerA, 7, kvdomains.ContractStorage, keyA, []byte("vA")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(db, ownerB, 0, kvdomains.ContractStorage, keyB, []byte("vB")); err != nil {
		t.Fatal(err)
	}

	return []rawdb.StateCommitmentUpdate{
		rawdb.NewStateCommitmentPut(rawdb.StateAccountLatestCommitmentKey(ownerA), []byte("acctA")),
		rawdb.NewStateCommitmentPut(rawdb.StateAccountLatestCommitmentKey(ownerB), []byte("acctB")),
		rawdb.NewStateCommitmentPut(rawdb.StateKVGenerationCommitmentKey(ownerA), rawdb.EncodeStateKVGenerationValue(7)),
		rawdb.NewStateCommitmentPut(rawdb.StateKVGenerationCommitmentKey(ownerB), rawdb.EncodeStateKVGenerationValue(0)),
		rawdb.NewStateCommitmentPut(rawdb.StateKVLatestCommitmentKey(ownerA, 7, kvdomains.ContractStorage, keyA), rawdb.EncodeStateKVLatestValue([]byte("vA"))),
		rawdb.NewStateCommitmentPut(rawdb.StateKVLatestCommitmentKey(ownerB, 0, kvdomains.ContractStorage, keyB), rawdb.EncodeStateKVLatestValue([]byte("vB"))),
	}
}

// TestStagedCommitmentUpdateMatchesRebuild is C4.1: applying the latest-domain
// updates through the staged store's incremental Update yields the same root as
// a fresh staged Rebuild() over the identical latest-domain rows. Both are the
// hex (staged) engine — this is a within-engine equivalence, not a comparison
// against the legacy bit tree.
func TestStagedCommitmentUpdateMatchesRebuild(t *testing.T) {
	// Update path: a fresh DB whose only commitment state is built by Update.
	updateDB := rawdb.NewMemoryDatabase()
	updates := seedLatestDomainRows(t, updateDB)
	updateStore := NewStagedCommitmentStore(updateDB)
	updateRoot, err := updateStore.Update(updates)
	if err != nil {
		t.Fatalf("staged Update: %v", err)
	}
	if updateRoot == (common.Hash{}) {
		t.Fatalf("staged Update produced zero root")
	}

	// Rebuild path: a separate DB with the same flat rows, root via Rebuild().
	rebuildDB := rawdb.NewMemoryDatabase()
	seedLatestDomainRows(t, rebuildDB)
	rebuildStore := NewStagedCommitmentStore(rebuildDB)
	rebuildRoot, err := rebuildStore.Rebuild()
	if err != nil {
		t.Fatalf("staged Rebuild: %v", err)
	}

	if updateRoot != rebuildRoot {
		t.Fatalf("staged Update root %x != staged Rebuild root %x", updateRoot, rebuildRoot)
	}

	// Both stores must persist the same root row.
	if stored, ok, err := rawdb.ReadLatestDomainCommitmentRoot(updateDB); err != nil || !ok || stored != updateRoot {
		t.Fatalf("Update store root row = %x ok=%v err=%v, want %x", stored, ok, err, updateRoot)
	}
	if stored, ok, err := rawdb.ReadLatestDomainCommitmentRoot(rebuildDB); err != nil || !ok || stored != rebuildRoot {
		t.Fatalf("Rebuild store root row = %x ok=%v err=%v, want %x", stored, ok, err, rebuildRoot)
	}
}

// TestStagedCommitmentRebuildClearsStaleBranches pins the rewind/fork-switch
// fallback contract: Rebuild must reflect EXACTLY the current latest-domain
// source rows, independent of any branch state left over from an earlier (taller)
// tip. The legacy engine's RebuildLatestDomainCommitment clears its "tree/node/"
// rows before re-folding; the staged engine must likewise clear its
// state-commitment-branch-v1- rows, or an orphaned branch contribution survives
// into the rebuilt root.
//
// Scenario: build branches for {A, B}, then make the latest-domain rows reflect
// only {A} (B's account was deleted), then Rebuild() on the SAME store. The
// rebuilt root must equal a from-scratch staged build over only {A}.
func TestStagedCommitmentRebuildClearsStaleBranches(t *testing.T) {
	ownerA := common.Address{0x41, 0x01}
	ownerB := common.Address{0x41, 0x02}
	valA := []byte("acctA")
	valB := []byte("acctB")

	// Reference: a fresh staged store whose only source row is A. This is the
	// root Rebuild() must reproduce after B is rewound out.
	refDB := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStateAccountLatest(refDB, ownerA, valA); err != nil {
		t.Fatal(err)
	}
	refRoot, err := newStagedCommitmentStore(refDB).Rebuild()
	if err != nil {
		t.Fatalf("reference Rebuild: %v", err)
	}
	if refRoot == (common.Hash{}) {
		t.Fatalf("reference Rebuild produced zero root")
	}

	// Subject: a store that first holds branches for {A, B}, then has B's source
	// row removed, then is rebuilt.
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStateAccountLatest(db, ownerA, valA); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateAccountLatest(db, ownerB, valB); err != nil {
		t.Fatal(err)
	}
	store := newStagedCommitmentStore(db)
	tallRoot, err := store.Rebuild()
	if err != nil {
		t.Fatalf("initial Rebuild over {A,B}: %v", err)
	}
	if tallRoot == refRoot {
		t.Fatalf("test precondition broken: {A,B} root must differ from {A}-only root")
	}

	// Rewind: B's account is gone from the latest-domain source rows. Its branch
	// rows, however, are still persisted from the {A,B} Rebuild above.
	if err := rawdb.DeleteStateAccountLatest(db, ownerB); err != nil {
		t.Fatalf("delete B source row: %v", err)
	}

	rebuiltRoot, err := store.Rebuild()
	if err != nil {
		t.Fatalf("Rebuild after rewind: %v", err)
	}
	if rebuiltRoot != refRoot {
		t.Fatalf("Rebuild after rewind = %x, want from-scratch {A}-only root %x "+
			"(stale B branch survived)", rebuiltRoot, refRoot)
	}
	if stored, ok, err := rawdb.ReadLatestDomainCommitmentRoot(db); err != nil || !ok || stored != refRoot {
		t.Fatalf("rebuilt root row = %x ok=%v err=%v, want %x", stored, ok, err, refRoot)
	}
}

// TestStagedCommitmentNoBootstrapOnNormalCommit is C4.4 (HEADLINE): the staged
// engine commits incrementally off persisted branch state. The orchestrator may
// bootstrap (full latest-domain scan) once on the first commit when no branch
// state exists, but a second normal commit with a few mutations must reuse the
// persisted branch rows and never re-run the bootstrap scan.
func TestStagedCommitmentNoBootstrapOnNormalCommit(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x10}

	// Seed initial flat rows so the first commit has latest-domain state.
	if err := rawdb.WriteStateKVGeneration(db, owner, 0); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner, 0, kvdomains.ContractStorage, []byte("k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}

	store := newStagedCommitmentStore(db)

	// Step (a): first commit. No branch state yet, so the orchestrator restores
	// (finds nothing), then bootstraps once via Rebuild, then applies the update.
	firstUpdate := []rawdb.StateCommitmentUpdate{
		rawdb.NewStateCommitmentPut(
			rawdb.StateKVLatestCommitmentKey(owner, 0, kvdomains.ContractStorage, []byte("k1")),
			rawdb.EncodeStateKVLatestValue([]byte("v1")),
		),
	}
	if _, err := ApplyLatestCommitmentWithStore(store, firstUpdate); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	if store.bootstrapCount > 1 {
		t.Fatalf("first commit ran bootstrap %d times, want <=1", store.bootstrapCount)
	}
	if store.bootstrapCount == 0 {
		t.Fatalf("first commit did not populate branch state via bootstrap")
	}
	bootstrapAfterFirst := store.bootstrapCount

	// Step (b): a SECOND normal commit with a few mutations. Branch state is now
	// persisted and the root row matches, so the orchestrator must apply the
	// update incrementally without a bootstrap scan.
	if err := rawdb.WriteStateKVLatest(db, owner, 0, kvdomains.ContractStorage, []byte("k2"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	secondUpdate := []rawdb.StateCommitmentUpdate{
		rawdb.NewStateCommitmentPut(
			rawdb.StateKVLatestCommitmentKey(owner, 0, kvdomains.ContractStorage, []byte("k2")),
			rawdb.EncodeStateKVLatestValue([]byte("v2")),
		),
	}
	if _, err := ApplyLatestCommitmentWithStore(store, secondUpdate); err != nil {
		t.Fatalf("second commit: %v", err)
	}
	if store.bootstrapCount != bootstrapAfterFirst {
		t.Fatalf("second commit ran bootstrap (count %d -> %d); staged engine must commit incrementally off persisted branch state",
			bootstrapAfterFirst, store.bootstrapCount)
	}
}
