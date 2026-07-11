package domains

import (
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// fakeBranchSnapshotSource is an in-memory CommitmentBranchSnapshotSource for the
// focused staged restore unit tests. It carries the snapshot root and the encoded
// branch rows captured at build time.
type fakeBranchSnapshotSource struct {
	root     common.Hash
	rootOK   bool
	branches []fakeBranchRow
}

type fakeBranchRow struct {
	prefix  []byte
	encoded []byte
}

func (s *fakeBranchSnapshotSource) GetCommitmentRoot(uint64) (common.Hash, bool, error) {
	return s.root, s.rootOK, nil
}

func (s *fakeBranchSnapshotSource) IterateCommitmentBranches(_ uint64, fn func(prefix, encoded []byte) (bool, error)) error {
	for _, row := range s.branches {
		cont, err := fn(append([]byte(nil), row.prefix...), append([]byte(nil), row.encoded...))
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return nil
}

// captureBranchSnapshot reads the live branch rows + root from db into a
// fakeBranchSnapshotSource, modelling what a cold snapshot would persist.
func captureBranchSnapshot(t *testing.T, db CommitmentDB) *fakeBranchSnapshotSource {
	t.Helper()
	root, ok, err := rawdb.ReadLatestDomainCommitmentRoot(db)
	if err != nil {
		t.Fatalf("read root: %v", err)
	}
	src := &fakeBranchSnapshotSource{root: root, rootOK: ok}
	if err := rawdb.IterateCommitmentBranches(db, func(prefix, encoded []byte) (bool, error) {
		src.branches = append(src.branches, fakeBranchRow{
			prefix:  append([]byte(nil), prefix...),
			encoded: append([]byte(nil), encoded...),
		})
		return true, nil
	}); err != nil {
		t.Fatalf("iterate branches: %v", err)
	}
	return src
}

func deleteStagedBranchRows(t *testing.T, db CommitmentDB) {
	t.Helper()
	var prefixes [][]byte
	if err := rawdb.IterateCommitmentBranches(db, func(prefix, _ []byte) (bool, error) {
		prefixes = append(prefixes, append([]byte(nil), prefix...))
		return true, nil
	}); err != nil {
		t.Fatalf("iterate branches: %v", err)
	}
	for _, prefix := range prefixes {
		if err := rawdb.DeleteCommitmentBranch(db, prefix); err != nil {
			t.Fatalf("delete branch %x: %v", prefix, err)
		}
	}
}

type batchCountingCommitmentDB struct {
	ethdb.KeyValueStore
	batches int
	writes  int
}

func (db *batchCountingCommitmentDB) NewBatch() ethdb.Batch {
	db.batches++
	return &batchCountingCommitmentBatch{Batch: db.KeyValueStore.NewBatch(), parent: db}
}

type batchCountingCommitmentBatch struct {
	ethdb.Batch
	parent *batchCountingCommitmentDB
}

func (b *batchCountingCommitmentBatch) Write() error {
	b.parent.writes++
	return b.Batch.Write()
}

// TestStagedRestoreNodesFromSnapshotRederivesRoot is the focused unit: a staged
// store that has committed branch state, had its hot branch rows captured into a
// snapshot source, then deleted, must re-derive the original root from the
// snapshot's branch rows alone — without a bootstrap Rebuild scan.
func TestStagedRestoreNodesFromSnapshotRederivesRoot(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	updates := seedLatestDomainRows(t, db)
	store := newStagedCommitmentStore(db)
	originalRoot, err := store.Update(updates)
	if err != nil {
		t.Fatalf("staged Update: %v", err)
	}
	if originalRoot == (common.Hash{}) {
		t.Fatalf("staged Update produced zero root")
	}

	// Capture the live branch rows + root into a snapshot source, then prune the
	// hot branch rows so only the snapshot can re-derive the root.
	src := captureBranchSnapshot(t, db)
	deleteStagedBranchRows(t, db)
	if _, ok, err := store.store.GetBranch(nil); err != nil || ok {
		t.Fatalf("precondition: root branch still present after prune (ok=%v err=%v)", ok, err)
	}

	bootstrapBefore := store.bootstrapCount
	ok, err := store.RestoreNodesFromSnapshot(src, 42, originalRoot)
	if err != nil {
		t.Fatalf("RestoreNodesFromSnapshot: %v", err)
	}
	if !ok {
		t.Fatalf("RestoreNodesFromSnapshot returned false, want true")
	}
	if store.bootstrapCount != bootstrapBefore {
		t.Fatalf("RestoreNodesFromSnapshot ran bootstrap (count %d -> %d); restore must not scan latest-domain rows",
			bootstrapBefore, store.bootstrapCount)
	}

	rederived, err := store.trie.Fold(nil)
	if err != nil {
		t.Fatalf("Fold after restore: %v", err)
	}
	if rederived != originalRoot {
		t.Fatalf("Fold(nil) after restore = %x, want original %x", rederived, originalRoot)
	}
}

func TestStagedRestoreNodesFromSnapshotBatchesBranchWrites(t *testing.T) {
	const rows = 4096
	db := &batchCountingCommitmentDB{KeyValueStore: rawdb.NewMemoryDatabase()}
	owner := common.Address{0x41, 0x58}
	if err := rawdb.WriteStateKVGeneration(db, owner, 0); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		key := []byte(fmt.Sprintf("slot-%04d", i))
		if err := rawdb.WriteStateKVLatest(db, owner, 0, kvdomains.ContractStorage, key, []byte("snapshot-restore-value")); err != nil {
			t.Fatalf("write latest row %d: %v", i, err)
		}
	}
	store := newStagedCommitmentStore(db)
	root, err := store.Rebuild()
	if err != nil {
		t.Fatalf("rebuild branch state: %v", err)
	}
	src := captureBranchSnapshot(t, db)
	if len(src.branches) < 2 {
		t.Fatalf("captured branches = %d, want multiple rows", len(src.branches))
	}
	deleteStagedBranchRows(t, db)
	db.batches = 0
	db.writes = 0

	ok, err := store.RestoreNodesFromSnapshot(src, 42, root)
	if err != nil {
		t.Fatalf("restore branch snapshot: %v", err)
	}
	if !ok {
		t.Fatal("restore branch snapshot returned false")
	}
	if db.batches != 1 {
		t.Fatalf("restore batches = %d, want 1 reusable batch", db.batches)
	}
	if db.writes < 2 {
		t.Fatalf("restore batch writes = %d, want multiple writes for a large branch snapshot", db.writes)
	}
	if rederived, err := store.trie.Fold(nil); err != nil || rederived != root {
		t.Fatalf("restored branch root %x err=%v, want %x", rederived, err, root)
	}
}

// TestStagedRestoreNodesFromSnapshotRejectsRootMismatch pins the self-verifying
// contract: a snapshot whose root does not match expectedRoot is ignored and no
// branch rows are written.
func TestStagedRestoreNodesFromSnapshotRejectsRootMismatch(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	updates := seedLatestDomainRows(t, db)
	store := newStagedCommitmentStore(db)
	originalRoot, err := store.Update(updates)
	if err != nil {
		t.Fatalf("staged Update: %v", err)
	}
	src := captureBranchSnapshot(t, db)
	deleteStagedBranchRows(t, db)

	wrongRoot := common.Hash{0xDE, 0xAD}
	if wrongRoot == originalRoot {
		t.Fatalf("test setup: wrong root collided with original")
	}
	ok, err := store.RestoreNodesFromSnapshot(src, 42, wrongRoot)
	if err != nil {
		t.Fatalf("RestoreNodesFromSnapshot: %v", err)
	}
	if ok {
		t.Fatalf("RestoreNodesFromSnapshot accepted a root-mismatched snapshot")
	}
	// No branch rows must have been written back.
	var rows int
	if err := rawdb.IterateCommitmentBranches(db, func(_, _ []byte) (bool, error) {
		rows++
		return true, nil
	}); err != nil {
		t.Fatalf("iterate branches: %v", err)
	}
	if rows != 0 {
		t.Fatalf("root-mismatched restore wrote %d branch rows, want 0", rows)
	}
}

// TestStagedRestoreNodesFromSnapshotNonBranchSourceFalls proves a plain
// CommitmentSnapshotSource (no branch iteration) is gracefully declined, so the
// orchestrator falls through to Rebuild.
func TestStagedRestoreNodesFromSnapshotNonBranchSourceFalls(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	store := newStagedCommitmentStore(db)
	ok, err := store.RestoreNodesFromSnapshot(noopCommitmentSnapshotSource{}, 1, common.Hash{0x01})
	if err != nil {
		t.Fatalf("RestoreNodesFromSnapshot: %v", err)
	}
	if ok {
		t.Fatalf("plain (non-branch) source returned true, want false")
	}
}

// TestStagedRestoreNodesFromSnapshotNilSource and zero root are declined.
func TestStagedRestoreNodesFromSnapshotNilOrZero(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	store := newStagedCommitmentStore(db)
	if ok, err := store.RestoreNodesFromSnapshot(nil, 1, common.Hash{0x01}); err != nil || ok {
		t.Fatalf("nil source = ok %v err %v, want false nil", ok, err)
	}
	src := &fakeBranchSnapshotSource{}
	if ok, err := store.RestoreNodesFromSnapshot(src, 1, common.Hash{}); err != nil || ok {
		t.Fatalf("zero expected root = ok %v err %v, want false nil", ok, err)
	}
}
