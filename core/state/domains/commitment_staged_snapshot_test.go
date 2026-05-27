package domains

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// fakeBranchSnapshotSource is an in-memory CommitmentBranchSnapshotSource for the
// focused staged restore unit tests. It carries the snapshot root and the encoded
// branch rows captured at build time.
type fakeBranchSnapshotSource struct {
	root     common.Hash
	rootOK   bool
	branches []fakeBranchRow
	// nodeErr, when set, is returned from IterateCommitmentNodes to prove the
	// staged restore never touches the legacy node-iteration path.
	nodeErr error
}

type fakeBranchRow struct {
	prefix  []byte
	encoded []byte
}

func (s *fakeBranchSnapshotSource) GetCommitmentRoot(uint64) (common.Hash, bool, error) {
	return s.root, s.rootOK, nil
}

func (s *fakeBranchSnapshotSource) IterateCommitmentNodes([]byte, uint64, func(logicalKey, value []byte) (bool, error)) error {
	return s.nodeErr
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
	src.nodeErr = errTouchedLegacyNodes
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

var errTouchedLegacyNodes = &touchedLegacyNodesError{}

type touchedLegacyNodesError struct{}

func (*touchedLegacyNodesError) Error() string {
	return "staged restore must not iterate legacy commitment nodes"
}
