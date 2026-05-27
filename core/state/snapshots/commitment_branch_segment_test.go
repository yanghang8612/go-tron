package snapshots

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// seedStagedBranchRows writes a representative set of staged branch rows (opaque
// to the snapshot layer, which never decodes BranchData) plus a root row, so the
// branch keyspace + root exist. It mirrors what a real staged Fold persists: a
// nil/root-prefix row alongside deeper hex-nibble-prefix rows of varying length.
// Returns the seeded root.
func seedStagedBranchRows(t *testing.T, db ethdb.KeyValueStore) common.Hash {
	t.Helper()
	rows := []struct {
		prefix  []byte
		encoded []byte
	}{
		{nil, []byte("root-branch-bytes")},
		{[]byte{0x04}, []byte("branch-0x04")},
		{[]byte{0x04, 0x01}, []byte("branch-0x04-0x01")},
		{[]byte{0x0a, 0x0b, 0x0c}, bytes.Repeat([]byte{0xFF}, 40)},
	}
	for _, row := range rows {
		if err := rawdb.WriteCommitmentBranch(db, row.prefix, row.encoded); err != nil {
			t.Fatalf("seed branch %x: %v", row.prefix, err)
		}
	}
	root := common.Hash{0x77, 0x88, 0x99}
	if err := rawdb.WriteLatestDomainCommitmentRoot(db, root); err != nil {
		t.Fatalf("seed root: %v", err)
	}
	return root
}

// TestCommitmentBranchSegmentRoundTrip pins the cold branch-row snapshot format:
// every live state-commitment-branch-v1- row (including the nil/root prefix and
// BranchData-encoded values) survives a build -> open -> iterate cycle byte for
// byte.
func TestCommitmentBranchSegmentRoundTrip(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	seedStagedBranchRows(t, db)

	// Capture the live rows for later comparison.
	want := map[string][]byte{}
	if err := rawdb.IterateCommitmentBranches(db, func(prefix, encoded []byte) (bool, error) {
		want[string(prefix)] = append([]byte(nil), encoded...)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate live branches: %v", err)
	}
	if len(want) == 0 {
		t.Fatalf("no live branch rows to snapshot")
	}

	dir := t.TempDir()
	ref, err := BuildCommitmentBranchSegmentFromDB(db, dir, "commitment/branches-10-10.json", 10, 10)
	if err != nil {
		t.Fatalf("build branch segment: %v", err)
	}
	if ref.Dataset != SegmentDatasetCommitmentBranch {
		t.Fatalf("ref dataset = %q, want %q", ref.Dataset, SegmentDatasetCommitmentBranch)
	}
	if ref.Size == 0 || ref.Checksum == "" {
		t.Fatalf("ref missing size/checksum: %+v", ref)
	}

	got := map[string][]byte{}
	seg, err := OpenCommitmentBranchSegment(dir, ref)
	if err != nil {
		t.Fatalf("open branch segment: %v", err)
	}
	if err := seg.Iterate(func(prefix, encoded []byte) (bool, error) {
		got[string(prefix)] = append([]byte(nil), encoded...)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate branch segment: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("round-trip row count = %d, want %d", len(got), len(want))
	}
	for k, wv := range want {
		gv, ok := got[k]
		if !ok {
			t.Fatalf("round-trip missing prefix %x", []byte(k))
		}
		if !bytes.Equal(gv, wv) {
			t.Fatalf("round-trip prefix %x value = %x, want %x", []byte(k), gv, wv)
		}
	}
}

// TestCommitmentBranchSourceComposes proves CommitmentBranchSource serves the
// snapshot root (delegated to the embedded Manager) and the branch rows from
// disk, and that txNum outside the segment range yields zero branch rows.
func TestCommitmentBranchSourceComposes(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	root := seedStagedBranchRows(t, db)

	dir := t.TempDir()
	rootRef, rootAccessorRef, rootBTreeRef, err := BuildCommitmentRootSegmentFilesFromDB(db, dir, 10, 10, "commitment/root-10-10.seg")
	if err != nil {
		t.Fatalf("build commitment root snapshot: %v", err)
	}
	if err := PublishManifest(dir, NewManifest(10, 10, []SegmentRef{rootRef, rootAccessorRef, rootBTreeRef})); err != nil {
		t.Fatalf("publish root manifest: %v", err)
	}
	branchRef, err := BuildCommitmentBranchSegmentFromDB(db, dir, "commitment/branches-10-10.json", 10, 10)
	if err != nil {
		t.Fatalf("build branch segment: %v", err)
	}

	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	src := NewCommitmentBranchSource(mgr, dir, branchRef)

	gotRoot, ok, err := src.GetCommitmentRoot(10)
	if err != nil || !ok {
		t.Fatalf("GetCommitmentRoot = ok %v err %v", ok, err)
	}
	if gotRoot != root {
		t.Fatalf("GetCommitmentRoot = %x, want %x", gotRoot, root)
	}

	rows := 0
	if err := src.IterateCommitmentBranches(10, func(_, _ []byte) (bool, error) {
		rows++
		return true, nil
	}); err != nil {
		t.Fatalf("IterateCommitmentBranches(in-range): %v", err)
	}
	if rows == 0 {
		t.Fatalf("in-range branch iteration produced no rows")
	}

	// Out-of-range txNum yields zero branch rows.
	outRows := 0
	if err := src.IterateCommitmentBranches(9, func(_, _ []byte) (bool, error) {
		outRows++
		return true, nil
	}); err != nil {
		t.Fatalf("IterateCommitmentBranches(out-of-range): %v", err)
	}
	if outRows != 0 {
		t.Fatalf("out-of-range branch iteration produced %d rows, want 0", outRows)
	}
}
