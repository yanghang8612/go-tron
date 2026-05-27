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

// TestCommitmentBranchRidesLatestBuild verifies that a seeded branch keyspace
// causes Aggregator.BuildLatest to produce a SegmentDatasetCommitmentBranch ref,
// that the manifest validates cleanly, and that the segment round-trips all rows.
func TestCommitmentBranchRidesLatestBuild(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	seedStagedBranchRows(t, db)

	// Capture the seeded rows for later comparison.
	want := map[string][]byte{}
	if err := rawdb.IterateCommitmentBranches(db, func(prefix, encoded []byte) (bool, error) {
		want[string(prefix)] = append([]byte(nil), encoded...)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate live branches: %v", err)
	}
	if len(want) == 0 {
		t.Fatalf("no seeded branch rows found")
	}

	dir := t.TempDir()
	agg := NewAggregator(dir)
	if _, err := agg.BuildLatest(db, AggregatorBuildOptions{FromTxNum: 1, ToTxNum: 100}); err != nil {
		t.Fatalf("BuildLatest: %v", err)
	}

	manifest, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("manifest.Validate(): %v", err)
	}

	// Find the branch ref in the manifest.
	var branchRef SegmentRef
	for _, ref := range manifest.Segments {
		if ref.Dataset == SegmentDatasetCommitmentBranch && ref.Kind == SegmentLatest {
			branchRef = ref
			break
		}
	}
	if branchRef.Path == "" {
		t.Fatalf("no CommitmentBranch ref found in manifest segments: %v", manifest.Segments)
	}
	if branchRef.FromTxNum != 1 || branchRef.ToTxNum != 100 {
		t.Fatalf("branch ref tx range = [%d,%d], want [1,100]", branchRef.FromTxNum, branchRef.ToTxNum)
	}

	// Open the segment and verify rows round-trip.
	seg, err := OpenCommitmentBranchSegment(dir, branchRef)
	if err != nil {
		t.Fatalf("OpenCommitmentBranchSegment: %v", err)
	}
	got := map[string][]byte{}
	if err := seg.Iterate(func(prefix, encoded []byte) (bool, error) {
		got[string(prefix)] = append([]byte(nil), encoded...)
		return true, nil
	}); err != nil {
		t.Fatalf("Iterate: %v", err)
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

// TestCommitmentBranchEmptyKeyspaceSkipped verifies that an empty branch
// keyspace causes BuildLatest to publish no SegmentDatasetCommitmentBranch ref.
// Other datasets (CommitmentRoot, etc.) may still produce refs; this test only
// asserts no branch ref appears.
func TestCommitmentBranchEmptyKeyspaceSkipped(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	// Seed only a commitment root so CommitmentRoot builder succeeds; leave the
	// branch keyspace empty.
	if err := rawdb.WriteLatestDomainCommitmentRoot(db, common.Hash{0x42}); err != nil {
		t.Fatalf("seed commitment root: %v", err)
	}

	dir := t.TempDir()
	agg := NewAggregator(dir)
	if _, err := agg.BuildLatest(db, AggregatorBuildOptions{FromTxNum: 1, ToTxNum: 100}); err != nil {
		t.Fatalf("BuildLatest: %v", err)
	}

	manifest, err := LoadManifest(dir)
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("manifest.Validate(): %v", err)
	}

	for _, ref := range manifest.Segments {
		if ref.Dataset == SegmentDatasetCommitmentBranch {
			t.Fatalf("expected no CommitmentBranch ref, found: %+v", ref)
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

// TestManagerIterateCommitmentBranchesDynamic proves that *Manager.IterateCommitmentBranches
// resolves the covering branch segment from the on-disk manifest ON EVERY CALL,
// not once at startup. A SINGLE mgr instance must see a newly-published [1,150]
// ref immediately after it is written, without re-opening the manager.
//
// The discriminating assertion is step-4: txNum=101 is outside [1,100] (yields
// nothing before the second build) but inside [1,150] (yields rows after the
// second build). This assertion would FAIL if IterateCommitmentBranches cached
// the covering ref at first call — the cached [1,100] ref would still exclude 101.
func TestManagerIterateCommitmentBranchesDynamic(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	seedStagedBranchRows(t, db)

	dir := t.TempDir()
	agg := NewAggregator(dir)

	// --- step 1: build [1,100] ---
	if _, err := agg.BuildLatest(db, AggregatorBuildOptions{FromTxNum: 1, ToTxNum: 100}); err != nil {
		t.Fatalf("BuildLatest [1,100]: %v", err)
	}

	// Open the manager ONCE; keep this single instance for all assertions below.
	mgr, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}

	// --- step 2: txNum=100 is in [1,100] → rows expected ---
	rows100 := 0
	if err := mgr.IterateCommitmentBranches(100, func(_, _ []byte) (bool, error) {
		rows100++
		return true, nil
	}); err != nil {
		t.Fatalf("IterateCommitmentBranches(100) after [1,100] build: %v", err)
	}
	if rows100 == 0 {
		t.Fatalf("IterateCommitmentBranches(100): want rows, got 0 — branch segment not served")
	}

	// txNum=101 is outside [1,100] → no rows expected yet.
	rows101before := 0
	if err := mgr.IterateCommitmentBranches(101, func(_, _ []byte) (bool, error) {
		rows101before++
		return true, nil
	}); err != nil {
		t.Fatalf("IterateCommitmentBranches(101) before [1,150] build: %v", err)
	}
	if rows101before != 0 {
		t.Fatalf("IterateCommitmentBranches(101) before re-build: want 0 rows, got %d", rows101before)
	}

	// --- step 3: publish a wider [1,150] ref — the SAME mgr must see it immediately ---
	if _, err := agg.BuildLatest(db, AggregatorBuildOptions{FromTxNum: 1, ToTxNum: 150}); err != nil {
		t.Fatalf("BuildLatest [1,150]: %v", err)
	}

	// --- step 4 (discriminating): txNum=101 is now covered by [1,150] ---
	// This assertion fails if IterateCommitmentBranches cached the [1,100] ref.
	rows101after := 0
	if err := mgr.IterateCommitmentBranches(101, func(_, _ []byte) (bool, error) {
		rows101after++
		return true, nil
	}); err != nil {
		t.Fatalf("IterateCommitmentBranches(101) after [1,150] build: %v", err)
	}
	if rows101after == 0 {
		t.Fatalf("IterateCommitmentBranches(101) after [1,150] build: want rows, got 0 — Manager did not re-read manifest (caching bug)")
	}
}
