package pruning

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statedomains "github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// seedCheckerCommitmentState writes the minimum hot state the checker requires:
//   - a commitment domain root (via StagedCommitmentStore.Rebuild), so the
//     "flat latest rows present without latest CommitmentDomain root" invariant
//     is satisfied even when hot LatestRows == 0.
//
// It does NOT write any flat latest rows; callers add those as needed.
func seedCheckerCommitmentState(t *testing.T, db ethdb.KeyValueStore) {
	t.Helper()
	if _, err := statedomains.NewStagedCommitmentStore(db).Rebuild(); err != nil {
		t.Fatalf("seed commitment state: %v", err)
	}
}

// TestCheckerCountsCommitmentBranchSnapshot verifies that when a valid branch
// snapshot exists in the manifest with ToTxNum == Progress.CommitmentFlushTxNum,
// Check() reports CommitmentBranchSnapshotRows > 0 and NO staleness warning.
func TestCheckerCountsCommitmentBranchSnapshot(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()

	// Rebuild the commitment trie first (clears the branch keyspace), then seed
	// branch rows so BuildLatest finds them. Rebuild is required to write the
	// CommitmentDomain root row that Check() insists on.
	seedCheckerCommitmentState(t, db)
	seedBranchRows(t, db)

	// Build branch snapshot + integrate into manifest via Aggregator.BuildLatest.
	// Since CommitmentBranch.TracksCommitmentFlush=true, the produced manifest
	// will have Progress.CommitmentFlushTxNum == ToTxNum == 100, so no staleness.
	agg := snapshots.NewAggregator(dir)
	if _, err := agg.BuildLatest(db, snapshots.AggregatorBuildOptions{FromTxNum: 1, ToTxNum: 100}); err != nil {
		t.Fatalf("BuildLatest: %v", err)
	}

	report, err := Check(db, ArchivePolicy(), 1, dir)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if report.CommitmentBranchSnapshotRows == 0 {
		t.Fatalf("CommitmentBranchSnapshotRows = 0, want > 0")
	}
	for _, w := range report.Warnings {
		if strings.Contains(w, "commitment branch snapshot") {
			t.Fatalf("unexpected staleness warning: %q", w)
		}
	}
}

// TestCheckerWarnsStaleCommitmentBranchSnapshot verifies that when the branch
// snapshot's ToTxNum is less than Progress.CommitmentFlushTxNum, Check()
// appends a staleness warning but does NOT return an error.
func TestCheckerWarnsStaleCommitmentBranchSnapshot(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()

	// Rebuild first (writes CommitmentDomain root), then seed branch rows.
	seedCheckerCommitmentState(t, db)
	seedBranchRows(t, db)

	// Build the branch segment manually so we control its ToTxNum.
	branchRef, err := snapshots.BuildCommitmentBranchSegmentFromDB(db, dir, "commitment/branch-1-50.json", 1, 50)
	if err != nil {
		t.Fatalf("BuildCommitmentBranchSegmentFromDB: %v", err)
	}

	// Craft a manifest whose branch ref ToTxNum (50) < CommitmentFlushTxNum (100).
	manifest := snapshots.NewManifest(1, 100, []snapshots.SegmentRef{branchRef})
	manifest.Progress = &snapshots.Progress{CommitmentFlushTxNum: 100}
	if err := snapshots.PublishManifest(dir, manifest); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}

	report, err := Check(db, ArchivePolicy(), 1, dir)
	if err != nil {
		t.Fatalf("Check returned error (want only a warning): %v", err)
	}
	found := false
	for _, w := range report.Warnings {
		if strings.Contains(w, "commitment branch snapshot") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected staleness warning containing \"commitment branch snapshot\", got warnings: %v", report.Warnings)
	}
}

func TestCheckerRejectsStaleStateDomainHistoryCompanion(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	_, _, _ = writeSnapPruningChange(t, db, 1, 10, 12)

	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 12, "history/state-domain-change-10-12.seg")
	if err != nil {
		t.Fatalf("build state-domain history: %v", err)
	}

	staleDB := rawdb.NewMemoryDatabase()
	staleDir := t.TempDir()
	_, _, _ = writeSnapPruningChange(t, staleDB, 2, 10, 12)
	staleRefs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(staleDB, staleDir, 10, 12, "history/state-domain-change-10-12.seg")
	if err != nil {
		t.Fatalf("build stale state-domain history: %v", err)
	}

	var staleAccessor snapshots.SegmentRef
	for _, ref := range staleRefs {
		if ref.Kind == snapshots.SegmentAccessor {
			staleAccessor = ref
			break
		}
	}
	if staleAccessor.Path == "" {
		t.Fatal("stale fixture did not build an accessor companion")
	}
	data, err := os.ReadFile(filepath.Join(staleDir, staleAccessor.Path))
	if err != nil {
		t.Fatalf("read stale accessor: %v", err)
	}

	mixedRefs := append([]snapshots.SegmentRef(nil), refs...)
	replaced := false
	for i, ref := range mixedRefs {
		if ref.Kind == snapshots.SegmentAccessor {
			if err := os.WriteFile(filepath.Join(dir, ref.Path), data, 0o644); err != nil {
				t.Fatalf("install stale accessor at primary path: %v", err)
			}
			staleAccessor.Path = ref.Path
			mixedRefs[i] = staleAccessor
			replaced = true
			break
		}
	}
	if !replaced {
		t.Fatal("primary fixture did not build an accessor companion")
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(10, 12, mixedRefs)); err != nil {
		t.Fatalf("publish mixed manifest: %v", err)
	}

	_, err = Check(db, ArchivePolicy(), 5, dir)
	if err == nil || !strings.Contains(err.Error(), "accessor") {
		t.Fatalf("Check stale accessor err = %v, want accessor mismatch", err)
	}
}

// seedBranchRows writes a small set of staged branch rows to db. It mirrors
// the snapshots-package helper (seedStagedBranchRows) without importing the
// test package.
func seedBranchRows(t *testing.T, db ethdb.KeyValueStore) {
	t.Helper()
	rows := []struct {
		prefix  []byte
		encoded []byte
	}{
		{nil, []byte("root-branch-bytes")},
		{[]byte{0x04}, []byte("branch-0x04")},
		{[]byte{0x04, 0x01}, []byte("branch-0x04-0x01")},
	}
	for _, row := range rows {
		if err := rawdb.WriteCommitmentBranch(db, row.prefix, row.encoded); err != nil {
			t.Fatalf("seedBranchRows WriteCommitmentBranch %x: %v", row.prefix, err)
		}
	}
}
