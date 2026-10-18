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
	if _, err := statedomains.RebuildMuHashCommitment(db); err != nil {
		t.Fatalf("seed commitment state: %v", err)
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
