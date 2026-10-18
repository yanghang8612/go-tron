package main

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func imageDigests(t *testing.T, path string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]string)
	for _, entry := range entries {
		if entry.Name() == "LOCK" {
			continue
		}
		sha, err := hashFile(filepath.Join(path, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[entry.Name()] = sha
	}
	return out
}

func TestDisposableClonePreservesSourceAndOtherHistory(t *testing.T) {
	root := t.TempDir()
	sourcePath := filepath.Join(root, "source")
	db, err := rawdb.NewPebbleDB(sourcePath, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range []uint64{1, 2, 3, 4} {
		if err := rawdb.WriteStateDomainChangePostingIndex(db, &rawdb.StateDomainChange{BlockNum: h, FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: common.Address{0x41, 1}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Put([]byte("unrelated-history"), []byte("preserve")); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	original := imageDigests(t, sourcePath)
	clonePath := filepath.Join(root, "new-private-clone")
	if err := copyImage(sourcePath, clonePath); err != nil {
		t.Fatal(err)
	}
	if err := copyImage(sourcePath, clonePath); err == nil {
		t.Fatal("reused an existing destination")
	}
	for name := range original {
		a, _ := os.Stat(filepath.Join(sourcePath, name))
		b, _ := os.Stat(filepath.Join(clonePath, name))
		if os.SameFile(a, b) != strings.HasSuffix(name, ".sst") {
			t.Fatalf("incorrect copy method for %s", name)
		}
	}
	db, err = rawdb.NewPebbleDB(clonePath, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	before, err := rawdb.InspectStateChangePostings(context.Background(), db, []uint64{2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	other, err := rawdb.InspectStateChangePostingComplement(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	r, err := rawdb.PruneStaleStateChangePostingChunkContext(context.Background(), db, 2, nil, rawdb.StateChangePostingPruneLimits{MaxScannedRows: 100, MaxScannedBytes: 1 << 20, MaxDeleteBytes: 1 << 20, MaxDuration: time.Second})
	if err != nil || !r.Complete || r.RowsDeleted != 2 || r.BytesDeleted != before.Cutoffs[0].Eligible.LogicalBytes {
		t.Fatalf("prune=%+v err=%v", r, err)
	}
	if err := db.Compact(nil, nil); err != nil {
		t.Fatal(err)
	}
	after, err := rawdb.InspectStateChangePostings(context.Background(), db, []uint64{2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := rawdb.InspectStateChangePostingComplement(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if after.Cutoffs[0].Eligible.Frames != 0 || !reflect.DeepEqual(after.Cutoffs[0].Newer, before.Cutoffs[0].Newer) || other != retained {
		t.Fatal("retained data changed")
	}
	if !reflect.DeepEqual(original, imageDigests(t, sourcePath)) {
		t.Fatal("experiment changed source payload files")
	}
	if _, err := measure(clonePath); err != nil {
		t.Fatal(err)
	}
}

func TestCloneRejectsSymlinksAndNonImages(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/dev/null", filepath.Join(source, "CURRENT")); err != nil {
		t.Fatal(err)
	}
	if err := copyImage(source, filepath.Join(root, "clone")); err == nil {
		t.Fatal("copied symlink")
	}
	if _, err := checkImage(root, capturedImage{Complete: true, Destination: root, Source: "/data/gtron/main/datadir/gtron"}); err == nil {
		t.Fatal("accepted missing source inventory")
	}
}

// Deletion order matters when the experiment disables automatic compaction.
// Sorted chunks must produce non-overlapping L0 sublevels, rather than stall
// permanently after accumulating many small flushes before the final Compact.
func TestSortedExperimentDeletionSurvivesManyFlushes(t *testing.T) {
	path := t.TempDir()
	db, err := rawdb.NewPebbleDB(path, 16, 32)
	if err != nil {
		t.Fatal(err)
	}
	batch := db.NewBatch()
	for i := uint64(0); i < 100000; i++ {
		owner := common.Address{0x41}
		binary.BigEndian.PutUint64(owner[len(owner)-8:], i+1)
		if err := rawdb.WriteStateDomainChangePostingIndex(batch, &rawdb.StateDomainChange{BlockNum: 1, FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: owner}); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	batch.Close()
	if err := db.Compact(nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	tune := rawdb.DefaultPebbleOptions()
	tune.DisableAutomaticCompactions = true
	tune.MemTableSizeBytes = 64 << 10
	tune.TargetFileSizeBytes = 32 << 10
	tune.L0CompactionThreshold = 4
	tune.L0StopWritesThreshold = 8
	db, err = rawdb.NewPebbleDBWithOptions(path, 16, 32, tune)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var cursor []byte
	var deleted uint64
	for {
		r, err := rawdb.PruneStaleStateChangePostingChunkContext(ctx, db, 1, cursor, rawdb.StateChangePostingPruneLimits{MaxScannedRows: 256, MaxScannedBytes: 1 << 20, MaxDeleteBytes: 1 << 20, MaxDuration: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		deleted += r.RowsDeleted
		cursor = r.NextCursor
		if r.Complete {
			break
		}
	}
	if deleted != 100000 {
		t.Fatalf("deleted %d", deleted)
	}
}
