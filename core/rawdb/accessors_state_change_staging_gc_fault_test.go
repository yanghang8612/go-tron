package rawdb

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
)

// Unlike postingChunkTestDB, this wrapper accepts directory iterators too.
// It injects failures at the actual Pebble iterator/batch boundary.
type stagingGCFaultDB struct {
	ethdb.KeyValueStore
	iteratorErr                 error
	writeErr                    error
	applyBeforeWriteError       bool
	onNext                      func()
	iterators, releases, writes int
}

func (db *stagingGCFaultDB) NewIterator(prefix, start []byte) ethdb.Iterator {
	db.iterators++
	return &stagingGCFaultIterator{Iterator: db.KeyValueStore.NewIterator(prefix, start), db: db}
}

func (db *stagingGCFaultDB) NewBatch() ethdb.Batch {
	return &stagingGCFaultBatch{Batch: db.KeyValueStore.NewBatch(), db: db}
}

type stagingGCFaultIterator struct {
	ethdb.Iterator
	db *stagingGCFaultDB
}

func (it *stagingGCFaultIterator) Next() bool {
	if it.db.onNext != nil {
		it.db.onNext()
	}
	return it.Iterator.Next()
}

func (it *stagingGCFaultIterator) Error() error {
	if it.db.iteratorErr != nil {
		return it.db.iteratorErr
	}
	return it.Iterator.Error()
}

func (it *stagingGCFaultIterator) Release() {
	it.db.releases++
	it.Iterator.Release()
}

type stagingGCFaultBatch struct {
	ethdb.Batch
	db *stagingGCFaultDB
}

func (b *stagingGCFaultBatch) Write() error {
	if b.db.iterators != b.db.releases {
		panic("directory batch write while iterator is live")
	}
	b.db.writes++
	if b.db.writeErr != nil && !b.db.applyBeforeWriteError {
		return b.db.writeErr
	}
	if err := b.Batch.Write(); err != nil {
		return err
	}
	return b.db.writeErr
}

func stagingGCPinnedView(t *testing.T, db ethdb.KeyValueStore) StagingIndexReadView {
	t.Helper()
	snapshot, err := db.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snapshot.Close() })
	return snapshot.(StagingIndexReadView)
}

func TestStagingDirectoryGCFailuresRetainCursorAndAllowRetry(t *testing.T) {
	for _, failure := range []string{"iterator", "seek", "encoding", "cancel", "write", "ambiguous-write"} {
		t.Run(failure, func(t *testing.T) {
			base := stagingGCTestPebble(t)
			first := stateChangeKeyDirectoryKey([]byte("a"))
			candidate := stateChangeKeyDirectoryKey([]byte("b"))
			for _, key := range [][]byte{first, candidate} {
				if err := base.Put(key, nil); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			view := &stagingGCSeekFault{StagingIndexReadView: stagingGCPinnedView(t, base)}
			db := &stagingGCFaultDB{KeyValueStore: base}
			injected := errors.New("injected staging directory fault")
			switch failure {
			case "iterator":
				db.iteratorErr = injected
			case "seek":
				view.err = injected
			case "encoding":
				// The pinned query must reject an invalid posting rather than
				// mistaking it for a valid reason to retain the directory.
				hash := stateChangePostingHash([]byte("b"))
				if err := base.Put(stateChangePostingKey(hash, 1024), []byte{0}); err != nil {
					t.Fatal(err)
				}
				view = &stagingGCSeekFault{StagingIndexReadView: stagingGCPinnedView(t, base)}
			case "cancel":
				view.cancel = cancel
			case "write":
				db.writeErr = injected
			case "ambiguous-write":
				db.writeErr, db.applyBeforeWriteError = injected, true
			}
			input := bytes.Clone(first)
			result, err := PruneStagingStateChangeDirectoryChunk(ctx, db, view, input, postingChunkTestLimits())
			if err == nil || !bytes.Equal(result.NextCursor, input) || result.RowsDeleted != 0 || result.BytesDeleted != 0 || result.Complete || result.StoppedByBudget {
				t.Fatalf("failure=%s result=%+v err=%v", failure, result, err)
			}
			if db.iterators != db.releases {
				t.Fatalf("iterator leaked: opened=%d released=%d", db.iterators, db.releases)
			}
			if failure != "ambiguous-write" {
				assertPostingChunkExists(t, base, candidate, true)
			}
			if failure == "write" || failure == "ambiguous-write" {
				db.writeErr = nil
				retry, err := PruneStagingStateChangeDirectoryChunk(context.Background(), db, view, input, postingChunkTestLimits())
				wantDeleted := uint64(1)
				if failure == "ambiguous-write" {
					// The first batch did apply; replay from the old cursor
					// finds no row and must still complete safely.
					wantDeleted = 0
				}
				if err != nil || !retry.Complete || retry.RowsDeleted != wantDeleted {
					t.Fatalf("retry result=%+v err=%v", retry, err)
				}
				assertPostingChunkExists(t, base, candidate, false)
			}
		})
	}
}

func TestStagingDirectoryGCBudgetsEventuallyFinish(t *testing.T) {
	db := stagingGCTestPebble(t)
	var keys [][]byte
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		key := stateChangeKeyDirectoryKey([]byte(name))
		if err := db.Put(key, nil); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}
	view := stagingGCPinnedView(t, db)
	limits := StateChangePostingPruneLimits{MaxScannedRows: 2, MaxScannedBytes: 1, MaxDeleteBytes: 1, MaxDuration: time.Nanosecond}
	var cursor []byte
	var scanned, deleted uint64
	completed := false
	for pass := 0; pass < len(keys)+2; pass++ {
		result, err := PruneStagingStateChangeDirectoryChunk(context.Background(), db, view, cursor, limits)
		if err != nil {
			t.Fatal(err)
		}
		if result.RowsScanned > limits.MaxScannedRows || result.RowsDeleted > 1 {
			t.Fatalf("unbounded chunk: %+v", result)
		}
		if result.RowsScanned > 0 && bytes.Compare(result.NextCursor, cursor) <= 0 {
			t.Fatalf("cursor did not advance: before=%x after=%x", cursor, result.NextCursor)
		}
		scanned += result.RowsScanned
		deleted += result.RowsDeleted
		cursor = result.NextCursor
		if result.Complete {
			completed = true
			break
		}
	}
	if !completed || scanned != uint64(len(keys)) || deleted != uint64(len(keys)) {
		t.Fatalf("complete=%v scanned=%d deleted=%d", completed, scanned, deleted)
	}
	for _, key := range keys {
		assertPostingChunkExists(t, db, key, false)
	}
}

func TestStagingIndexGCPinnedPebbleViewRetainsPreDeleteRows(t *testing.T) {
	db := stagingGCTestPebble(t)
	posting, _ := putPostingChunkFrame(t, db, 1, 1024, 2047)
	directory := stateChangeKeyDirectoryKey([]byte("orphan-after-posting-prune"))
	if err := db.Put(directory, nil); err != nil {
		t.Fatal(err)
	}
	old := stagingGCPinnedView(t, db)
	if result, err := PruneStagingStateChangePostingChunk(context.Background(), db, 2047, nil, postingChunkTestLimits()); err != nil || result.RowsDeleted != 1 {
		t.Fatalf("posting prune result=%+v err=%v", result, err)
	}
	assertPostingChunkExists(t, db, posting, false)
	assertPostingChunkExists(t, old.(ethdb.KeyValueReader), posting, true)
	// The directory query must use a fresh coherent view of the pruned
	// posting set; an older pinned reader still sees both original rows.
	fresh := stagingGCPinnedView(t, db)
	if result, err := PruneStagingStateChangeDirectoryChunk(context.Background(), db, fresh, nil, postingChunkTestLimits()); err != nil || result.RowsDeleted != 1 {
		t.Fatalf("directory prune result=%+v err=%v", result, err)
	}
	assertPostingChunkExists(t, db, directory, false)
	assertPostingChunkExists(t, old.(ethdb.KeyValueReader), directory, true)
	key, _, present, err := old.SeekPrefix(stateChangePostingPrefix, nil)
	if err != nil || !present || !bytes.Equal(key, posting) {
		t.Fatalf("old pinned posting seek key=%x present=%v err=%v", key, present, err)
	}
}
