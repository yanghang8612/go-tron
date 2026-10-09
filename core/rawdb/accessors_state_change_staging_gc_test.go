package rawdb

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
)

func stagingGCTestPebble(t *testing.T) ethdb.KeyValueStore {
	t.Helper()
	db, err := NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestStagingIndexGCWholeFramesAndBoundedRetry(t *testing.T) {
	db := stagingGCTestPebble(t)
	zero, _ := putPostingChunkFrame(t, db, 1, 1, 1023)
	lowCross, _ := putPostingChunkFrame(t, db, 2, 1023, 1024)
	stale, _ := putPostingChunkFrame(t, db, 3, 1024, 2047)
	highCross, _ := putPostingChunkFrame(t, db, 4, 2047, 2048)
	live, _ := putPostingChunkFrame(t, db, 5, 2048)
	limits := postingChunkTestLimits()
	limits.MaxScannedRows = 1
	var cursor []byte
	var scans, deletes uint64
	for pass := 0; pass < 10; pass++ {
		r, err := PruneStagingStateChangePostingChunk(context.Background(), db, 2047, cursor, limits)
		if err != nil {
			t.Fatal(err)
		}
		if r.RowsScanned > 1 {
			t.Fatal("row budget exceeded")
		}
		scans += r.RowsScanned
		deletes += r.RowsDeleted
		cursor = r.NextCursor
		if r.Complete {
			break
		}
	}
	if scans != 5 || deletes != 1 {
		t.Fatalf("scans=%d deletes=%d", scans, deletes)
	}
	assertPostingChunkExists(t, db, stale, false)
	for _, key := range [][]byte{zero, lowCross, highCross, live} {
		assertPostingChunkExists(t, db, key, true)
	}
	for _, boundary := range []uint64{0, 1023, 1024, 2046, 2048} {
		if _, err := PruneStagingStateChangePostingChunk(context.Background(), db, boundary, nil, limits); err == nil {
			t.Fatalf("accepted boundary %d", boundary)
		}
	}
}

func TestStagingIndexGCPostingFailureRetainsCursor(t *testing.T) {
	for _, failure := range []string{"write", "ambiguous-write", "iterator", "cancel", "malformed"} {
		t.Run(failure, func(t *testing.T) {
			base := stagingGCTestPebble(t)
			key, _ := putPostingChunkFrame(t, base, 1, 1024)
			db := &postingChunkTestDB{KeyValueStore: base}
			want := errors.New("injected failure")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch failure {
			case "write":
				db.writeErr = want
			case "ambiguous-write":
				db.writeErr, db.applyBeforeWriteError = want, true
			case "iterator":
				db.iteratorErr = want
			case "cancel":
				db.onNext = cancel
			case "malformed":
				bad, _ := putPostingChunkFrame(t, base, 2, 1025)
				if err := base.Put(bad, []byte{0}); err != nil {
					t.Fatal(err)
				}
			}
			r, err := PruneStagingStateChangePostingChunk(ctx, db, 2047, nil, postingChunkTestLimits())
			if err == nil || len(r.NextCursor) != 0 || r.RowsDeleted != 0 || r.Complete {
				t.Fatalf("failed result=%+v err=%v", r, err)
			}
			if db.iterators != db.releases {
				t.Fatal("iterator leaked")
			}
			if failure != "ambiguous-write" {
				assertPostingChunkExists(t, base, key, true)
			}
			if failure == "write" || failure == "ambiguous-write" {
				db.writeErr = nil
				if _, err := PruneStagingStateChangePostingChunk(context.Background(), db, 2047, nil, postingChunkTestLimits()); err != nil {
					t.Fatal(err)
				}
				assertPostingChunkExists(t, base, key, false)
			}
		})
	}
}

type stagingGCSeekFault struct {
	StagingIndexReadView
	err    error
	cancel func()
	seeks  int
}

func (s *stagingGCSeekFault) SeekPrefix(prefix, start []byte) ([]byte, []byte, bool, error) {
	s.seeks++
	if s.cancel != nil {
		s.cancel()
	}
	if s.err != nil {
		return nil, nil, false, s.err
	}
	return s.StagingIndexReadView.SeekPrefix(prefix, start)
}

func TestStagingIndexGCDirectoryExactPresenceAndSeekFailure(t *testing.T) {
	db := stagingGCTestPebble(t)
	orphan, live := []byte("orphan"), []byte("live")
	for _, key := range [][]byte{orphan, live} {
		if err := db.Put(stateChangeKeyDirectoryKey(key), nil); err != nil {
			t.Fatal(err)
		}
	}
	hash := stateChangePostingHash(live)
	value, err := encodeStateChangePosting([]uint64{4096})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(stateChangePostingKey(hash, 4096), value); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	view := &stagingGCSeekFault{StagingIndexReadView: snapshot.(StagingIndexReadView), err: errors.New("seek failed")}
	r, err := PruneStagingStateChangeDirectoryChunk(context.Background(), db, view, nil, postingChunkTestLimits())
	if err == nil || len(r.NextCursor) != 0 || r.RowsDeleted != 0 {
		t.Fatalf("seek error=%+v %v", r, err)
	}
	assertPostingChunkExists(t, db, stateChangeKeyDirectoryKey(orphan), true)
	view.err = nil
	limits := postingChunkTestLimits()
	limits.MaxScannedRows = 1
	var cursor []byte
	for pass := 0; pass < 5; pass++ {
		r, err := PruneStagingStateChangeDirectoryChunk(context.Background(), db, view, cursor, limits)
		if err != nil {
			t.Fatal(err)
		}
		if r.RowsScanned > 1 {
			t.Fatal("directory page exceeded row bound")
		}
		cursor = bytes.Clone(r.NextCursor)
		if r.Complete {
			break
		}
	}
	assertPostingChunkExists(t, db, stateChangeKeyDirectoryKey(orphan), false)
	assertPostingChunkExists(t, db, stateChangeKeyDirectoryKey(live), true)
	if view.seeks != 3 {
		t.Fatalf("seeks=%d want=3 including failed seek", view.seeks)
	}
}
