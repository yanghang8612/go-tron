package rawdb

import (
	"context"
	"testing"
)

func TestStateChangePostingCleanupScansWholeFramesAndDirectories(t *testing.T) {
	db := NewMemoryDatabase()
	defer db.Close()
	latest := []byte("state-kv-latest-v2-account")
	hash := stateChangePostingHash(latest)
	encoded, err := encodeStateChangePosting([]uint64{1024, 1026, 1100})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(stateChangePostingKey(hash, 1024), encoded); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(stateChangeKeyDirectoryKey(latest), nil); err != nil {
		t.Fatal(err)
	}
	frames := 0
	if err := ScanStateChangePostingFrames(context.Background(), db, func(frame StateChangePostingFrameCursor) error {
		frames++
		if frame.Hash != hash || frame.FirstBlock != 1024 || frame.LastBlock != 1100 || frame.Rows != 3 {
			t.Fatalf("incorrect validated frame: %+v", frame)
		}
		return nil
	}); err != nil || frames != 1 {
		t.Fatalf("posting scan frames=%d error=%v", frames, err)
	}
	dirs := 0
	if err := ScanStateChangeKeyDirectory(context.Background(), db, func(row StateChangeDirectoryCursor) error {
		dirs++
		if row.Hash != hash {
			t.Fatal("directory and posting hash disagree")
		}
		return nil
	}); err != nil || dirs != 1 {
		t.Fatalf("directory scan rows=%d error=%v", dirs, err)
	}
	if err := db.Put(stateChangePostingKey(hash, 2048), []byte{1, 2, 0}); err != nil {
		t.Fatal(err)
	}
	if err := ScanStateChangePostingFrames(context.Background(), db, func(StateChangePostingFrameCursor) error { return nil }); err == nil {
		t.Fatal("zero delta frame accepted")
	}
}

func TestStateChangeCleanupPageResumesAfterDeletedCursor(t *testing.T) {
	db := NewMemoryDatabase()
	defer db.Close()
	var keys [][]byte
	for i := byte(1); i <= 3; i++ {
		hash := [32]byte{i}
		key := stateChangePostingKey(hash, 1024)
		encoded, err := encodeStateChangePosting([]uint64{1024})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Put(key, encoded); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}
	var visited int
	var after []byte
	for {
		next, complete, err := ScanStateChangePostingFramePage(context.Background(), db, after, 1, func(frame StateChangePostingFrameCursor) error {
			visited++
			return db.Delete(frame.Key)
		})
		if err != nil {
			t.Fatal(err)
		}
		if complete {
			break
		}
		after = next
	}
	if visited != len(keys) {
		t.Fatalf("visited %d of %d frames after deleting page cursors", visited, len(keys))
	}
}
