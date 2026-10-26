package pebbledb

import (
	"bytes"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/tronprotocol/go-tron/core/pointread"
)

func TestStorageCursorPinnedCutOwnershipAndCallbacks(t *testing.T) {
	db, err := New(t.TempDir(), 16, 16, "test/storage-cursor/", false, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put([]byte("key"), []byte("original")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.NewKeyValueSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	cursor, err := snapshot.(pointread.KeyValueCursorSnapshot).NewPointReadCursor()
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	if err := db.Put([]byte("key"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	callbackFailure := errors.New("callback error")
	if found, err := cursor.View([]byte("key"), func(value []byte) error {
		if !bytes.Equal(value, []byte("original")) {
			t.Fatalf("moving base %q", value)
		}
		return callbackFailure
	}); !found || !errors.Is(err, callbackFailure) {
		t.Fatalf("callback %v/%v", found, err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("missing callback panic")
			}
		}()
		_, _ = cursor.View([]byte("key"), func([]byte) error { panic("consumer") })
	}()
	if _, err := cursor.View([]byte("key"), func([]byte) error { return nil }); err != nil {
		t.Fatalf("value not released after panic: %v", err)
	}
	if found, err := cursor.View([]byte("absent"), func([]byte) error { t.Fatal("missing callback"); return nil }); found || err != nil {
		t.Fatalf("miss %v/%v", found, err)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := cursor.View([]byte("key"), func([]byte) error { return nil }); !errors.Is(err, pebble.ErrClosed) {
		t.Fatalf("closed %v", err)
	}
}

func TestStorageCursorPropagatesReadError(t *testing.T) {
	fs := &pointGetFailFS{FS: vfs.NewMem()}
	cache := pebble.NewCache(0)
	engine, err := pebble.Open("db", &pebble.Options{FS: fs, Cache: cache, Comparer: exactPointComparer})
	cache.Unref()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	if err := engine.Set([]byte("key"), []byte("value"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := engine.Flush(); err != nil {
		t.Fatal(err)
	}
	db := &Database{db: engine}
	snapshot, err := db.NewKeyValueSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	cursor, err := snapshot.(pointread.KeyValueCursorSnapshot).NewPointReadCursor()
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	fs.fail.Store(true)
	defer fs.fail.Store(false)
	if found, err := cursor.View([]byte("key"), func([]byte) error { t.Fatal("callback after failure"); return nil }); found || !errors.Is(err, errPointGetIO) {
		t.Fatalf("read error %v/%v", found, err)
	}
}
