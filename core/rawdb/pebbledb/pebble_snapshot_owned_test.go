package pebbledb

import (
	"bytes"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/tronprotocol/go-tron/core/pointread"
)

func TestKeyValueSnapshotGetOwnedBytes(t *testing.T) {
	db, err := New(t.TempDir(), 16, 16, "test/snapshot-owned/", false, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	key := []byte("key")
	source := bytes.Repeat([]byte("original"), 1<<14)
	if err := db.Put(key, source); err != nil {
		t.Fatal(err)
	}
	view, err := db.NewKeyValueSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	owned, ok := view.(pointread.OwnedKeyValueReader)
	if !ok || !owned.GetReturnsOwnedBytes() {
		t.Fatal("snapshot lacks explicit owned Get contract")
	}
	first, err := view.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	second, err := view.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	first[0] ^= 0xff
	if !bytes.Equal(second, source) {
		t.Fatal("Get results alias")
	}
	if err := db.Put(key, []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	third, err := view.Get(key)
	if err != nil || !bytes.Equal(third, source) {
		t.Fatal("snapshot lost fixed or independent bytes", err)
	}
	if _, err := view.Get([]byte("missing")); !errors.Is(err, pebble.ErrNotFound) {
		t.Fatalf("missing = %v", err)
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(second, source) || !bytes.Equal(third, source) {
		t.Fatal("Get bytes invalid after Close")
	}
	if _, err := view.Get(key); !errors.Is(err, pebble.ErrClosed) {
		t.Fatalf("closed = %v", err)
	}
}
