package rawdb

import (
	"bytes"
	"testing"
)

// TestNewPebbleDB_Smoke verifies that NewPebbleDB, with the tuned Options
// applied by pebbledb.DefaultOptions, can open a fresh database, round-trip a
// key/value, and close cleanly. It deliberately does not poke at Pebble
// internals — those checks would couple the test to upstream version bumps.
func TestNewPebbleDB_Smoke(t *testing.T) {
	dir := t.TempDir()
	db, err := NewPebbleDB(dir, 16, 16) // minCache / minHandles
	if err != nil {
		t.Fatalf("NewPebbleDB: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	key := []byte("smoke-key")
	val := []byte("smoke-value")
	if err := db.Put(key, val); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("round-trip mismatch: got %x, want %x", got, val)
	}
	if err := db.Delete(key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if has, err := db.Has(key); err != nil {
		t.Fatalf("Has after Delete: %v", err)
	} else if has {
		t.Fatal("key still present after Delete")
	}
}
