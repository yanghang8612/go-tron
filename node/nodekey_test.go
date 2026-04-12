package node

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadOrCreateNodeIDFresh(t *testing.T) {
	dir := t.TempDir()
	id1, err := LoadOrCreateNodeID(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(id1) != NodeIDLen {
		t.Fatalf("length %d", len(id1))
	}
	// File must exist
	if _, err := os.Stat(filepath.Join(dir, "nodekey")); err != nil {
		t.Fatal(err)
	}

	// Second call returns the same ID
	id2, err := LoadOrCreateNodeID(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(id1, id2) {
		t.Fatal("second load returned different ID; should persist")
	}
}

func TestLoadOrCreateNodeIDEphemeral(t *testing.T) {
	// Empty dataDir → ephemeral ID.
	id, err := LoadOrCreateNodeID("")
	if err != nil {
		t.Fatal(err)
	}
	if len(id) != NodeIDLen {
		t.Fatalf("length %d", len(id))
	}
}

func TestLoadOrCreateNodeIDWrongLength(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "nodekey"), []byte("bad"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadOrCreateNodeID(dir)
	if err == nil {
		t.Fatal("expected error on wrong-length nodekey")
	}
}
