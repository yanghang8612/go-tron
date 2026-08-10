package etl

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCleanupStaleCollectorsRemovesOnlyOwnedScratch(t *testing.T) {
	parent := t.TempDir()
	stale := filepath.Join(parent, "gtron-etl-stale")
	if err := os.MkdirAll(filepath.Join(stale, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, "run.bin"), []byte("1234"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, "nested", "run.bin"), []byte("567"), 0o600); err != nil {
		t.Fatal(err)
	}
	preservedDir := filepath.Join(parent, "operator-data")
	if err := os.Mkdir(preservedDir, 0o755); err != nil {
		t.Fatal(err)
	}
	preservedFile := filepath.Join(parent, "gtron-etl-note")
	if err := os.WriteFile(preservedFile, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(parent, "gtron-etl-link")
	if err := os.Symlink(preservedDir, symlink); err != nil {
		t.Fatal(err)
	}

	stats, err := CleanupStaleCollectors(parent)
	if err != nil {
		t.Fatalf("cleanup stale collectors: %v", err)
	}
	if stats.Directories != 1 || stats.Files != 2 || stats.Bytes != 7 {
		t.Fatalf("stats = %+v, want 1 directory, 2 files, 7 bytes", stats)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale directory still exists: %v", err)
	}
	for _, path := range []string{preservedDir, preservedFile, symlink} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("preserved path %q missing: %v", path, err)
		}
	}
}

func TestCleanupStaleCollectorsMissingParentIsNoop(t *testing.T) {
	stats, err := CleanupStaleCollectors(filepath.Join(t.TempDir(), "missing"))
	if err != nil {
		t.Fatalf("cleanup missing parent: %v", err)
	}
	if stats != (CleanupStats{}) {
		t.Fatalf("stats = %+v, want zero", stats)
	}
}
