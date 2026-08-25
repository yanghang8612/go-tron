package snapshots

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCleanupStaleTempFilesRemovesOnlyOldHiddenScratch(t *testing.T) {
	dir := t.TempDir()
	nested := filepath.Join(dir, "history")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	oldHidden := filepath.Join(nested, ".state-history.seg.123.tmp")
	recentHidden := filepath.Join(nested, ".state-history.seg.456.tmp")
	partial := filepath.Join(nested, ".state-history.seg.partial")
	visibleTemp := filepath.Join(nested, "operator-export.tmp")
	for path, contents := range map[string][]byte{
		oldHidden:    []byte("abandoned"),
		recentHidden: []byte("active"),
		partial:      []byte("resumable"),
		visibleTemp:  []byte("operator"),
	} {
		if err := os.WriteFile(path, contents, 0o600); err != nil {
			t.Fatalf("WriteFile %s: %v", path, err)
		}
	}
	oldTime := time.Now().Add(-2 * DefaultStaleTempFileAge)
	if err := os.Chtimes(oldHidden, oldTime, oldTime); err != nil {
		t.Fatalf("Chtimes old hidden temp: %v", err)
	}

	result, err := CleanupStaleTempFiles(dir, DefaultStaleTempFileAge)
	if err != nil {
		t.Fatalf("CleanupStaleTempFiles: %v", err)
	}
	if result.Files != 1 || result.Bytes != uint64(len("abandoned")) {
		t.Fatalf("cleanup result = %+v, want one abandoned file", result)
	}
	if _, err := os.Stat(oldHidden); !os.IsNotExist(err) {
		t.Fatalf("old hidden temp still exists or stat failed: %v", err)
	}
	for _, path := range []string{recentHidden, partial, visibleTemp} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("preserved file %s: %v", path, err)
		}
	}
}

func TestCleanupStaleTempFilesDoesNotFollowSymlinks(t *testing.T) {
	dir := t.TempDir()
	external := t.TempDir()
	externalTemp := filepath.Join(external, ".external.tmp")
	if err := os.WriteFile(externalTemp, []byte("keep"), 0o600); err != nil {
		t.Fatalf("WriteFile external temp: %v", err)
	}
	oldTime := time.Now().Add(-2 * DefaultStaleTempFileAge)
	if err := os.Chtimes(externalTemp, oldTime, oldTime); err != nil {
		t.Fatalf("Chtimes external temp: %v", err)
	}
	if err := os.Symlink(external, filepath.Join(dir, "linked")); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	result, err := CleanupStaleTempFiles(dir, DefaultStaleTempFileAge)
	if err != nil {
		t.Fatalf("CleanupStaleTempFiles: %v", err)
	}
	if result.Files != 0 || result.Bytes != 0 {
		t.Fatalf("cleanup result = %+v, want no symlink traversal", result)
	}
	if _, err := os.Stat(externalTemp); err != nil {
		t.Fatalf("external temp was removed: %v", err)
	}
}
