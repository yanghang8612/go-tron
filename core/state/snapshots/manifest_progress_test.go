package snapshots

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestManifestProgressMatchesFullPublicationAndOwnsViews(t *testing.T) {
	for _, mode := range []string{"trusted", "disabled", "rejected"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
			t.Setenv(manifestCacheBudgetEnv, "268435456")
			loadedManifestCache.clear()
			t.Cleanup(loadedManifestCache.clear)
			dir, oracle := t.TempDir(), t.TempDir()
			m := manifestValidationFixture(3)
			m.Progress = &Progress{HistoryBuildTxNum: 29, HotPruneBlockNum: 1, HotPruneTxNum: 2}
			m.HistoryStagingResetEpoch = 7
			if err := PublishManifest(dir, m); err != nil {
				t.Fatal(err)
			}
			old, err := loadProductionManifestReadOnly(dir)
			if err != nil {
				t.Fatal(err)
			}
			oldCopy := cloneDecodedManifest(old)
			if mode == "disabled" {
				t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "0")
			}
			if mode == "rejected" {
				t.Setenv(manifestCacheBudgetEnv, "1")
			}
			expected := cloneDecodedManifest(m)
			expected.Progress.HotPruneBlockNum = 4
			expected.Progress.HotPruneTxNum = 8
			expected.Progress.StateChangeIndexPruneBlockNum = 3
			if err := PublishManifest(oracle, expected); err != nil {
				t.Fatal(err)
			}
			if err := UpdateHotPruneProgress(dir, 4, 8); err != nil {
				t.Fatal(err)
			}
			if err := UpdateStateChangeIndexPruneProgress(dir, 3); err != nil {
				t.Fatal(err)
			}
			got, _ := os.ReadFile(filepath.Join(dir, ManifestFile))
			want, _ := os.ReadFile(filepath.Join(oracle, ManifestFile))
			if !bytes.Equal(got, want) {
				t.Fatal("progress bytes differ from full publication")
			}
			if !reflect.DeepEqual(old, oldCopy) {
				t.Fatal("old borrowed generation mutated")
			}
			public, err := LoadProductionManifest(dir)
			if err != nil {
				t.Fatal(err)
			}
			public.Segments[0].Path = "mutated"
			public.Progress.HotPruneTxNum = 999
			again, err := LoadProductionManifest(dir)
			if err != nil || again.Progress.HotPruneTxNum != 8 || again.Segments[0].Path == "mutated" {
				t.Fatal("public mutable view aliases private generation")
			}
			before, err := os.Stat(filepath.Join(dir, ManifestFile))
			if err != nil {
				t.Fatal(err)
			}
			if err := UpdateHotPruneProgress(dir, 1, 1); err != nil {
				t.Fatal(err)
			}
			after, err := os.Stat(filepath.Join(dir, ManifestFile))
			if err != nil || !os.SameFile(before, after) {
				t.Fatal("noop rewrote manifest")
			}
			loadedManifestCache.clear()
			restarted, err := LoadProductionManifest(dir)
			if err != nil || !reflect.DeepEqual(restarted, expected) {
				t.Fatal("restart differs", err)
			}
		})
	}
}

func TestManifestProgressAuthenticatesReplacementAndRejectsCorruption(t *testing.T) {
	dir := t.TempDir()
	m := manifestValidationFixture(2)
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	old, _ := loadProductionManifestReadOnly(dir)
	replacement := cloneDecodedManifest(m)
	replacement.Generation++
	replacement.Segments[0].Size++
	raw, err := json.Marshal(replacement)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, ManifestFile)
	original, _ := os.ReadFile(path)
	if len(original) != len(raw) {
		t.Fatal("fixture must be same length")
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := UpdateHotPruneProgress(dir, 2, 3); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadProductionManifest(dir)
	if err != nil || loaded.Generation != replacement.Generation || loaded.Segments[0].Size != replacement.Segments[0].Size {
		t.Fatal("stale generation reused", err)
	}
	if old.Generation != m.Generation || old.Progress != nil {
		t.Fatal("pinned generation mutated")
	}
	bad := []byte(`{"version":`)
	if err := os.WriteFile(path, bad, 0600); err != nil {
		t.Fatal(err)
	}
	if err := UpdateHotPruneProgress(dir, 0, 0); err == nil {
		t.Fatal("noop accepted corrupt current bytes")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(after, bad) {
		t.Fatal("failed read replaced corruption with stale data")
	}
}

func TestManifestProgressConcurrentPublishUsesFreshCatalog(t *testing.T) {
	dir := t.TempDir()
	m := manifestValidationFixture(2)
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	for iteration := 0; iteration < 20; iteration++ {
		next := cloneDecodedManifest(m)
		next.Generation += uint64(iteration + 1)
		next.Segments[0].Size += uint64(iteration + 1)
		// Holding the write lock makes both goroutines wait. Whichever wins, the
		// progress updater must load inside that lock rather than before it.
		manifestPublicationMu.Lock()
		var wg sync.WaitGroup
		wg.Add(2)
		errs := make(chan error, 2)
		go func() { defer wg.Done(); errs <- PublishManifest(dir, next) }()
		go func() { defer wg.Done(); errs <- UpdateHotPruneProgress(dir, 10, 20) }()
		manifestPublicationMu.Unlock()
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		loaded, err := LoadProductionManifest(dir)
		if err != nil || loaded.Generation != next.Generation || loaded.Segments[0].Size != next.Segments[0].Size {
			t.Fatal("progress overwrote fresh catalog", err)
		}
		// A complete public writer intentionally retains its prior full-generation
		// semantics: if it wins last, its explicit Progress may replace progress.
	}
}

func TestManifestProgressWriteFailureDoesNotSeed(t *testing.T) {
	dir := t.TempDir()
	m := manifestValidationFixture(1)
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	prior, _ := loadProductionManifestReadOnly(dir)
	raw, _ := json.Marshal(m)
	if err := os.Remove(filepath.Join(dir, ManifestFile)); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, ManifestFile), 0700); err != nil {
		t.Fatal(err)
	}
	if err := writeManifestBytes(dir, raw); err == nil {
		t.Fatal("rename over directory succeeded")
	}
	loadedManifestCache.mu.Lock()
	same := loadedManifestCache.manifest == prior
	loadedManifestCache.mu.Unlock()
	if !same {
		t.Fatal("failed bytes publication seeded cache")
	}
	files, _ := filepath.Glob(filepath.Join(dir, ".manifest-*.tmp"))
	if len(files) != 0 {
		t.Fatal("failed publication leaked temp files")
	}
}

func TestManifestProgressInvalidUTF8FallsBackToPublicEncoding(t *testing.T) {
	dir := t.TempDir()
	m := manifestValidationFixture(1)
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, ManifestFile)
	raw, _ := os.ReadFile(path)
	needle := []byte(m.Segments[0].Path)
	at := bytes.Index(raw, needle)
	if at < 0 {
		t.Fatal("path absent")
	}
	raw[at] = 0xff
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	expected, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	expected.Progress = &Progress{HotPruneBlockNum: 1, HotPruneTxNum: 2}
	oracle := t.TempDir()
	if err := PublishManifest(oracle, expected); err != nil {
		t.Fatal(err)
	}
	if err := UpdateHotPruneProgress(dir, 1, 2); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(path)
	want, _ := os.ReadFile(filepath.Join(oracle, ManifestFile))
	if !bytes.Equal(got, want) {
		t.Fatal("invalid UTF8 canonical encoding differs")
	}
}

func TestManifestProgressMissingAndInvalidCacheConfiguration(t *testing.T) {
	if err := UpdateHotPruneProgress("", 1, 2); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := UpdateHotPruneProgress(dir, 1, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, ManifestFile)); !os.IsNotExist(err) {
		t.Fatal("missing update created manifest")
	}
	if err := PublishManifest(dir, manifestValidationFixture(1)); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(filepath.Join(dir, ManifestFile))
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "invalid")
	if err := UpdateHotPruneProgress(dir, 1, 2); err == nil {
		t.Fatal("invalid cache setting swallowed")
	}
	after, _ := os.ReadFile(filepath.Join(dir, ManifestFile))
	if !bytes.Equal(before, after) {
		t.Fatal("config failure changed publication")
	}
}

func TestManifestProgressLegacyHeaderUsesNormalPublicationDefaults(t *testing.T) {
	dir := t.TempDir()
	m := manifestValidationFixture(1)
	m.Generation = 0
	m.PublishedUnix = 0
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ManifestFile), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := UpdateHotPruneProgress(dir, 1, 2); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadProductionManifest(dir)
	if err != nil || loaded.Generation != 1 || loaded.PublishedUnix == 0 {
		t.Fatal("publication defaults changed", err)
	}
}

func TestManifestProgressTrustedCatalogRemainsSharedAndImmutable(t *testing.T) {
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	t.Setenv(manifestCacheBudgetEnv, "268435456")
	dir := t.TempDir()
	m := manifestValidationFixture(2)
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	prior, err := loadProductionManifestReadOnly(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := UpdateHotPruneProgress(dir, 2, 3); err != nil {
		t.Fatal(err)
	}
	after, err := loadProductionManifestReadOnly(dir)
	if err != nil {
		t.Fatal(err)
	}
	if prior == after || &prior.Segments[0] != &after.Segments[0] || prior.Progress != nil || after.Progress.HotPruneTxNum != 3 {
		t.Fatal("progress did not derive immutable shared catalog")
	}
}

func TestManifestProgressRetryAfterDirectorySyncFailure(t *testing.T) {
	for _, mode := range []string{"trusted", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
			t.Setenv(manifestCacheBudgetEnv, "268435456")
			dir := t.TempDir()
			if err := PublishManifest(dir, manifestValidationFixture(1)); err != nil {
				t.Fatal(err)
			}
			prior, err := loadProductionManifestReadOnly(dir)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "disabled" {
				t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "0")
			}
			fail, calls := true, 0
			syncDir := func(path string) error {
				calls++
				if fail {
					return os.ErrPermission
				}
				return syncSnapshotDir(path)
			}
			update := func() error { return updateManifestPruneProgressWithSync(dir, 5, 8, 0, syncDir) }
			if err := update(); !errors.Is(err, os.ErrPermission) || calls != 1 {
				t.Fatalf("post-rename sync failure lost: calls=%d err=%v", calls, err)
			}
			if mode == "trusted" {
				loadedManifestCache.mu.Lock()
				same := loadedManifestCache.manifest == prior
				loadedManifestCache.mu.Unlock()
				if !same {
					t.Fatal("failed durable publication seeded cache")
				}
			}
			path := filepath.Join(dir, ManifestFile)
			before, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			visible, err := LoadProductionManifest(dir)
			if err != nil || visible.Progress == nil || visible.Progress.HotPruneTxNum != 8 {
				t.Fatal("expected rename-completed bytes", err)
			}
			if err := update(); !errors.Is(err, os.ErrPermission) || calls != 2 {
				t.Fatalf("same-cursor retry hid persistent sync failure: calls=%d err=%v", calls, err)
			}
			fail = false
			if err := update(); err != nil || calls != 3 {
				t.Fatalf("same-cursor retry did not repair durability: calls=%d err=%v", calls, err)
			}
			after, err := os.Stat(path)
			if err != nil || !os.SameFile(before, after) {
				t.Fatal("same-cursor retry rewrote JSON/inode")
			}
			files, _ := filepath.Glob(filepath.Join(dir, ".manifest-*.tmp"))
			if len(files) != 0 {
				t.Fatal("failed directory sync leaked temporary files")
			}
			loadedManifestCache.clear()
			restarted, err := LoadProductionManifest(dir)
			if err != nil || restarted.Progress.HotPruneBlockNum != 5 || restarted.Progress.HotPruneTxNum != 8 {
				t.Fatal("successful retry lost durable progress", err)
			}
		})
	}
}
