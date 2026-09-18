package snapshots

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func retiredMetadataTestRef(path string, from uint64) SegmentRef {
	return SegmentRef{
		Dataset:   SegmentDatasetKVLatest,
		Domain:    kvdomains.ContractStorage,
		Kind:      SegmentLatest,
		FromTxNum: from,
		ToTxNum:   from,
		Path:      filepath.ToSlash(path),
		Size:      from + 1,
	}
}

func retiredMetadataTestManifest(retired ...SegmentRef) *Manifest {
	m := NewManifest(0, 100, nil)
	m.Generation = 17
	m.Progress = &Progress{HistoryBuildTxNum: 91, HotPruneTxNum: 73}
	m.Retired = append([]SegmentRef(nil), retired...)
	sortSegments(m.Retired)
	return m
}

func retiredMetadataPrepareTest(t testing.TB, dir string, m *Manifest, cursor retiredMetadataSweepCursor, limit int, fs retiredMetadataSweepFS) retiredMetadataSweepResult {
	t.Helper()
	got, err := prepareRetiredMetadataSweepWithFS(context.Background(), dir, m, cursor, limit, fs)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestPrepareRetiredMetadataSweepOnlyForgetsConfirmedMissing(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "latest"), 0o755); err != nil {
		t.Fatal(err)
	}
	missing := retiredMetadataTestRef("latest/missing.json", 1)
	present := retiredMetadataTestRef("latest/present.json", 2)
	dangling := retiredMetadataTestRef("latest/dangling.json", 3)
	directory := retiredMetadataTestRef("latest/directory.json", 4)
	active := retiredMetadataTestRef("latest/active.json", 5)
	active.Checksum = "sha256:active-identity-must-stay-unchanged"
	if err := os.WriteFile(filepath.Join(dir, present.Path), []byte("present"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("missing-target", filepath.Join(dir, dangling.Path)); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, directory.Path), 0o755); err != nil {
		t.Fatal(err)
	}
	m := retiredMetadataTestManifest(missing, present, dangling, directory, active)
	m.Segments = []SegmentRef{active}
	wantOld := cloneManifest(m)

	got := retiredMetadataPrepareTest(t, dir, m, retiredMetadataSweepCursor{}, 32, retiredMetadataOS)
	if got.Scanned != 5 || got.Removed != 1 || got.Deferred != "" {
		t.Fatalf("sweep = scanned %d removed %d deferred %q", got.Scanned, got.Removed, got.Deferred)
	}
	if len(got.Manifest.Retired) != 4 {
		t.Fatalf("retired = %d, want 4", len(got.Manifest.Retired))
	}
	for _, ref := range got.Manifest.Retired {
		if ref.Path == missing.Path {
			t.Fatal("confirmed missing ref was retained")
		}
	}
	if !reflect.DeepEqual(m, wantOld) {
		t.Fatal("sweep mutated caller-owned manifest")
	}
	if !reflect.DeepEqual(got.Manifest.Segments, m.Segments) || !reflect.DeepEqual(got.Manifest.Progress, m.Progress) {
		t.Fatal("sweep changed active refs or progress")
	}
}

func TestPrepareRetiredMetadataSweepFailsClosed(t *testing.T) {
	newFixture := func(t *testing.T) (string, *Manifest, SegmentRef) {
		t.Helper()
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, "latest"), 0o755); err != nil {
			t.Fatal(err)
		}
		ref := retiredMetadataTestRef("latest/missing.json", 1)
		return dir, retiredMetadataTestManifest(ref), ref
	}
	t.Run("pending migration journal", func(t *testing.T) {
		dir, m, _ := newFixture(t)
		if err := os.WriteFile(filepath.Join(dir, historyReferenceMigrationJournalFile), []byte("pending"), 0o600); err != nil {
			t.Fatal(err)
		}
		got := retiredMetadataPrepareTest(t, dir, m, retiredMetadataSweepCursor{}, 32, retiredMetadataOS)
		if got.Deferred != retiredMetadataDeferredMigration || got.Scanned != 0 || got.Manifest != m {
			t.Fatalf("journal sweep = %+v", got)
		}
	})
	t.Run("journal check error", func(t *testing.T) {
		dir, m, _ := newFixture(t)
		fs := retiredMetadataOS
		base := fs.lstat
		fs.lstat = func(path string) (os.FileInfo, error) {
			if filepath.Base(path) == historyReferenceMigrationJournalFile {
				return nil, os.ErrPermission
			}
			return base(path)
		}
		got := retiredMetadataPrepareTest(t, dir, m, retiredMetadataSweepCursor{}, 32, fs)
		if got.Deferred != retiredMetadataDeferredMigration || got.Scanned != 0 {
			t.Fatalf("journal error sweep = %+v", got)
		}
	})
	t.Run("leaf check error", func(t *testing.T) {
		dir, m, ref := newFixture(t)
		fs := retiredMetadataOS
		base := fs.lstat
		fs.lstat = func(path string) (os.FileInfo, error) {
			if path == filepath.Join(dir, filepath.FromSlash(ref.Path)) {
				return nil, os.ErrPermission
			}
			return base(path)
		}
		got := retiredMetadataPrepareTest(t, dir, m, retiredMetadataSweepCursor{}, 32, fs)
		if got.Removed != 0 || len(got.Manifest.Retired) != 1 {
			t.Fatalf("leaf error sweep = %+v", got)
		}
	})
	t.Run("legacy catalog", func(t *testing.T) {
		dir, m, _ := newFixture(t)
		if err := os.WriteFile(filepath.Join(dir, SnapshotCatalogFile), []byte("legacy-or-invalid"), 0o644); err != nil {
			t.Fatal(err)
		}
		got := retiredMetadataPrepareTest(t, dir, m, retiredMetadataSweepCursor{}, 32, retiredMetadataOS)
		if got.Deferred != retiredMetadataDeferredCatalog || got.Scanned != 0 {
			t.Fatalf("catalog sweep = %+v", got)
		}
	})
	t.Run("published lease", func(t *testing.T) {
		dir, m, _ := newFixture(t)
		published := filepath.Join(dir, SnapshotPublishedDir)
		if err := os.MkdirAll(published, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(published, "manifest-possible.json"), []byte("lease"), 0o644); err != nil {
			t.Fatal(err)
		}
		got := retiredMetadataPrepareTest(t, dir, m, retiredMetadataSweepCursor{}, 32, retiredMetadataOS)
		if got.Deferred != retiredMetadataDeferredPublished || got.Scanned != 0 {
			t.Fatalf("published sweep = %+v", got)
		}
	})
	t.Run("published directory error", func(t *testing.T) {
		dir, m, _ := newFixture(t)
		published := filepath.Join(dir, SnapshotPublishedDir)
		if err := os.MkdirAll(published, 0o755); err != nil {
			t.Fatal(err)
		}
		fs := retiredMetadataOS
		fs.readDir = func(path string) ([]os.DirEntry, error) {
			if path == published {
				return nil, os.ErrPermission
			}
			return os.ReadDir(path)
		}
		got := retiredMetadataPrepareTest(t, dir, m, retiredMetadataSweepCursor{}, 32, fs)
		if got.Deferred != retiredMetadataDeferredPublished || got.Scanned != 0 {
			t.Fatalf("published error sweep = %+v", got)
		}
	})
}

func TestPrepareRetiredMetadataSweepMixedMissingParentsProgresses(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "latest"), 0o755); err != nil {
		t.Fatal(err)
	}
	bad := retiredMetadataTestRef("absent-parent/bad.json", 1)
	good := retiredMetadataTestRef("latest/good.json", 2)
	m := retiredMetadataTestManifest(bad, good)
	got := retiredMetadataPrepareTest(t, dir, m, retiredMetadataSweepCursor{}, 32, retiredMetadataOS)
	if got.Removed != 1 || len(got.Manifest.Retired) != 1 || got.Manifest.Retired[0].Path != bad.Path {
		t.Fatalf("mixed-parent sweep = %+v", got)
	}
}

func TestRetiredMetadataSweepCursorBoundedAndMutationSafe(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "latest"), 0o755); err != nil {
		t.Fatal(err)
	}
	shared := retiredMetadataTestRef("latest/shared.json", 7)
	refs := make([]SegmentRef, 5)
	for i := range refs {
		refs[i] = shared
		refs[i].AggregationSteps = uint64(i + 1)
		refs[i].Size = uint64(i + 1)
		refs[i].Checksum = fmt.Sprintf("sha256:%d", i)
	}
	if err := os.WriteFile(filepath.Join(dir, shared.Path), []byte("present"), 0o644); err != nil {
		t.Fatal(err)
	}
	m := retiredMetadataTestManifest(refs...)
	fs := retiredMetadataOS
	baseLstat := fs.lstat
	sharedStats := 0
	fs.lstat = func(path string) (os.FileInfo, error) {
		if path == filepath.Join(dir, shared.Path) {
			sharedStats++
		}
		return baseLstat(path)
	}
	first := retiredMetadataPrepareTest(t, dir, m, retiredMetadataSweepCursor{}, 2, fs)
	if first.Scanned != 2 || first.Removed != 0 || first.Next.Offset != 2 {
		t.Fatalf("first sweep = %+v", first)
	}
	if sharedStats != 1 {
		t.Fatalf("same-path rows caused %d leaf stats, want 1", sharedStats)
	}
	if err := os.Remove(filepath.Join(dir, shared.Path)); err != nil {
		t.Fatal(err)
	}
	cursor := first.Next
	m = first.Manifest
	for pass := 0; len(m.Retired) > 0 && pass < 8; pass++ {
		got := retiredMetadataPrepareTest(t, dir, m, cursor, 2, retiredMetadataOS)
		if got.Scanned > 2 {
			t.Fatalf("pass %d scanned %d", pass, got.Scanned)
		}
		m, cursor = got.Manifest, got.Next
	}
	if len(m.Retired) != 0 {
		t.Fatalf("same-key group was permanently skipped: %d refs remain", len(m.Retired))
	}

	// Insert a new, lower-sorted missing ref after the cursor has advanced.
	refs = []SegmentRef{
		retiredMetadataTestRef("latest/b.json", 2),
		retiredMetadataTestRef("latest/c.json", 3),
		retiredMetadataTestRef("latest/d.json", 4),
	}
	m = retiredMetadataTestManifest(refs...)
	first = retiredMetadataPrepareTest(t, dir, m, retiredMetadataSweepCursor{}, 1, retiredMetadataOS)
	m, cursor = first.Manifest, first.Next
	m.Retired = append(m.Retired, retiredMetadataTestRef("latest/a.json", 1))
	sortSegments(m.Retired)
	for pass := 0; len(m.Retired) > 0 && pass < 8; pass++ {
		got := retiredMetadataPrepareTest(t, dir, m, cursor, 1, retiredMetadataOS)
		m, cursor = got.Manifest, got.Next
	}
	if len(m.Retired) != 0 {
		t.Fatalf("manifest mutation was permanently skipped: %+v", m.Retired)
	}
}

func TestRunnerRetiredMetadataCursorCommitsOnlyAfterIntegration(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "latest"), 0o755); err != nil {
		t.Fatal(err)
	}
	old := retiredMetadataTestManifest(retiredMetadataTestRef("latest/missing.json", 1))
	wantOld := cloneManifest(old)
	runner := &Runner{cfg: Config{Dir: dir}}
	aggregator := NewAggregator(dir)
	valid := retiredMetadataTestRef("latest/new.json", 100)
	// A directory at manifest.json makes the final atomic rename fail without
	// relying on platform- or privilege-dependent directory permissions.
	if err := os.Mkdir(filepath.Join(dir, ManifestFile), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, sweep, err := runner.integrateWithRetiredMetadataSweep(context.Background(), aggregator, 0, 100, []SegmentRef{valid}, old); err == nil {
		t.Fatal("manifest publication unexpectedly succeeded")
	} else if sweep.Removed != 1 || runner.retiredMetadataCursor.Valid || !reflect.DeepEqual(old, wantOld) {
		t.Fatalf("failed integration committed cursor: sweep=%+v cursor=%+v", sweep, runner.retiredMetadataCursor)
	}
	if err := os.Remove(filepath.Join(dir, ManifestFile)); err != nil {
		t.Fatal(err)
	}
	// The unchanged cursor models both retry and process restart: the same
	// missing metadata is reconsidered until a real publication succeeds.
	manifest, sweep, err := runner.integrateWithRetiredMetadataSweep(context.Background(), aggregator, 0, 100, []SegmentRef{valid}, old)
	if err != nil {
		t.Fatalf("successful integration: %v", err)
	}
	if sweep.Removed != 1 || !runner.retiredMetadataCursor.Valid || len(manifest.Retired) != 0 {
		t.Fatalf("successful integration did not commit sweep: sweep=%+v cursor=%+v retired=%d", sweep, runner.retiredMetadataCursor, len(manifest.Retired))
	}
	loaded, err := LoadProductionManifest(dir)
	if err != nil || len(loaded.Retired) != 0 {
		t.Fatalf("disk manifest retained missing metadata: retired=%d err=%v", len(loaded.Retired), err)
	}
}

func TestRunnerRetiredMetadataSweepCancellationAndGuardRace(t *testing.T) {
	newFixture := func(t *testing.T) (string, *Manifest, SegmentRef) {
		t.Helper()
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, "latest"), 0o755); err != nil {
			t.Fatal(err)
		}
		ref := retiredMetadataTestRef("latest/missing.json", 1)
		return dir, retiredMetadataTestManifest(ref), ref
	}
	t.Run("cancel during leaf stat", func(t *testing.T) {
		dir, old, retired := newFixture(t)
		wantOld := cloneManifest(old)
		ctx, cancel := context.WithCancel(context.Background())
		fs := retiredMetadataOS
		base := fs.lstat
		fs.lstat = func(path string) (os.FileInfo, error) {
			if path == filepath.Join(dir, retired.Path) {
				cancel()
				return nil, os.ErrNotExist
			}
			return base(path)
		}
		runner := &Runner{cfg: Config{Dir: dir}}
		_, sweep, err := runner.integrateWithRetiredMetadataSweepFS(ctx, NewAggregator(dir), 0, 100, []SegmentRef{retiredMetadataTestRef("latest/new.json", 100)}, old, fs)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
		if sweep.Removed != 0 || runner.retiredMetadataCursor.Valid || !reflect.DeepEqual(old, wantOld) {
			t.Fatalf("cancel changed metadata state: sweep=%+v cursor=%+v", sweep, runner.retiredMetadataCursor)
		}
		if _, err := os.Lstat(filepath.Join(dir, ManifestFile)); !os.IsNotExist(err) {
			t.Fatalf("cancel published manifest: %v", err)
		}
	})
	t.Run("journal appears before publish", func(t *testing.T) {
		dir, old, retired := newFixture(t)
		fs := retiredMetadataOS
		base := fs.lstat
		created := false
		fs.lstat = func(path string) (os.FileInfo, error) {
			if path == filepath.Join(dir, retired.Path) && !created {
				created = true
				if err := os.WriteFile(filepath.Join(dir, historyReferenceMigrationJournalFile), []byte("pending"), 0o600); err != nil {
					t.Fatal(err)
				}
				return nil, os.ErrNotExist
			}
			return base(path)
		}
		runner := &Runner{cfg: Config{Dir: dir}}
		manifest, sweep, err := runner.integrateWithRetiredMetadataSweepFS(context.Background(), NewAggregator(dir), 0, 100, []SegmentRef{retiredMetadataTestRef("latest/new.json", 100)}, old, fs)
		if err != nil {
			t.Fatal(err)
		}
		if sweep.Removed != 0 || sweep.Deferred != retiredMetadataDeferredMigration || runner.retiredMetadataCursor.Valid {
			t.Fatalf("late guard did not discard GC: sweep=%+v cursor=%+v", sweep, runner.retiredMetadataCursor)
		}
		if len(manifest.Retired) != 1 || manifest.Retired[0].Path != retired.Path {
			t.Fatalf("late guard forgot retired ref: %+v", manifest.Retired)
		}
	})
	t.Run("cancel during final guard", func(t *testing.T) {
		dir, old, _ := newFixture(t)
		wantOld := cloneManifest(old)
		ctx, cancel := context.WithCancel(context.Background())
		fs := retiredMetadataOS
		base := fs.lstat
		journalChecks := 0
		fs.lstat = func(path string) (os.FileInfo, error) {
			if path == filepath.Join(dir, historyReferenceMigrationJournalFile) {
				journalChecks++
				if journalChecks == 2 {
					cancel()
				}
			}
			return base(path)
		}
		runner := &Runner{cfg: Config{Dir: dir}}
		_, sweep, err := runner.integrateWithRetiredMetadataSweepFS(ctx, NewAggregator(dir), 0, 100, []SegmentRef{retiredMetadataTestRef("latest/new.json", 100)}, old, fs)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
		if journalChecks != 2 || sweep.Removed != 1 || runner.retiredMetadataCursor.Valid || !reflect.DeepEqual(old, wantOld) {
			t.Fatalf("final-guard cancel changed metadata state: checks=%d sweep=%+v cursor=%+v", journalChecks, sweep, runner.retiredMetadataCursor)
		}
		if _, err := os.Lstat(filepath.Join(dir, ManifestFile)); !os.IsNotExist(err) {
			t.Fatalf("final-guard cancel published manifest: %v", err)
		}
	})
}

func BenchmarkRetiredMetadataSmallIncrementPublish(b *testing.B) {
	const (
		activeCount  = 124088
		retiredCount = 130290
	)
	for _, tc := range []struct {
		name        string
		retiredRows int
		deferGC     bool
		wantScanned int
	}{
		{name: "original_manifest_baseline", retiredRows: retiredCount, deferGC: true},
		{name: "first_bounded_missing_gc", retiredRows: retiredCount, wantScanned: retiredMetadataSweepLimit},
		{name: "steady_after_retired_cleared"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			dir := b.TempDir()
			if err := os.Mkdir(filepath.Join(dir, "latest"), 0o755); err != nil {
				b.Fatal(err)
			}
			if tc.deferGC {
				if err := os.WriteFile(filepath.Join(dir, SnapshotCatalogFile), []byte("defer benchmark GC"), 0o644); err != nil {
					b.Fatal(err)
				}
			}
			old := retiredMetadataTestManifest()
			old.Generation = 52676
			old.VisibleTxEnd = 400000
			old.Segments = make([]SegmentRef, activeCount)
			for i := range old.Segments {
				old.Segments[i] = retiredMetadataTestRef(fmt.Sprintf("latest/active-%06d.json", i), uint64(i+1))
			}
			old.Retired = make([]SegmentRef, tc.retiredRows)
			for i := range old.Retired {
				old.Retired[i] = retiredMetadataTestRef(fmt.Sprintf("latest/retired-%06d.json", i), uint64(activeCount+i+1))
			}
			runner := &Runner{cfg: Config{Dir: dir}}
			aggregator := NewAggregator(dir)
			var scanned int
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ref := retiredMetadataTestRef(fmt.Sprintf("latest/increment-%06d.json", i), 300000+uint64(i))
				manifest, sweep, err := runner.integrateWithRetiredMetadataSweep(context.Background(), aggregator, 0, ref.ToTxNum, []SegmentRef{ref}, old)
				if err != nil {
					b.Fatal(err)
				}
				loaded, err := LoadProductionManifest(dir)
				if err != nil {
					b.Fatal(err)
				}
				old = loaded
				scanned += sweep.Scanned
				if manifest.Generation != loaded.Generation {
					b.Fatal("loaded generation mismatch")
				}
			}
			b.ReportMetric(float64(scanned)/float64(b.N), "retired-scanned/op")
			b.ReportMetric(float64((retiredCount+retiredMetadataSweepLimit-1)/retiredMetadataSweepLimit), "passes/full-sweep")
			if b.N == 1 && scanned != tc.wantScanned {
				b.Fatalf("scanned = %d, want %d", scanned, tc.wantScanned)
			}
		})
	}
}
