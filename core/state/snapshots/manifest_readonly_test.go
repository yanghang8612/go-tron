package snapshots

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestManifestReadOnlyKeepsPublicOwnershipAndOldGeneration(t *testing.T) {
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	t.Setenv(manifestCacheBudgetEnv, "")
	loadedManifestCache.clear()
	t.Cleanup(loadedManifestCache.clear)
	dir := t.TempDir()
	original := cachedManifestFixture(4)
	if err := PublishManifest(dir, original); err != nil {
		t.Fatal(err)
	}
	view, err := loadProductionManifestReadOnly(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := cloneDecodedManifest(view)
	second, err := loadProductionManifestReadOnly(dir)
	if err != nil || second != view {
		t.Fatal("warm private loads did not borrow the immutable generation", err)
	}
	public, err := LoadProductionManifest(dir)
	if err != nil || public == view || &public.Segments[0] == &view.Segments[0] {
		t.Fatal("public loader exposed borrowed containers", err)
	}
	public.Segments[0].Path = "mutated"
	public.Retired[0].Checksum = "mutated"
	public.Chain.GenesisHash = "mutated"
	public.Progress.LatestBuildTxNum++
	if !reflect.DeepEqual(view, want) {
		t.Fatal("public mutation changed a borrowed generation")
	}
	next := cloneDecodedManifest(original)
	next.Generation++
	if err := PublishManifest(dir, next); err != nil {
		t.Fatal(err)
	}
	current, err := loadProductionManifestReadOnly(dir)
	if err != nil || current == view || current.Generation != next.Generation || !reflect.DeepEqual(view, want) {
		t.Fatal("replacement mutated an existing borrow or missed the new generation", err)
	}
	// The publisher also retains a mutable view; it cannot mutate the cache.
	next.Segments[0].Path = "changed by publisher"
	if current.Segments[0].Path == next.Segments[0].Path {
		t.Fatal("publisher shares private containers")
	}
}

func TestManifestReadOnlyAuthenticatesSameStatAndRejectsMissingOrCorruptFile(t *testing.T) {
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	t.Setenv(manifestCacheBudgetEnv, "")
	loadedManifestCache.clear()
	t.Cleanup(loadedManifestCache.clear)
	dir := t.TempDir()
	if err := PublishManifest(dir, cachedManifestFixture(1200)); err != nil {
		t.Fatal(err)
	}
	view, err := loadProductionManifestReadOnly(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, ManifestFile)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	changed := bytes.Replace(raw, []byte("123450"), []byte("123451"), 1)
	if len(changed) != len(raw) || bytes.Equal(changed, raw) {
		t.Fatal("invalid same-length fixture")
	}
	if err := os.WriteFile(path, changed, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	current, err := loadProductionManifestReadOnly(dir)
	if err != nil || current.PublishedUnix != 123451 || view.PublishedUnix != 123450 {
		t.Fatal("same-stat overwrite reused old bytes", err)
	}
	changed[0] = '!'
	if err := os.WriteFile(path, changed, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadProductionManifestReadOnly(dir); err == nil {
		t.Fatal("corrupt current input returned a cached view")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := loadProductionManifestReadOnly(dir); !os.IsNotExist(err) {
		t.Fatal("missing input returned a cached view", err)
	}
}

func TestManifestReadOnlyDisableAndBudgetRejection(t *testing.T) {
	for _, budget := range []string{"0", "1"} {
		t.Run(budget, func(t *testing.T) {
			t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
			t.Setenv(manifestCacheBudgetEnv, "")
			loadedManifestCache.clear()
			t.Cleanup(loadedManifestCache.clear)
			dir := t.TempDir()
			if err := PublishManifest(dir, cachedManifestFixture(2)); err != nil {
				t.Fatal(err)
			}
			old, err := loadProductionManifestReadOnly(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv(manifestCacheBudgetEnv, budget)
			got, err := loadProductionManifestReadOnly(dir)
			if err != nil || got == old || !reflect.DeepEqual(got, old) {
				t.Fatal("disabled/rejected load changed observed metadata", err)
			}
			loadedManifestCache.mu.Lock()
			resident := loadedManifestCache.manifest
			loadedManifestCache.mu.Unlock()
			if resident != nil {
				t.Fatal("old resident retained past reduced budget")
			}
		})
	}
}

func TestManifestReadOnlyConcurrentPublication(t *testing.T) {
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	t.Setenv(manifestCacheBudgetEnv, "")
	loadedManifestCache.clear()
	t.Cleanup(loadedManifestCache.clear)
	dir := t.TempDir()
	initial := cachedManifestFixture(8)
	if err := PublishManifest(dir, initial); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errors := make(chan error, 3)
	for worker := 0; worker < 3; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				if worker == 0 {
					next := cloneDecodedManifest(initial)
					next.Generation += uint64(i + 1)
					if err := PublishManifest(dir, next); err != nil {
						errors <- err
						return
					}
					continue
				}
				view, err := loadProductionManifestReadOnly(dir)
				if err != nil {
					errors <- err
					return
				}
				before := cloneDecodedManifest(view)
				public, err := LoadProductionManifest(dir)
				if err != nil {
					errors <- err
					return
				}
				public.Segments[0].Path = "caller-owned"
				if !reflect.DeepEqual(view, before) {
					errors <- fmt.Errorf("borrowed generation mutated during publication")
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
}

// Same authenticated file, same coverage decision; only the private readonly
// borrow versus the public detached-container contract differs. This includes
// the full current-byte comparison rather than timing an in-memory clone.
func BenchmarkManifestReadOnlyCoverage(b *testing.B) {
	for _, n := range []int{8, 5000, 12000} {
		for _, mode := range []struct {
			name string
			load func(string) (*Manifest, error)
		}{{"public-copy", LoadProductionManifest}, {"private-readonly", loadProductionManifestReadOnly}} {
			b.Run(fmt.Sprintf("%d/%s", n, mode.name), func(b *testing.B) {
				b.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
				b.Setenv(manifestCacheBudgetEnv, "")
				loadedManifestCache.clear()
				b.Cleanup(loadedManifestCache.clear)
				dir := b.TempDir()
				fixture := cachedManifestFixture(n)
				fixture.Retired = nil
				if err := PublishManifest(dir, fixture); err != nil {
					b.Fatal(err)
				}
				first, err := mode.load(dir)
				if err != nil {
					b.Fatal(err)
				}
				want, err := coldSnapshotVisibleTxEndFromManifest(first, SegmentDatasetStateDomainChange)
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					m, err := mode.load(dir)
					if err != nil {
						b.Fatal(err)
					}
					if got, err := coldSnapshotVisibleTxEndFromManifest(m, SegmentDatasetStateDomainChange); err != nil || got != want {
						b.Fatalf("coverage changed: %d/%d %v", got, want, err)
					}
				}
			})
		}
	}
}
