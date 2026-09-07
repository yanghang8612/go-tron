package snapshots

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func cachedManifestFixture(n int) *Manifest {
	m := manifestValidationFixture(n)
	m.Generation, m.PublishedUnix = 7, 123450
	m.Chain = &ChainIdentity{ChainID: 1, NetworkID: 11111, GenesisHash: strings.Repeat("a", 64)}
	m.Progress = &Progress{LatestBuildTxNum: 7}
	for i := range m.Segments {
		m.Segments[i].Size = 1024
		m.Segments[i].Checksum = "sha256:" + strings.Repeat("b", 64)
	}
	m.Retired = append([]SegmentRef(nil), m.Segments...)
	return m
}

func TestManifestCacheDetachedViews(t *testing.T) {
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	dir := t.TempDir()
	if err := PublishManifest(dir, cachedManifestFixture(4)); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	want, err := decodeProductionManifest(raw)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		got, err := LoadProductionManifest(dir)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("view changed: %v", err)
		}
		got.Generation++
		got.Chain.GenesisHash = "bad"
		got.Progress.LatestBuildTxNum++
		got.Segments[0].Path = "mutated"
		got.Retired[0].Checksum = "mutated"
		got.Segments = append(got.Segments, SegmentRef{})
	}
	got, err := LoadManifest(dir)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("base view changed: %v", err)
	}
	// A caller may modify and publish its own copy without poisoning the old
	// cached view or requiring a process-global publication notification.
	got.PublishedUnix++
	if err := PublishManifest(dir, got); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadProductionManifest(dir)
	if err != nil || loaded.PublishedUnix != got.PublishedUnix {
		t.Fatalf("published update invisible: %v", err)
	}
}

func TestManifestCacheAuthenticatesCurrentBytes(t *testing.T) {
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	dir := t.TempDir()
	if err := PublishManifest(dir, cachedManifestFixture(2)); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, ManifestFile)
	old, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadProductionManifest(dir); err != nil {
		t.Fatal(err)
	}
	next := bytes.Replace(old, []byte("123450"), []byte("123451"), 1)
	if len(next) != len(old) || bytes.Equal(next, old) {
		t.Fatal("fixture did not change")
	}
	if err := os.WriteFile(path, next, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	got, err := LoadProductionManifest(dir)
	if err != nil || got.PublishedUnix != 123451 || got.Generation != 7 {
		t.Fatalf("same-stat update missed: %v", err)
	}
	next[0] = '!'
	if err := os.WriteFile(path, next, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadProductionManifest(dir); err == nil {
		t.Fatal("invalid bytes reused old manifest")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadProductionManifest(dir); !os.IsNotExist(err) {
		t.Fatalf("missing file: %v", err)
	}
}

func TestManifestCacheKeepsProductionValidation(t *testing.T) {
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	dir := t.TempDir()
	m := NewManifest(100, 150, []SegmentRef{{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: 100, ToTxNum: 150, Path: "history/state-domain-change-100-150.json"}})
	if err := PublishManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := LoadManifest(dir); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadProductionManifest(dir); err == nil {
			t.Fatal("base validation bypassed production gate")
		}
	}
}

func TestManifestCachePreservesEmptyArrays(t *testing.T) {
	for _, raw := range []string{`{"version":1,"visibleTxStart":0,"visibleTxEnd":0,"segments":[]}`, `{"version":1,"visibleTxStart":0,"visibleTxEnd":0,"segments":null,"retired":[]}`} {
		var cache manifestDecodeCache
		want, err := decodeManifest([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		for range 2 {
			got, err := cache.decodeOwned([]byte(raw), false, manifestCacheMaxBytes)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("empty slice shape changed: %v", err)
			}
		}
	}
}

func TestManifestCacheBudgetAndDisable(t *testing.T) {
	raw, err := json.Marshal(cachedManifestFixture(8))
	if err != nil {
		t.Fatal(err)
	}
	var cache manifestDecodeCache
	want, err := decodeProductionManifest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.decodeOwned(bytes.Clone(raw), true, manifestCacheMaxBytes); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		got, err := cache.decodeOwned(bytes.Clone(raw), true, 1)
		if err != nil || !reflect.DeepEqual(got, want) || cache.manifest != nil {
			t.Fatalf("oversized entry retained: %v", err)
		}
	}
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ManifestFile), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadProductionManifest(dir); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "0")
	got, err := LoadProductionManifest(dir)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("disabled result changed: %v", err)
	}
	loadedManifestCache.mu.Lock()
	empty := loadedManifestCache.manifest == nil && loadedManifestCache.data == nil && loadedManifestCache.charge == 0
	loadedManifestCache.mu.Unlock()
	if !empty {
		t.Fatal("disable retained cache")
	}
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "invalid")
	if _, err := LoadManifest(dir); err == nil {
		t.Fatal("invalid configuration accepted")
	}
}

func TestManifestCacheConcurrentPublication(t *testing.T) {
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	dir := t.TempDir()
	if err := PublishManifest(dir, cachedManifestFixture(12)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 5)
	start := make(chan struct{})
	for worker := 0; worker < 5; worker++ {
		wg.Add(1)
		go func(writer bool) {
			defer wg.Done()
			<-start
			for i := 0; i < 100; i++ {
				if writer {
					m := cachedManifestFixture(12)
					m.Generation, m.Progress.LatestBuildTxNum = uint64(i+8), uint64(i+8)
					if err := PublishManifest(dir, m); err != nil {
						errs <- err
						return
					}
					continue
				}
				m, err := LoadProductionManifest(dir)
				if err != nil {
					errs <- err
					return
				}
				if m.Generation != m.Progress.LatestBuildTxNum || m.Segments[0].Size != 1024 {
					errs <- fmt.Errorf("mixed or shared generation")
					return
				}
				m.Segments[0].Size++
				m.Progress.LatestBuildTxNum++
			}
		}(worker == 0)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestManifestCacheDoesNotCacheSegmentVerification(t *testing.T) {
	t.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
	dir := t.TempDir()
	refs := writeCompactionStateDomainChangeSegment(t, dir, 1, 1, binaryStateDomainChange(1, 1, 1, "original"))
	if err := PublishManifest(dir, NewManifest(1, 1, refs)); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyManifestFiles(dir, VerifyManifestOptions{RequireChecksums: true}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, refs[0].Path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 1
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyManifestFiles(dir, VerifyManifestOptions{RequireChecksums: true}); err == nil {
		t.Fatal("cached metadata concealed corrupt segment")
	}
}

func benchmarkManifestCacheModes(b *testing.B, dir string, workloads []string) {
	for _, workload := range workloads {
		b.Run(workload, func(b *testing.B) {
			for _, mode := range []string{"0", "1"} {
				b.Run("cache="+mode, func(b *testing.B) {
					b.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", mode)
					if _, err := LoadProductionManifest(dir); err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					if workload == "parallel" {
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								if _, err := LoadProductionManifest(dir); err != nil {
									b.Error(err)
									return
								}
							}
						})
					} else {
						for b.Loop() {
							if workload == "cold" {
								loadedManifestCache.clear()
							}
							if _, err := LoadProductionManifest(dir); err != nil {
								b.Fatal(err)
							}
						}
					}
					b.StopTimer()
					loadedManifestCache.mu.Lock()
					b.ReportMetric(float64(loadedManifestCache.charge), "resident-charge-B")
					loadedManifestCache.mu.Unlock()
				})
			}
		})
	}
}

func BenchmarkManifestCacheLoad(b *testing.B) {
	for _, groups := range []int{512, 2048, 6000} {
		b.Run(fmt.Sprintf("groups=%d", groups), func(b *testing.B) {
			dir := b.TempDir()
			if err := PublishManifest(dir, cachedManifestFixture(groups)); err != nil {
				b.Fatal(err)
			}
			benchmarkManifestCacheModes(b, dir, []string{"warm", "cold", "parallel"})
		})
	}
}

// Use an immutable copy of a production manifest; no segment files or database
// are opened, and the source copy is never changed by this benchmark.
func BenchmarkManifestCacheProduction(b *testing.B) {
	path := os.Getenv("GTRON_BENCH_MANIFEST")
	if path == "" {
		b.Skip("set GTRON_BENCH_MANIFEST to a fixed manifest copy")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		b.Fatal(err)
	}
	m, err := decodeProductionManifest(raw)
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("fixture: bytes=%d generation=%d active=%d retired=%d", len(raw), m.Generation, len(m.Segments), len(m.Retired))
	dir := b.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ManifestFile), raw, 0o600); err != nil {
		b.Fatal(err)
	}
	benchmarkManifestCacheModes(b, dir, []string{"warm", "cold", "parallel"})
}
