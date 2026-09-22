package snapshots

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// Frozen pre-streaming loader: every public load allocated the complete JSON
// with os.ReadFile before checking the same validated decode cache.
func baselineManifestReadFileLoad(dir string) (*Manifest, error) {
	budget, err := manifestCacheConfiguredBudget()
	if err != nil {
		return nil, err
	}
	loadedManifestCache.mu.Lock()
	loadedManifestCache.resizeLocked(budget)
	loadedManifestCache.mu.Unlock()
	data, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		return nil, err
	}
	if budget == 0 {
		manifestCacheBypasses.Inc(1)
		return decodeProductionManifest(data)
	}
	return loadedManifestCache.decodeOwned(data, true, budget)
}

var manifestStreamBenchmarkSink *Manifest

func BenchmarkManifestStreamLifecycle(b *testing.B) {
	for _, shape := range []struct {
		name     string
		manifest *Manifest
	}{
		{"small", cachedManifestFixture(8)},
		{"medium", cachedManifestFixture(5000)},
		{"large-production-counts", cachedManifestLargeCatalogFixture(false)},
	} {
		b.Run(shape.name, func(b *testing.B) {
			for _, mode := range []struct {
				name string
				load func(string) (*Manifest, error)
			}{{"baseline", baselineManifestReadFileLoad}, {"stream", LoadProductionManifest}} {
				for _, workload := range []string{"warm", "first-miss", "equal-length-changes", "same-inode-overwrite", "publish-then-load"} {
					b.Run(workload+"/"+mode.name, func(b *testing.B) {
						b.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
						b.Setenv(manifestCacheBudgetEnv, "")
						loadedManifestCache.clear()
						b.Cleanup(loadedManifestCache.clear)
						dir, otherDir := b.TempDir(), b.TempDir()
						base := cloneDecodedManifest(shape.manifest)
						other := cloneDecodedManifest(shape.manifest)
						other.Generation++
						if err := PublishManifest(dir, base); err != nil {
							b.Fatal(err)
						}
						if err := PublishManifest(otherDir, other); err != nil {
							b.Fatal(err)
						}
						firstBytes, err := os.ReadFile(filepath.Join(dir, ManifestFile))
						if err != nil {
							b.Fatal(err)
						}
						otherBytes, err := os.ReadFile(filepath.Join(otherDir, ManifestFile))
						if err != nil || len(firstBytes) != len(otherBytes) {
							b.Fatalf("equal-length change fixture: %v", err)
						}
						if got, err := mode.load(dir); err != nil || got.Generation != base.Generation {
							b.Fatalf("warmup: %v", err)
						}
						initialGeneration := base.Generation
						generation, alternate := initialGeneration, false
						b.ReportAllocs()
						b.ResetTimer()
						for b.Loop() {
							loadDir := dir
							switch workload {
							case "first-miss":
								loadedManifestCache.clear()
							case "equal-length-changes":
								alternate = !alternate
								if alternate {
									loadDir, generation = otherDir, other.Generation
								} else {
									generation = initialGeneration
								}
							case "same-inode-overwrite":
								alternate = !alternate
								content := firstBytes
								generation = initialGeneration
								if alternate {
									content, generation = otherBytes, other.Generation
								}
								file, err := os.OpenFile(filepath.Join(dir, ManifestFile), os.O_WRONLY, 0)
								if err != nil {
									b.Fatal(err)
								}
								n, writeErr := file.WriteAt(content, 0)
								closeErr := file.Close()
								if writeErr != nil || closeErr != nil || n != len(content) {
									b.Fatalf("same-inode rewrite %d/%d: write=%v close=%v", n, len(content), writeErr, closeErr)
								}
							case "publish-then-load":
								alternate = !alternate
								generation = initialGeneration
								if alternate {
									generation = other.Generation
								}
								base.Generation = generation
								if err := PublishManifest(dir, base); err != nil {
									b.Fatal(err)
								}
							}
							got, err := mode.load(loadDir)
							if err != nil || got == nil || got.Generation != generation {
								b.Fatalf("load generation=%v, want=%d: %v", got, generation, err)
							}
							manifestStreamBenchmarkSink = got
						}
						b.StopTimer()
						loadedManifestCache.mu.Lock()
						charge, dataCap := loadedManifestCache.charge, cap(loadedManifestCache.data)
						loadedManifestCache.mu.Unlock()
						b.ReportMetric(float64(len(firstBytes)), "json-B")
						b.ReportMetric(float64(charge), "resident-charge-B")
						b.ReportMetric(float64(dataCap), "resident-json-cap-B")
					})
				}
			}
		})
	}
}

func BenchmarkManifestStreamGrowth(b *testing.B) {
	base := cachedManifestFixture(200)
	grown := cachedManifestFixture(8000)
	baseRaw, err := json.Marshal(base)
	if err != nil {
		b.Fatal(err)
	}
	grownRaw, err := json.Marshal(grown)
	if err != nil {
		b.Fatal(err)
	}
	for _, mode := range []struct {
		name string
		load func(string) (*Manifest, error)
	}{{"baseline", baselineManifestReadFileLoad}, {"stream", LoadProductionManifest}} {
		b.Run(mode.name, func(b *testing.B) {
			b.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
			b.Setenv(manifestCacheBudgetEnv, "")
			loadedManifestCache.clear()
			b.Cleanup(loadedManifestCache.clear)
			smallDir, largeDir := b.TempDir(), b.TempDir()
			if err := os.WriteFile(filepath.Join(smallDir, ManifestFile), baseRaw, 0o600); err != nil {
				b.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(largeDir, ManifestFile), grownRaw, 0o600); err != nil {
				b.Fatal(err)
			}
			if _, err := mode.load(smallDir); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				got, err := mode.load(largeDir)
				if err != nil || len(got.Segments) != len(grown.Segments) {
					b.Fatalf("grown load: %v", err)
				}
				manifestStreamBenchmarkSink = got
				loadedManifestCache.clear()
				if _, err := mode.load(smallDir); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(len(baseRaw)), "base-json-B")
			b.ReportMetric(float64(len(grownRaw)), "grown-json-B")
		})
	}
}
