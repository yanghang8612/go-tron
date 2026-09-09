package snapshots

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// Fixed enabled-cache baseline from the previous loader: full ReadFile, whole
// byte comparison, decoded-capacity admission at 64 MiB and detached copies.
// Fixture arrays are nonempty, so cloneManifest is the old clone's exact path.
type legacyManifestCache64 struct {
	manifestDecodeCache
	candidate uint64
}

func (c *legacyManifestCache64) load(dir string) (*Manifest, error) {
	data, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		return nil, err
	}
	if uint64(cap(data))*2 > 64<<20 {
		return decodeProductionManifest(data)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.manifest != nil && bytes.Equal(data, c.data) {
		manifestCacheHits.Inc(1)
		if c.productionErr != nil {
			return nil, c.productionErr
		}
		return cloneManifest(c.manifest), nil
	}
	manifestCacheMisses.Inc(1)
	m, err := decodeManifest(data)
	if err != nil {
		return nil, err
	}
	productionErr := m.ValidateProduction()
	charge := manifestCacheCharge(data, m)
	c.candidate = charge
	if charge <= 64<<20 {
		c.data, c.manifest, c.productionErr, c.charge = data, cloneManifest(m), productionErr, charge
		manifestCacheResident.Update(int64(charge))
	} else {
		manifestCacheBypasses.Inc(1)
	}
	if productionErr != nil {
		return nil, productionErr
	}
	return m, nil
}

func BenchmarkManifestCacheBudgetCatalog(b *testing.B) {
	for _, shape := range []struct {
		name      string
		longPaths bool
	}{{"production-counts-synthetic-paths", false}, {"long-path-budget-pressure", true}} {
		b.Run(shape.name, func(b *testing.B) {
			raw, err := json.Marshal(cachedManifestLargeCatalogFixture(shape.longPaths))
			if err != nil {
				b.Fatal(err)
			}
			want, err := decodeProductionManifest(raw)
			if err != nil {
				b.Fatal(err)
			}
			dir := b.TempDir()
			if err := os.WriteFile(filepath.Join(dir, ManifestFile), raw, 0o600); err != nil {
				b.Fatal(err)
			}
			b.Logf("synthetic catalog, no segment I/O: json=%d active=%d retired=%d", len(raw), len(want.Segments), len(want.Retired))
			for _, mode := range []string{"legacy64", "compact256"} {
				for _, workload := range []string{"warm", "cold"} {
					b.Run(mode+"/"+workload, func(b *testing.B) {
						b.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
						b.Setenv(manifestCacheBudgetEnv, "268435456")
						var legacy legacyManifestCache64
						load := func() (*Manifest, error) { return LoadProductionManifest(dir) }
						clear := loadedManifestCache.clear
						if mode == "legacy64" {
							load = func() (*Manifest, error) { return legacy.load(dir) }
							clear = legacy.clear
						}
						clear()
						got, err := load()
						if err != nil || !reflect.DeepEqual(got, want) {
							b.Fatalf("first result differs: %v", err)
						}
						b.ReportAllocs()
						b.ResetTimer()
						for b.Loop() {
							if workload == "cold" {
								clear()
							}
							got, err = load()
							if err != nil {
								b.Fatal(err)
							}
						}
						b.StopTimer()
						if !reflect.DeepEqual(got, want) {
							b.Fatal("last result differs")
						}
						charge, candidate := loadedManifestCache.charge, uint64(manifestCacheCandidate.Snapshot().Value())
						if mode == "legacy64" {
							charge, candidate = legacy.charge, legacy.candidate
						}
						b.ReportMetric(float64(len(raw)), "json-B")
						b.ReportMetric(float64(charge), "resident-charge-B")
						b.ReportMetric(float64(candidate), "candidate-charge-B")
						clear()
					})
				}
			}
		})
	}
}
