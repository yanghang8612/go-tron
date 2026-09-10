package snapshots

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// Frozen pre-seeding publication path. Both benchmark paths perform the same
// validation, JSON encoding, file fsync, atomic rename and directory fsync.
func legacyPublishManifestWithoutSeed(dir string, manifest *Manifest) error {
	if manifest == nil {
		return errors.New("snapshots: nil manifest")
	}
	if manifest.Version == 0 {
		manifest.Version = CurrentManifestVersion
	}
	if manifest.Generation == 0 {
		manifest.Generation = 1
	}
	normalizeChainIdentity(manifest.Chain)
	if manifest.PublishedUnix == 0 {
		manifest.PublishedUnix = time.Now().Unix()
	}
	sortSegments(manifest.Segments)
	sortSegments(manifest.Retired)
	if err := manifest.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".manifest-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, filepath.Join(dir, ManifestFile)); err != nil {
		return err
	}
	return syncSnapshotDir(dir)
}

func BenchmarkManifestPublishThenLoad(b *testing.B) {
	// Same observed active/retired counts used by the previous budget benchmark;
	// paths/checksums are synthetic and no segment contents are opened.
	for _, variant := range []struct {
		name    string
		publish func(string, *Manifest) error
	}{{"legacy", legacyPublishManifestWithoutSeed}, {"seeded", PublishManifest}} {
		b.Run(variant.name, func(b *testing.B) {
			b.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
			b.Setenv(manifestCacheBudgetEnv, "")
			loadedManifestCache.clear()
			b.Cleanup(loadedManifestCache.clear)
			m := cachedManifestLargeCatalogFixture(false)
			dir := b.TempDir()
			if err := variant.publish(dir, m); err != nil {
				b.Fatal(err)
			}
			got, err := LoadProductionManifest(dir)
			if err != nil || !reflect.DeepEqual(got, m) {
				b.Fatalf("publication load changed its input: %v", err)
			}
			misses, seeds := manifestCacheMisses.Snapshot().Count(), manifestCachePublicationSeeds.Snapshot().Count()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				m.Generation++
				if err := variant.publish(dir, m); err != nil {
					b.Fatal(err)
				}
				got, err = LoadProductionManifest(dir)
				if err != nil || got.Generation != m.Generation || len(got.Segments) != len(m.Segments) || len(got.Retired) != len(m.Retired) {
					b.Fatalf("publication load changed: %v", err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(manifestCacheMisses.Snapshot().Count()-misses)/float64(b.N), "decodes/op")
			b.ReportMetric(float64(manifestCachePublicationSeeds.Snapshot().Count()-seeds)/float64(b.N), "seeds/op")
			if !reflect.DeepEqual(got, m) {
				b.Fatal("final detached manifest changed")
			}
		})
	}
}
