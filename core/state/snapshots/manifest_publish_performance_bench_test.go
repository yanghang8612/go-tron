package snapshots

import (
	"os"
	"path/filepath"
	"testing"
)

// Complete durable publication plus the next production load. Both revisions
// use identical 162k-reference metadata and cache settings. No segment files
// are opened; the fixture is synthetic metadata, not a server timing replay.
func BenchmarkManifestArchivePublication(b *testing.B) {
	for _, mode := range []string{"publish-load", "integrate-publish-load"} {
		b.Run(mode, func(b *testing.B) {
			b.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
			b.Setenv(manifestCacheBudgetEnv, "268435456")
			loadedManifestCache.clear()
			b.Cleanup(loadedManifestCache.clear)
			base := manifestValidationMixedFixture()
			if err := base.Validate(); err != nil {
				b.Fatal(err)
			}
			if err := base.ValidateProduction(); err != nil {
				b.Fatal(err)
			}
			refs := manifestValidationFixture(1).Segments
			for i := range refs {
				refs[i].Size, refs[i].Checksum = base.Segments[0].Size, base.Segments[0].Checksum
			}
			dir := b.TempDir()
			a := NewAggregator(dir)
			if err := PublishManifest(dir, base); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				base.Generation++
				generation := base.Generation
				if mode == "publish-load" {
					if err := PublishManifest(dir, base); err != nil {
						b.Fatal(err)
					}
				} else {
					m, err := a.integrateWithManifest(base.VisibleTxStart, base.VisibleTxEnd, refs, base)
					if err != nil {
						b.Fatal(err)
					}
					generation = m.Generation
				}
				m, err := LoadProductionManifest(dir)
				if err != nil || m.Generation != generation || len(m.Segments) != len(base.Segments) {
					b.Fatalf("load publication: %v", err)
				}
			}
			b.StopTimer()
			info, err := os.Stat(filepath.Join(dir, ManifestFile))
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(info.Size()), "json-B/op")
			b.ReportMetric(float64(len(base.Segments)), "active-refs")
			b.ReportMetric(float64(len(base.Retired)), "retired-refs")
		})
	}
}
