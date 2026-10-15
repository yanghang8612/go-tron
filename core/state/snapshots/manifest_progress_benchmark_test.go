package snapshots

import "testing"

// This complete fixed-input benchmark also compiles unchanged on ff6f0820.
// The 70,198 active/92,667 retired fixture models the production-sized catalog.
func BenchmarkManifestProgressMaintenance(b *testing.B) {
	for _, mode := range []string{"advance", "noop", "publish-load"} {
		b.Run(mode, func(b *testing.B) {
			b.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
			b.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE_BUDGET_BYTES", "268435456")
			loadedManifestCache.clear()
			b.Cleanup(loadedManifestCache.clear)
			m := manifestValidationMixedFixture()
			m.Progress = &Progress{HotPruneBlockNum: 1, HotPruneTxNum: 1}
			dir := b.TempDir()
			if err := PublishManifest(dir, m); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				switch mode {
				case "advance":
					if err := UpdateHotPruneProgress(dir, uint64(i+2), uint64(i+2)); err != nil {
						b.Fatal(err)
					}
				case "noop":
					if err := UpdateHotPruneProgress(dir, 1, 1); err != nil {
						b.Fatal(err)
					}
				case "publish-load":
					m.Generation++
					if err := PublishManifest(dir, m); err != nil {
						b.Fatal(err)
					}
					loaded, err := LoadProductionManifest(dir)
					if err != nil || loaded.Generation != m.Generation {
						b.Fatal("load", err)
					}
				}
			}
			b.StopTimer()
			loaded, err := LoadProductionManifest(dir)
			if err != nil {
				b.Fatal(err)
			}
			want := uint64(1)
			if mode == "advance" {
				want = uint64(b.N + 1)
			}
			if loaded.Progress.HotPruneTxNum != want || len(loaded.Segments) != len(m.Segments) {
				b.Fatal("publication mismatch")
			}
		})
	}
}
