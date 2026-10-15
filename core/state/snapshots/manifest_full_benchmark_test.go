package snapshots

import (
	"fmt"
	"os"
	"runtime"
	"testing"
)

// End-to-end metadata operations: real durable file writes, normal cache
// configuration, complete current-file authentication and public detached loads.
// It deliberately does not include segment IO or destructive hot-row deletion.
// This file compiles unchanged on c1e77d5 for the before/after comparison.
func BenchmarkManifestFullMaintenance(b *testing.B) {
	scales := []string{"small", "mainnet"}
	if os.Getenv("GTRON_MANIFEST_VALIDATION_REPLAY") != "" {
		scales = append(scales, "native")
	}
	for _, scale := range scales {
		for _, cache := range []string{"hit", "disabled", "reject"} {
			for _, operation := range []string{"publish-load", "integrate-load-prune", "restart-readonly"} {
				b.Run(scale+"/"+cache+"/"+operation, func(b *testing.B) {
					b.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "1")
					b.Setenv(manifestCacheBudgetEnv, "268435456")
					if cache == "disabled" {
						b.Setenv("GTRON_SNAPSHOT_MANIFEST_CACHE", "0")
					} else if cache == "reject" {
						b.Setenv(manifestCacheBudgetEnv, "1")
					}
					loadedManifestCache.clear()
					b.Cleanup(loadedManifestCache.clear)
					m := manifestValidationMixedFixture()
					if scale == "small" {
						m = manifestValidationFixture(64)
						m.Retired = append([]SegmentRef(nil), m.Segments[:256]...)
					} else if scale == "native" {
						m = validationBenchmarkManifest(b)
					}
					m.Progress = &Progress{HotPruneBlockNum: 1, HotPruneTxNum: 1}
					dir := b.TempDir()
					if err := PublishManifest(dir, m); err != nil {
						b.Fatal(err)
					}
					var refs []SegmentRef
					for _, ref := range m.Segments {
						if ref.Kind == SegmentEventLog {
							refs = append(refs, ref)
							break
						}
					}
					for _, ref := range m.Segments {
						if ref.Kind == SegmentEventLogIndex && ref.FromTxNum == refs[0].FromTxNum && ref.ToTxNum == refs[0].ToTxNum {
							refs = append(refs, ref)
							break
						}
					}
					if len(refs) != 2 {
						b.Fatal("missing integration fixture pair")
					}
					a := NewAggregator(dir)
					var before, after runtime.MemStats
					runtime.ReadMemStats(&before)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						switch operation {
						case "publish-load":
							m.Generation++
							if err := PublishManifest(dir, m); err != nil {
								b.Fatal(err)
							}
							if got, err := LoadProductionManifest(dir); err != nil || got.Generation != m.Generation {
								b.Fatal("load", err)
							}
						case "integrate-load-prune":
							for j := range refs {
								refs[j].Size = uint64(i + 2048)
							}
							if _, err := a.Integrate(m.VisibleTxStart, m.VisibleTxEnd, refs); err != nil {
								b.Fatal(err)
							}
							if _, err := LoadProductionManifest(dir); err != nil {
								b.Fatal(err)
							}
							if err := UpdateHotPruneProgress(dir, uint64(i+2), uint64(i+2)); err != nil {
								b.Fatal(err)
							}
						case "restart-readonly":
							loadedManifestCache.clear()
							if _, err := loadProductionManifestReadOnly(dir); err != nil {
								b.Fatal(err)
							}
						}
					}
					b.StopTimer()
					runtime.ReadMemStats(&after)
					b.ReportMetric(float64(after.NumGC-before.NumGC)/float64(b.N), "GCs/op")
					b.ReportMetric(float64(after.PauseTotalNs-before.PauseTotalNs)/float64(b.N), "GCpause-ns/op")
					b.ReportMetric(float64(len(m.Segments)), "active-refs")
					b.ReportMetric(float64(len(m.Retired)), "retired-refs")
					got, err := LoadProductionManifest(dir)
					if err != nil || len(got.Segments) != len(m.Segments) {
						b.Fatal(fmt.Sprint("final load: ", err))
					}
				})
			}
		}
	}
}
