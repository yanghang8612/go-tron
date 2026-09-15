package rawdb_test

import (
	"context"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// Complete same-input comparison, fixed read4/CDC1, including creation and
// teardown of a fresh per-trio cache. Controlled source reuse is not a claim
// about production hit rates. Both modes still build/verify the same full trio.
func BenchmarkStateHistoryBuildChunkCache(b *testing.B) {
	for _, shape := range []string{"Unique", "HighReuseRaw", "HighReuseSnappy"} {
		b.Run(shape, func(b *testing.B) {
			db, from, to, total := rawdb.OwnedReadBenchmarkFixtureForTest(b, shape)
			for _, enabled := range []bool{false, true} {
				name := "Off"
				if enabled {
					name = "On"
				}
				b.Run(name, func(b *testing.B) {
					dir := b.TempDir()
					b.SetBytes(int64(total))
					b.ReportAllocs()
					var hits, misses, peak uint64
					for b.Loop() {
						view, release, err := rawdb.AcquireStateHistoryReadView(db)
						if err != nil {
							b.Fatal(err)
						}
						var cache *rawdb.StateHistoryChunkCache
						if enabled {
							view, cache, err = rawdb.AcquireStateHistoryChunkCacheView(context.Background(), view)
							if err != nil {
								release()
								b.Fatal(err)
							}
						}
						refs, err := snapshots.BuildDiagnosticStateHistoryTrioWithReadWorkersAndCompressionContext(context.Background(), view, dir, from, to, from, to, "state-domain-change-cache-bench.seg", "auto", etl.Options{}, true, 4, 1)
						if cache != nil {
							s := cache.Stats()
							hits += s.Hits
							misses += s.Misses
							peak = max(peak, s.PeakPayloadBytes)
							if closeErr := cache.Close(); closeErr != nil {
								release()
								b.Fatal(closeErr)
							}
							if s := cache.Stats(); !s.Closed || s.Entries != 0 || s.PayloadBytes != 0 {
								release()
								b.Fatal("cache not released")
							}
						}
						closeErr := release()
						if err != nil || closeErr != nil || len(refs) != 3 {
							b.Fatalf("refs=%d err=%v close=%v", len(refs), err, closeErr)
						}
					}
					if enabled {
						b.ReportMetric(float64(hits)/float64(b.N), "cache_hits/op")
						b.ReportMetric(float64(misses)/float64(b.N), "cache_misses/op")
						b.ReportMetric(float64(peak), "cache_peak_B")
					}
				})
			}
		})
	}
}
