package blockbuffer

import "testing"

// Full real-Pebble/Buffer operation: fixed-input reset for cold controls, parent
// topology + snapshot/session capture, 32 exact prefetches and scoped reads,
// session close, layer publication and FlushUpTo. It includes canonical cache
// promotion and Pebble writes, but not fsync or full commitment trie folding.
// Legacy runs the frozen session with its own equivalent context pool; there
// is no production toggle or extra per-read lookup to emulate the old exposure.
func BenchmarkCommitmentPrefetchOwnershipFullSession(b *testing.B) {
	b.Setenv("GTRON_PEBBLE_BOUNDED_POINT_READ", "get")
	for _, kind := range []string{"legacy", "delta"} {
		for _, mode := range []string{"cold_same", "cold_shrink", "cold_grow", "hot_same", "hot_shrink_grow", "no_flush", "direct_get"} {
			for _, candidate := range []bool{false, true} {
				name := "legacy"
				if candidate {
					name = "candidate"
				}
				b.Run(kind+"/"+mode+"/"+name, func(b *testing.B) {
					const keys = 32
					r := newCommitmentOwnershipReplay(b, candidate, kind, mode, keys)
					// Warm the session/context pools and the hot-control cache.
					if _, err := r.run(); err != nil {
						b.Fatal(err)
					}
					var reads uint64
					b.ReportAllocs()
					for b.Loop() {
						count, err := r.run()
						if err != nil {
							b.Fatal(err)
						}
						reads += count
					}
					b.ReportMetric(float64(keys), "keys/op")
					b.ReportMetric(float64(reads)/float64(b.N), "durable_reads/op")
				})
			}
		}
	}
}
