package blockbuffer

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// The same benchmark runs with the production files overlaid from HEAD. All
// keys are exact depth-6 physical commitment keys; readers/values are allocated
// before timing so a diagnostic-only regression cannot hide behind I/O.
func BenchmarkCommitmentDeepCacheDiagnostics(b *testing.B) {
	for _, kind := range []string{"get", "snapshot_view", "prefetch_probe", "flush_window", "probation_miss", "epoch_rejected"} {
		b.Run(kind, func(b *testing.B) {
			cache := newBaseReadCacheWithTrunk(1<<20, 4, rawdb.CommitmentBranchKeyPrefix)
			if kind == "probation_miss" || kind == "epoch_rejected" {
				cache = newBaseReadCache(1<<20, rawdb.CommitmentBranchKeyPrefix)
			}
			key := append([]byte(rawdb.CommitmentBranchKeyPrefix), bytes.Repeat([]byte{3}, 6)...)
			keyString := string(key)
			value := []byte("immutable benchmark branch")
			consume := func([]byte, bool) error { return nil }
			_, _, epoch := cache.getWithEpoch(key)
			if kind == "epoch_rejected" {
				cache.del(key)
			} else if kind != "probation_miss" {
				testBaseReadCacheSet(cache, key, value)
				// Stable successful paths start with saturated reference state.
				// This matches repeatedly used branches rather than measuring an
				// entry's first source transition on every iteration.
				for range 4 {
					cache.getWithEpoch(key)
					cache.probeAtVersionForPrefetch(key, 0)
					cache.setFlushed(keyString, value)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				switch kind {
				case "get":
					cache.getWithEpoch(key)
				case "snapshot_view":
					if _, _, _, _, _, _, err := cache.viewAtVersion(key, 0, consume); err != nil {
						b.Fatal(err)
					}
				case "prefetch_probe":
					cache.probeAtVersionForPrefetch(key, 0)
				case "flush_window":
					cache.setFlushed(keyString, value)
				case "probation_miss":
					// Six nibbles provide 16M distinct paths, beyond the short
					// comparison runs. A wrap is harmless but no longer a cold scan.
					for nibble := range 6 {
						key[len(key)-1-nibble] = byte(i>>(4*nibble)) & 15
					}
					_, _, currentEpoch := cache.getWithEpoch(key)
					cache.storeIfEpoch(key, value, currentEpoch)
				case "epoch_rejected":
					cache.storeIfEpoch(key, value, epoch)
				}
			}
		})
	}
}

// Exercise real pooled contexts and Close aggregation, including the existing
// first/every-64-close occupancy+diagnostic publication. No publisher or shard
// lock is contended in this fixture; other tests cover the skip/retry path.
func BenchmarkCommitmentCacheSessionCloseDiagnostics(b *testing.B) {
	for _, readers := range []int{1, 65} {
		b.Run(fmt.Sprintf("readers_%d", readers), func(b *testing.B) {
			cache := newBaseReadCacheWithTrunk(1<<20, 4, rawdb.CommitmentBranchKeyPrefix)
			key := append([]byte(rawdb.CommitmentBranchKeyPrefix), bytes.Repeat([]byte{3}, 6)...)
			for i := range 16 {
				key[len(key)-1] = byte(i)
				testBaseReadCacheSet(cache, key, []byte("immutable benchmark branch"))
				cache.getWithEpoch(key)
			}
			session := new(commitmentParentReadSession)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				session.cache = cache
				session.snapshot = benchmarkCommitmentSnapshot{}
				session.readContexts = borrowCommitmentParentReadContexts(session, readers)
				session.keyScratch = borrowCommitmentParentKeyScratch(readers)
				if err := session.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
