package blockbuffer

import (
	"fmt"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// Identical cache policy stays active in both variants. These measure observer
// cost, not saved durable reads or whole-node synchronization throughput.
func BenchmarkBaseReadCacheReuseObserver(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		for _, operation := range []string{"hit", "window_flush_cycle", "probation_flush_cycle", "other_eviction_live", "stats_live"} {
			b.Run(fmt.Sprintf("enabled_%v/%s", enabled, operation), func(b *testing.B) {
				value := "0"
				if enabled {
					value = "1"
				}
				b.Setenv("GTRON_BASE_CACHE_REUSE_OBSERVER", value)
				trunk := 4
				if operation == "probation_flush_cycle" || operation == "stats_live" || operation == "other_eviction_live" {
					trunk = -1
				}
				cache := newBaseReadCacheWithTrunk(4<<20, trunk, rawdb.CommitmentBranchKeyPrefix)
				keys := make([][]byte, 4096)
				strings := make([]string, len(keys))
				for i := range keys {
					keys[i] = reuseObserverKey(6+i%2, i%3 == 0, uint32(i))
					if operation == "other_eviction_live" {
						keys[i] = []byte(fmt.Sprintf("other-fixture-%d", i))
					}
					strings[i] = string(keys[i])
				}
				payload := []byte("benchmark-parent")
				if operation == "hit" {
					testBaseReadCacheSet(cache, keys[0], payload)
					for range 4 {
						cache.getWithEpoch(keys[0])
					}
				}
				if operation == "stats_live" || operation == "other_eviction_live" {
					// Use the same real sampled keys even when disabled. Filling
					// all slots gives an explicit worst-case bounded live scan.
					var occupied [baseReadCacheShardCount][baseReadCacheReuseSlots]bool
					filled := 0
					for id := uint32(0); filled < baseReadCacheShardCount*baseReadCacheReuseSlots; id++ {
						key := reuseObserverKey(7, true, id)
						index, sampled := baseReadCacheReuseIndex(string(key))
						shard := baseReadCacheShardIndex(key)
						if !sampled || occupied[shard][index] {
							continue
						}
						occupied[shard][index] = true
						filled++
						_, _, epoch := cache.getWithEpoch(key)
						cache.storeIfEpoch(key, payload, epoch)
						cache.setFlushed(string(key), payload)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					index := i & (len(keys) - 1)
					key := keys[index]
					s := &cache.shards[baseReadCacheShardIndex(key)]
					switch operation {
					case "hit":
						cache.getWithEpoch(keys[0])
					case "stats_live":
						cache.stats()
					case "window_flush_cycle", "probation_flush_cycle":
						_, _, epoch := cache.getWithEpoch(key)
						cache.storeIfEpoch(key, payload, epoch)
						cache.setFlushed(strings[index], payload)
						s.mu.Lock()
						if operation == "window_flush_cycle" {
							s.evictWindowOne()
						}
						for s.entries[strings[index]] != nil {
							s.evictOne(false)
						}
						s.mu.Unlock()
					case "other_eviction_live":
						_, _, epoch := cache.getWithEpoch(key)
						cache.prefetchIfEpoch(key, payload, epoch)
						s.mu.Lock()
						for s.entries[strings[index]] != nil {
							s.evictOne(true)
						}
						s.mu.Unlock()
					}
				}
			})
		}
	}
}
