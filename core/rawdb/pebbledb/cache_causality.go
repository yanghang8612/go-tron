package pebbledb

import (
	"github.com/cockroachdb/pebble"
	"sync/atomic"
)

// CacheCausalitySnapshot is one immutable meter publication. Cache fields are
// co-sampled per shard, not an atomic instantaneous snapshot across all shards.
// Generation identifies a publication, never an HTTP collection or DB epoch.
// SourceID identifies one cache instance within the current process epoch.
type CacheCausalitySnapshot struct {
	Namespace  string              `json:"namespace"`
	Generation uint64              `json:"generation"`
	Cache      pebble.CacheMetrics `json:"cache"`
}
type cacheCausalityPublication struct {
	generation uint64 // one existing meter writer per namespace owner
	latest     atomic.Pointer[CacheCausalitySnapshot]
}

func (m *engineSpaceMetrics) publishCacheCausality(stats pebble.CacheMetrics) {
	m.causality.generation++
	m.causality.latest.Store(&CacheCausalitySnapshot{Namespace: m.namespace, Generation: m.causality.generation, Cache: stats})
}

// CacheCausalitySnapshots reads only owner-held immutable meter publications.
// It takes the existing namespace-owner lock; no cache lock, DB call, key scan,
// gauge-by-gauge assembly, or new hot-path mutex is introduced. Values are copied
// so callers cannot mutate the next HTTP reader's evidence.
func CacheCausalitySnapshots() map[string]CacheCausalitySnapshot {
	spaceMetricsOwners.Lock()
	defer spaceMetricsOwners.Unlock()
	out := make(map[string]CacheCausalitySnapshot)
	for namespace, db := range spaceMetricsOwners.byNamespace {
		if db.engineSpace == nil {
			continue
		}
		if snapshot := db.engineSpace.causality.latest.Load(); snapshot != nil {
			out[namespace+"storage/engine/block_cache/causality"] = *snapshot
		}
	}
	return out
}
