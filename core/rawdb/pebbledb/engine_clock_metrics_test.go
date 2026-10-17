package pebbledb

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/metrics"
)

func TestEngineClockMetricsNamespaceSnapshotGauges(t *testing.T) {
	first := &pebble.Metrics{}
	first.BlockCache.Size = 350
	first.BlockCache.Clock.Shards = 10
	first.BlockCache.Clock.HotCount = 3
	first.BlockCache.Clock.ColdCount = 7
	first.BlockCache.Clock.TestCount = 11
	first.BlockCache.Clock.HotBytes = 100
	first.BlockCache.Clock.ColdBytes = 250
	first.BlockCache.Clock.TestBytes = 400
	first.BlockCache.Clock.ColdTargetBytes = 900
	first.BlockCache.Clock.ColdCountMin = 0
	first.BlockCache.Clock.ColdCountMax = 513
	first.BlockCache.Clock.ColdCountBuckets = [6]int64{1, 1, 2, 2, 2, 2}
	const a = "test/engine-clock-a/"
	const b = "test/engine-clock-b/"
	m := newEngineSpaceMetrics(a)
	other := newEngineSpaceMetrics(b)
	m.update(first)
	m.update(first) // Snapshot readings must replace, never accumulate.
	want := map[string]int64{
		"shards": 10, "hot/entries": 3, "cold/entries": 7, "test/entries": 11,
		"hot/bytes": 100, "cold/bytes": 250, "test/ghost_bytes": 400, "cold_target_bytes": 900,
		"cold_count/min": 0, "cold_count/max": 513,
		"cold_count/shards_0": 1, "cold_count/shards_1": 1, "cold_count/shards_2_7": 2,
		"cold_count/shards_8_63": 2, "cold_count/shards_64_511": 2, "cold_count/shards_512_plus": 2,
	}
	read := func(namespace, name string) int64 {
		t.Helper()
		g, ok := metrics.DefaultRegistry.Get(namespace + "storage/engine/block_cache/clock/" + name).(*metrics.Gauge)
		if !ok {
			t.Fatalf("%s%s not registered as a Gauge", namespace, name)
		}
		return g.Snapshot().Value()
	}
	for name, value := range want {
		if got := read(a, name); got != value {
			t.Fatalf("%s=%d want %d", name, got, value)
		}
		if got := read(b, name); got != 0 {
			t.Fatalf("namespace B leaked %s=%d", name, got)
		}
	}
	if m.blockCacheBytes.Snapshot().Value() != 350 {
		t.Fatal("ghost or target bytes were added to resident block bytes")
	}
	other.update(&pebble.Metrics{})
	m.update(nil)
	if read(a, "test/ghost_bytes") != 400 {
		t.Fatal("nil sample discarded valid snapshot")
	}
	m.update(&pebble.Metrics{})
	for name := range want {
		if read(a, name) != 0 {
			t.Fatalf("empty sample failed to reset %s", name)
		}
	}
}
