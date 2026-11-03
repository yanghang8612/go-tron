package pebbledb

import (
	"github.com/cockroachdb/pebble"
	"sync"
	"testing"
)

func TestCacheCausalityOwnerCoherentPublication(t *testing.T) {
	const namespace = "test/causality-owner/"
	m := newEngineSpaceMetrics(namespace)
	owner := &Database{namespace: namespace, engineSpace: m}
	if !acquireSpaceMetrics(owner) {
		t.Fatal("namespace unexpectedly owned")
	}
	defer releaseSpaceMetrics(owner)
	secondary := &Database{namespace: namespace, engineSpace: newEngineSpaceMetrics(namespace)}
	if acquireSpaceMetrics(secondary) {
		t.Fatal("secondary acquired namespace")
	}
	key := namespace + "storage/engine/block_cache/causality"
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(1); i <= 100; i++ {
			stats := &pebble.Metrics{}
			stats.BlockCache.SourceID = 17
			stats.BlockCache.Size = i
			stats.BlockCache.Activity.AddedBytes = i
			m.update(stats)
		}
	}()
	for i := 0; i < 100; i++ {
		if sample, ok := CacheCausalitySnapshots()[key]; ok {
			if sample.Namespace != namespace || sample.Generation == 0 || sample.Cache.SourceID != 17 || sample.Cache.Size != sample.Cache.Activity.Balance() {
				t.Fatalf("mixed publication: %+v", sample)
			}
		}
	}
	wg.Wait()
	before := CacheCausalitySnapshots()[key]
	secondary.engineSpace.update(&pebble.Metrics{})
	after := CacheCausalitySnapshots()[key]
	if after != before {
		t.Fatal("nonowner mixed source")
	}
	altered := after
	altered.Cache.SourceID = 999
	if CacheCausalitySnapshots()[key] != before {
		t.Fatal("reader modified immutable publication")
	}
	releaseSpaceMetrics(owner)
	if _, ok := CacheCausalitySnapshots()[key]; ok {
		t.Fatal("closed owner retained publication")
	}
}
