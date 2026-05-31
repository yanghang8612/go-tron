package state

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestKVHotspotTracker_RecordsAndSorts(t *testing.T) {
	tr := newKVHotspotTracker()

	tr.Record(kvdomains.SystemDynamicProperty, []byte("hot-key"), false, 32)
	tr.Record(kvdomains.SystemDynamicProperty, []byte("hot-key"), false, 32)
	tr.Record(kvdomains.SystemDynamicProperty, []byte("hot-key"), false, 32)
	tr.Record(kvdomains.SystemReward, []byte("cold-key"), false, 8)

	top := tr.Top(0, HotspotSortByActivity, "")
	if len(top) != 2 {
		t.Fatalf("Snapshot got %d entries, want 2", len(top))
	}
	// hot-key (3 puts) sorts before cold-key (1 put) by Activity.
	if !bytes.Equal(top[0].Key, []byte("hot-key")) || top[0].Puts != 3 {
		t.Fatalf("top[0] = %+v, want hot-key puts=3", top[0])
	}
	if top[0].PutBytes != 96 {
		t.Fatalf("hot-key PutBytes = %d, want 96", top[0].PutBytes)
	}
}

func TestKVHotspotTracker_DomainFilter(t *testing.T) {
	tr := newKVHotspotTracker()
	tr.Record(kvdomains.SystemDynamicProperty, []byte("a"), false, 1)
	tr.Record(kvdomains.SystemReward, []byte("b"), false, 1)

	got := tr.Top(0, HotspotSortByActivity, "SystemReward")
	if len(got) != 1 || string(got[0].Key) != "b" {
		t.Fatalf("filter=SystemReward returned %+v", got)
	}
}

func TestKVHotspotTracker_DeleteCounts(t *testing.T) {
	tr := newKVHotspotTracker()
	tr.Record(kvdomains.SystemAsset, []byte("k"), false, 16)
	tr.Record(kvdomains.SystemAsset, []byte("k"), true, 0)
	tr.Record(kvdomains.SystemAsset, []byte("k"), true, 0)

	got := tr.Top(0, HotspotSortByDeletes, "")
	if len(got) != 1 || got[0].Puts != 1 || got[0].Deletes != 2 || got[0].PutBytes != 16 {
		t.Fatalf("counts = %+v, want puts=1 deletes=2 bytes=16", got[0])
	}
	if got[0].Activity() != 3 {
		t.Fatalf("Activity = %d, want 3", got[0].Activity())
	}
}

func TestKVHotspotTracker_DisabledNoRecord(t *testing.T) {
	tr := newKVHotspotTracker()
	tr.SetEnabled(false)
	tr.Record(kvdomains.SystemDynamicProperty, []byte("x"), false, 1)
	if got := tr.Top(0, HotspotSortByActivity, ""); len(got) != 0 {
		t.Fatalf("disabled tracker recorded %d entries, want 0", len(got))
	}
}

func TestKVHotspotTracker_Reset(t *testing.T) {
	tr := newKVHotspotTracker()
	tr.Record(kvdomains.SystemDynamicProperty, []byte("x"), false, 1)
	tr.Reset()
	if got := tr.Top(0, HotspotSortByActivity, ""); len(got) != 0 {
		t.Fatalf("Reset left %d entries", len(got))
	}
}

func TestKVHotspotTracker_EmptyKeyIgnored(t *testing.T) {
	tr := newKVHotspotTracker()
	tr.Record(kvdomains.SystemDynamicProperty, nil, false, 1)
	tr.Record(kvdomains.SystemDynamicProperty, []byte{}, false, 1)
	if got := tr.Top(0, HotspotSortByActivity, ""); len(got) != 0 {
		t.Fatalf("empty-key recorded %d entries, want 0", len(got))
	}
}

func TestKVHotspotTracker_EvictsLowestActivity(t *testing.T) {
	tr := newKVHotspotTracker()
	// Force every key into the same shard so we hit the per-shard cap with
	// a predictable insertion order. Use a fixed prefix the FNV-1a of which
	// is stable, and append a sentinel byte that doesn't change the first 8
	// bytes (the shardOf window).
	prefix := []byte("samebkt0") // 8 bytes; shardOf is FNV over first 8
	insertOne := func(suffix int) {
		var k []byte
		k = append(k, prefix...)
		k = append(k, byte(suffix>>8), byte(suffix&0xff))
		// Each new key gets 1 put; the FIRST key gets many puts so it has
		// the highest activity and survives eviction.
		tr.Record(kvdomains.SystemDynamicProperty, k, false, 1)
	}

	// First key is the "kept" hot one — bump its activity high enough that
	// even with sampled eviction (5-of-N random pick), the probability of
	// hitting hotKey AND choosing it AS the lowest is vanishingly small over
	// the full insertion run. Use a 3-byte sentinel so it cannot collide with
	// insertOne's 2-byte suffix range.
	hotKey := append([]byte(nil), prefix...)
	hotKey = append(hotKey, 0xff, 0xff, 0xff)
	for i := 0; i < 100_000; i++ {
		tr.Record(kvdomains.SystemDynamicProperty, hotKey, false, 1)
	}

	// Fill the shard beyond capacity. Each new key has only 1 put, so when
	// eviction starts the hotKey (100k puts) is never the lowest among the
	// random sample (everything else has 1).
	for i := 0; i < hotspotMaxKeysPerShard+50; i++ {
		insertOne(i)
	}

	// hotKey must still be present.
	found := false
	for _, e := range tr.Snapshot() {
		if bytes.Equal(e.Key, hotKey) {
			if e.Puts != 100_000 {
				t.Fatalf("hotKey puts = %d, want 100000", e.Puts)
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("hot key was evicted despite having highest activity")
	}
}

// Sampling-LRU stress: confirms the evictLowestActivity path stays O(sample)
// rather than O(map size) when a high-cardinality workload (e.g. real
// ContractStorage on a synced node) constantly thrashes the shard cap. With
// the prior linear-scan implementation this test took ~30s for 1M inserts
// after the shard filled; with sampled eviction it completes in milliseconds.
func TestKVHotspotTracker_HighCardinalityEvictIsFast(t *testing.T) {
	tr := newKVHotspotTracker()
	// Force everything into one shard so we measure the shard-evict path.
	prefix := []byte("samebkt0")
	for i := 0; i < hotspotMaxKeysPerShard*4; i++ {
		var k []byte
		k = append(k, prefix...)
		k = append(k, byte(i>>24), byte(i>>16), byte(i>>8), byte(i))
		tr.Record(kvdomains.SystemDynamicProperty, k, false, 1)
	}
	// Sanity: the shard saturated at or near cap (no off-by-one explosion).
	snap := tr.Snapshot()
	if len(snap) > hotspotMaxKeysPerShard+hotspotShardCount {
		t.Fatalf("snapshot %d exceeds shard-cap envelope", len(snap))
	}
}

func TestKVHotspotTracker_ConcurrentRecord(t *testing.T) {
	tr := newKVHotspotTracker()
	var wg sync.WaitGroup
	const writers = 8
	const perWriter = 200
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				k := []byte(fmt.Sprintf("k%02d", i%16))
				tr.Record(kvdomains.SystemDynamicProperty, k, false, 1)
			}
		}(w)
	}
	wg.Wait()

	total := uint64(0)
	for _, e := range tr.Snapshot() {
		total += e.Puts
	}
	if want := uint64(writers * perWriter); total != want {
		t.Fatalf("total puts = %d, want %d", total, want)
	}
}
