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

	// First key is the "kept" hot one — bump its activity high.
	hotKey := append([]byte(nil), prefix...)
	hotKey = append(hotKey, 0xff, 0xff)
	for i := 0; i < 100; i++ {
		tr.Record(kvdomains.SystemDynamicProperty, hotKey, false, 1)
	}

	// Fill the shard beyond capacity. Each new key has only 1 put, so when
	// eviction starts the hotKey (100 puts) is never the lowest-activity.
	for i := 0; i < hotspotMaxKeysPerShard+50; i++ {
		insertOne(i)
	}

	// hotKey must still be present.
	found := false
	for _, e := range tr.Snapshot() {
		if bytes.Equal(e.Key, hotKey) {
			if e.Puts != 100 {
				t.Fatalf("hotKey puts = %d, want 100", e.Puts)
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("hot key was evicted despite having highest activity")
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
