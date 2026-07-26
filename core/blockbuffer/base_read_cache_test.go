package blockbuffer

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"unsafe"
)

func testBaseReadCacheSet(c *baseReadCache, key, value []byte) {
	for attempt := 0; attempt < 2; attempt++ {
		_, _, epoch := c.getWithEpoch(key)
		if _, stored := c.setIfEpoch(key, value, epoch); stored {
			return
		}
	}
	panic("base-read cache test fill did not complete admission")
}

func TestBaseReadCache_TwoHitAdmissionRejectsOneHitScan(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	value := []byte("durable-value")
	for i := 0; i < 10_000; i++ {
		key := []byte(fmt.Sprintf("cold-branch-%08d", i))
		_, _, epoch := c.getWithEpoch(key)
		if _, stored := c.setIfEpoch(key, value, epoch); stored {
			t.Fatalf("one-hit key %q was admitted", key)
		}
	}
	resident := 0
	for i := range c.shards {
		resident += len(c.shards[i].entries)
	}
	if resident != 0 {
		t.Fatalf("one-hit scan retained %d resident entries, want 0", resident)
	}

	hotKey := []byte("repeated-hot-branch")
	_, _, epoch := c.getWithEpoch(hotKey)
	if _, stored := c.setIfEpoch(hotKey, value, epoch); stored {
		t.Fatal("first hot-key sighting bypassed probation")
	}
	_, _, epoch = c.getWithEpoch(hotKey)
	if _, stored := c.setIfEpoch(hotKey, value, epoch); !stored {
		t.Fatal("second hot-key sighting was not admitted")
	}
	if got, ok, _ := c.getWithEpoch(hotKey); !ok || !bytes.Equal(got, value) {
		t.Fatalf("admitted hot key = (%q,%v), want (%q,true)", got, ok, value)
	}
}

func TestBaseReadCache_ProductionAdmissionHistoryBudget(t *testing.T) {
	c := newBaseReadCache(128 << 20)
	var totalSlots int
	for i := range c.shards {
		if got := len(c.shards[i].admission); got != baseReadCacheMaxAdmissionSlots {
			t.Fatalf("shard %d admission slots = %d, want %d", i, got, baseReadCacheMaxAdmissionSlots)
		}
		totalSlots += len(c.shards[i].admission)
	}
	if got, want := totalSlots*8, 1<<20; got != want {
		t.Fatalf("admission history bytes = %d, want %d", got, want)
	}
}

func TestBaseReadCache_SnapshotVersionRejectsFutureReplacement(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	key := []byte("commitment-hot-branch")
	testBaseReadCacheSet(c, key, []byte("old"))
	captured := c.version.Load()
	if got, ok, _, _ := c.getAtVersion(key, captured); !ok || string(got) != "old" {
		t.Fatalf("captured cache = (%q,%v), want old/true", got, ok)
	}
	c.advanceVersion()
	c.setFlushed(string(key), []byte("new"))
	if got, ok, _, cacheable := c.getAtVersion(key, captured); ok || got != nil || cacheable {
		t.Fatalf("future replacement = (%q,%v), want nil/false", got, ok)
	}
	if got, ok, _, _ := c.getAtVersion(key, c.version.Load()); !ok || string(got) != "new" {
		t.Fatalf("current cache = (%q,%v), want new/true", got, ok)
	}
}

func TestBaseReadCache_MissingAdmissionAndFlushRefresh(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	key := []byte("missing-permission-row")

	_, _, epoch := c.getWithEpoch(key)
	if c.setMissingIfEpoch(key, epoch) {
		t.Fatal("first missing-row sighting bypassed probation")
	}
	_, _, epoch = c.getWithEpoch(key)
	if !c.setMissingIfEpoch(key, epoch) {
		t.Fatal("second missing-row sighting was not admitted")
	}
	if got, ok, _ := c.getWithEpoch(key); !ok || got != nil {
		t.Fatalf("cached missing row = (%v,%v), want (nil,true)", got, ok)
	}

	// A canonical put refreshes the resident miss before its layer is dropped.
	c.setFlushed(string(key), []byte("permission"))
	if got, ok, _ := c.getWithEpoch(key); !ok || string(got) != "permission" {
		t.Fatalf("flushed replacement = (%q,%v), want (permission,true)", got, ok)
	}

	// Present empty values must stay distinct from the nil miss sentinel.
	c.setFlushed(string(key), []byte{})
	if got, ok, _ := c.getWithEpoch(key); !ok || got == nil || len(got) != 0 {
		t.Fatalf("present empty replacement = (%v,%v), want (non-nil empty,true)", got, ok)
	}
}

func TestBaseReadCache_BoundedPayloadAndInvalidationQueue(t *testing.T) {
	const size = 64 * 256
	c := newBaseReadCache(size)
	totalLimit := 0
	for i := range c.shards {
		totalLimit += c.shards[i].limit
	}
	if totalLimit != size {
		t.Fatalf("shard limits sum to %d, want exact configured budget %d", totalLimit, size)
	}
	value := make([]byte, 96)
	for i := 0; i < 10_000; i++ {
		key := []byte(fmt.Sprintf("branch-%08d", i))
		testBaseReadCacheSet(c, key, value)
	}
	for i := range c.shards {
		s := &c.shards[i]
		if s.used > s.limit {
			t.Fatalf("shard %d retained %d bytes above limit %d", i, s.used, s.limit)
		}
	}

	// Repeated populate→flush-invalidate cycles must not accumulate one stale
	// CLOCK queue entry per block for the lifetime of a long sync session.
	key := []byte("repeated-hot-branch")
	shard := &c.shards[baseReadCacheShardIndex(key)]
	for i := 0; i < 10_000; i++ {
		testBaseReadCacheSet(c, key, value)
		c.del(key)
	}
	if live := len(shard.queue) - shard.head; live >= 2048 {
		t.Fatalf("invalidation queue retained %d stale entries, want <2048", live)
	}
}

func TestBaseReadCache_SecondChancePreservesReferencedOldestEntry(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	keys := make([][]byte, 0, 3)
	for candidate := 0; len(keys) < 3; candidate++ {
		key := []byte(fmt.Sprintf("clock-key-%08d", candidate))
		if len(keys) == 0 || baseReadCacheShardIndex(key) == baseReadCacheShardIndex(keys[0]) {
			keys = append(keys, key)
		}
	}
	value := []byte("v")
	charge := len(keys[0]) + len(value) + baseReadCacheEntryOverhead
	s := &c.shards[baseReadCacheShardIndex(keys[0])]
	s.limit = 2 * charge

	testBaseReadCacheSet(c, keys[0], value)
	testBaseReadCacheSet(c, keys[1], value)
	if _, ok, _ := c.getWithEpoch(keys[0]); !ok {
		t.Fatal("oldest entry missed before marking it referenced")
	}
	testBaseReadCacheSet(c, keys[2], value)

	if _, ok, _ := c.getWithEpoch(keys[0]); !ok {
		t.Fatal("referenced oldest entry did not receive a second chance")
	}
	if _, ok, _ := c.getWithEpoch(keys[1]); ok {
		t.Fatal("unreferenced entry survived ahead of the referenced oldest entry")
	}
	if _, ok, _ := c.getWithEpoch(keys[2]); !ok {
		t.Fatal("newly admitted entry was not retained")
	}
	if s.used > s.limit {
		t.Fatalf("used bytes=%d exceed limit=%d", s.used, s.limit)
	}
}

func TestBaseReadCache_ConcurrentHitAndFlushRefresh(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	key := []byte("concurrent-cache-hit-and-flush")
	valueA := bytes.Repeat([]byte{'a'}, 128)
	valueB := bytes.Repeat([]byte{'b'}, 128)
	testBaseReadCacheSet(c, key, valueA)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 10_000; i++ {
			got, ok, _ := c.getWithEpoch(key)
			if !ok || (!bytes.Equal(got, valueA) && !bytes.Equal(got, valueB)) {
				t.Errorf("concurrent hit = (%x,%v)", got, ok)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10_000; i++ {
			if i&1 == 0 {
				c.setFlushed(string(key), valueB)
			} else {
				c.setFlushed(string(key), valueA)
			}
		}
	}()
	wg.Wait()
}

func TestBaseReadCache_SetFlushedRefreshesOnlyCachedKeys(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	cachedKey := []byte("cached-commitment-branch")
	uncachedKey := "unrelated-block-metadata"
	oldValue := []byte("old")
	// Model commitment's putBranchesSorted layout: this tiny value is a capped
	// subslice of a much larger sibling arena. The cache must not retain it.
	arena := make([]byte, 1<<20)
	copy(arena[123:126], "new")
	newValue := arena[123:126:126]
	testBaseReadCacheSet(c, cachedKey, oldValue)
	shard := &c.shards[baseReadCacheShardIndex(cachedKey)]
	foundCachedKey := false
	for key := range shard.entries {
		if key == string(cachedKey) {
			foundCachedKey = true
			break
		}
	}
	if !foundCachedKey {
		t.Fatal("cached key missing before refresh")
	}
	keyArena := strings.Repeat("x", 1<<20) + string(cachedKey)
	flushedKey := keyArena[1<<20:]

	c.setFlushed(flushedKey, newValue)
	got, ok, _ := c.getWithEpoch(cachedKey)
	if !ok || string(got) != "new" {
		t.Fatalf("flushed cached value = (%q,%v), want (new,true)", got, ok)
	}
	if len(got) == 0 || &got[0] == &newValue[0] {
		t.Fatal("flushed arena slice was retained instead of copied into cache-owned storage")
	}
	for key := range shard.entries {
		if key == string(cachedKey) && unsafe.StringData(key) == unsafe.StringData(flushedKey) {
			t.Fatal("flush retained the layer-arena string instead of a cache-owned key")
		}
	}

	c.setFlushed(uncachedKey, []byte("metadata"))
	if _, ok, _ := c.getWithEpoch([]byte(uncachedKey)); ok {
		t.Fatal("flush admitted a key that was never read through the cache")
	}

	for i := 0; i < 10_000; i++ {
		c.setFlushed(string(cachedKey), newValue)
	}
	if live := len(shard.queue) - shard.head; live != 1 {
		t.Fatalf("flush refresh queue retained %d entries, want the original 1", live)
	}
}

func TestBaseReadCache_FlushRefreshKeepsCanonicalKeySeparateFromValue(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	key := []byte("state-commitment-branch-v1-canonical-key")
	testBaseReadCacheSet(c, key, []byte("old-value"))

	s := &c.shards[baseReadCacheShardIndex(key)]
	before := s.entries[string(key)]
	if len(s.queue) != 1 {
		t.Fatalf("queue entries=%d, want 1", len(s.queue))
	}
	var keyPtr *byte
	for residentKey := range s.entries {
		keyPtr = unsafe.StringData(residentKey)
	}
	if keyPtr == nil || unsafe.StringData(s.queue[0].key) != keyPtr {
		t.Fatal("map entry and CLOCK queue entry do not share the canonical key")
	}
	oldValuePtr := unsafe.SliceData(before.value)
	beforeUsed := s.used

	c.setFlushed(string(key), []byte("replacement-value"))
	after := s.entries[string(key)]
	if after != before {
		t.Fatal("flush refresh replaced the stable cache entry")
	}
	var refreshedKeyPtr *byte
	for residentKey := range s.entries {
		refreshedKeyPtr = unsafe.StringData(residentKey)
	}
	if refreshedKeyPtr != keyPtr || unsafe.StringData(s.queue[0].key) != keyPtr {
		t.Fatal("flush refresh replaced the canonical key")
	}
	if unsafe.SliceData(after.value) == oldValuePtr {
		t.Fatal("flush refresh reused mutable value storage")
	}
	if got := string(after.value); got != "replacement-value" {
		t.Fatalf("replacement value=%q", got)
	}
	if want := beforeUsed + len("replacement-value") - len("old-value"); s.used != want {
		t.Fatalf("used bytes=%d, want %d after differently-sized refresh", s.used, want)
	}

	equalValue := append([]byte(nil), after.value...)
	if unsafe.SliceData(equalValue) == unsafe.SliceData(after.value) {
		t.Fatal("test equal value unexpectedly aliases resident storage")
	}
	residentKey := string(key)
	residentValuePtr := unsafe.SliceData(after.value)
	if allocs := testing.AllocsPerRun(100, func() {
		c.setFlushed(residentKey, equalValue)
	}); allocs != 0 {
		t.Fatalf("byte-identical flush allocations=%v, want 0", allocs)
	}
	if unsafe.SliceData(after.value) != residentValuePtr {
		t.Fatal("byte-identical flush replaced immutable resident storage")
	}
}

func TestBaseReadCache_FlushPreservesReadBeforeWriteProbation(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	key := []byte("frequently-mutated-commitment-branch")

	// The first durable parent read records frequency evidence without
	// retaining its value.
	_, _, epoch := c.getWithEpoch(key)
	if _, stored := c.setIfEpoch(key, []byte("parent-v1"), epoch); stored {
		t.Fatal("first parent read bypassed probation")
	}

	// Committing the block must invalidate the old durable generation without
	// erasing that frequency evidence or directly admitting the written value.
	c.setFlushed(string(key), []byte("child-v2"))
	if _, ok, _ := c.getWithEpoch(key); ok {
		t.Fatal("flush directly admitted a probationary key")
	}

	// The next durable-parent read is the second observation and should enter
	// the resident cache. Before this regression fix every flush cleared the
	// fingerprint, so a branch modified every block could remain permanently
	// probationary and hit Pebble forever.
	_, _, epoch = c.getWithEpoch(key)
	if got, stored := c.setIfEpoch(key, []byte("parent-v2"), epoch); !stored || string(got) != "parent-v2" {
		t.Fatalf("second parent read = (%q,%v), want admitted parent-v2", got, stored)
	}
}

func TestBaseReadCache_SetFlushedRejectsLateOldGenerationFill(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	key := []byte("racing-commitment-branch")
	_, _, oldEpoch := c.getWithEpoch(key)

	// There is no resident entry to refresh, but the flush must still advance
	// the generation so a read that began before it cannot publish stale bytes.
	c.setFlushed(string(key), []byte("new"))
	if _, stored := c.setIfEpoch(key, []byte("old"), oldEpoch); stored {
		t.Fatal("pre-flush read populated stale bytes after the flush")
	}
	if _, ok, _ := c.getWithEpoch(key); ok {
		t.Fatal("uncached flush should invalidate without admitting the key")
	}
}

func TestBaseReadCache_UnrelatedSameShardFlushKeepsFillEligible(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	key := []byte("hot-account-latest-row")
	keyShard := baseReadCacheShardIndex(key)
	keySlot := baseReadCacheInvalidationSlotBytes(key, len(c.invalidations))

	var unrelated string
	for i := 0; i < 100_000; i++ {
		candidate := fmt.Sprintf("unrelated-flushed-row-%08d", i)
		if baseReadCacheShardIndexString(candidate) == keyShard &&
			baseReadCacheInvalidationSlotString(candidate, len(c.invalidations)) != keySlot {
			unrelated = candidate
			break
		}
	}
	if unrelated == "" {
		t.Fatal("failed to find test keys sharing a payload shard but not an invalidation slot")
	}

	// Complete the hot key's first probation sighting.
	_, _, epoch := c.getWithEpoch(key)
	if _, stored := c.setIfEpoch(key, []byte("first"), epoch); stored {
		t.Fatal("first sighting bypassed probation")
	}

	// Capture the generation for its second durable read, then publish an
	// unrelated key routed to the SAME 64-way payload shard. The old shard-wide
	// epoch rejected this fill even though the hot key did not change.
	_, _, epoch = c.getWithEpoch(key)
	c.setFlushed(unrelated, []byte("unrelated"))
	if _, stored := c.setIfEpoch(key, []byte("second"), epoch); !stored {
		t.Fatal("unrelated same-shard flush falsely rejected hot-key fill")
	}
}

func TestBaseReadCache_InvalidationSlotByteStringParity(t *testing.T) {
	for _, size := range []int{baseReadCacheShardCount * 256, 1 << 20, 128 << 20} {
		slots := baseReadCacheInvalidationSlots(size)
		for i := 0; i < 1_000; i++ {
			key := fmt.Sprintf("state-commitment-branch-v1-%02x-%08x-tail", i&15, i*0x9e37)
			gotBytes := baseReadCacheInvalidationSlotBytes([]byte(key), slots)
			gotString := baseReadCacheInvalidationSlotString(key, slots)
			if gotBytes != gotString {
				t.Fatalf("size=%d key=%q byte slot=%d string slot=%d", size, key, gotBytes, gotString)
			}
		}
	}
	if got := baseReadCacheInvalidationSlots(128 << 20); got != baseReadCacheMaxInvalidationSlots {
		t.Fatalf("128 MiB invalidation slots=%d, want %d", got, baseReadCacheMaxInvalidationSlots)
	}
}

func TestBaseReadCache_SetFlushedDropsOversizedReplacement(t *testing.T) {
	// 256 bytes per shard: the original row fits, the replacement does not.
	c := newBaseReadCache(baseReadCacheShardCount * 256)
	key := []byte("hot-branch")
	testBaseReadCacheSet(c, key, []byte("old"))
	if _, ok, _ := c.getWithEpoch(key); !ok {
		t.Fatal("test setup did not cache original value")
	}

	c.setFlushed(string(key), make([]byte, 512))
	if _, ok, _ := c.getWithEpoch(key); ok {
		t.Fatal("oversized flushed replacement retained a stale or over-budget entry")
	}
}

func TestBaseReadCache_InvalidationReleasesQueuedEntryPayload(t *testing.T) {
	c := newBaseReadCache(8 << 20)
	key := []byte("queued-entry-with-large-owned-payload")
	value := bytes.Repeat([]byte{0x5a}, 64<<10)
	testBaseReadCacheSet(c, key, value)

	s := &c.shards[baseReadCacheShardIndex(key)]
	entry := s.entries[string(key)]
	if entry == nil || len(s.queue) != 1 || s.queue[0] != entry {
		t.Fatal("test setup did not retain one stable queued entry")
	}
	c.del(key)
	if entry.live || entry.key != "" || entry.value != nil {
		t.Fatalf("invalidated queued entry retained live=%v key=%q valueBytes=%d", entry.live, entry.key, len(entry.value))
	}
	if len(s.queue) != 1 || s.queue[0] != entry {
		t.Fatal("test requires the cleared entry to remain as a stale queue pointer")
	}
}

func TestBaseReadCache_RacingSameEpochFillsPublishOnce(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	key := []byte("same-generation-branch")
	_, _, epoch := c.getWithEpoch(key)
	first, stored := c.setIfEpoch(key, []byte("durable-value"), epoch)
	if stored {
		t.Fatal("first fill bypassed probation")
	}
	second, stored := c.setIfEpoch(key, []byte("duplicate-read"), epoch)
	if !stored || string(second) != "duplicate-read" {
		t.Fatalf("second racing fill = (%q,%v), want admitted duplicate-read", second, stored)
	}
	third, stored := c.setIfEpoch(key, []byte("late-read"), epoch)
	if !stored || string(third) != "duplicate-read" {
		t.Fatalf("late racing fill = (%q,%v), want existing duplicate-read", third, stored)
	}
	_ = first
	s := &c.shards[baseReadCacheShardIndex(key)]
	if len(s.entries) != 1 || len(s.queue)-s.head != 1 {
		t.Fatalf("racing fills published map=%d queue=%d, want 1/1", len(s.entries), len(s.queue)-s.head)
	}
}

func TestBaseReadCache_PromoteFlushedShard(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	keys := make([]string, 0, 3)
	for i := 0; len(keys) < cap(keys); i++ {
		key := fmt.Sprintf("promote-shard-%08x", i)
		if baseReadCacheShardIndexString(key) == 0 {
			keys = append(keys, key)
		}
	}
	refreshKey, deleteKey, uncachedKey := keys[0], keys[1], keys[2]
	testBaseReadCacheSet(c, []byte(refreshKey), []byte("old-refresh"))
	testBaseReadCacheSet(c, []byte(deleteKey), []byte("old-delete"))
	_, _, staleEpoch := c.getWithEpoch([]byte(uncachedKey))

	c.promoteFlushedShard(
		map[string][]byte{
			refreshKey:  []byte("new-refresh"),
			uncachedKey: []byte("uncached-write"),
		},
		map[string]struct{}{deleteKey: {}},
		0,
	)
	if got, ok, _ := c.getWithEpoch([]byte(refreshKey)); !ok || string(got) != "new-refresh" {
		t.Fatalf("refreshed value = (%q,%v), want new-refresh", got, ok)
	}
	if _, ok, _ := c.getWithEpoch([]byte(deleteKey)); ok {
		t.Fatal("deleted key remained resident")
	}
	if _, ok, _ := c.getWithEpoch([]byte(uncachedKey)); ok {
		t.Fatal("uncached flushed write was admitted")
	}
	if _, stored := c.setIfEpoch([]byte(uncachedKey), []byte("stale"), staleEpoch); stored {
		t.Fatal("pre-promotion epoch published after shard promotion")
	}
}

func BenchmarkBaseReadCacheFlushedHotKey(b *testing.B) {
	key := []byte("state-commitment-branch-v1-hot-prefix")
	keyString := string(key)
	value := make([]byte, 1500)
	changedValue := make([]byte, len(value))
	changedValue[0] = 1

	b.Run("invalidate_and_refill", func(b *testing.B) {
		c := newBaseReadCache(1 << 20)
		testBaseReadCacheSet(c, key, value)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			c.del(key)
			_, _, epoch := c.getWithEpoch(key)
			if _, stored := c.setIfEpoch(key, value, epoch); stored {
				b.Fatal("first refill bypassed probation")
			}
			if _, stored := c.setIfEpoch(key, value, epoch); !stored {
				b.Fatal("second refill rejected")
			}
		}
	})

	b.Run("refresh_from_layer", func(b *testing.B) {
		c := newBaseReadCache(1 << 20)
		testBaseReadCacheSet(c, key, value)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			c.setFlushed(keyString, value)
		}
	})

	b.Run("refresh_from_known_layer_shard", func(b *testing.B) {
		c := newBaseReadCache(1 << 20)
		testBaseReadCacheSet(c, key, value)
		shard := baseReadCacheShardIndexString(keyString)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			c.setFlushedAt(keyString, value, shard)
		}
	})

	b.Run("refresh_changed_from_known_layer_shard", func(b *testing.B) {
		c := newBaseReadCache(1 << 20)
		testBaseReadCacheSet(c, key, value)
		shard := baseReadCacheShardIndexString(keyString)
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			next := value
			if i&1 == 0 {
				next = changedValue
			}
			c.setFlushedAt(keyString, next, shard)
		}
	})
}

func BenchmarkBaseReadCachePromoteShard(b *testing.B) {
	const keyCount = 1024
	value := bytes.Repeat([]byte{0xab}, 128)
	writes := make(map[string][]byte, keyCount)
	for i := 0; len(writes) < keyCount; i++ {
		key := fmt.Sprintf("commitment-branch-%08x-%08x", i*2654435761, i)
		if baseReadCacheShardIndexString(key) == 0 {
			writes[key] = value
		}
	}

	for _, bulk := range []bool{false, true} {
		name := "per-key-lock"
		if bulk {
			name = "shard-lock"
		}
		b.Run(name, func(b *testing.B) {
			c := newBaseReadCache(64 << 20)
			for key := range writes {
				testBaseReadCacheSet(c, []byte(key), value)
			}
			b.ReportAllocs()
			b.ReportMetric(keyCount, "keys/op")
			b.ResetTimer()
			for range b.N {
				if bulk {
					c.promoteFlushedShard(writes, nil, 0)
					continue
				}
				for key, value := range writes {
					c.setFlushedAt(key, value, 0)
				}
			}
		})
	}
}

func BenchmarkBaseReadCacheHit(b *testing.B) {
	for _, keyLen := range []int{32, 64, 96, 128} {
		b.Run(fmt.Sprintf("key=%d", keyLen), func(b *testing.B) {
			c := newBaseReadCache(1 << 20)
			key := bytes.Repeat([]byte{0xa5}, keyLen)
			// Give the tail/middle bytes representative entropy rather than
			// benchmarking a degenerate repeated-byte key.
			for i := range key {
				key[i] ^= byte(i * 37)
			}
			testBaseReadCacheSet(c, key, []byte("value"))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, ok, _ := c.getWithEpoch(key); !ok {
					b.Fatal("cache hit missed")
				}
			}
		})
	}
}
