package blockbuffer

import (
	"bytes"
	"fmt"
	"strings"
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
	// FIFO token per block for the lifetime of a long sync session.
	key := []byte("repeated-hot-branch")
	shard := &c.shards[baseReadCacheShardIndex(key)]
	for i := 0; i < 10_000; i++ {
		testBaseReadCacheSet(c, key, value)
		c.del(key)
	}
	if live := len(shard.queue) - shard.head; live >= 2048 {
		t.Fatalf("invalidation queue retained %d stale tokens, want <2048", live)
	}
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
		t.Fatalf("flush refresh queue retained %d tokens, want the original 1", live)
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
		t.Fatal("entry and FIFO token do not share the canonical key")
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
	if len(s.entries) != 1 || len(s.queue)-s.head != 1 || s.nextGen != 1 {
		t.Fatalf("racing fills published entries=%d tokens=%d generation=%d, want 1/1/1", len(s.entries), len(s.queue)-s.head, s.nextGen)
	}
}

func BenchmarkBaseReadCacheFlushedHotKey(b *testing.B) {
	key := []byte("state-commitment-branch-v1-hot-prefix")
	keyString := string(key)
	value := make([]byte, 1500)

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
