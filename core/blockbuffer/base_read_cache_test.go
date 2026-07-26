package blockbuffer

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"unsafe"
)

var baseReadCacheEntryBenchmarkSink *baseReadCacheEntry

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

func TestBaseReadCache_RecyclesPrivateKeyStorage(t *testing.T) {
	shard := baseReadCacheShard{limit: 1 << 20}
	firstKey := strings.Repeat("a", 64)
	firstValue := bytes.Repeat([]byte{0x11}, 128)
	entry := shard.acquireEntryString(firstKey, firstValue, false, 1)
	firstStorage := unsafe.StringData(entry.key)
	firstValueStorage := unsafe.SliceData(entry.value)
	shard.recycleEntry(entry)
	if got := shard.freeValueBytes; got != cap(entry.value) {
		t.Fatalf("free value bytes = %d, want %d", got, cap(entry.value))
	}

	secondKey := strings.Repeat("b", 48)
	secondValue := bytes.Repeat([]byte{0x22}, 96)
	reused := shard.acquireEntryString(secondKey, secondValue, false, 2)
	if reused != entry {
		t.Fatal("recycled entry metadata was not reused")
	}
	if got := unsafe.StringData(reused.key); got != firstStorage {
		t.Fatalf("recycled key storage pointer = %p, want %p", got, firstStorage)
	}
	if reused.keyCapacity != uint32(len(firstKey)) {
		t.Fatalf("recycled key capacity = %d, want %d", reused.keyCapacity, len(firstKey))
	}
	if got := unsafe.SliceData(reused.value); got != firstValueStorage {
		t.Fatalf("recycled value storage pointer = %p, want %p", got, firstValueStorage)
	}
	if reused.key != secondKey || !bytes.Equal(reused.value, secondValue) {
		t.Fatalf("reused entry key/value mismatch: key=%q valueBytes=%d", reused.key, len(reused.value))
	}
	if got, want := reused.charge, int(reused.keyCapacity)+cap(reused.value)+baseReadCacheEntryOverhead; got != want {
		t.Fatalf("reused entry charge = %d, want physical capacity charge %d", got, want)
	}
	if shard.freeValueBytes != 0 {
		t.Fatalf("borrowed value storage remained charged to free pool: %d", shard.freeValueBytes)
	}
	shard.recycleEntry(reused)
	thirdKey := strings.Repeat("c", 60)
	reused = shard.acquireEntryBytes([]byte(thirdKey), nil, true, 3)
	if got := unsafe.StringData(reused.key); got != firstStorage {
		t.Fatalf("capacity lost after shorter key: pointer = %p, want %p", got, firstStorage)
	}
	if reused.key != thirdKey {
		t.Fatalf("byte-source reused key = %q, want %q", reused.key, thirdKey)
	}
}

func TestBaseReadCache_RecycledValueStorageIsBoundedAndUnexposed(t *testing.T) {
	shard := baseReadCacheShard{limit: 8 << 10}
	exposed := shard.acquireEntryString("exposed-key", bytes.Repeat([]byte{0x31}, 512), false, 1)
	exposed.exposed.Store(true)
	shard.recycleEntry(exposed)
	if exposed.value != nil || shard.freeValueBytes != 0 {
		t.Fatalf("exposed value entered free pool: valueBytes=%d freeBytes=%d", len(exposed.value), shard.freeValueBytes)
	}

	first := shard.acquireEntryString("first-key", bytes.Repeat([]byte{0x41}, 768), false, 2)
	second := shard.acquireEntryString("second-key", bytes.Repeat([]byte{0x42}, 768), false, 3)
	shard.recycleEntry(first)
	shard.recycleEntry(second)
	if got, want := shard.freeValueBytes, 768; got != want {
		t.Fatalf("bounded free value bytes = %d, want %d", got, want)
	}
	if second.value != nil {
		t.Fatal("value exceeding the shard free budget was retained")
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

func TestBaseReadCache_EvictionReusesEntryMetadata(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	keys := make([][]byte, 0, 3)
	for candidate := 0; len(keys) < cap(keys); candidate++ {
		key := []byte(fmt.Sprintf("recycle-entry-%08d", candidate))
		if len(keys) == 0 || baseReadCacheShardIndex(key) == baseReadCacheShardIndex(keys[0]) {
			keys = append(keys, key)
		}
	}
	value := []byte("v")
	s := &c.shards[baseReadCacheShardIndex(keys[0])]
	s.limit = len(keys[0]) + len(value) + baseReadCacheEntryOverhead

	testBaseReadCacheSet(c, keys[0], value)
	first := s.entries[string(keys[0])]
	if first == nil {
		t.Fatal("first entry was not admitted")
	}
	testBaseReadCacheSet(c, keys[1], value)
	if _, ok := s.entries[string(keys[0])]; ok {
		t.Fatal("first entry survived a one-entry cache eviction")
	}
	testBaseReadCacheSet(c, keys[2], value)
	if got := s.entries[string(keys[2])]; got != first {
		t.Fatalf("third admission entry = %p, want recycled first entry %p", got, first)
	}
	if first.key != string(keys[2]) || string(first.value) != string(value) || !first.live {
		t.Fatalf("recycled entry = {key:%q value:%q live:%v}", first.key, first.value, first.live)
	}
	if s.freeEntryCount != 1 {
		t.Fatalf("free entry count = %d, want the evicted second entry", s.freeEntryCount)
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

func TestBaseReadCache_ScopedViewAllowsInPlaceFlushRefresh(t *testing.T) {
	c := newBaseReadCache(1<<20, "state-commitment-branch-v1-")
	key := []byte("state-commitment-branch-v1-scoped-refresh")
	oldValue := []byte("branch-value-one")
	newValue := []byte("branch-value-two")

	// Complete two-hit admission without returning the cache-owned slice.
	for attempt := 0; attempt < 2; attempt++ {
		_, _, epoch := c.getWithEpoch(key)
		stored := c.storeIfEpoch(key, oldValue, epoch)
		if stored != (attempt == 1) {
			t.Fatalf("attempt %d stored=%v", attempt, stored)
		}
	}

	shard := &c.shards[baseReadCacheShardIndex(key)]
	entry := shard.entries[string(key)]
	before := unsafe.SliceData(entry.value)
	called := 0
	cached, present, _, err := c.viewWithEpoch(key, func(value []byte, stable bool) error {
		called++
		if stable || !bytes.Equal(value, oldValue) {
			t.Fatalf("scoped cache view = (%q, stable=%v)", value, stable)
		}
		return nil
	})
	if err != nil || !cached || !present || called != 1 {
		t.Fatalf("scoped view = cached=%v present=%v called=%d err=%v", cached, present, called, err)
	}

	c.setFlushed(string(key), newValue)
	if got := string(entry.value); got != string(newValue) {
		t.Fatalf("refreshed value=%q, want %q", got, newValue)
	}
	if unsafe.SliceData(entry.value) != before {
		t.Fatal("callback-scoped refresh replaced reusable value storage")
	}
}

func TestBaseReadCache_DirectGetPreventsInPlaceFlushRefresh(t *testing.T) {
	c := newBaseReadCache(1<<20, "state-commitment-branch-v1-")
	key := []byte("state-commitment-branch-v1-direct-refresh")
	oldValue := []byte("branch-value-one")
	newValue := []byte("branch-value-two")

	for attempt := 0; attempt < 2; attempt++ {
		_, _, epoch := c.getWithEpoch(key)
		c.storeIfEpoch(key, oldValue, epoch)
	}
	retained, ok, _ := c.getWithEpoch(key)
	if !ok || !bytes.Equal(retained, oldValue) {
		t.Fatalf("direct cache get = (%q,%v)", retained, ok)
	}
	retainedPtr := unsafe.SliceData(retained)
	c.setFlushed(string(key), newValue)
	if !bytes.Equal(retained, oldValue) {
		t.Fatalf("directly retained value mutated to %q", retained)
	}
	entry := c.shards[baseReadCacheShardIndex(key)].entries[string(key)]
	if got := string(entry.value); got != string(newValue) {
		t.Fatalf("refreshed value=%q, want %q", got, newValue)
	}
	if unsafe.SliceData(entry.value) == retainedPtr {
		t.Fatal("directly exposed backing was reused by flush")
	}
}

func TestBaseReadCache_FlushAdmitsReadBeforeWriteValue(t *testing.T) {
	c := newBaseReadCache(1<<20, "frequently-mutated-commitment-")
	key := []byte("frequently-mutated-commitment-branch")

	// The first durable parent read records frequency evidence without
	// retaining its value.
	_, _, epoch := c.getWithEpoch(key)
	if _, stored := c.setIfEpoch(key, []byte("parent-v1"), epoch); stored {
		t.Fatal("first parent read bypassed probation")
	}

	// Committing the block is the second observation. It must invalidate the old
	// durable generation and directly admit the newer canonical value, avoiding
	// one otherwise-mandatory Pebble read in the next block.
	c.setFlushed(string(key), []byte("child-v2"))
	if got, ok, _ := c.getWithEpoch(key); !ok || string(got) != "child-v2" {
		t.Fatalf("flush-admitted value = (%q,%v), want child-v2/true", got, ok)
	}
	if got := len(c.shards[baseReadCacheShardIndex(key)].queue); got != 1 {
		t.Fatalf("flush-admitted queue entries = %d, want 1", got)
	}
}

func TestBaseReadCache_WriteOnlyFlushDoesNotDisplaceReadProbation(t *testing.T) {
	c := newBaseReadCache(1<<20, "read-probation-")
	hotKey := []byte("read-probation-key")
	hotFingerprint := baseReadCacheAdmissionFingerprint(hotKey)
	hotShard := &c.shards[baseReadCacheShardIndex(hotKey)]
	hotIndex := hotFingerprint & uint64(len(hotShard.admission)-1)

	_, _, epoch := c.getWithEpoch(hotKey)
	if _, stored := c.setIfEpoch(hotKey, []byte("parent"), epoch); stored {
		t.Fatal("first parent read bypassed probation")
	}
	if hotShard.admission[hotIndex] != hotFingerprint {
		t.Fatal("parent read did not establish probation")
	}

	// Find a write-only key that maps to the same payload shard and probation
	// slot. Its flush must neither be admitted nor replace the read evidence.
	var writeOnly string
	for i := 0; i < 1_000_000; i++ {
		candidate := fmt.Sprintf("read-probation-write-only-%08d", i)
		fingerprint := baseReadCacheAdmissionFingerprintString(candidate)
		if baseReadCacheShardIndexString(candidate) == baseReadCacheShardIndex(hotKey) &&
			fingerprint&uint64(len(hotShard.admission)-1) == hotIndex &&
			fingerprint != hotFingerprint {
			writeOnly = candidate
			break
		}
	}
	if writeOnly == "" {
		t.Fatal("failed to find colliding write-only key")
	}
	c.setFlushed(writeOnly, []byte("metadata"))
	if _, ok, _ := c.getWithEpoch([]byte(writeOnly)); ok {
		t.Fatal("write-only flush was admitted")
	}
	if hotShard.admission[hotIndex] != hotFingerprint {
		t.Fatal("write-only flush displaced read probation")
	}

	_, _, epoch = c.getWithEpoch(hotKey)
	if got, stored := c.setIfEpoch(hotKey, []byte("parent-v2"), epoch); !stored || string(got) != "parent-v2" {
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

func TestBaseReadCache_FlushAdmissionRejectsLateOldGenerationFill(t *testing.T) {
	c := newBaseReadCache(1<<20, "racing-read-before-write-")
	key := []byte("racing-read-before-write-branch")
	_, _, oldEpoch := c.getWithEpoch(key)
	if _, stored := c.setIfEpoch(key, []byte("old-parent"), oldEpoch); stored {
		t.Fatal("first parent read bypassed probation")
	}

	// A second reader started against the old generation before the canonical
	// flush admits the replacement. Its late fill must not replace new bytes.
	_, _, racingEpoch := c.getWithEpoch(key)
	c.setFlushed(string(key), []byte("new-child"))
	if _, stored := c.setIfEpoch(key, []byte("late-old-parent"), racingEpoch); stored {
		t.Fatal("late old-generation fill replaced flush-admitted value")
	}
	if got, ok, _ := c.getWithEpoch(key); !ok || string(got) != "new-child" {
		t.Fatalf("post-race cache = (%q,%v), want new-child/true", got, ok)
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
	if entry.live || entry.key != string(key) || entry.value != nil || entry.keyCapacity != uint32(len(key)) {
		t.Fatalf("invalidated queued entry retained live=%v key=%q valueBytes=%d keyCap=%d", entry.live, entry.key, len(entry.value), entry.keyCapacity)
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

	b.Run("uncached-write-only", func(b *testing.B) {
		c := newBaseReadCache(64 << 20)
		b.ReportAllocs()
		b.ReportMetric(keyCount, "keys/op")
		b.ResetTimer()
		for range b.N {
			c.promoteFlushedShard(writes, nil, 0)
		}
	})

	b.Run("uncached-priority-namespace", func(b *testing.B) {
		c := newBaseReadCache(64<<20, "commitment-branch-")
		b.ReportAllocs()
		b.ReportMetric(keyCount, "keys/op")
		b.ResetTimer()
		for range b.N {
			c.promoteFlushedShard(writes, nil, 0)
		}
	})
}

func BenchmarkBaseReadCacheRecycledEntryStorage(b *testing.B) {
	keys := [...]string{
		strings.Repeat("a", 96),
		strings.Repeat("b", 80),
	}
	values := [...][]byte{
		bytes.Repeat([]byte{0xa1}, 1024),
		bytes.Repeat([]byte{0xb2}, 768),
	}
	shard := baseReadCacheShard{limit: 1 << 20}
	entry := shard.acquireEntryString(keys[0], values[0], false, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		shard.recycleEntry(entry)
		index := i & 1
		entry = shard.acquireEntryString(keys[index], values[index], false, uint64(i+2))
	}
	baseReadCacheEntryBenchmarkSink = entry
}

func BenchmarkBaseReadCacheAdmissionChurn(b *testing.B) {
	const keyCount = 256
	keys := make([]string, 0, keyCount)
	keyBytes := make([][]byte, 0, keyCount)
	for i := 0; len(keys) < keyCount; i++ {
		key := fmt.Sprintf("commitment-branch-%08x-%08x", i*2654435761, i)
		if baseReadCacheShardIndexString(key) != 0 {
			continue
		}
		keys = append(keys, key)
		keyBytes = append(keyBytes, []byte(key))
	}
	value := bytes.Repeat([]byte{0x5c}, 512)
	c := newBaseReadCache(layerShardCount*4096, "commitment-")
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		index := i % keyCount
		_, found, epoch := c.getWithEpoch(keyBytes[index])
		if found {
			b.Fatal("churn key unexpectedly remained resident for a full cycle")
		}
		if c.storeIfEpoch(keyBytes[index], value, epoch) {
			b.Fatal("first churn sighting bypassed probation")
		}
		c.setFlushedAt(keys[index], value, 0)
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
