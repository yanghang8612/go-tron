package blockbuffer

import (
	"bytes"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
)

// baseReadCacheShardCount matches the overlay layer sharding. Both caches serve
// the same physical state keys, so they share the tested O(1) shard selector
// rather than hashing every key twice (once here and once again in the Go map).
const baseReadCacheShardCount = layerShardCount

// baseReadCacheEntryOverhead is a conservative charge for the map entry, queue
// token, slice/string headers, and allocator bookkeeping. The byte budget is an
// operational bound rather than an exact heap accounting value, so charging the
// payload plus this overhead is preferable to silently retaining substantially
// more memory than configured.
const baseReadCacheEntryOverhead = 64

// Recycled entry metadata is outside the payload byte budget. Keep enough per
// shard to absorb steady eviction churn and stale-token compaction without
// retaining an unbounded historical high-water mark after the cache empties.
const baseReadCacheMaxFreeEntries = 2048

// baseReadCacheMaxAdmissionSlots bounds the direct-mapped two-hit admission
// history per shard. The history stores fingerprints only (no key/value
// objects), so a 128 MiB or larger cache spends at most 1 MiB across all 16
// shards to keep one-hit historical-sync scans out of the resident cache.
//
// Historical mainnet sync performs enough durable commitment reads that the
// former 2,048-slot cap was routinely overwritten between a hot branch's first
// and second sighting. Those collisions conservatively behaved like another
// first sighting, but prevented genuinely hot rows from ever reaching the
// payload cache. 8,192 slots keeps the metadata small while reducing that
// direct-map collision window fourfold; the two-hit admission policy itself is
// unchanged.
const baseReadCacheMaxAdmissionSlots = 8192

// baseReadCacheMaxInvalidationSlots bounds the generation table used to reject
// a durable read that races a flush of the SAME key. Cache payload is split into
// 64 lock shards, but using those shards as invalidation generations makes every
// unrelated flush in a shard reject all in-flight fills in that shard. Sustained
// historical sync writes thousands of keys while commitment workers read the
// durable base, so the 64-way false-conflict rate is effectively 100%.
//
// A 128 MiB production cache gets 65,536 generation slots (512 KiB). Collisions
// remain conservative false rejections; they can never publish a stale value.
const baseReadCacheMaxInvalidationSlots = 1 << 16

// baseReadCache is a bounded, sharded FIFO/CLOCK cache for values read from
// Buffer's durable base. It is intentionally below the overlay layers: in-flight and
// committed writes/tombstones are always resolved first, so a fork discard only
// removes overlays and never needs to roll this cache back.
//
// A successful flush refreshes already-cached keys from immutable layer values
// before dropping the layer and invalidates every tombstone. Writes that were
// never read through this cache are not admitted, preventing unrelated block
// metadata from evicting commitment/latest-state rows. Discard clears the whole
// cache before callers perform an out-of-band reset/unwind. Those lifecycle
// hooks make cached values generation-safe without tagging every lookup with
// the current head hash.
type baseReadCache struct {
	shards        [baseReadCacheShardCount]baseReadCacheShard
	invalidations []atomic.Uint64
	// flushAdmissionPrefix narrows read-before-write admission to a schema
	// whose successful canonical writes are expected to be read again soon.
	// Ordinary write-only metadata never pays the full-key probation hash.
	flushAdmissionPrefix string
	// version advances once for every durable-base flush/cache reset. Entries
	// retain the version at which they became valid, allowing a point-in-time
	// commitment session to reuse old hot entries while rejecting replacements
	// published after its Pebble snapshot.
	version atomic.Uint64
}

type baseReadCacheShard struct {
	mu      sync.RWMutex
	entries map[string]*baseReadCacheEntry
	queue   []*baseReadCacheEntry
	// freeEntries reuses metadata only after its sole CLOCK token has been
	// consumed or removed by compaction. Explicit invalidation leaves the
	// cleared entry queued, so it is deliberately not linked here at that point.
	freeEntries    *baseReadCacheEntry
	freeEntryCount int
	// admission is a direct-mapped fingerprint table. A durable miss is
	// admitted only when the same fingerprint is observed twice without being
	// displaced. Commitment sync walks a large number of cold branches once,
	// while upper trie branches and flat-latest rows are revisited quickly; this
	// tiny probation stage preserves the latter without retaining the former.
	admission []uint64
	head      int
	used      int
	limit     int
}

// baseReadCacheEpoch identifies one key's direct-mapped invalidation slot and
// the generation observed before its durable read. A slot collision only drops
// a cache fill; resident entries still compare the complete key.
type baseReadCacheEpoch struct {
	slot  uint32
	value uint64
}

type baseReadCacheEntry struct {
	// key is the same immutable string used by entries. Keeping it on the
	// stable entry lets the CLOCK queue retain only one pointer instead of a
	// second string header plus generation for every resident row.
	key string
	// A nil value is the durable-miss sentinel. Present empty values are stored
	// as a non-nil zero-length slice by cloneBaseReadCacheValue, so callers
	// can distinguish the two without growing every entry with another field.
	value   []byte
	charge  int
	version uint64
	live    bool
	// exposed is set when a direct Get path returns value beyond the cache
	// shard lock. The next changed flush must replace that backing allocation;
	// callback-scoped reads instead hold RLock through consumption and leave it
	// false, allowing a same-capacity refresh to reuse the bytes in place.
	exposed atomic.Bool
	// referenced is set only by a resident cache hit (not admission or flush).
	// Eviction gives such entries one CLOCK-style second chance, preserving hot
	// upper commitment branches without promoting two-hit scan noise forever.
	referenced atomic.Bool
	// keyCapacity records the private key allocation size across shorter-key
	// reuse. It occupies existing alignment padding, so the entry remains in the
	// same 80-byte allocator size class.
	keyCapacity uint32
	// nextFree is used only after the entry's CLOCK token has left the queue.
	// It grows the 72-byte payload to 80 bytes, which is the allocator size
	// class the entry already occupied before this field was added.
	nextFree *baseReadCacheEntry
}

func newBaseReadCache(sizeBytes int, flushAdmissionPrefix ...string) *baseReadCache {
	if sizeBytes <= 0 {
		return nil
	}
	c := &baseReadCache{}
	if len(flushAdmissionPrefix) > 0 {
		c.flushAdmissionPrefix = flushAdmissionPrefix[0]
	}
	c.invalidations = make([]atomic.Uint64, baseReadCacheInvalidationSlots(sizeBytes))
	perShard := sizeBytes / baseReadCacheShardCount
	remainder := sizeBytes % baseReadCacheShardCount
	for i := range c.shards {
		c.shards[i].limit = perShard
		if i < remainder {
			c.shards[i].limit++
		}
		c.shards[i].entries = make(map[string]*baseReadCacheEntry)
		c.shards[i].admission = make([]uint64, baseReadCacheAdmissionSlots(c.shards[i].limit))
	}
	return c
}

// getWithEpoch returns the key's invalidation-slot generation on a miss. A miss
// caller passes it to setIfEpoch after reading the durable base, preventing a
// concurrent same-key flush from being undone by a late cache fill.
func (c *baseReadCache) getWithEpoch(key []byte) ([]byte, bool, baseReadCacheEpoch) {
	if c == nil {
		return nil, false, baseReadCacheEpoch{}
	}
	s := &c.shards[baseReadCacheShardIndex(key)]
	s.mu.RLock()
	e, ok := s.entries[string(key)]
	if ok {
		if !e.referenced.Load() {
			e.referenced.Store(true)
		}
		value := e.value
		if value != nil {
			e.exposed.Store(true)
		}
		s.mu.RUnlock()
		return value, true, baseReadCacheEpoch{}
	}
	s.mu.RUnlock()
	// Compute/load the invalidation slot only on a miss. The dominant cache-hit
	// path therefore keeps its existing O(1) shard lookup cost.
	slot := baseReadCacheInvalidationSlotBytes(key, len(c.invalidations))
	return nil, false, baseReadCacheEpoch{slot: slot, value: c.invalidations[slot].Load()}
}

// getAtVersion is the snapshot-session read path. Entries older than or equal
// to maxVersion agree with that session's durable snapshot; an entry refreshed
// by a later flush is deliberately bypassed in favor of the snapshot cursor.
func (c *baseReadCache) getAtVersion(key []byte, maxVersion uint64) (value []byte, found bool, epoch baseReadCacheEpoch, cacheable bool) {
	if c == nil {
		return nil, false, baseReadCacheEpoch{}, false
	}
	s := &c.shards[baseReadCacheShardIndex(key)]
	s.mu.RLock()
	e, ok := s.entries[string(key)]
	if ok {
		if e.version <= maxVersion {
			if !e.referenced.Load() {
				e.referenced.Store(true)
			}
			value := e.value
			if value != nil {
				e.exposed.Store(true)
			}
			s.mu.RUnlock()
			return value, true, baseReadCacheEpoch{}, false
		}
		s.mu.RUnlock()
		// A newer flush refreshed this key after the session snapshot. The
		// session must use its snapshot value and must not overwrite the newer
		// resident entry with that older value.
		return nil, false, baseReadCacheEpoch{}, false
	}
	s.mu.RUnlock()
	slot := baseReadCacheInvalidationSlotBytes(key, len(c.invalidations))
	return nil, false, baseReadCacheEpoch{slot: slot, value: c.invalidations[slot].Load()}, true
}

// viewWithEpoch is the callback counterpart of getWithEpoch. A hit in the
// configured read-before-write namespace stays protected by the shard read
// lock until fn returns and is reported as stable=false; other immutable cache
// hits retain stable=true and become exposed. The scoped form lets a later
// commitment flush reuse its value allocation without racing a decoder or
// invalidating a borrowed leaf key.
func (c *baseReadCache) viewWithEpoch(key []byte, fn func(value []byte, stable bool) error) (cached, present bool, epoch baseReadCacheEpoch, err error) {
	if c == nil {
		return false, false, baseReadCacheEpoch{}, nil
	}
	s := &c.shards[baseReadCacheShardIndex(key)]
	s.mu.RLock()
	e, ok := s.entries[string(key)]
	if ok {
		if !e.referenced.Load() {
			e.referenced.Store(true)
		}
		if e.value == nil {
			s.mu.RUnlock()
			return true, false, baseReadCacheEpoch{}, nil
		}
		if !c.scopedRefreshKey(key) {
			e.exposed.Store(true)
			value := e.value
			s.mu.RUnlock()
			return true, true, baseReadCacheEpoch{}, fn(value, true)
		}
		defer s.mu.RUnlock()
		return true, true, baseReadCacheEpoch{}, fn(e.value, false)
	}
	s.mu.RUnlock()
	slot := baseReadCacheInvalidationSlotBytes(key, len(c.invalidations))
	return false, false, baseReadCacheEpoch{slot: slot, value: c.invalidations[slot].Load()}, nil
}

// viewAtVersion applies viewWithEpoch's scoped lifetime to a commitment
// snapshot session. A replacement newer than maxVersion is bypassed exactly as
// in getAtVersion and cannot be overwritten by the older snapshot value.
func (c *baseReadCache) viewAtVersion(key []byte, maxVersion uint64, fn func(value []byte, stable bool) error) (cached, present bool, epoch baseReadCacheEpoch, cacheable bool, err error) {
	if c == nil {
		return false, false, baseReadCacheEpoch{}, false, nil
	}
	s := &c.shards[baseReadCacheShardIndex(key)]
	s.mu.RLock()
	e, ok := s.entries[string(key)]
	if ok {
		if e.version <= maxVersion {
			if !e.referenced.Load() {
				e.referenced.Store(true)
			}
			if e.value == nil {
				s.mu.RUnlock()
				return true, false, baseReadCacheEpoch{}, false, nil
			}
			if !c.scopedRefreshKey(key) {
				e.exposed.Store(true)
				value := e.value
				s.mu.RUnlock()
				return true, true, baseReadCacheEpoch{}, false, fn(value, true)
			}
			defer s.mu.RUnlock()
			return true, true, baseReadCacheEpoch{}, false, fn(e.value, false)
		}
		s.mu.RUnlock()
		return false, false, baseReadCacheEpoch{}, false, nil
	}
	s.mu.RUnlock()
	slot := baseReadCacheInvalidationSlotBytes(key, len(c.invalidations))
	return false, false, baseReadCacheEpoch{slot: slot, value: c.invalidations[slot].Load()}, true, nil
}

// scopedRefreshKey reports whether callback consumers of key may receive a
// cache-owned value only for the duration of the callback. Restricting this to
// the configured read-before-write namespace keeps immutable state-code and
// flat-latest cache hits shareable while commitment branches gain safe in-place
// flush refreshes.
func (c *baseReadCache) scopedRefreshKey(key []byte) bool {
	if c == nil || c.flushAdmissionPrefix == "" || len(key) < len(c.flushAdmissionPrefix) {
		return false
	}
	for i := range c.flushAdmissionPrefix {
		if key[i] != c.flushAdmissionPrefix[i] {
			return false
		}
	}
	return true
}

func (c *baseReadCache) advanceVersion() uint64 {
	if c == nil {
		return 0
	}
	return c.version.Add(1)
}

// setIfEpoch copies key/value into cache-owned immutable storage only if no
// flush/reset invalidated the target shard since the caller's cache miss.
// Returning the stored slice lets the caller decode it without a second lookup
// or depending on the base reader's value lifetime. The boolean reports
// whether the returned slice is cache-owned; a callback-backed reader must copy
// it before returning when false.
func (c *baseReadCache) setIfEpoch(key, value []byte, epoch baseReadCacheEpoch) ([]byte, bool) {
	return c.setEntryIfEpoch(key, value, false, true, epoch)
}

// storeIfEpoch admits an immutable copy without returning its backing bytes.
// Callback-style durable readers consume their original scoped/owned value and
// use this form so the cache entry remains eligible for in-place flush refresh.
func (c *baseReadCache) storeIfEpoch(key, value []byte, epoch baseReadCacheEpoch) bool {
	_, stored := c.setEntryIfEpoch(key, value, false, false, epoch)
	return stored
}

// setMissingIfEpoch records a confirmed durable-base miss. Missing rows are
// subject to the same two-hit admission and generation checks as values: cold
// one-shot scans stay out of the resident cache, while repeated permission and
// storage probes stop reopening Pebble iterators. Overlay layers are consulted
// first, and flush/discard invalidation uses the same complete physical key.
func (c *baseReadCache) setMissingIfEpoch(key []byte, epoch baseReadCacheEpoch) bool {
	_, stored := c.setEntryIfEpoch(key, nil, true, false, epoch)
	return stored
}

func (c *baseReadCache) setEntryIfEpoch(key, value []byte, missing, expose bool, epoch baseReadCacheEpoch) ([]byte, bool) {
	if c == nil {
		return value, false
	}
	charge := len(key) + len(value) + baseReadCacheEntryOverhead
	s := &c.shards[baseReadCacheShardIndex(key)]
	if charge > s.limit {
		return value, false
	}

	s.mu.Lock()
	if c.invalidations[epoch.slot].Load() != epoch.value {
		s.mu.Unlock()
		return value, false
	}
	// Another reader may have observed the same miss/epoch and completed its
	// durable read while this caller was in Pebble. Reuse that immutable value
	// instead of copying the same key/value again, appending a stale queue entry,
	// and replacing an entry from the identical durable generation.
	if current, ok := s.entries[string(key)]; ok {
		current.referenced.Store(true)
		value := current.value
		if expose && value != nil {
			current.exposed.Store(true)
		}
		s.mu.Unlock()
		return value, true
	}
	if !s.admit(key) {
		s.mu.Unlock()
		return value, false
	}
	var v []byte
	if !missing {
		v = cloneBaseReadCacheValue(value)
	}
	entry := s.acquireEntryBytes(key, v, charge, c.version.Load())
	if expose && v != nil {
		entry.exposed.Store(true)
	}
	s.entries[entry.key] = entry
	s.queue = append(s.queue, entry)
	s.used += charge
	s.evict()
	s.compactIfSparse()
	s.mu.Unlock()
	return v, true
}

// setFlushed refreshes an already-cached key from a successfully flushed
// committed layer. A key absent from the cache is admitted only when a prior
// durable read left the same key's probation fingerprint. Thus read-then-write
// counts as the two observations required by admission, while unrelated
// buffered metadata (which has no read-side fingerprint) still cannot churn
// through the cache on every canonical flush.
// Keys never read through the cache have no fingerprint and remain unadmitted.
// Cached replacements are copied into exact cache-owned storage: commitment
// sibling writes arena-pack hundreds of branch values, so retaining one small
// layer slice directly could pin the whole arena while the byte budget charged
// only the slice length.
//
// Advancing the key's invalidation slot before replacement rejects any late
// cache fill that started against the pre-flush durable generation. If the
// value is too large for its shard, the old entry is still invalidated and no
// replacement is retained.
func (c *baseReadCache) setFlushed(key string, value []byte) {
	c.setFlushedAt(key, value, baseReadCacheShardIndexString(key))
}

// setFlushedAt is setFlushed for callers that already know key's cache shard.
// Layer maps and the base cache intentionally share the same selector, so
// flush promotion can carry that index forward instead of sampling the key a
// second time for every durable write.
func (c *baseReadCache) setFlushedAt(key string, value []byte, shard uint32) {
	if c == nil {
		return
	}
	c.advanceInvalidationString(key)
	s := &c.shards[shard]
	s.mu.Lock()
	c.setFlushedLocked(s, key, value)
	s.compactIfSparse()
	s.mu.Unlock()
}

// setFlushedLocked refreshes one resident value after its invalidation epoch
// has advanced. The caller owns s.mu.
func (c *baseReadCache) setFlushedLocked(s *baseReadCacheShard, key string, value []byte) {
	charge := len(key) + len(value) + baseReadCacheEntryOverhead
	old, cached := s.entries[key]
	if !cached && charge <= s.limit && c.flushAdmissionPrefix != "" &&
		strings.HasPrefix(key, c.flushAdmissionPrefix) && s.admitObservedString(key) {
		// The first durable read already paid for this value and established
		// frequency evidence. The successful canonical flush supplies the
		// second observation and a newer immutable value, so retain it now
		// instead of forcing the next block to read Pebble once more merely to
		// complete admission. Clone the layer-owned key/value: sibling batches
		// may share large arenas which are released after layer promotion.
		entry := s.acquireEntryString(key, cloneBaseReadCacheValue(value), charge, c.version.Load())
		s.entries[entry.key] = entry
		s.queue = append(s.queue, entry)
		s.used += charge
		s.evict()
		return
	}
	if cached && charge <= s.limit {
		// Preserve the stable entry and its CLOCK queue pointer. This is a value
		// refresh, not a new admission; appending one pointer per block would grow
		// stale queue metadata for the lifetime of a hot commitment branch.
		// Copy only the replacement value and update the stable entry in place.
		// The map and stable entry retain a separately allocated key. Previously key
		// and value shared one allocation, so that entry pinned the first value for
		// the entry's entire lifetime even after the map installed a newer clone.
		// State setters can dirty a row whose final value is byte-identical to
		// the durable parent. Its commitment ancestors are then encoded and
		// flushed again even though their immutable bytes did not change. Keep
		// the existing cache-owned value in that case instead of allocating an
		// identical replacement. A nil old value is the cached-miss sentinel and
		// must still be replaced by a non-nil present-empty value.
		if old.value == nil || !bytes.Equal(old.value, value) {
			if old.value != nil && !old.exposed.Load() && len(value) <= cap(old.value) {
				// No direct Get has exposed this backing and callback readers are
				// excluded by s.mu, so reuse it without changing the conservative
				// capacity-based charge retained from its original allocation.
				old.value = old.value[:len(value)]
				copy(old.value, value)
				old.version = c.version.Load()
				return
			}
			old.value = cloneBaseReadCacheValue(value)
			old.exposed.Store(false)
		}
		s.used += charge - old.charge
		old.charge = charge
		old.version = c.version.Load()
		s.evict()
	} else {
		if cached {
			delete(s.entries, key)
			s.used -= old.charge
			old.live = false
			retainBaseReadCacheKeyStorage(old)
		}
	}
}

// promoteFlushedShard applies one immutable layer shard to its matching cache
// shard under a single lock. Overlay reads continue to resolve the source layer
// until promotion completes, so batching these resident-cache refreshes cannot
// expose an old cached value between the durable batch commit and layer drop.
func (c *baseReadCache) promoteFlushedShard(writes map[string][]byte, deletes map[string]struct{}, shard uint32) {
	if c == nil || (len(writes) == 0 && len(deletes) == 0) {
		return
	}
	s := &c.shards[shard]
	s.mu.Lock()
	for key, value := range writes {
		c.advanceInvalidationString(key)
		c.setFlushedLocked(s, key, value)
	}
	for key := range deletes {
		c.advanceInvalidationString(key)
		c.delStringLocked(s, key)
	}
	s.compactIfSparse()
	s.mu.Unlock()
}

// Keys and values intentionally use separate backing. Map keys and queue
// entries live for the entry's whole residency, while a successful flush may
// replace its value many times. Sharing their backing would make the long-lived
// key pin the first stale value. Recycled metadata may lend its prior private
// key backing to the next key without coupling either key to a value payload.
func cloneBaseReadCacheValue(value []byte) []byte {
	storage := make([]byte, len(value))
	copy(storage, value)
	return storage
}

func (s *baseReadCacheShard) takeEntry() *baseReadCacheEntry {
	entry := s.freeEntries
	if entry == nil {
		return new(baseReadCacheEntry)
	}
	s.freeEntries = entry.nextFree
	s.freeEntryCount--
	entry.nextFree = nil
	return entry
}

func (s *baseReadCacheShard) acquireEntryBytes(key []byte, value []byte, charge int, version uint64) *baseReadCacheEntry {
	entry := s.takeEntry()
	entry.key, entry.keyCapacity = cloneBaseReadCacheKeyBytes(key, entry.value)
	entry.value = value
	entry.charge = charge
	entry.version = version
	entry.live = true
	return entry
}

func (s *baseReadCacheShard) acquireEntryString(key string, value []byte, charge int, version uint64) *baseReadCacheEntry {
	entry := s.takeEntry()
	entry.key, entry.keyCapacity = cloneBaseReadCacheKeyString(key, entry.value)
	entry.value = value
	entry.charge = charge
	entry.version = version
	entry.live = true
	return entry
}

func cloneBaseReadCacheKeyBytes(key, storage []byte) (string, uint32) {
	if len(key) == 0 {
		return "", 0
	}
	if len(key) <= cap(storage) {
		capacity := cap(storage)
		storage = storage[:len(key)]
		copy(storage, key)
		return unsafe.String(unsafe.SliceData(storage), len(storage)), uint32(capacity)
	}
	return string(key), uint32(len(key))
}

func cloneBaseReadCacheKeyString(key string, storage []byte) (string, uint32) {
	if len(key) == 0 {
		return "", 0
	}
	if len(key) <= cap(storage) {
		capacity := cap(storage)
		storage = storage[:len(key)]
		copy(storage, key)
		return unsafe.String(unsafe.SliceData(storage), len(storage)), uint32(capacity)
	}
	return strings.Clone(key), uint32(len(key))
}

// retainBaseReadCacheKeyStorage moves the private key backing into value while
// an entry waits for its CLOCK token to be consumed or sits on the free list.
// No caller can observe entry keys, and the shard lock excludes map readers
// before removal, so a later admission may safely overwrite these bytes.
func retainBaseReadCacheKeyStorage(entry *baseReadCacheEntry) {
	if entry.key == "" {
		return
	}
	entry.value = unsafe.Slice(unsafe.StringData(entry.key), int(entry.keyCapacity))
	entry.key = ""
}

func (s *baseReadCacheShard) recycleEntry(entry *baseReadCacheEntry) {
	retainBaseReadCacheKeyStorage(entry)
	entry.charge = 0
	entry.version = 0
	entry.live = false
	entry.exposed.Store(false)
	entry.referenced.Store(false)
	entry.nextFree = nil
	if s.freeEntryCount >= baseReadCacheMaxFreeEntries {
		return
	}
	entry.nextFree = s.freeEntries
	s.freeEntries = entry
	s.freeEntryCount++
}

func (c *baseReadCache) del(key []byte) {
	if c == nil {
		return
	}
	c.delStringAt(string(key), baseReadCacheShardIndex(key))
}

func (c *baseReadCache) delString(key string) {
	if c == nil {
		return
	}
	c.delStringAt(key, baseReadCacheShardIndexString(key))
}

func (c *baseReadCache) delStringAt(key string, shard uint32) {
	c.advanceInvalidationString(key)
	s := &c.shards[shard]
	s.mu.Lock()
	c.delStringLocked(s, key)
	s.compactIfSparse()
	s.mu.Unlock()
}

// delStringLocked removes one resident value and its admission history. The
// caller owns s.mu and has already advanced the key's invalidation epoch.
func (c *baseReadCache) delStringLocked(s *baseReadCacheShard, key string) {
	if old, ok := s.entries[key]; ok {
		delete(s.entries, key)
		s.used -= old.charge
		old.live = false
		retainBaseReadCacheKeyStorage(old)
	}
	s.forgetAdmissionString(key)
}

func (c *baseReadCache) clear() {
	if c == nil {
		return
	}
	c.advanceVersion()
	// Bracket the clear with generation advances. A read that began before or
	// during the clear cannot publish after it; reads beginning after the second
	// pass observe the new stable generation. Clear/discard is rare, so touching
	// the bounded table twice is preferable to adding a global atomic read to
	// every ordinary cache miss.
	c.advanceAllInvalidations()
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		clear(s.entries)
		clear(s.queue)
		s.queue = s.queue[:0]
		clear(s.admission)
		s.head = 0
		s.used = 0
		s.freeEntries = nil
		s.freeEntryCount = 0
		s.mu.Unlock()
	}
	c.advanceAllInvalidations()
}

func baseReadCacheInvalidationSlots(sizeBytes int) int {
	target := sizeBytes / 2048
	if target < baseReadCacheShardCount {
		target = baseReadCacheShardCount
	}
	if target > baseReadCacheMaxInvalidationSlots {
		target = baseReadCacheMaxInvalidationSlots
	}
	slots := 1
	for slots<<1 <= target {
		slots <<= 1
	}
	return slots
}

func (c *baseReadCache) advanceInvalidationString(key string) {
	slot := baseReadCacheInvalidationSlotString(key, len(c.invalidations))
	c.invalidations[slot].Add(1)
}

func (c *baseReadCache) advanceAllInvalidations() {
	for i := range c.invalidations {
		c.invalidations[i].Add(1)
	}
}

// The byte/string forms must match. Physical state keys put their entropy in
// the middle and tail (commitment nibbles, addresses and storage slots), so a
// handful of sampled bytes plus avalanche mixing distributes them without
// hashing the full 30-100 byte key on every flushed write.
func baseReadCacheInvalidationSlotBytes(key []byte, slots int) uint32 {
	n := len(key)
	if n == 0 {
		return 0
	}
	h := uint32(n) * 0x9e3779b1
	mix := func(b byte) {
		h ^= uint32(b)
		h *= 0x85ebca6b
		h ^= h >> 13
	}
	mix(key[n-1])
	if n > 1 {
		mix(key[n-2])
	}
	if n > 3 {
		mix(key[n-4])
	}
	if n > 8 {
		mix(key[n/2])
	}
	if n > 16 {
		mix(key[n/3])
		mix(key[(n*2)/3])
	}
	return h & uint32(slots-1)
}

func baseReadCacheInvalidationSlotString(key string, slots int) uint32 {
	n := len(key)
	if n == 0 {
		return 0
	}
	h := uint32(n) * 0x9e3779b1
	mix := func(b byte) {
		h ^= uint32(b)
		h *= 0x85ebca6b
		h ^= h >> 13
	}
	mix(key[n-1])
	if n > 1 {
		mix(key[n-2])
	}
	if n > 3 {
		mix(key[n-4])
	}
	if n > 8 {
		mix(key[n/2])
	}
	if n > 16 {
		mix(key[n/3])
		mix(key[(n*2)/3])
	}
	return h & uint32(slots-1)
}

func baseReadCacheAdmissionSlots(limit int) int {
	// Keep probation metadata small relative to the configured payload budget:
	// one 8-byte slot per 256 bytes, rounded down to a power of two for direct
	// indexing. Real deployments hit the cap; tiny unit-test caches do not
	// silently allocate a deployment-sized table.
	target := limit / 256
	if target < 8 {
		target = 8
	}
	if target > baseReadCacheMaxAdmissionSlots {
		target = baseReadCacheMaxAdmissionSlots
	}
	slots := 1
	for slots<<1 <= target {
		slots <<= 1
	}
	return slots
}

// admit reports whether key has completed its probationary first sighting.
// Fingerprint collisions can only cause a cold row to be admitted early; they
// cannot return an incorrect value because resident entries still compare the
// complete key. Clearing the slot on admission means a later eviction requires
// fresh evidence before the row can pollute the cache again.
func (s *baseReadCacheShard) admit(key []byte) bool {
	fingerprint := baseReadCacheAdmissionFingerprint(key)
	index := fingerprint & uint64(len(s.admission)-1)
	if s.admission[index] == fingerprint {
		s.admission[index] = 0
		return true
	}
	s.admission[index] = fingerprint
	return false
}

// admitObservedString admits key only when an earlier read already populated
// its probation slot. Unlike admit, a miss does not install the fingerprint:
// calling this from flush promotion must not let write-only metadata displace
// evidence collected by durable reads.
func (s *baseReadCacheShard) admitObservedString(key string) bool {
	fingerprint := baseReadCacheAdmissionFingerprintString(key)
	index := fingerprint & uint64(len(s.admission)-1)
	if s.admission[index] != fingerprint {
		return false
	}
	s.admission[index] = 0
	return true
}

func (s *baseReadCacheShard) forgetAdmissionString(key string) {
	fingerprint := baseReadCacheAdmissionFingerprintString(key)
	index := fingerprint & uint64(len(s.admission)-1)
	if s.admission[index] == fingerprint {
		s.admission[index] = 0
	}
}

func baseReadCacheAdmissionFingerprint(key []byte) uint64 {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)
	hash := offset
	for _, b := range key {
		hash = (hash ^ uint64(b)) * prime
	}
	// Zero denotes an empty direct-mapped slot.
	if hash == 0 {
		return 1
	}
	return hash
}

func baseReadCacheAdmissionFingerprintString(key string) uint64 {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)
	hash := offset
	for i := 0; i < len(key); i++ {
		hash = (hash ^ uint64(key[i])) * prime
	}
	if hash == 0 {
		return 1
	}
	return hash
}

func (s *baseReadCacheShard) evict() {
	for s.used > s.limit && s.head < len(s.queue) {
		entry := s.queue[s.head]
		s.queue[s.head] = nil
		s.head++
		if entry == nil {
			continue
		}
		if entry.live {
			if entry.referenced.Swap(false) {
				// Move the same stable entry to the tail. Newly admitted entries
				// start unreferenced, so a
				// one-time two-hit scan is evicted before a genuinely reused row.
				s.queue = append(s.queue, entry)
				continue
			}
			delete(s.entries, entry.key)
			s.used -= entry.charge
		}
		s.recycleEntry(entry)
	}
	// Avoid retaining an ever-growing stale-entry prefix when invalidated keys
	// are later inserted again. Copy only occasionally so steady-state hits stay
	// allocation-free.
	if s.head >= 1024 && s.head*2 >= len(s.queue) {
		copy(s.queue, s.queue[s.head:])
		s.queue = s.queue[:len(s.queue)-s.head]
		s.head = 0
	}
}

// compactIfSparse bounds stale queue entries left by explicit invalidation. A
// sync node may cache a branch, flush and invalidate it, then repeat that cycle
// for millions of blocks without ever exceeding the payload byte limit; without
// this ratio gate the FIFO metadata alone would grow for the whole session.
func (s *baseReadCacheShard) compactIfSparse() {
	liveTokens := len(s.queue) - s.head
	if liveTokens < 2048 || liveTokens <= len(s.entries)*2+1024 {
		return
	}
	queue := make([]*baseReadCacheEntry, 0, len(s.entries))
	for _, entry := range s.entries {
		queue = append(queue, entry)
	}
	for _, entry := range s.queue[s.head:] {
		if entry != nil && !entry.live {
			s.recycleEntry(entry)
		}
	}
	clear(s.queue)
	s.queue = queue
	s.head = 0
}

func baseReadCacheShardIndex(key []byte) uint32 {
	return layerShardIndexBytes(key)
}

func baseReadCacheShardIndexString(key string) uint32 {
	return layerShardIndexString(key)
}

func (b *Buffer) promoteBaseReadCacheLayer(l *layer) {
	if b == nil || b.baseReadCache == nil || l == nil {
		return
	}
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.RLock()
		b.baseReadCache.promoteFlushedShard(s.writes, s.deletes, uint32(i))
		s.mu.RUnlock()
	}
}

func (b *Buffer) promoteBaseReadCacheLayers(layers []*layer) {
	if b == nil || b.baseReadCache == nil {
		return
	}
	for start := 0; start < len(layers); {
		end := start
		queuedValueSize := 0
		queuedEncodedSize := pebbleBatchHeaderSize
		for end < len(layers) {
			valueSize, encodedSize := layerWriteStats(layers[end])
			if end > start && (queuedValueSize+valueSize > maxFlushBatchValueSize ||
				queuedEncodedSize+encodedSize > maxFlushBatchEncodedSize) {
				break
			}
			queuedValueSize += valueSize
			queuedEncodedSize += encodedSize
			end++
			if queuedValueSize >= maxFlushBatchValueSize || queuedEncodedSize >= maxFlushBatchEncodedSize {
				break
			}
		}
		if end-start == 1 {
			b.promoteBaseReadCacheLayer(layers[start])
			start = end
			continue
		}
		merged := borrowFlushMergedOps()
		mergeLayers(layers[start:end], merged)
		for k, op := range merged.ops {
			if op.delete {
				b.baseReadCache.delStringAt(k, uint32(op.shard))
			} else {
				b.baseReadCache.setFlushedAt(k, op.value, uint32(op.shard))
			}
		}
		returnFlushMergedOps(merged)
		start = end
	}
}

func (b *Buffer) clearBaseReadCache() {
	if b != nil && b.baseReadCache != nil {
		b.baseReadCache.clear()
	}
}
