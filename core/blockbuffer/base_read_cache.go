package blockbuffer

import (
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

// baseReadCacheMaxAdmissionSlots bounds the direct-mapped two-hit admission
// history per shard. The history stores fingerprints only (no key/value
// objects), so a 128 MiB or larger cache spends at most 1 MiB across all 64
// shards to keep one-hit historical-sync scans out of the resident cache.
const baseReadCacheMaxAdmissionSlots = 2048

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

// baseReadCache is a bounded, sharded FIFO cache for values read from Buffer's
// durable base. It is intentionally below the overlay layers: in-flight and
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
}

type baseReadCacheShard struct {
	mu      sync.RWMutex
	entries map[string]baseReadCacheEntry
	queue   []baseReadCacheToken
	// admission is a direct-mapped fingerprint table. A durable miss is
	// admitted only when the same fingerprint is observed twice without being
	// displaced. Commitment sync walks a large number of cold branches once,
	// while upper trie branches and flat-latest rows are revisited quickly; this
	// tiny probation stage preserves the latter without retaining the former.
	admission []uint64
	head      int
	used      int
	limit     int
	nextGen   uint64
}

// baseReadCacheEpoch identifies one key's direct-mapped invalidation slot and
// the generation observed before its durable read. A slot collision only drops
// a cache fill; resident entries still compare the complete key.
type baseReadCacheEpoch struct {
	slot  uint32
	value uint64
}

type baseReadCacheEntry struct {
	// A nil value is the durable-miss sentinel. Present empty values are stored
	// as a non-nil zero-length slice by cloneBaseReadCacheKeyValue, so callers
	// can distinguish the two without growing every entry with another field.
	value  []byte
	charge int
	gen    uint64
}

type baseReadCacheToken struct {
	key string
	gen uint64
}

func newBaseReadCache(sizeBytes int) *baseReadCache {
	if sizeBytes <= 0 {
		return nil
	}
	c := &baseReadCache{}
	c.invalidations = make([]atomic.Uint64, baseReadCacheInvalidationSlots(sizeBytes))
	perShard := sizeBytes / baseReadCacheShardCount
	remainder := sizeBytes % baseReadCacheShardCount
	for i := range c.shards {
		c.shards[i].limit = perShard
		if i < remainder {
			c.shards[i].limit++
		}
		c.shards[i].entries = make(map[string]baseReadCacheEntry)
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
	s.mu.RUnlock()
	if ok {
		return e.value, true, baseReadCacheEpoch{}
	}
	// Compute/load the invalidation slot only on a miss. The dominant cache-hit
	// path therefore keeps its existing O(1) shard lookup cost.
	slot := baseReadCacheInvalidationSlotBytes(key, len(c.invalidations))
	return nil, false, baseReadCacheEpoch{slot: slot, value: c.invalidations[slot].Load()}
}

// setIfEpoch copies key/value into cache-owned immutable storage only if no
// flush/reset invalidated the target shard since the caller's cache miss.
// Returning the stored slice lets the caller decode it without a second lookup
// or depending on the base reader's value lifetime. The boolean reports
// whether the returned slice is cache-owned; a callback-backed reader must copy
// it before returning when false.
func (c *baseReadCache) setIfEpoch(key, value []byte, epoch baseReadCacheEpoch) ([]byte, bool) {
	return c.setEntryIfEpoch(key, value, false, epoch)
}

// setMissingIfEpoch records a confirmed durable-base miss. Missing rows are
// subject to the same two-hit admission and generation checks as values: cold
// one-shot scans stay out of the resident cache, while repeated permission and
// storage probes stop reopening Pebble iterators. Overlay layers are consulted
// first, and flush/discard invalidation uses the same complete physical key.
func (c *baseReadCache) setMissingIfEpoch(key []byte, epoch baseReadCacheEpoch) bool {
	_, stored := c.setEntryIfEpoch(key, nil, true, epoch)
	return stored
}

func (c *baseReadCache) setEntryIfEpoch(key, value []byte, missing bool, epoch baseReadCacheEpoch) ([]byte, bool) {
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
	// instead of copying the same key/value again, appending a stale FIFO token,
	// and replacing an entry from the identical durable generation.
	if current, ok := s.entries[string(key)]; ok {
		s.mu.Unlock()
		return current.value, true
	}
	if !s.admit(key) {
		s.mu.Unlock()
		return value, false
	}
	k, v := cloneBaseReadCacheKeyValue(key, value)
	if missing {
		v = nil
	}
	s.nextGen++
	gen := s.nextGen
	s.entries[k] = baseReadCacheEntry{value: v, charge: charge, gen: gen}
	s.queue = append(s.queue, baseReadCacheToken{key: k, gen: gen})
	s.used += charge
	s.evict()
	s.compactIfSparse()
	s.mu.Unlock()
	return v, true
}

// setFlushed refreshes an already-cached key from a successfully flushed
// committed layer. A key absent from the cache is invalidated but not admitted;
// otherwise unrelated buffered metadata would churn through the cache on every
// canonical flush. Cached replacements are copied into exact cache-owned
// storage: commitment sibling writes arena-pack hundreds of branch values, so
// retaining one small layer slice directly could pin the whole arena while the
// byte budget charged only the slice length.
//
// Advancing the key's invalidation slot before replacement rejects any late
// cache fill that started against the pre-flush durable generation. If the
// value is too large for its shard, the old entry is still invalidated and no
// replacement is retained.
func (c *baseReadCache) setFlushed(key string, value []byte) {
	if c == nil {
		return
	}
	charge := len(key) + len(value) + baseReadCacheEntryOverhead
	c.advanceInvalidationString(key)
	s := &c.shards[baseReadCacheShardIndexString(key)]
	s.mu.Lock()
	old, cached := s.entries[key]
	if cached && charge <= s.limit {
		// Preserve the existing generation and its FIFO token. This is a value
		// refresh, not a new admission; appending one token per block would grow
		// stale queue metadata for the lifetime of a hot commitment branch.
		// Clone key and value into one exact-sized per-entry allocation before
		// replacing: using either incoming slice/string directly would retain the
		// block layer's shared key/value arenas well beyond the layer lifetime.
		ownedKey, ownedValue := cloneBaseReadCacheKeyValue(
			unsafe.Slice(unsafe.StringData(key), len(key)), value,
		)
		delete(s.entries, key)
		s.entries[ownedKey] = baseReadCacheEntry{value: ownedValue, charge: charge, gen: old.gen}
		s.used += charge - old.charge
		s.evict()
	} else {
		if cached {
			delete(s.entries, key)
			s.used -= old.charge
		}
		s.forgetAdmissionString(key)
	}
	s.compactIfSparse()
	s.mu.Unlock()
}

// cloneBaseReadCacheKeyValue copies key and value into one exact-sized backing
// allocation. The returned string and slice share only that per-entry storage;
// neither can retain a larger Pebble or block-layer arena.
func cloneBaseReadCacheKeyValue(key, value []byte) (string, []byte) {
	storage := make([]byte, len(key)+len(value))
	copy(storage, key)
	copy(storage[len(key):], value)
	var ownedKey string
	if len(key) > 0 {
		ownedKey = unsafe.String(unsafe.SliceData(storage), len(key))
	}
	ownedValue := storage[len(key):len(storage):len(storage)]
	return ownedKey, ownedValue
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
	if old, ok := s.entries[key]; ok {
		delete(s.entries, key)
		s.used -= old.charge
	}
	s.forgetAdmissionString(key)
	s.compactIfSparse()
	s.mu.Unlock()
}

func (c *baseReadCache) clear() {
	if c == nil {
		return
	}
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
		tok := s.queue[s.head]
		s.queue[s.head] = baseReadCacheToken{}
		s.head++
		if current, ok := s.entries[tok.key]; ok && current.gen == tok.gen {
			delete(s.entries, tok.key)
			s.used -= current.charge
		}
	}
	// Avoid retaining an ever-growing stale-token prefix when invalidated keys
	// are later inserted again. Copy only occasionally so steady-state hits stay
	// allocation-free.
	if s.head >= 1024 && s.head*2 >= len(s.queue) {
		copy(s.queue, s.queue[s.head:])
		s.queue = s.queue[:len(s.queue)-s.head]
		s.head = 0
	}
}

// compactIfSparse bounds stale queue tokens left by explicit invalidation. A
// sync node may cache a branch, flush and invalidate it, then repeat that cycle
// for millions of blocks without ever exceeding the payload byte limit; without
// this ratio gate the FIFO metadata alone would grow for the whole session.
func (s *baseReadCacheShard) compactIfSparse() {
	liveTokens := len(s.queue) - s.head
	if liveTokens < 2048 || liveTokens <= len(s.entries)*2+1024 {
		return
	}
	queue := make([]baseReadCacheToken, 0, len(s.entries))
	for k, e := range s.entries {
		queue = append(queue, baseReadCacheToken{key: k, gen: e.gen})
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
		for k, v := range s.writes {
			b.baseReadCache.setFlushed(k, v)
		}
		for k := range s.deletes {
			b.baseReadCache.delString(k)
		}
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
				b.baseReadCache.delString(k)
			} else {
				b.baseReadCache.setFlushed(k, op.value)
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
