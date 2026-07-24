package blockbuffer

import "sync"

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
	shards [baseReadCacheShardCount]baseReadCacheShard
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
	// epoch advances before this shard is invalidated or cleared. A base read
	// captures it on cache miss; if a flush advances it before that read returns,
	// setIfEpoch rejects the now-stale late fill.
	epoch   uint64
	nextGen uint64
}

type baseReadCacheEntry struct {
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

// getWithEpoch returns the shard epoch observed atomically with the lookup. A
// miss caller passes it to setIfEpoch after reading the durable base, preventing
// a concurrent flush invalidation from being undone by a late cache fill.
func (c *baseReadCache) getWithEpoch(key []byte) ([]byte, bool, uint64) {
	if c == nil {
		return nil, false, 0
	}
	s := &c.shards[baseReadCacheShardIndex(key)]
	s.mu.RLock()
	e, ok := s.entries[string(key)]
	epoch := s.epoch
	s.mu.RUnlock()
	if !ok {
		return nil, false, epoch
	}
	// Values are immutable after publication. delete/set replace the entry and
	// never mutate its backing bytes, so returning the alias is safe even when a
	// concurrent invalidation removes the map entry immediately afterward.
	return e.value, true, epoch
}

// setIfEpoch copies key/value into cache-owned immutable storage only if no
// flush/reset invalidated the target shard since the caller's cache miss.
// Returning the stored slice lets the caller decode it without a second lookup
// or depending on the base reader's value lifetime. The boolean reports
// whether the returned slice is cache-owned; a callback-backed reader must copy
// it before returning when false.
func (c *baseReadCache) setIfEpoch(key, value []byte, epoch uint64) ([]byte, bool) {
	if c == nil {
		return value, false
	}
	charge := len(key) + len(value) + baseReadCacheEntryOverhead
	s := &c.shards[baseReadCacheShardIndex(key)]
	if charge > s.limit {
		return value, false
	}

	s.mu.Lock()
	if s.epoch != epoch {
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
	k := string(key)
	v := append([]byte(nil), value...)
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
// committed layer. Committed layer values are immutable and owned by the
// Buffer, so the cache can retain value directly instead of allocating the copy
// setIfEpoch needs for a Pebble-borrowed read result. A key absent from the
// cache is invalidated but not admitted; otherwise unrelated buffered metadata
// would churn through the cache on every canonical flush.
//
// Advancing the shard epoch before replacement rejects any late cache fill
// that started against the pre-flush durable generation. If the value is too
// large for its shard, the old entry is still invalidated and no replacement
// is retained.
func (c *baseReadCache) setFlushed(key string, value []byte) {
	if c == nil {
		return
	}
	charge := len(key) + len(value) + baseReadCacheEntryOverhead
	s := &c.shards[baseReadCacheShardIndexString(key)]
	s.mu.Lock()
	s.epoch++
	old, cached := s.entries[key]
	if cached {
		delete(s.entries, key)
		s.used -= old.charge
	}
	if cached && charge <= s.limit {
		// Preserve the existing generation and its FIFO token. This is a value
		// refresh, not a new admission; appending one token per block would grow
		// stale queue metadata for the lifetime of a hot commitment branch.
		s.entries[key] = baseReadCacheEntry{value: value, charge: charge, gen: old.gen}
		s.used += charge
		s.evict()
	} else {
		s.forgetAdmissionString(key)
	}
	s.compactIfSparse()
	s.mu.Unlock()
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
	s := &c.shards[shard]
	s.mu.Lock()
	s.epoch++
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
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		s.epoch++
		clear(s.entries)
		clear(s.queue)
		s.queue = s.queue[:0]
		clear(s.admission)
		s.head = 0
		s.used = 0
		s.mu.Unlock()
	}
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
	for _, l := range layers {
		b.promoteBaseReadCacheLayer(l)
	}
}

func (b *Buffer) clearBaseReadCache() {
	if b != nil && b.baseReadCache != nil {
		b.baseReadCache.clear()
	}
}
