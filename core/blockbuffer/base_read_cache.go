package blockbuffer

import "sync"

// baseReadCacheShardCount keeps cache lookup contention low when commitment
// folds and flat-latest readers run concurrently. A power of two lets the FNV
// hash select a shard with a mask.
const baseReadCacheShardCount = 64

// baseReadCacheEntryOverhead is a conservative charge for the map entry, queue
// token, slice/string headers, and allocator bookkeeping. The byte budget is an
// operational bound rather than an exact heap accounting value, so charging the
// payload plus this overhead is preferable to silently retaining substantially
// more memory than configured.
const baseReadCacheEntryOverhead = 64

// baseReadCache is a bounded, sharded FIFO cache for values read from Buffer's
// durable base. It is intentionally below the overlay layers: in-flight and
// committed writes/tombstones are always resolved first, so a fork discard only
// removes overlays and never needs to roll this cache back.
//
// Flush invalidates every key written to the durable base before dropping its
// layer. Discard clears the whole cache before callers perform an out-of-band
// reset/unwind. Those two lifecycle hooks make cached values generation-safe
// without tagging every lookup with the current head hash.
type baseReadCache struct {
	shards [baseReadCacheShardCount]baseReadCacheShard
}

type baseReadCacheShard struct {
	mu      sync.RWMutex
	entries map[string]baseReadCacheEntry
	queue   []baseReadCacheToken
	head    int
	used    int
	limit   int
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
	k := string(key)
	v := append([]byte(nil), value...)
	if old, ok := s.entries[k]; ok {
		s.used -= old.charge
	}
	s.nextGen++
	gen := s.nextGen
	s.entries[k] = baseReadCacheEntry{value: v, charge: charge, gen: gen}
	s.queue = append(s.queue, baseReadCacheToken{key: k, gen: gen})
	s.used += charge
	s.evict()
	s.mu.Unlock()
	return v, true
}

func (c *baseReadCache) del(key []byte) {
	if c == nil {
		return
	}
	s := &c.shards[baseReadCacheShardIndex(key)]
	s.mu.Lock()
	s.epoch++
	k := string(key)
	if old, ok := s.entries[k]; ok {
		delete(s.entries, k)
		s.used -= old.charge
	}
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
		s.head = 0
		s.used = 0
		s.mu.Unlock()
	}
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
	const (
		offset32 = uint32(2166136261)
		prime32  = uint32(16777619)
	)
	h := offset32
	for _, b := range key {
		h ^= uint32(b)
		h *= prime32
	}
	return h & (baseReadCacheShardCount - 1)
}

func (b *Buffer) invalidateBaseReadCacheLayer(l *layer) {
	if b == nil || b.baseReadCache == nil || l == nil {
		return
	}
	for k := range l.writes {
		b.baseReadCache.del([]byte(k))
	}
	for k := range l.deletes {
		b.baseReadCache.del([]byte(k))
	}
}

func (b *Buffer) invalidateBaseReadCacheLayers(layers []*layer) {
	if b == nil || b.baseReadCache == nil {
		return
	}
	for _, l := range layers {
		b.invalidateBaseReadCacheLayer(l)
	}
}

func (b *Buffer) clearBaseReadCache() {
	if b != nil && b.baseReadCache != nil {
		b.baseReadCache.clear()
	}
}
