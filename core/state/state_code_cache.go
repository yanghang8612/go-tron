package state

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/metrics"
	tcommon "github.com/tronprotocol/go-tron/common"
)

const (
	// DefaultStateCodeCacheSizeBytes is deliberately smaller than the state and
	// Pebble caches. Contract bytecode is immutable and content-addressed, so a
	// modest cache is enough to retain the repeatedly executed working set.
	DefaultStateCodeCacheSizeBytes = 64 * 1024 * 1024

	// codeCacheEntryOverhead charges the cache for the hash, map/list links and
	// allocation headers in addition to the bytecode payload. Go's map bucket
	// layout is implementation-specific; this conservative charge keeps the
	// configured limit meaningful rather than treating metadata as free.
	codeCacheEntryOverhead = 128
)

var (
	stateCodeCacheHitCounter    = metrics.NewRegisteredCounter("state/code_cache/hits", nil)
	stateCodeCacheMissCounter   = metrics.NewRegisteredCounter("state/code_cache/misses", nil)
	stateCodeCacheAdmitCounter  = metrics.NewRegisteredCounter("state/code_cache/admissions", nil)
	stateCodeCacheEvictCounter  = metrics.NewRegisteredCounter("state/code_cache/evictions", nil)
	stateCodeCacheRejectCounter = metrics.NewRegisteredCounter("state/code_cache/hash_rejections", nil)
	stateCodeCacheBytesGauge    = metrics.NewRegisteredGauge("state/code_cache/bytes", nil)
	stateCodeCacheBytesTotal    atomic.Int64
)

// stateCodeCache is owned by one Database. Keeping it at this layer lets all
// short-lived StateDB execution views for that database share successfully
// loaded code without allowing entries to leak into another chain/database.
//
// Values stored in the cache are never returned directly. get clones the
// immutable canonical bytes so callers cannot mutate a slice shared by other
// StateDBs or poison the cache. Misses and read errors are never cached.
type stateCodeCache struct {
	mu       sync.Mutex
	maxBytes int64
	bytes    int64
	entries  map[tcommon.Hash]*list.Element
	lru      list.List
	closed   bool
}

type stateCodeCacheEntry struct {
	hash   tcommon.Hash
	code   []byte
	charge int64
}

func newStateCodeCache(maxBytes int) *stateCodeCache {
	if maxBytes <= 0 {
		return nil
	}
	return &stateCodeCache{
		maxBytes: int64(maxBytes),
		entries:  make(map[tcommon.Hash]*list.Element),
	}
}

func (c *stateCodeCache) get(hash tcommon.Hash) ([]byte, bool) {
	if c == nil || hash == (tcommon.Hash{}) {
		return nil, false
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, false
	}
	elem := c.entries[hash]
	if elem == nil {
		c.mu.Unlock()
		stateCodeCacheMissCounter.Inc(1)
		return nil, false
	}
	c.lru.MoveToFront(elem)
	// Keep only the LRU mutation under the mutex. Eviction drops the cache's
	// reference but never mutates entry bytes, and this local slice keeps the
	// backing allocation alive while the caller-owned copy is made.
	canonical := elem.Value.(*stateCodeCacheEntry).code
	c.mu.Unlock()
	code := append([]byte(nil), canonical...)
	stateCodeCacheHitCounter.Inc(1)
	return code, true
}

// admit retains only non-empty positive results whose bytes match their
// content-addressed key. A malformed durable/cold row keeps its pre-existing
// read behavior, but is not allowed to poison later reads through the cache.
func (c *stateCodeCache) admit(hash tcommon.Hash, code []byte) bool {
	if c == nil || hash == (tcommon.Hash{}) || len(code) == 0 {
		return false
	}
	if tcommon.Keccak256(code) != hash {
		stateCodeCacheRejectCounter.Inc(1)
		return false
	}
	charge := int64(len(code)) + codeCacheEntryOverhead
	if charge > c.maxBytes {
		return false
	}
	owned := append([]byte(nil), code...)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false
	}
	if elem := c.entries[hash]; elem != nil {
		c.lru.MoveToFront(elem)
		c.mu.Unlock()
		return false
	}
	entry := &stateCodeCacheEntry{hash: hash, code: owned, charge: charge}
	c.entries[hash] = c.lru.PushFront(entry)
	c.bytes += charge

	var evicted int64
	var released int64
	for c.bytes > c.maxBytes {
		tail := c.lru.Back()
		if tail == nil {
			break
		}
		victim := tail.Value.(*stateCodeCacheEntry)
		delete(c.entries, victim.hash)
		c.lru.Remove(tail)
		c.bytes -= victim.charge
		released += victim.charge
		evicted++
	}
	if delta := charge - released; delta != 0 {
		updateStateCodeCacheBytes(delta)
	}
	c.mu.Unlock()
	stateCodeCacheAdmitCounter.Inc(1)
	if evicted != 0 {
		stateCodeCacheEvictCounter.Inc(evicted)
	}
	return true
}

func (c *stateCodeCache) close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	released := c.bytes
	c.bytes = 0
	clear(c.entries)
	c.lru.Init()
	c.mu.Unlock()
	if released != 0 {
		updateStateCodeCacheBytes(-released)
	}
}

func updateStateCodeCacheBytes(delta int64) {
	total := stateCodeCacheBytesTotal.Add(delta)
	stateCodeCacheBytesGauge.Update(total)
}
