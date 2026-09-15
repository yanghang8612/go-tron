package rawdb

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

const StateHistoryChunkCachePayloadBudget = uint64(64 << 20)
const StateHistoryChunkCacheEntryLimit = 4096

var ErrStateHistoryChunkCacheClosed = errors.New("rawdb: build-scoped history chunk cache closed")

// StateHistoryChunkCacheStats counts lookup hits (which omit chunk reads/SHA),
// not pack authentication. Every complete pack SHA still runs. Payload accounts
// for owned cache slice capacities only, not the fixed entry table (~0.3MiB),
// allocator overhead, pack output, codec/ETL buffers or total heap/RSS.
type StateHistoryChunkCacheStats struct {
	Hits               uint64 `json:"hits"`
	Misses             uint64 `json:"misses"`
	Inserts            uint64 `json:"inserts"`
	Evictions          uint64 `json:"evictions"`
	PayloadBytes       uint64 `json:"payload_bytes"`
	PeakPayloadBytes   uint64 `json:"peak_payload_bytes"`
	Entries            uint64 `json:"entries"`
	PeakEntries        uint64 `json:"peak_entries"`
	PayloadBudgetBytes uint64 `json:"payload_budget_bytes"`
	EntryLimit         uint64 `json:"entry_limit"`
	Closed             bool   `json:"closed"`
}

type historyChunkCacheKey struct {
	bucket uint64
	hash   [32]byte
	want   uint32
}
type historyChunkCacheEntry struct {
	key  historyChunkCacheKey
	data []byte
}

// StateHistoryChunkCache owns only one explicitly opted-in build's authenticated
// chunk copies. Its private view must not be reused across builds. Callers must
// join all reads/iterators before Close, exactly as for the underlying snapshot.
// Close is idempotent but is not a concurrent-read cancellation/join primitive.
// No cached slice is returned. In particular, hit copies remain under RLock so
// evicted payload cannot escape the retained-byte accounting while being copied.
type StateHistoryChunkCache struct {
	mu       sync.RWMutex
	entries  []historyChunkCacheEntry
	clock    int
	stats    StateHistoryChunkCacheStats
	hits     atomic.Uint64
	misses   atomic.Uint64
	closed   atomic.Bool
	close    sync.Once
	release  func() error
	closeErr error
	scope    context.Context
}

// AcquireStateHistoryChunkCacheView explicitly opts into build-scoped shared
// chunk authentication reuse on one audited immutable pinned owned view. The
// ordinary public reader/default builder does not construct this adapter.
// First misses retain Has/Get, codec/length and chunk SHA; later hits use only
// already-authenticated private bytes. Thus transient I/O error traces on hits
// differ from ordinary reads. Whole-pack SHA and both build passes remain intact.
// The returned cache must be closed after the entire trio (including worker
// joins), before releasing a caller-owned source snapshot. Factories acquired
// here are released by Close; borrowed sources remain owned by their caller.
func AcquireStateHistoryChunkCacheView(ctx context.Context, source any) (StateHistoryReadView, *StateHistoryChunkCache, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	view, release, err := AcquireStateHistoryReadView(source)
	if err != nil {
		return nil, nil, err
	}
	capability, ok := view.(pointread.ConcurrentOwnedKeyValueView)
	if !ok || !capability.IsPinnedKeyValueView() || !capability.GetReturnsOwnedBytes() || !capability.ConcurrentOwnedHistoryReads() {
		return nil, nil, errors.Join(ErrStateHistoryPipelineView, release())
	}
	if _, ok := view.(interface {
		GetWithPresence([]byte) ([]byte, bool, error)
	}); ok {
		return nil, nil, errors.Join(ErrStateHistoryPipelineView, release())
	}
	if _, ok := view.(historyChunkCacheProvider); ok {
		return nil, nil, errors.Join(errors.New("rawdb: nested build-scoped history chunk cache"), release())
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, errors.Join(err, release())
	}
	c := newStateHistoryChunkCache(StateHistoryChunkCachePayloadBudget, StateHistoryChunkCacheEntryLimit, release)
	c.scope = ctx
	return &stateHistoryChunkCachedView{StateHistoryReadView: view, cache: c, ctx: ctx}, c, nil
}

func newStateHistoryChunkCache(budget uint64, entries int, release func() error) *StateHistoryChunkCache {
	return &StateHistoryChunkCache{entries: make([]historyChunkCacheEntry, entries), release: release, stats: StateHistoryChunkCacheStats{PayloadBudgetBytes: budget, EntryLimit: uint64(entries)}}
}

func (c *StateHistoryChunkCache) Stats() StateHistoryChunkCacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s := c.stats
	s.Hits = c.hits.Load()
	s.Misses = c.misses.Load()
	s.Closed = c.closed.Load()
	return s
}

func (c *StateHistoryChunkCache) Close() error {
	if c == nil {
		return nil
	}
	c.close.Do(func() {
		c.mu.Lock()
		c.closed.Store(true)
		clear(c.entries)
		c.entries = nil
		c.stats.PayloadBytes, c.stats.Entries = 0, 0
		c.mu.Unlock()
		if c.release != nil {
			c.closeErr = c.release()
		}
	})
	return c.closeErr
}

// Keys represent this one schema family's complete physical identity plus the
// reference's expected decoded length. The physical lookup itself always uses
// stateHistoryChunkKey; this index hash is only a bounded replacement policy.
func historyChunkCacheIndex(key historyChunkCacheKey, slots int) int {
	mix := binary.LittleEndian.Uint64(key.hash[:8]) ^ key.bucket*0x9e3779b97f4a7c15 ^ uint64(key.want)*0xbf58476d1ce4e5b9
	return int(mix % uint64(slots))
}

func (c *StateHistoryChunkCache) copyOrRead(ctx context.Context, db ethdb.KeyValueReader, dst []byte, bucket uint64, want int, hash [32]byte) error {
	if err := c.readContextErr(ctx); err != nil {
		return err
	}
	if c.closed.Load() {
		return ErrStateHistoryChunkCacheClosed
	}
	if want <= 0 || want > historychunk.MaxSize || len(dst) != want {
		return readStateHistorySharedChunkInto(db, dst, bucket, want, hash)
	}
	key := historyChunkCacheKey{bucket: bucket, hash: hash, want: uint32(want)}
	c.mu.RLock()
	if err := c.readContextErr(ctx); err != nil {
		c.mu.RUnlock()
		return err
	}
	if c.closed.Load() {
		c.mu.RUnlock()
		return ErrStateHistoryChunkCacheClosed
	}
	if len(c.entries) > 0 {
		e := &c.entries[historyChunkCacheIndex(key, len(c.entries))]
		if e.data != nil && e.key == key {
			c.hits.Add(1)
			copy(dst, e.data)
			c.mu.RUnlock()
			return nil
		}
	}
	c.misses.Add(1)
	c.mu.RUnlock()
	// Decode/authenticate directly into the already-owned pack output. There
	// is no extra miss buffer or singleflight, and failures are never cached.
	if err := readStateHistorySharedChunkInto(db, dst, bucket, want, hash); err != nil {
		return err
	}
	if err := c.readContextErr(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.readContextErr(ctx); err != nil {
		return err
	}
	if c.closed.Load() {
		return ErrStateHistoryChunkCacheClosed
	}
	if len(c.entries) == 0 || uint64(want) > c.stats.PayloadBudgetBytes {
		return nil
	}
	i := historyChunkCacheIndex(key, len(c.entries))
	if e := &c.entries[i]; e.data != nil && e.key == key {
		return nil
	} // independent miss already installed it
	c.evict(i)
	// Bounded clock eviction. Clear all old payload references before cloning;
	// there is no uncharged retired slice retained in a local entry copy.
	for scanned := 0; uint64(want) > c.stats.PayloadBudgetBytes-c.stats.PayloadBytes && scanned < len(c.entries); scanned++ {
		c.evict(c.clock)
		c.clock = (c.clock + 1) % len(c.entries)
	}
	if uint64(want) > c.stats.PayloadBudgetBytes-c.stats.PayloadBytes {
		return nil
	}
	owned := make([]byte, len(dst))
	copy(owned, dst)
	c.entries[i] = historyChunkCacheEntry{key: key, data: owned}
	c.stats.Inserts++
	c.stats.Entries++
	c.stats.PayloadBytes += uint64(cap(owned))
	c.stats.PeakEntries = max(c.stats.PeakEntries, c.stats.Entries)
	c.stats.PeakPayloadBytes = max(c.stats.PeakPayloadBytes, c.stats.PayloadBytes)
	return nil
}

func (c *StateHistoryChunkCache) readContextErr(ctx context.Context) error {
	if c.scope != nil {
		if err := c.scope.Err(); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (c *StateHistoryChunkCache) evict(i int) {
	if c.entries[i].data == nil {
		return
	}
	c.stats.PayloadBytes -= uint64(cap(c.entries[i].data))
	c.stats.Entries--
	c.stats.Evictions++
	c.entries[i] = historyChunkCacheEntry{}
}

// This capability is private to rawdb so external wrappers cannot attach a
// cache to another source. Pipeline forwarding keeps the original job context.
type historyChunkCacheProvider interface {
	historyChunkCacheState() (*StateHistoryChunkCache, context.Context)
}
type stateHistoryChunkCachedView struct {
	StateHistoryReadView
	cache *StateHistoryChunkCache
	ctx   context.Context
}

func (v *stateHistoryChunkCachedView) historyChunkCacheState() (*StateHistoryChunkCache, context.Context) {
	return v.cache, v.ctx
}
func (v *stateHistoryChunkCachedView) IsPinnedKeyValueView() bool { return !v.cache.closed.Load() }
func (v *stateHistoryChunkCachedView) GetReturnsOwnedBytes() bool { return !v.cache.closed.Load() }
func (v *stateHistoryChunkCachedView) ConcurrentOwnedHistoryReads() bool {
	return !v.cache.closed.Load()
}
func (v *stateHistoryChunkCachedView) Get(key []byte) ([]byte, error) {
	if v.cache.closed.Load() {
		return nil, ErrStateHistoryChunkCacheClosed
	}
	if err := v.cache.readContextErr(v.ctx); err != nil {
		return nil, err
	}
	return v.StateHistoryReadView.Get(key)
}
func (v *stateHistoryChunkCachedView) Has(key []byte) (bool, error) {
	if v.cache.closed.Load() {
		return false, ErrStateHistoryChunkCacheClosed
	}
	if err := v.cache.readContextErr(v.ctx); err != nil {
		return false, err
	}
	return v.StateHistoryReadView.Has(key)
}
func (v *stateHistoryChunkCachedView) NewIterator(prefix, start []byte) ethdb.Iterator {
	if v.cache.closed.Load() {
		return &stateHistoryErrorIterator{err: ErrStateHistoryChunkCacheClosed}
	}
	if err := v.cache.readContextErr(v.ctx); err != nil {
		return &stateHistoryErrorIterator{err: err}
	}
	return v.StateHistoryReadView.NewIterator(prefix, start)
}
