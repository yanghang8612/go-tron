package blockbuffer

import (
	"bytes"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/tronprotocol/go-tron/core/rawdb"
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

// Allocate stable entry metadata in modest slabs. Maps, CLOCK queues and the
// free list still hold ordinary *baseReadCacheEntry pointers, but one heap
// object now owns many adjacent 80-byte entries. This materially reduces GC
// object count and findObject work without changing payload ownership or the
// configured byte budget. Interior pointers keep a slab alive while any of its
// entries remain resident or recyclable.
const baseReadCacheEntryBatchSize = 64

// Recycled entry metadata is outside the payload byte budget. One slab-sized
// reserve per shard absorbs steady eviction churn; the small cap also bounds
// whole-slab retention after the cache empties, even if free pointers happen to
// come from different slabs.
const baseReadCacheMaxFreeEntries = baseReadCacheEntryBatchSize

// baseReadCacheMaxReferenceCredit lets repeated resident hits accumulate a
// small amount of CLOCK protection. A single bit loses frequency information:
// a branch read hundreds of times between two eviction sweeps receives the
// same protection as a row read once. Historical sync continuously admits new
// commitment paths, so that policy lets the hot upper trie working set fall
// out after one scan wave and sends the fold back to Pebble. Three credits keep
// genuinely hot rows through short admission bursts while remaining strictly
// bounded; newly admitted/two-hit scan rows still start at zero and are the
// first eviction candidates.
const (
	baseReadCacheMaxReferenceCredit = 3
	// The CLOCK credit uses only the low bits of references. Reuse one otherwise
	// idle high bit to mark a direct read-ahead admission without growing every
	// cache entry from the allocator's 80-byte class to 88 bytes.
	baseReadCachePrefetchedReference uint32 = 1 << 31
)

// Recycle only modest, commonly sized unexposed values and keep their total
// backing below one eighth of each shard's live payload limit. This makes the
// allocation cache useful for commitment branches without letting a historical
// high-water mark silently double the configured base-cache budget.
const (
	baseReadCacheMaxFreeValueSize       = 16 << 10
	baseReadCacheFreeValueBudgetDivisor = 8
)

// baseReadCacheMaxAdmissionSlots bounds the direct-mapped two-hit admission
// history per shard. The history stores fingerprints only (no key/value
// objects), so a 128 MiB or larger cache spends 4 MiB across all 16 shards to
// keep one-hit historical-sync scans out of the resident cache. When namespace
// weighting is enabled, the non-commitment class gets a separate 1 MiB table so
// commitment scans cannot erase its probation evidence.
//
// Historical mainnet sync performs enough durable commitment reads that the
// former 2,048-slot cap was routinely overwritten between a hot branch's first
// and second sighting. Those collisions conservatively behaved like another
// first sighting, but prevented genuinely hot rows from ever reaching the
// payload cache. The stored-mainnet replay still performs roughly 6,000 cold
// branch reads per second with the 8,192-slot cap (only 64 KiB of history per
// shard), so a branch revisited after a few seconds can lose its frequency
// evidence to an unrelated direct-map collision. 32,768 slots preserve a
// materially longer observation window for only 4 MiB of process-wide metadata;
// the two-hit admission policy itself is unchanged.
const baseReadCacheMaxAdmissionSlots = 32768

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

// Keep a bounded share of the common cache available to flat-latest, account
// and code rows when commitment replay continuously streams branch keys. The
// reservation is deliberately soft: either class may borrow unused capacity,
// while contention converges toward three quarters commitment and one quarter
// other. Total retained charge remains unchanged.
const baseReadCacheOtherReserveDivisor = 4

// Keep the global commitment-trie trunk resident, following Erigon's split
// branch-cache design: depths 0-4 use a fixed tier while deeper branches compete
// in the adaptive tail. A 16-way hex trie has at most 69,905 branches through
// depth four, so one eighth of the configured cache is a conservative bounded
// budget (64 MiB with the production 512 MiB cache). The reservation is hard per
// shard: once full, additional shallow rows fall back to normal two-hit/CLOCK
// admission instead of growing memory without bound.
const (
	baseReadCacheTrunkDepth         = 4
	baseReadCacheTrunkBudgetDivisor = 8
	// Deep commitment rows get a small first-read window before the ordinary
	// two-hit CLOCK tail. This adapts Erigon's first-read branch-cache tail to
	// Pebble without letting one-shot historical scans occupy the main cache:
	// rows reused while resident are promoted, while untouched rows leave at
	// the window boundary. The window is part of (not additional to) limit.
	baseReadCacheWindowBudgetDivisor = 8
	// Window admission learns from completed FIFO outcomes. Historical sync has
	// long scan phases where every first-read row is evicted untouched, but can
	// also move into phases with strong short-term path reuse. Every outcome
	// batch adjusts first-read sampling by one power of two: dry scans converge
	// to 1/64 admission, while useful windows recover to full admission.
	baseReadCacheWindowOutcomeBatch      = 1024
	baseReadCacheWindowLowReuseDivisor   = 16
	baseReadCacheWindowHighReuseDivisor  = 4
	baseReadCacheWindowMaxAdmissionShift = 6
)

// Occupancy gauges are diagnostics rather than correctness state. Publishing
// on the first commitment-session close and then once per 64 closes keeps the
// 16 short shard RLocks off the read path while staying fresher than the normal
// operator sampling interval.
const baseReadCacheMetricsPublishInterval = uint64(64)

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
	// trunkDepth >= 0 enables fixed residency for commitment paths through this
	// depth. The ordinary cache constructor leaves it at -1; the chain wiring
	// explicitly opts in only for the physical commitment-branch namespace.
	trunkDepth int
	// version advances once for every durable-base flush/cache reset. Entries
	// retain the version at which they became valid, allowing a point-in-time
	// commitment session to reuse old hot entries while rejecting replacements
	// published after its Pebble snapshot.
	version                atomic.Uint64
	metricsPublishSequence atomic.Uint64
}

type baseReadCacheShard struct {
	mu      sync.RWMutex
	entries map[string]*baseReadCacheEntry
	// queue is the commitment/default CLOCK. nonCommitmentQueue is enabled only
	// when a flush-admission prefix configures the production namespace split.
	// The entry map and invalidation/version lifecycle remain shared; probation
	// tables are class-local so a commitment scan cannot erase flat-state
	// frequency evidence before its second observation.
	queue              []*baseReadCacheEntry
	nonCommitmentQueue []*baseReadCacheEntry
	// windowQueue is a bounded FIFO for present commitment branches below the
	// fixed trunk. It removes the otherwise mandatory second Pebble read from
	// short-term reuse while keeping scan pollution out of queue.
	windowQueue []*baseReadCacheEntry
	// freeEntries reuses metadata only after its sole CLOCK token has been
	// consumed or removed by compaction. Explicit invalidation leaves the
	// cleared entry queued, so it is deliberately not linked here at that point.
	freeEntries    *baseReadCacheEntry
	freeEntryCount int
	freeValueBytes int
	// admission is a direct-mapped fingerprint table. A durable miss is
	// admitted only when the same fingerprint is observed twice without being
	// displaced. Commitment sync walks a large number of cold branches once,
	// while upper trie branches and flat-latest rows are revisited quickly; this
	// tiny probation stage preserves the latter without retaining the former.
	admission              []uint64
	nonCommitmentAdmission []uint64
	head                   int
	nonCommitmentHead      int
	windowHead             int
	used                   int
	nonCommitmentUsed      int
	trunkUsed              int
	windowUsed             int
	// Tail entries are derived from len(entries) minus these mutually exclusive
	// flagged classes, so ordinary commitment admission adds no count write.
	nonCommitmentEntries int
	trunkEntries         int
	windowEntries        int
	// windowAdmissions is process-lifetime for this cache owner. It stays
	// monotonic across clear so the metrics publisher can expose it as a gauge
	// without adding a contended global atomic to every durable admission.
	windowAdmissions       uint64
	limit                  int
	nonCommitmentLimit     int
	trunkLimit             int
	windowLimit            int
	windowAdmissionCounter uint32
	windowProbeCandidates  uint16
	windowProbeAdmissions  uint16
	windowOutcomeCount     uint16
	windowPromotions       uint16
	windowAdmissionShift   uint8
	windowHitEvents        atomic.Uint32
}

// baseReadCacheEpoch identifies one key's direct-mapped invalidation slot and
// the generation observed before its durable read. A slot collision only drops
// a cache fill; resident entries still compare the complete key.
type baseReadCacheEpoch struct {
	slot  uint32
	value uint64
}

type baseReadCacheEntry struct {
	// key is the same immutable string used by entries. Keeping it on the stable
	// entry lets the CLOCK queue retain only one pointer instead of a second
	// string header plus generation for every resident row. Once removed from
	// the map, the private backing remains on recycled metadata for the next key.
	key string
	// A nil value is the durable-miss sentinel. Present empty values are stored
	// as a non-nil zero-length slice by cloneBaseReadCacheValue, so callers
	// can distinguish the two without growing every entry with another field.
	value   []byte
	charge  int
	version uint64
	live    bool
	// nonCommitment selects the protected flat/account/code CLOCK segment. It
	// fits in the structure's existing boolean/alignment padding.
	nonCommitment bool
	// trunk marks a shallow commitment branch held outside the CLOCK queues.
	// Its charge is included in used and trunkUsed, both bounded by shard limits.
	trunk bool
	// window marks a deep commitment branch admitted on its first durable read.
	// A resident hit gives it CLOCK credit; the FIFO boundary then promotes it
	// into the main queue, otherwise it is discarded as a one-hit scan row.
	window bool
	// exposed is set when a direct Get path returns value beyond the cache
	// shard lock. The next changed flush must replace that backing allocation;
	// callback-scoped reads instead hold RLock through consumption and leave it
	// false, allowing a same-capacity refresh to reuse the bytes in place.
	exposed atomic.Bool
	// references stores a small saturating CLOCK credit in its low bits and the
	// direct-read-ahead marker in its high bit. Eviction consumes only credit;
	// the first ordinary cache hit consumes the marker for usefulness metrics.
	references atomic.Uint32
	// keyCapacity records the private key allocation size across shorter-key
	// reuse. It occupies existing alignment padding, so the entry remains in the
	// same 80-byte allocator size class.
	keyCapacity uint32
	// nextFree is used only after the entry's CLOCK token has left the queue.
	// It grows the 72-byte payload to 80 bytes, which is the allocator size
	// class the entry already occupied before this field was added.
	nextFree *baseReadCacheEntry
}

func (e *baseReadCacheEntry) reference() {
	for {
		state := e.references.Load()
		credit := state &^ baseReadCachePrefetchedReference
		if credit >= baseReadCacheMaxReferenceCredit {
			return
		}
		if e.references.CompareAndSwap(state, state+1) {
			return
		}
	}
}

func (e *baseReadCacheEntry) consumeReference() bool {
	for {
		state := e.references.Load()
		credit := state &^ baseReadCachePrefetchedReference
		if credit == 0 {
			return false
		}
		if e.references.CompareAndSwap(state, state-1) {
			return true
		}
	}
}

func (e *baseReadCacheEntry) recordUsefulPrefetch() bool {
	for {
		state := e.references.Load()
		if state&baseReadCachePrefetchedReference == 0 {
			return false
		}
		if e.references.CompareAndSwap(state, state&^baseReadCachePrefetchedReference) {
			baseReadCachePrefetchUsefulCounter.Inc(1)
			return true
		}
	}
}

func (e *baseReadCacheEntry) hasUnusedPrefetch() bool {
	return e != nil && e.references.Load()&baseReadCachePrefetchedReference != 0
}

func newBaseReadCache(sizeBytes int, flushAdmissionPrefix ...string) *baseReadCache {
	return newBaseReadCacheWithTrunk(sizeBytes, -1, flushAdmissionPrefix...)
}

func newBaseReadCacheWithTrunk(sizeBytes, trunkDepth int, flushAdmissionPrefix ...string) *baseReadCache {
	if sizeBytes <= 0 {
		return nil
	}
	c := &baseReadCache{trunkDepth: trunkDepth}
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
		if c.flushAdmissionPrefix != "" {
			c.shards[i].nonCommitmentLimit = c.shards[i].limit / baseReadCacheOtherReserveDivisor
			if trunkDepth >= 0 {
				c.shards[i].trunkLimit = c.shards[i].limit / baseReadCacheTrunkBudgetDivisor
				c.shards[i].windowLimit = c.shards[i].limit / baseReadCacheWindowBudgetDivisor
			}
		}
		c.shards[i].entries = make(map[string]*baseReadCacheEntry)
		c.shards[i].admission = make([]uint64, baseReadCacheAdmissionSlots(c.shards[i].limit))
		if c.shards[i].nonCommitmentLimit > 0 {
			// Keep the classes' probation evidence independent without doubling
			// the existing 4 MiB production table. The protected class receives
			// one quarter as many slots (1 MiB process-wide), matching its reserved
			// share; commitment retains its full historical window.
			otherSlots := len(c.shards[i].admission) / baseReadCacheOtherReserveDivisor
			if otherSlots < 8 {
				otherSlots = 8
			}
			c.shards[i].nonCommitmentAdmission = make([]uint64, otherSlots)
		}
	}
	return c
}

func (c *baseReadCache) isOtherKeyBytes(key []byte) bool {
	_, commitment := c.commitmentKeyDepthBytes(key)
	return c != nil && c.flushAdmissionPrefix != "" && !commitment
}

func (c *baseReadCache) isOtherKeyString(key string) bool {
	_, commitment := c.commitmentKeyDepthString(key)
	return c != nil && c.flushAdmissionPrefix != "" && !commitment
}

func (c *baseReadCache) isCommitmentTrunkKey(key []byte) bool {
	depth, ok := c.commitmentKeyDepthBytes(key)
	return ok && c.trunkDepth >= 0 && depth <= c.trunkDepth
}

func (c *baseReadCache) isCommitmentWindowKey(key []byte) bool {
	depth, ok := c.commitmentKeyDepthBytes(key)
	return ok && c.trunkDepth >= 0 && depth > c.trunkDepth
}

// commitmentKeyDepthBytes recognizes both the complete legacy branch table and
// generation-qualified hot deltas when the cache is configured for the staged
// commitment family. The eight generation bytes are schema, not trie depth.
func (c *baseReadCache) commitmentKeyDepthBytes(key []byte) (int, bool) {
	if c == nil || c.flushAdmissionPrefix == "" {
		return 0, false
	}
	if c.flushAdmissionPrefix == rawdb.CommitmentBranchKeyPrefix {
		legacy := []byte(rawdb.CommitmentBranchKeyPrefix)
		if bytes.HasPrefix(key, legacy) {
			return len(key) - len(legacy), true
		}
		delta := []byte(rawdb.CommitmentBranchDeltaKeyPrefix)
		schemaLen := len(delta) + 8
		if len(key) >= schemaLen && bytes.HasPrefix(key, delta) {
			return len(key) - schemaLen, true
		}
		return 0, false
	}
	prefix := []byte(c.flushAdmissionPrefix)
	if !bytes.HasPrefix(key, prefix) {
		return 0, false
	}
	return len(key) - len(prefix), true
}

func (c *baseReadCache) commitmentKeyDepthString(key string) (int, bool) {
	if c == nil || c.flushAdmissionPrefix == "" {
		return 0, false
	}
	if c.flushAdmissionPrefix == rawdb.CommitmentBranchKeyPrefix {
		if strings.HasPrefix(key, rawdb.CommitmentBranchKeyPrefix) {
			return len(key) - len(rawdb.CommitmentBranchKeyPrefix), true
		}
		schemaLen := len(rawdb.CommitmentBranchDeltaKeyPrefix) + 8
		if len(key) >= schemaLen && strings.HasPrefix(key, rawdb.CommitmentBranchDeltaKeyPrefix) {
			return len(key) - schemaLen, true
		}
		return 0, false
	}
	if !strings.HasPrefix(key, c.flushAdmissionPrefix) {
		return 0, false
	}
	return len(key) - len(c.flushAdmissionPrefix), true
}

// getWithEpoch returns the key's invalidation-slot generation on a miss. A miss
// caller passes it to setIfEpoch after reading the durable base, preventing a
// concurrent same-key flush from being undone by a late cache fill.
func (c *baseReadCache) getWithEpoch(key []byte) ([]byte, bool, baseReadCacheEpoch) {
	return c.getWithEpochMode(key, true)
}

func (c *baseReadCache) getForPrefetchWithEpoch(key []byte) ([]byte, bool, baseReadCacheEpoch) {
	return c.getWithEpochMode(key, false)
}

func (c *baseReadCache) getWithEpochMode(key []byte, recordUseful bool) ([]byte, bool, baseReadCacheEpoch) {
	if c == nil {
		return nil, false, baseReadCacheEpoch{}
	}
	s := &c.shards[baseReadCacheShardIndex(key)]
	s.mu.RLock()
	e, ok := s.entries[string(key)]
	if ok {
		e.reference()
		if recordUseful {
			e.recordUsefulPrefetch()
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
			e.reference()
			e.recordUsefulPrefetch()
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

// probeAtVersionForPrefetch is the ownership-free counterpart of
// getAtVersion. A planned read-ahead only needs presence: it must neither expose
// the resident value backing nor consume the prefetched marker that an eventual
// foreground fold hit uses to measure usefulness.
func (c *baseReadCache) probeAtVersionForPrefetch(key []byte, maxVersion uint64) (cached, present bool, epoch baseReadCacheEpoch, cacheable bool) {
	if c == nil {
		return false, false, baseReadCacheEpoch{}, false
	}
	s := &c.shards[baseReadCacheShardIndex(key)]
	s.mu.RLock()
	e, ok := s.entries[string(key)]
	if ok {
		if e.version <= maxVersion {
			e.reference()
			present = e.value != nil
			s.mu.RUnlock()
			return true, present, baseReadCacheEpoch{}, false
		}
		s.mu.RUnlock()
		// The resident row belongs to a flush newer than this session's Pebble
		// snapshot. Read the pinned snapshot, but never replace the newer row.
		return false, false, baseReadCacheEpoch{}, false
	}
	s.mu.RUnlock()
	slot := baseReadCacheInvalidationSlotBytes(key, len(c.invalidations))
	return false, false, baseReadCacheEpoch{slot: slot, value: c.invalidations[slot].Load()}, true
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
		e.reference()
		e.recordUsefulPrefetch()
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
func (c *baseReadCache) viewAtVersion(key []byte, maxVersion uint64, fn func(value []byte, stable bool) error) (cached, present, windowHit, usefulPrefetch bool, epoch baseReadCacheEpoch, cacheable bool, err error) {
	if c == nil {
		return false, false, false, false, baseReadCacheEpoch{}, false, nil
	}
	s := &c.shards[baseReadCacheShardIndex(key)]
	s.mu.RLock()
	e, ok := s.entries[string(key)]
	if ok {
		if e.version <= maxVersion {
			e.reference()
			usefulPrefetch = e.recordUsefulPrefetch()
			if usefulPrefetch {
				commitmentParentPrefetchUsefulCounter.Inc(1)
			}
			windowHit = e.window
			if windowHit {
				// Only the commitment-session path needs admission feedback. Keeping
				// this atomic off ordinary Get hits preserves their existing hot path.
				s.windowHitEvents.Add(1)
			}
			if e.value == nil {
				s.mu.RUnlock()
				return true, false, windowHit, usefulPrefetch, baseReadCacheEpoch{}, false, nil
			}
			if !c.scopedRefreshKey(key) {
				e.exposed.Store(true)
				value := e.value
				s.mu.RUnlock()
				return true, true, windowHit, usefulPrefetch, baseReadCacheEpoch{}, false, fn(value, true)
			}
			defer s.mu.RUnlock()
			return true, true, windowHit, usefulPrefetch, baseReadCacheEpoch{}, false, fn(e.value, false)
		}
		s.mu.RUnlock()
		return false, false, false, false, baseReadCacheEpoch{}, false, nil
	}
	s.mu.RUnlock()
	slot := baseReadCacheInvalidationSlotBytes(key, len(c.invalidations))
	return false, false, false, false, baseReadCacheEpoch{slot: slot, value: c.invalidations[slot].Load()}, true, nil
}

// scopedRefreshKey reports whether callback consumers of key may receive a
// cache-owned value only for the duration of the callback. Restricting this to
// the configured read-before-write namespace keeps immutable state-code and
// flat-latest cache hits shareable while commitment branches gain safe in-place
// flush refreshes.
func (c *baseReadCache) scopedRefreshKey(key []byte) bool {
	_, ok := c.commitmentKeyDepthBytes(key)
	return ok
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
	return c.setEntryIfEpoch(key, value, false, true, false, epoch)
}

// storeIfEpoch admits an immutable copy without returning its backing bytes.
// Callback-style durable readers consume their original scoped/owned value and
// use this form so the cache entry remains eligible for in-place flush refresh.
func (c *baseReadCache) storeIfEpoch(key, value []byte, epoch baseReadCacheEpoch) bool {
	_, stored := c.setEntryIfEpoch(key, value, false, false, false, epoch)
	return stored
}

// setMissingIfEpoch records a confirmed durable-base miss. Missing rows are
// subject to the same two-hit admission and generation checks as values: cold
// one-shot scans stay out of the resident cache, while repeated permission and
// storage probes stop reopening Pebble iterators. Overlay layers are consulted
// first, and flush/discard invalidation uses the same complete physical key.
func (c *baseReadCache) setMissingIfEpoch(key []byte, epoch baseReadCacheEpoch) bool {
	_, stored := c.setEntryIfEpoch(key, nil, true, false, false, epoch)
	return stored
}

func (c *baseReadCache) prefetchIfEpoch(key, value []byte, epoch baseReadCacheEpoch) ([]byte, bool) {
	return c.setEntryIfEpoch(key, value, false, true, true, epoch)
}

func (c *baseReadCache) prefetchMissingIfEpoch(key []byte, epoch baseReadCacheEpoch) bool {
	_, stored := c.setEntryIfEpoch(key, nil, true, false, true, epoch)
	return stored
}

func (c *baseReadCache) setEntryIfEpoch(key, value []byte, missing, expose, force bool, epoch baseReadCacheEpoch) ([]byte, bool) {
	if c == nil {
		return value, false
	}
	other := c.isOtherKeyBytes(key)
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
		if _, commitment := c.commitmentKeyDepthBytes(key); commitment {
			commitmentParentDurablePublishRaceCounter.Inc(1)
			if force {
				commitmentParentPrefetchPublishRaceCounter.Inc(1)
			} else {
				commitmentParentForegroundPublishRaceCounter.Inc(1)
			}
		}
		current.reference()
		value := current.value
		if expose && value != nil {
			current.exposed.Store(true)
		}
		s.mu.Unlock()
		return value, true
	}
	trunk := !missing && c.isCommitmentTrunkKey(key) && s.trunkUsed+charge <= s.trunkLimit
	window := false
	if !trunk && !force {
		if !missing && !other && c.isCommitmentWindowKey(key) && s.windowLimit > 0 {
			// Preserve the two-hit fingerprint even while retaining this first
			// value in the bounded window. If it is evicted untouched, a later
			// sighting can enter the main CLOCK directly; a second observation
			// already present in probation skips the window altogether.
			if s.admit(key, false) {
				window = false
			} else if s.admitWindowFirstRead() {
				window = true
			} else {
				baseReadCacheWindowAdmissionBypassedCounter.Inc(1)
				s.mu.Unlock()
				return value, false
			}
		} else if !s.admit(key, other) {
			s.mu.Unlock()
			return value, false
		}
	}
	entry := s.acquireEntryBytes(key, value, missing, c.version.Load())
	if trunk && s.trunkUsed+entry.charge > s.trunkLimit {
		// Recycled value/key storage can have more capacity than the requested
		// payload. Account that real retained capacity before publishing the entry;
		// if it no longer fits, fall back to ordinary probation without weakening
		// the trunk's hard byte bound.
		trunk = false
		if !s.admit(key, other) {
			s.recycleEntry(entry)
			s.mu.Unlock()
			return value, false
		}
	}
	if window && entry.charge > s.windowLimit {
		// Recycled backing may be larger than the requested value. Retain the
		// first-sighting fingerprint, but never exceed the hard window bound.
		s.recycleEntry(entry)
		s.mu.Unlock()
		return value, false
	}
	entry.nonCommitment = other
	entry.trunk = trunk
	entry.window = window
	if force {
		entry.references.Store(baseReadCachePrefetchedReference)
	}
	if expose && entry.value != nil {
		entry.exposed.Store(true)
	}
	storedValue := entry.value
	s.entries[entry.key] = entry
	if trunk {
		s.trunkUsed += entry.charge
		s.trunkEntries++
	} else if other {
		s.nonCommitmentQueue = append(s.nonCommitmentQueue, entry)
		s.nonCommitmentUsed += entry.charge
		s.nonCommitmentEntries++
	} else if window {
		s.windowQueue = append(s.windowQueue, entry)
		s.windowUsed += entry.charge
		s.windowEntries++
		s.windowAdmissions++
	} else {
		s.queue = append(s.queue, entry)
	}
	s.used += entry.charge
	s.evict()
	s.compactIfSparse()
	s.mu.Unlock()
	return storedValue, true
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
	_, commitment := c.commitmentKeyDepthString(key)
	if !cached && charge <= s.limit && commitment && s.admitObservedString(key, false) {
		// The first durable read already paid for this value and established
		// frequency evidence. The successful canonical flush supplies the
		// second observation and a newer immutable value, so retain it now
		// instead of forcing the next block to read Pebble once more merely to
		// complete admission. Clone the layer-owned key/value: sibling batches
		// may share large arenas which are released after layer promotion.
		entry := s.acquireEntryString(key, value, false, c.version.Load())
		entry.nonCommitment = false
		s.entries[entry.key] = entry
		s.queue = append(s.queue, entry)
		s.used += entry.charge
		s.evict()
		return
	}
	if cached && charge <= s.limit {
		if old.window {
			// A durable read followed by a successful canonical write is the same
			// second observation used by ordinary probation. Give the stable entry
			// promotion credit, but leave it in the window until its FIFO token is
			// consumed. Appending a main token here would give one recyclable entry
			// two queue owners and make later invalidation unsafe.
			old.reference()
			s.forgetAdmissionString(old.key, false)
		}
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
			newCharge := int(old.keyCapacity) + cap(old.value) + baseReadCacheEntryOverhead
			delta := newCharge - old.charge
			if old.trunk && s.trunkUsed+delta > s.trunkLimit {
				// A replacement can grow beyond the bounded trunk reservation.
				// Demote it to the ordinary CLOCK tail rather than exceeding the
				// fixed tier; its stable entry/map identity remains unchanged.
				old.trunk = false
				s.trunkUsed -= old.charge
				s.trunkEntries--
				s.queue = append(s.queue, old)
			}
			s.used += delta
			if old.trunk {
				s.trunkUsed += delta
			}
			if old.nonCommitment {
				s.nonCommitmentUsed += delta
			}
			if old.window {
				s.windowUsed += delta
			}
			old.charge = newCharge
		}
		old.version = c.version.Load()
		s.evict()
	} else {
		if cached {
			delete(s.entries, key)
			s.used -= old.charge
			if old.trunk {
				s.trunkUsed -= old.charge
				s.trunkEntries--
				s.recycleEntry(old)
				return
			}
			if old.window {
				s.windowUsed -= old.charge
				s.windowEntries--
			}
			if old.nonCommitment {
				s.nonCommitmentUsed -= old.charge
				s.nonCommitmentEntries--
			}
			retireBaseReadCacheEntry(old)
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

func cloneBaseReadCacheValueInto(value, storage []byte, maxCapacity int) []byte {
	if len(value) == 0 {
		return []byte{}
	}
	if len(value) <= cap(storage) && cap(storage) <= maxCapacity {
		storage = storage[:len(value)]
		copy(storage, value)
		return storage
	}
	return cloneBaseReadCacheValue(value)
}

func (s *baseReadCacheShard) takeEntry() *baseReadCacheEntry {
	entry := s.freeEntries
	if entry == nil {
		batch := new([baseReadCacheEntryBatchSize]baseReadCacheEntry)
		for i := 1; i < len(batch); i++ {
			batch[i].nextFree = s.freeEntries
			s.freeEntries = &batch[i]
		}
		s.freeEntryCount += len(batch) - 1
		return &batch[0]
	}
	s.freeEntries = entry.nextFree
	s.freeEntryCount--
	s.freeValueBytes -= cap(entry.value)
	entry.nextFree = nil
	return entry
}

func (s *baseReadCacheShard) acquireEntryBytes(key, value []byte, missing bool, version uint64) *baseReadCacheEntry {
	entry := s.takeEntry()
	keyStorage := baseReadCacheEntryKeyStorage(entry)
	valueStorage := entry.value
	entry.key, entry.keyCapacity = cloneBaseReadCacheKeyBytes(key, keyStorage)
	if missing {
		entry.value = nil
	} else {
		maxValueCapacity := s.limit - int(entry.keyCapacity) - baseReadCacheEntryOverhead
		entry.value = cloneBaseReadCacheValueInto(value, valueStorage, maxValueCapacity)
	}
	entry.charge = int(entry.keyCapacity) + cap(entry.value) + baseReadCacheEntryOverhead
	entry.version = version
	entry.live = true
	return entry
}

func (s *baseReadCacheShard) acquireEntryString(key string, value []byte, missing bool, version uint64) *baseReadCacheEntry {
	entry := s.takeEntry()
	keyStorage := baseReadCacheEntryKeyStorage(entry)
	valueStorage := entry.value
	entry.key, entry.keyCapacity = cloneBaseReadCacheKeyString(key, keyStorage)
	if missing {
		entry.value = nil
	} else {
		maxValueCapacity := s.limit - int(entry.keyCapacity) - baseReadCacheEntryOverhead
		entry.value = cloneBaseReadCacheValueInto(value, valueStorage, maxValueCapacity)
	}
	entry.charge = int(entry.keyCapacity) + cap(entry.value) + baseReadCacheEntryOverhead
	entry.version = version
	entry.live = true
	return entry
}

func baseReadCacheEntryKeyStorage(entry *baseReadCacheEntry) []byte {
	if entry.key == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(entry.key), int(entry.keyCapacity))
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

// retireBaseReadCacheEntry leaves the private key backing available for later
// metadata reuse but releases the value immediately. Explicit invalidation can
// leave a stale CLOCK token queued for a while, so it must not retain payloads.
func retireBaseReadCacheEntry(entry *baseReadCacheEntry) {
	entry.value = nil
	entry.live = false
	entry.exposed.Store(false)
	entry.references.Store(0)
}

func (s *baseReadCacheShard) recycleEntry(entry *baseReadCacheEntry) {
	retainValue := entry.live && entry.value != nil && !entry.exposed.Load() &&
		cap(entry.value) <= baseReadCacheMaxFreeValueSize &&
		s.freeValueBytes+cap(entry.value) <= s.limit/baseReadCacheFreeValueBudgetDivisor
	if !retainValue {
		entry.value = nil
	}
	entry.charge = 0
	entry.version = 0
	entry.live = false
	entry.nonCommitment = false
	entry.trunk = false
	entry.window = false
	entry.exposed.Store(false)
	entry.references.Store(0)
	entry.nextFree = nil
	if s.freeEntryCount >= baseReadCacheMaxFreeEntries {
		entry.key = ""
		entry.keyCapacity = 0
		entry.value = nil
		return
	}
	s.freeValueBytes += cap(entry.value)
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
	other := c.isOtherKeyString(key)
	if old, ok := s.entries[key]; ok {
		other = old.nonCommitment
		delete(s.entries, key)
		s.used -= old.charge
		if old.trunk {
			s.trunkUsed -= old.charge
			s.trunkEntries--
			s.recycleEntry(old)
			s.forgetAdmissionString(key, other)
			return
		}
		if old.window {
			s.windowUsed -= old.charge
			s.windowEntries--
		}
		if old.nonCommitment {
			s.nonCommitmentUsed -= old.charge
			s.nonCommitmentEntries--
		}
		retireBaseReadCacheEntry(old)
	}
	s.forgetAdmissionString(key, other)
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
		clear(s.nonCommitmentQueue)
		s.nonCommitmentQueue = s.nonCommitmentQueue[:0]
		clear(s.windowQueue)
		s.windowQueue = s.windowQueue[:0]
		clear(s.admission)
		clear(s.nonCommitmentAdmission)
		s.head = 0
		s.nonCommitmentHead = 0
		s.windowHead = 0
		s.used = 0
		s.nonCommitmentUsed = 0
		s.trunkUsed = 0
		s.windowUsed = 0
		s.nonCommitmentEntries = 0
		s.trunkEntries = 0
		s.windowEntries = 0
		s.windowAdmissionCounter = 0
		s.windowProbeCandidates = 0
		s.windowProbeAdmissions = 0
		s.windowOutcomeCount = 0
		s.windowPromotions = 0
		s.windowAdmissionShift = 0
		s.windowHitEvents.Store(0)
		s.freeEntries = nil
		s.freeEntryCount = 0
		s.freeValueBytes = 0
		s.mu.Unlock()
	}
	c.advanceAllInvalidations()
	// Clear is rare and already outside the ordinary read path. Publish the
	// empty resident set immediately so an unwind/rebuild cannot leave stale
	// occupancy indefinitely when no later commitment session is opened.
	c.metricsPublishSequence.Store(0)
	publishBaseReadCacheMetrics(c.stats())
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
func (s *baseReadCacheShard) admissionFor(other bool) []uint64 {
	if other && len(s.nonCommitmentAdmission) != 0 {
		return s.nonCommitmentAdmission
	}
	return s.admission
}

func (s *baseReadCacheShard) admit(key []byte, other bool) bool {
	admission := s.admissionFor(other)
	fingerprint := baseReadCacheAdmissionFingerprint(key)
	index := fingerprint & uint64(len(admission)-1)
	if admission[index] == fingerprint {
		admission[index] = 0
		return true
	}
	admission[index] = fingerprint
	return false
}

// admitObservedString admits key only when an earlier read already populated
// its probation slot. Unlike admit, a miss does not install the fingerprint:
// calling this from flush promotion must not let write-only metadata displace
// evidence collected by durable reads.
func (s *baseReadCacheShard) admitObservedString(key string, other bool) bool {
	admission := s.admissionFor(other)
	fingerprint := baseReadCacheAdmissionFingerprintString(key)
	index := fingerprint & uint64(len(admission)-1)
	if admission[index] != fingerprint {
		return false
	}
	admission[index] = 0
	return true
}

func (s *baseReadCacheShard) forgetAdmissionString(key string, other bool) {
	admission := s.admissionFor(other)
	fingerprint := baseReadCacheAdmissionFingerprintString(key)
	index := fingerprint & uint64(len(admission)-1)
	if admission[index] == fingerprint {
		admission[index] = 0
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
	// Enforce the first-read window independently of total cache occupancy. A
	// referenced row moves into the main CLOCK; an untouched row leaves before
	// it can displace established frequency evidence.
	for s.windowUsed > s.windowLimit {
		if !s.evictWindowOne() {
			break
		}
	}
	commitmentLimit := s.limit - s.nonCommitmentLimit
	// Both classes may borrow unused capacity. Once the shard is full, reclaim
	// whichever class exceeds its weighted share; when entry-size granularity
	// puts both slightly over, prefer retaining the foreground/other share.
	for s.used > s.limit {
		commitmentUsed := s.used - s.nonCommitmentUsed
		if s.nonCommitmentLimit > 0 &&
			s.nonCommitmentUsed > s.nonCommitmentLimit && commitmentUsed <= commitmentLimit {
			if s.evictOne(true) {
				continue
			}
		}
		if commitmentUsed > commitmentLimit && s.evictCommitmentOne() {
			continue
		}
		if s.nonCommitmentUsed > s.nonCommitmentLimit && s.evictOne(true) {
			continue
		}
		if s.evictCommitmentOne() {
			continue
		}
		if s.evictOne(true) {
			continue
		}
		break
	}
	s.compactConsumedPrefix(false)
	s.compactConsumedPrefix(true)
	s.compactWindowConsumedPrefix()
}

func (s *baseReadCacheShard) promoteWindowEntry(entry *baseReadCacheEntry) {
	if entry == nil || !entry.live || !entry.window {
		return
	}
	entry.window = false
	s.windowUsed -= entry.charge
	s.windowEntries--
	s.queue = append(s.queue, entry)
	s.forgetAdmissionString(entry.key, false)
}

func (s *baseReadCacheShard) admitWindowFirstRead() bool {
	s.windowAdmissionCounter++
	sampled := s.windowAdmissionShift == 0
	if !sampled {
		mask := uint32(1<<s.windowAdmissionShift) - 1
		sampled = s.windowAdmissionCounter&mask == 0
	}
	s.windowProbeCandidates++
	if sampled {
		s.windowProbeAdmissions++
	}
	if s.windowProbeCandidates >= baseReadCacheWindowOutcomeBatch {
		hits := s.windowHitEvents.Swap(0)
		admissions := uint32(s.windowProbeAdmissions)
		if s.windowAdmissionShift > 0 && admissions > 0 &&
			hits*baseReadCacheWindowHighReuseDivisor >= admissions {
			s.windowAdmissionShift--
			baseReadCacheWindowAdmissionRelaxedCounter.Inc(1)
		}
		s.windowProbeCandidates = 0
		s.windowProbeAdmissions = 0
	}
	return sampled
}

func (s *baseReadCacheShard) observeWindowOutcome(promoted bool) {
	s.windowOutcomeCount++
	if promoted {
		s.windowPromotions++
	}
	if s.windowOutcomeCount < baseReadCacheWindowOutcomeBatch {
		return
	}
	promotions := uint32(s.windowPromotions)
	outcomes := uint32(s.windowOutcomeCount)
	switch {
	case promotions*baseReadCacheWindowLowReuseDivisor < outcomes &&
		s.windowAdmissionShift < baseReadCacheWindowMaxAdmissionShift:
		s.windowAdmissionShift++
		baseReadCacheWindowAdmissionThrottledCounter.Inc(1)
	case promotions*baseReadCacheWindowHighReuseDivisor >= outcomes && s.windowAdmissionShift > 0:
		s.windowAdmissionShift--
		baseReadCacheWindowAdmissionRelaxedCounter.Inc(1)
	}
	s.windowOutcomeCount = 0
	s.windowPromotions = 0
}

// evictCommitmentOne gives the low-confidence first-read window priority over
// the established CLOCK tail. A window hit promotes rather than evicts the row;
// the outer loop can then reclaim another window row or eventually sweep the
// promoted row using its bounded reference credits.
func (s *baseReadCacheShard) evictCommitmentOne() bool {
	if s.evictWindowOne() {
		return true
	}
	return s.evictOne(false)
}

func (s *baseReadCacheShard) evictWindowOne() bool {
	for s.windowHead < len(s.windowQueue) {
		entry := s.windowQueue[s.windowHead]
		s.windowQueue[s.windowHead] = nil
		s.windowHead++
		if entry == nil {
			continue
		}
		if !entry.live {
			s.recycleEntry(entry)
			return true
		}
		if !entry.window {
			// A flush can promote the stable entry while this original FIFO token
			// remains queued. Its main-CLOCK token now owns recycling.
			continue
		}
		if entry.consumeReference() {
			s.promoteWindowEntry(entry)
			s.observeWindowOutcome(true)
			baseReadCacheWindowPromotedCounter.Inc(1)
			return true
		}
		delete(s.entries, entry.key)
		s.used -= entry.charge
		s.windowUsed -= entry.charge
		s.windowEntries--
		s.recycleEntry(entry)
		s.observeWindowOutcome(false)
		baseReadCacheWindowEvictedCounter.Inc(1)
		return true
	}
	return false
}

// evictOne consumes one CLOCK candidate from the requested class. It returns
// false only when that queue has no remaining token. A referenced entry spends
// one bounded credit for another chance in the same class; stale invalidation
// tokens are recycled without touching resident accounting.
func (s *baseReadCacheShard) evictOne(other bool) bool {
	queue := &s.queue
	head := &s.head
	if other {
		queue = &s.nonCommitmentQueue
		head = &s.nonCommitmentHead
	}
	for *head < len(*queue) {
		entry := (*queue)[*head]
		(*queue)[*head] = nil
		*head = *head + 1
		if entry == nil {
			continue
		}
		if entry.live {
			if entry.consumeReference() {
				// Newly admitted entries start without credit, so a one-time two-hit
				// scan is evicted before a genuinely reused row. Repeated resident
				// hits can carry a hot row across a short burst of scan admissions.
				*queue = append(*queue, entry)
				return true
			}
			delete(s.entries, entry.key)
			s.used -= entry.charge
			if entry.nonCommitment {
				s.nonCommitmentUsed -= entry.charge
				s.nonCommitmentEntries--
			}
			if entry.hasUnusedPrefetch() && isCommitmentBranchCacheKey(entry.key) {
				commitmentParentPrefetchUnusedCapacityEvictedCounter.Inc(1)
				commitmentParentPrefetchUnusedCapacityEvictedBytes.Inc(int64(entry.charge))
			}
		}
		s.recycleEntry(entry)
		return true
	}
	return false
}

func isCommitmentBranchCacheKey(key string) bool {
	if strings.HasPrefix(key, rawdb.CommitmentBranchKeyPrefix) {
		return true
	}
	return len(key) >= len(rawdb.CommitmentBranchDeltaKeyPrefix)+8 &&
		strings.HasPrefix(key, rawdb.CommitmentBranchDeltaKeyPrefix)
}

func (s *baseReadCacheShard) compactConsumedPrefix(other bool) {
	queue := &s.queue
	head := &s.head
	if other {
		queue = &s.nonCommitmentQueue
		head = &s.nonCommitmentHead
	}
	// Avoid retaining an ever-growing consumed prefix. Copy only occasionally so
	// steady-state hits stay allocation-free.
	if *head >= 1024 && *head*2 >= len(*queue) {
		copy(*queue, (*queue)[*head:])
		*queue = (*queue)[:len(*queue)-*head]
		*head = 0
	}
}

func (s *baseReadCacheShard) compactWindowConsumedPrefix() {
	if s.windowHead >= 1024 && s.windowHead*2 >= len(s.windowQueue) {
		copy(s.windowQueue, s.windowQueue[s.windowHead:])
		s.windowQueue = s.windowQueue[:len(s.windowQueue)-s.windowHead]
		s.windowHead = 0
	}
}

// compactIfSparse bounds stale queue entries left by explicit invalidation. A
// sync node may cache a branch, flush and invalidate it, then repeat that cycle
// for millions of blocks without ever exceeding the payload byte limit; without
// this ratio gate the FIFO metadata alone would grow for the whole session.
func (s *baseReadCacheShard) compactIfSparse() {
	liveTokens := len(s.queue) - s.head +
		len(s.nonCommitmentQueue) - s.nonCommitmentHead +
		len(s.windowQueue) - s.windowHead
	if liveTokens < 2048 || liveTokens <= len(s.entries)*2+1024 {
		return
	}
	nonCommitmentEntries := 0
	trunkEntries := 0
	windowEntries := 0
	for _, entry := range s.entries {
		if entry.trunk {
			trunkEntries++
		} else if entry.nonCommitment {
			nonCommitmentEntries++
		} else if entry.window {
			windowEntries++
		}
	}
	queue := make([]*baseReadCacheEntry, 0, len(s.entries)-nonCommitmentEntries-trunkEntries-windowEntries)
	nonCommitmentQueue := make([]*baseReadCacheEntry, 0, nonCommitmentEntries)
	windowQueue := make([]*baseReadCacheEntry, 0, windowEntries)
	for _, entry := range s.entries {
		if entry.trunk {
			continue
		}
		if entry.nonCommitment {
			nonCommitmentQueue = append(nonCommitmentQueue, entry)
		} else if entry.window {
			windowQueue = append(windowQueue, entry)
		} else {
			queue = append(queue, entry)
		}
	}
	for _, entry := range s.queue[s.head:] {
		if entry != nil && !entry.live {
			s.recycleEntry(entry)
		}
	}
	for _, entry := range s.nonCommitmentQueue[s.nonCommitmentHead:] {
		if entry != nil && !entry.live {
			s.recycleEntry(entry)
		}
	}
	for _, entry := range s.windowQueue[s.windowHead:] {
		if entry != nil && !entry.live {
			s.recycleEntry(entry)
		}
	}
	clear(s.queue)
	s.queue = queue
	s.head = 0
	clear(s.nonCommitmentQueue)
	s.nonCommitmentQueue = nonCommitmentQueue
	s.nonCommitmentHead = 0
	clear(s.windowQueue)
	s.windowQueue = windowQueue
	s.windowHead = 0
}

type baseReadCacheStats struct {
	entries          [4]int64
	bytes            [4]int64
	capacity         int64
	budgets          [3]int64
	windowAdmissions int64
}

const (
	baseReadCacheStatsTrunk = iota
	baseReadCacheStatsWindow
	baseReadCacheStatsTail
	baseReadCacheStatsOther
)

// stats returns an eventually consistent process diagnostic: every shard is
// internally exact under its RLock, while concurrent mutations may occur
// between shards. Bytes are the cache's retained-capacity charge (key capacity
// + value capacity + entry overhead), not payload or exact heap bytes.
func (c *baseReadCache) stats() baseReadCacheStats {
	var stats baseReadCacheStats
	if c == nil {
		return stats
	}
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.RLock()
		addBaseReadCacheShardStats(&stats, s)
		s.mu.RUnlock()
	}
	return stats
}

// addBaseReadCacheShardStats reads fields protected by s.mu. The caller owns a
// read or write lock for s.
func addBaseReadCacheShardStats(stats *baseReadCacheStats, s *baseReadCacheShard) {
	trunkEntries := s.trunkEntries
	windowEntries := s.windowEntries
	otherEntries := s.nonCommitmentEntries
	tailEntries := len(s.entries) - trunkEntries - windowEntries - otherEntries
	trunkBytes := s.trunkUsed
	windowBytes := s.windowUsed
	otherBytes := s.nonCommitmentUsed
	tailBytes := s.used - trunkBytes - windowBytes - otherBytes

	stats.entries[baseReadCacheStatsTrunk] += int64(trunkEntries)
	stats.entries[baseReadCacheStatsWindow] += int64(windowEntries)
	stats.entries[baseReadCacheStatsTail] += int64(tailEntries)
	stats.entries[baseReadCacheStatsOther] += int64(otherEntries)
	stats.bytes[baseReadCacheStatsTrunk] += int64(trunkBytes)
	stats.bytes[baseReadCacheStatsWindow] += int64(windowBytes)
	stats.bytes[baseReadCacheStatsTail] += int64(tailBytes)
	stats.bytes[baseReadCacheStatsOther] += int64(otherBytes)
	stats.capacity += int64(s.limit)
	stats.budgets[0] += int64(s.trunkLimit)
	stats.budgets[1] += int64(s.windowLimit)
	stats.budgets[2] += int64(s.nonCommitmentLimit)
	stats.windowAdmissions += int64(s.windowAdmissions)
}

// tryStats is the fold-close variant. A flush promotion can hold one shard's
// write lock while applying a whole group, so diagnostics skip that sample
// instead of extending the critical fold completion path.
func (c *baseReadCache) tryStats() (baseReadCacheStats, bool) {
	var stats baseReadCacheStats
	if c == nil {
		return stats, true
	}
	for i := range c.shards {
		s := &c.shards[i]
		if !s.mu.TryRLock() {
			return baseReadCacheStats{}, false
		}
		addBaseReadCacheShardStats(&stats, s)
		s.mu.RUnlock()
	}
	return stats, true
}

func publishBaseReadCacheMetrics(stats baseReadCacheStats) {
	for class := range stats.entries {
		baseReadCacheOccupancyGauges[class][0].Update(stats.entries[class])
		baseReadCacheOccupancyGauges[class][1].Update(stats.bytes[class])
	}
	baseReadCacheCapacityGauge.Update(stats.capacity)
	for tier := range stats.budgets {
		baseReadCacheBudgetGauges[tier].Update(stats.budgets[tier])
	}
	baseReadCacheWindowAdmittedGauge.Update(stats.windowAdmissions)
}

func (c *baseReadCache) maybePublishMetrics() {
	if c == nil {
		return
	}
	sequence := c.metricsPublishSequence.Add(1)
	if (sequence-1)%baseReadCacheMetricsPublishInterval != 0 {
		return
	}
	stats, ok := c.tryStats()
	if !ok {
		// Retry on the next close rather than waiting another full interval.
		// Concurrent closes may cause an extra harmless publication.
		c.metricsPublishSequence.Store(0)
		return
	}
	publishBaseReadCacheMetrics(stats)
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
	if l.hasRangeDeletes() {
		// Readers that captured the pre-delete durable snapshot may have filled
		// cache entries after DeleteRange first cleared it. The range is now
		// durable, so invalidate the complete cache before promoting the
		// replacement point writes from this layer.
		b.baseReadCache.clear()
	}
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.RLock()
		b.baseReadCache.promoteFlushedShard(s.writes, s.deletes, uint32(i))
		s.mu.RUnlock()
	}
}

// promoteBaseReadCacheMerged applies the already-coalesced final operations
// from one successfully committed flush group. Grouping entries by their known
// layer shard trades one cache lock per output key for at most one lock per
// shard and carries the already-loaded map value into the locked pass. The
// grouping scratch belongs to the pooled merge object and is reused by later
// flushes.
func (b *Buffer) promoteBaseReadCacheMerged(merged *flushMergedOps) {
	if b == nil || b.baseReadCache == nil || merged == nil {
		return
	}
	for key, op := range merged.ops {
		shard := int(op.shard)
		merged.promotions[shard] = append(merged.promotions[shard], mergedPromotion{key: key, op: op})
	}
	for shard, promotions := range merged.promotions {
		if len(promotions) == 0 {
			continue
		}
		cacheShard := &b.baseReadCache.shards[shard]
		cacheShard.mu.Lock()
		for _, promotion := range promotions {
			b.baseReadCache.advanceInvalidationString(promotion.key)
			if promotion.op.delete {
				b.baseReadCache.delStringLocked(cacheShard, promotion.key)
			} else {
				b.baseReadCache.setFlushedLocked(cacheShard, promotion.key, promotion.op.value)
			}
		}
		cacheShard.compactIfSparse()
		cacheShard.mu.Unlock()
		clear(promotions)
		merged.promotions[shard] = promotions[:0]
	}
}

func (b *Buffer) clearBaseReadCache() {
	if b != nil && b.baseReadCache != nil {
		b.baseReadCache.clear()
	}
}
