// Package blockbuffer provides an in-memory layered write-set over an
// ethdb.KeyValueReader (typically the on-disk store), used by core.BlockChain
// to defer post-applyBlock rawdb-direct writes so that switchFork can
// discard the layers belonging to orphaned-branch blocks.
//
// One layer is opened per applyBlock via BeginBlock(hash). Direct writes during
// the block go to the active layer, and batch operations bind to the active
// layer at Put/Delete time so a later batch Write can still land in the block
// that produced the operation. CommitBlock promotes the active layer onto the
// layered stack (newest at the top). DiscardBlock(hash) removes a specific
// layer (used in switchFork for orphan rewinds). DiscardActive drops the
// in-progress layer (used on applyBlock failure). Reads check the active layer
// first, then layers newest-first, then fall through to the base reader.
// Tombstones for deletes return a not-found error.
//
// The buffer is single-writer: callers must serialize all method calls. In
// core/blockchain.go this is provided by bc.chainmu.
//
// Slice 1 of the fork-rewind fix integrates only the witness-statistics
// writer (consensus/dpos.ApplyBlockStatistics). Other rawdb-direct writers
// continue to write to disk directly until slice 2 — see
// docs/superpowers/specs/2026-04-30-fork-rewind-fix-design.md.
package blockbuffer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
)

// ErrNotFound is returned by Get/Has when the key is tombstoned in a layer.
// It is also the sentinel returned by the underlying base reader for
// missing keys (memorydb / pebble both return non-nil errors for misses;
// callers normally check err != nil rather than identity).
var ErrNotFound = errors.New("blockbuffer: not found")

// ErrReadSnapshotUnsupported reports that the durable base cannot pin an MVCC
// sequence. Callers that require a cross-operation consistent view must retain
// their external writer lock or reject the operation instead of silently using
// the moving Buffer read surface.
var ErrReadSnapshotUnsupported = errors.New("blockbuffer: durable read snapshot unsupported")

var (
	flushInputOpsCounter                         = metrics.NewRegisteredCounter("blockbuffer/flush/input/ops", nil)
	flushOutputOpsCounter                        = metrics.NewRegisteredCounter("blockbuffer/flush/output/ops", nil)
	flushInputBytesCounter                       = metrics.NewRegisteredCounter("blockbuffer/flush/input/bytes", nil)
	flushOutputBytesCounter                      = metrics.NewRegisteredCounter("blockbuffer/flush/output/bytes", nil)
	flushLayersCounter                           = metrics.NewRegisteredCounter("blockbuffer/flush/layers", nil)
	flushGroupsCounter                           = metrics.NewRegisteredCounter("blockbuffer/flush/groups", nil)
	flushExtendedGroupsCounter                   = metrics.NewRegisteredCounter("blockbuffer/flush/extended/groups", nil)
	flushExtendedLayersCounter                   = metrics.NewRegisteredCounter("blockbuffer/flush/extended/layers", nil)
	flushCallsCounter                            = metrics.NewRegisteredCounter("blockbuffer/flush/calls", nil)
	commitmentParentOverlayResolvedCounter       = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/overlay/resolved", nil)
	commitmentParentCacheResolvedCounter         = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/cache/resolved", nil)
	commitmentParentDurableReadsCounter          = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/durable/reads", nil)
	commitmentParentDurableHitsCounter           = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/durable/hits", nil)
	commitmentParentTrunkCacheCounter            = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/trunk/cache_resolved", nil)
	commitmentParentTrunkDurableCounter          = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/trunk/durable_reads", nil)
	commitmentParentWindowCacheCounter           = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/window/cache_resolved", nil)
	commitmentParentPrefetchPlannedCounter       = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/prefetch/planned", nil)
	commitmentParentPrefetchOverlayCounter       = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/prefetch/overlay_resolved", nil)
	commitmentParentPrefetchCacheCounter         = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/prefetch/cache_resolved", nil)
	commitmentParentPrefetchDurableCounter       = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/prefetch/durable_reads", nil)
	commitmentParentPrefetchDurableHitCounter    = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/prefetch/durable_hits", nil)
	commitmentParentPrefetchUsefulCounter        = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/prefetch/useful_hits", nil)
	baseReadCacheWindowPromotedCounter           = metrics.NewRegisteredCounter("blockbuffer/base_cache/window/promoted", nil)
	baseReadCacheWindowEvictedCounter            = metrics.NewRegisteredCounter("blockbuffer/base_cache/window/evicted", nil)
	baseReadCacheWindowAdmittedGauge             = metrics.NewRegisteredGauge("blockbuffer/base_cache/window/admitted", nil)
	baseReadCacheWindowAdmissionBypassedCounter  = metrics.NewRegisteredCounter("blockbuffer/base_cache/window/admission_bypassed", nil)
	baseReadCacheWindowAdmissionThrottledCounter = metrics.NewRegisteredCounter("blockbuffer/base_cache/window/admission_throttled", nil)
	baseReadCacheWindowAdmissionRelaxedCounter   = metrics.NewRegisteredCounter("blockbuffer/base_cache/window/admission_relaxed", nil)
	baseReadCachePrefetchUsefulCounter           = metrics.NewRegisteredCounter("blockbuffer/base_cache/prefetch/useful_hits", nil)
	baseReadCacheOccupancyGauges                 = [...][2]*metrics.Gauge{
		{
			metrics.NewRegisteredGauge("blockbuffer/base_cache/trunk/entries", nil),
			metrics.NewRegisteredGauge("blockbuffer/base_cache/trunk/bytes", nil),
		},
		{
			metrics.NewRegisteredGauge("blockbuffer/base_cache/window/entries", nil),
			metrics.NewRegisteredGauge("blockbuffer/base_cache/window/bytes", nil),
		},
		{
			metrics.NewRegisteredGauge("blockbuffer/base_cache/tail/entries", nil),
			metrics.NewRegisteredGauge("blockbuffer/base_cache/tail/bytes", nil),
		},
		{
			metrics.NewRegisteredGauge("blockbuffer/base_cache/other/entries", nil),
			metrics.NewRegisteredGauge("blockbuffer/base_cache/other/bytes", nil),
		},
	}
	baseReadCacheCapacityGauge = metrics.NewRegisteredGauge("blockbuffer/base_cache/capacity/bytes", nil)
	baseReadCacheBudgetGauges  = [...]*metrics.Gauge{
		metrics.NewRegisteredGauge("blockbuffer/base_cache/trunk/budget/bytes", nil),
		metrics.NewRegisteredGauge("blockbuffer/base_cache/window/budget/bytes", nil),
		metrics.NewRegisteredGauge("blockbuffer/base_cache/other/budget/bytes", nil),
	}
	flushFamilyOpsCounters             = newFlushFamilyCounters("sampled_ops")
	flushFamilyBytesCounters           = newFlushFamilyCounters("sampled_bytes")
	flushFamilySampledGroupsCounter    = metrics.NewRegisteredCounter("blockbuffer/flush/family/sampled_groups", nil)
	commitmentParentDepthCacheCounters = [...]*metrics.Counter{
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/depth_5_8/cache_resolved", nil),
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/depth_9_16/cache_resolved", nil),
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/depth_17_32/cache_resolved", nil),
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/depth_33_plus/cache_resolved", nil),
	}
	commitmentParentDepthDurableCounters = [...]*metrics.Counter{
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/depth_5_8/durable_reads", nil),
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/depth_9_16/durable_reads", nil),
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/depth_17_32/durable_reads", nil),
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/depth_33_plus/durable_reads", nil),
	}
	commitmentParentExactDepthCacheCounters = [...]*metrics.Counter{
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/depth_5/cache_resolved", nil),
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/depth_6/cache_resolved", nil),
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/depth_7/cache_resolved", nil),
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/depth_8/cache_resolved", nil),
	}
	commitmentParentExactDepthDurableCounters = [...]*metrics.Counter{
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/depth_5/durable_reads", nil),
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/depth_6/durable_reads", nil),
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/depth_7/durable_reads", nil),
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/depth_8/durable_reads", nil),
	}
	commitmentParentPrefetchDepthPlannedCounters = [...]*metrics.Counter{
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/prefetch/depth_5/planned", nil),
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/prefetch/depth_6_plus/planned", nil),
	}
	commitmentParentPrefetchDepthCacheCounters = [...]*metrics.Counter{
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/prefetch/depth_5/cache_resolved", nil),
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/prefetch/depth_6_plus/cache_resolved", nil),
	}
	commitmentParentPrefetchDepthDurableCounters = [...]*metrics.Counter{
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/prefetch/depth_5/durable_reads", nil),
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/prefetch/depth_6_plus/durable_reads", nil),
	}
	commitmentParentPrefetchDepthUsefulCounters = [...]*metrics.Counter{
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/prefetch/depth_5/useful_hits", nil),
		metrics.NewRegisteredCounter("blockbuffer/commitment_parent/prefetch/depth_6_plus/useful_hits", nil),
	}
)

var (
	commitmentParentDurablePublishRaceCounter            = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/durable_publish_races", nil)
	commitmentParentPrefetchPublishRaceCounter           = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/durable_publish_races/prefetch", nil)
	commitmentParentForegroundPublishRaceCounter         = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/durable_publish_races/foreground", nil)
	commitmentParentPrefetchUnusedCapacityEvictedCounter = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/prefetch/unused_capacity_evicted", nil)
	commitmentParentPrefetchUnusedCapacityEvictedBytes   = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/prefetch/unused_capacity_evicted_bytes", nil)
	commitmentParentSingleflightLeadersCounter           = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/singleflight/leaders", nil)
	commitmentParentSingleflightWaitersCounter           = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/singleflight/waiters", nil)
	commitmentParentSingleflightSharedCounter            = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/singleflight/shared_results", nil)
	commitmentParentSingleflightForegroundSharedCounter  = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/singleflight/shared_results/foreground", nil)
	commitmentParentSingleflightPrefetchSharedCounter    = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/singleflight/shared_results/prefetch", nil)
	commitmentParentSingleflightSharedPresentCounter     = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/singleflight/shared_present", nil)
	commitmentParentSingleflightSharedMissingCounter     = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/singleflight/shared_missing", nil)
	commitmentParentSingleflightLeaderErrorsCounter      = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/singleflight/leader_errors", nil)
	commitmentParentSingleflightWaitNanosCounter         = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/singleflight/wait_nanos", nil)
	commitmentParentSingleflightForegroundWaitersCounter = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/singleflight/waiters/foreground", nil)
	commitmentParentSingleflightPrefetchWaitersCounter   = metrics.NewRegisteredCounter("blockbuffer/commitment_parent/singleflight/waiters/prefetch", nil)
)

const flushPhysicalFamilySampleInterval = uint64(32)

var flushPhysicalFamilySampleSequence atomic.Uint64

func newFlushFamilyCounters(unit string) [rawdb.PhysicalKeyFamilyCount]*metrics.Counter {
	var counters [rawdb.PhysicalKeyFamilyCount]*metrics.Counter
	for family := rawdb.PhysicalKeyFamily(0); family < rawdb.PhysicalKeyFamilyCount; family++ {
		name := "blockbuffer/flush/family/" + rawdb.PhysicalKeyFamilyName(family) + "/" + unit
		counters[family] = metrics.NewRegisteredCounter(name, nil)
	}
	return counters
}

// layer is a single applyBlock's worth of buffered mutations.
//
// A layer shards its writes/deletes maps by key. This is important for the
// commitment fold: its 16 root workers concurrently walk the same few buffered
// layers, and a single layer-wide RWMutex turns every read-lock operation into
// contention on one cache line. Independent shards preserve the exact same
// per-key last-write/tombstone semantics while letting unrelated branch keys be
// read concurrently.
//
// Shard locks are INDEPENDENT of b.mu (which guards the inflight/layers slices).
// Lock ordering is always b.mu → shard.mu, never the reverse: map writers capture
// the target under a brief b.mu.RLock, release it, then take one shard lock. Hot
// readers load an immutable topology view atomically and take only one shard
// RLock at a time. No path holds a shard lock while acquiring b.mu, so the two
// never deadlock.
type layer struct {
	blockHash common.Hash
	number    uint64
	// owner/state are guarded by the owning Buffer.mu and make queued-batch
	// target classification constant-time. A range-owned batch can contain
	// thousands of operations while mainnet keeps roughly 20 committed layers;
	// rescanning both topology slices for every operation was pure bookkeeping
	// on the block-import hot path. owner also rejects handles originating from
	// another Buffer without relying on pointer scans.
	owner   *Buffer
	state   layerState
	bloom   atomic.Pointer[layerBloom]
	segment atomic.Pointer[layerBloomSegment]
	// ownedKeyArena packs commitment physical keys across sibling batches.
	// Reservations are disjoint before the caller copies bytes, so the lock is
	// never held with a shard lock and concurrent fold workers can populate
	// their reserved spans independently.
	ownedKeyArenaMu      sync.Mutex
	ownedKeyArena        []byte
	ownedKeyArenaBatches int
	// rangeDeletes is an immutable, atomically published union of half-open
	// key ranges deleted by this layer. A commitment rebuild can retire a very
	// large derived keyspace with one range tombstone instead of materialising
	// one in-memory point tombstone per durable row.
	rangeDeleteMu sync.Mutex
	rangeDeletes  atomic.Pointer[[]layerRangeDelete]
	shards        [layerShardCount]layerShard
}

type layerRangeDelete struct {
	start string
	end   string
}

func (r layerRangeDelete) containsString(key string) bool {
	return key >= r.start && (r.end == "" || key < r.end)
}

func (r layerRangeDelete) containsBytes(key []byte) bool {
	return r.containsString(unsafe.String(unsafe.SliceData(key), len(key)))
}

func (l *layer) loadRangeDeletes() []layerRangeDelete {
	if l == nil {
		return nil
	}
	ranges := l.rangeDeletes.Load()
	if ranges == nil {
		return nil
	}
	return *ranges
}

func (l *layer) hasRangeDeletes() bool { return len(l.loadRangeDeletes()) != 0 }

func (l *layer) rangeDeletesString(key string) bool {
	for _, r := range l.loadRangeDeletes() {
		if r.containsString(key) {
			return true
		}
	}
	return false
}

func (l *layer) rangeDeletesBytes(key []byte) bool {
	for _, r := range l.loadRangeDeletes() {
		if r.containsBytes(key) {
			return true
		}
	}
	return false
}

type layerState uint8

const (
	layerDetached layerState = iota
	layerInflight
	layerCommitted
)

const layerShardCount = 16

const layerOwnedKeyArenaMaxChunk = 64 << 10

// layerShard is padded to one 64-byte cache line on the deployment target
// (amd64). Without the padding, adjacent shard RWMutex counters can still
// false-share under the 16-way commitment fold even though their maps differ.
// The fixed ~1 KiB per live layer is small relative to the layer values and the
// configured 24 GiB Pebble cache, and maps remain lazily allocated.
type layerShard struct {
	mu      sync.RWMutex
	writes  map[string][]byte
	deletes map[string]struct{}
	// durableWrites keeps overlay values that an ordered direct metadata batch
	// already persisted. Reads still see them, while a later layer flush skips
	// byte-identical rows.
	durableWrites      map[string][]byte
	prefixBucketIndex  *layerPrefixBucketIndex
	pendingOwnedPuts   uint32
	commitmentReserved bool
	_                  [3]byte
}

// layerPrefixBucketIndex is the lazily allocated per-shard iterator index. Its
// bucket half lets structured account-KV iterators visit one account-ID bucket;
// its sorted-key half lets generic prefix iterators binary-search the requested
// range. Sharing one holder preserves layerShard's one-cache-line footprint.
// The account schema prefix is supplied by rawdb, so blockbuffer does not
// duplicate storage-prefix knowledge.
type layerPrefixBucketIndex struct {
	prefix            string
	buckets           map[byte][]string
	built             [4]uint64
	iteratorKeys      []string
	iteratorKeysBuilt bool
}

func newLayerPrefixBucketIndex(prefix string) *layerPrefixBucketIndex {
	return &layerPrefixBucketIndex{prefix: prefix}
}

// ensureIteratorKeys builds the generic iterator's sorted union of write and
// tombstone keys. Callers hold the shard lock. The string slice only retains
// map-owned immutable key strings, so the index is compact and needs no key
// byte copies.
func (i *layerPrefixBucketIndex) ensureIteratorKeys(writes map[string][]byte, deletes map[string]struct{}) {
	if i == nil || i.iteratorKeysBuilt {
		return
	}
	keys := make([]string, 0, len(writes)+len(deletes))
	for key := range writes {
		keys = append(keys, key)
	}
	for key := range deletes {
		// Writes and deletes are mutually exclusive in normal operation. Keep
		// the index robust for hand-built layers used by tests and diagnostics.
		if _, exists := writes[key]; !exists {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	i.iteratorKeys = keys
	i.iteratorKeysBuilt = true
}

func (i *layerPrefixBucketIndex) invalidateIteratorKeys() {
	if i == nil || !i.iteratorKeysBuilt {
		return
	}
	i.iteratorKeys = nil
	i.iteratorKeysBuilt = false
}

func (i *layerPrefixBucketIndex) bucketBuilt(bucket byte) bool {
	return i != nil && i.built[bucket>>6]&(uint64(1)<<(bucket&63)) != 0
}

func (i *layerPrefixBucketIndex) anyBucketBuilt() bool {
	if i == nil {
		return false
	}
	for _, word := range i.built {
		if word != 0 {
			return true
		}
	}
	return false
}

func (i *layerPrefixBucketIndex) ensureBucket(bucket byte, writes map[string][]byte, deletes map[string]struct{}) {
	if i == nil || i.bucketBuilt(bucket) {
		return
	}
	// Preserve the old one-account cold path and its small retained index. Once
	// a second distinct account bucket is requested, the layer is demonstrating
	// the multi-account access pattern seen during sync; switch to one packed
	// build rather than rescanning both maps for every later bucket.
	if !i.anyBucketBuilt() {
		i.buildBucket(bucket, writes, deletes)
		return
	}
	i.buildAllBuckets(writes, deletes)
}

func (i *layerPrefixBucketIndex) buildBucket(bucket byte, writes map[string][]byte, deletes map[string]struct{}) {
	for key := range writes {
		i.addToBucket(bucket, key)
	}
	for key := range deletes {
		i.addToBucket(bucket, key)
	}
	i.built[bucket>>6] |= uint64(1) << (bucket & 63)
}

// buildAllBuckets groups every key in the indexed schema by the first account
// byte in one packed build. Structured account-KV reads encounter
// many distinct accounts while a sync layer is live; rebuilding one bucket at
// a time rescans the same writes/deletes map for every distinct first byte.
//
// Two map passes keep the retained index compact: the first computes exact
// bucket sizes and the second fills one shared string arena. Each published
// bucket has cap == len, so a mutation after the build appends into independent
// storage instead of overwriting the next bucket's arena segment. Callers hold
// the layer-shard lock across this build and every later mutation.
func (i *layerPrefixBucketIndex) buildAllBuckets(writes map[string][]byte, deletes map[string]struct{}) {
	if i == nil {
		return
	}
	var counts [256]int
	total := 0
	count := func(key string) {
		if len(key) <= len(i.prefix) || !strings.HasPrefix(key, i.prefix) {
			return
		}
		counts[key[len(i.prefix)]]++
		total++
	}
	for key := range writes {
		count(key)
	}
	for key := range deletes {
		count(key)
	}

	if total != 0 {
		var offsets [257]int
		bucketCount := 0
		for bucket, size := range counts {
			offsets[bucket+1] = offsets[bucket] + size
			if size != 0 {
				bucketCount++
			}
		}
		arena := make([]string, total)
		next := offsets
		place := func(key string) {
			if len(key) <= len(i.prefix) || !strings.HasPrefix(key, i.prefix) {
				return
			}
			bucket := key[len(i.prefix)]
			arena[next[bucket]] = key
			next[bucket]++
		}
		for key := range writes {
			place(key)
		}
		for key := range deletes {
			place(key)
		}

		i.buckets = make(map[byte][]string, bucketCount)
		for bucket, size := range counts {
			if size == 0 {
				continue
			}
			start, end := offsets[bucket], offsets[bucket+1]
			i.buckets[byte(bucket)] = arena[start:end:end]
		}
	}
	for word := range i.built {
		i.built[word] = ^uint64(0)
	}
}

func (i *layerPrefixBucketIndex) addToBucket(bucket byte, key string) {
	if i == nil || len(key) <= len(i.prefix) || !strings.HasPrefix(key, i.prefix) {
		return
	}
	if key[len(i.prefix)] != bucket {
		return
	}
	if i.buckets == nil {
		i.buckets = make(map[byte][]string)
	}
	i.buckets[bucket] = append(i.buckets[bucket], key)
}

// trackPrefixBucketKeyBeforeMutation records a key only on its first mutation
// in this layer. Callers hold s.mu. Tracking generic writes after lazy index
// construction is required for ethdb correctness: a caller may write a raw
// physical state-KV key without using rawdb's structured writer fast path.
func (s *layerShard) trackPrefixBucketKeyBeforeMutation(key string) {
	if s.prefixBucketIndex == nil {
		return
	}
	if _, exists := s.writes[key]; exists {
		return
	}
	if _, exists := s.deletes[key]; exists {
		return
	}
	index := s.prefixBucketIndex
	if len(key) <= len(index.prefix) || !strings.HasPrefix(key, index.prefix) {
		return
	}
	bucket := key[len(index.prefix)]
	if index.bucketBuilt(bucket) {
		index.addToBucket(bucket, key)
	}
}

// trackIteratorKeyBeforeMutation invalidates the sorted generic-key index only
// when a mutation introduces a previously unseen key. Changing a write into a
// tombstone (or replacing its value) preserves membership and therefore keeps
// the index valid. Callers hold s.mu.
func (s *layerShard) trackIteratorKeyBeforeMutation(key string) {
	index := s.prefixBucketIndex
	if index == nil || !index.iteratorKeysBuilt {
		return
	}
	if _, exists := s.writes[key]; exists {
		return
	}
	if _, exists := s.deletes[key]; exists {
		return
	}
	index.invalidateIteratorKeys()
}

// bufferReadView is an immutable snapshot of the layer topology used by the
// read hot path. Structural writers build a fresh slice copy under Buffer.mu
// and publish it atomically; readers can then walk stable layer references
// without contending on the global RWMutex. Individual layer contents remain
// protected by their shard locks.
type bufferReadView struct {
	inflight      []*layer
	layers        []*layer
	baseReadCache *baseReadCache
}

const (
	// The Pebble adapter has bounded reusable batch buffers through 32 MiB. Use
	// that whole range for the FINAL solid-layer batch. Production commitment
	// branches are hot full post-images: a 32 MiB stream of source layers often
	// collapses to only a few MiB. Permit a larger source window, Erigon-style,
	// while checking every appended layer against the final 32 MiB output. This
	// preserves the bounded Pebble batch/WAL allocation and atomicity contract.
	maxFlushBatchValueSize   = 32 << 20
	maxFlushBatchEncodedSize = 32 << 20
	maxFlushMergeValueSize   = 128 << 20
	maxFlushMergeEncodedSize = 128 << 20
)

func newLayer(hash common.Hash, number uint64) *layer {
	return &layer{
		blockHash: hash,
		number:    number,
	}
}

// reserveOwnedKeyBytes returns a unique immutable-after-fill span owned by l.
// A sparse single-batch fold keeps an exact allocation. For a sibling fold,
// each reservation scales only by the batches that have not arrived yet. A
// small headroom absorbs normal batch-size variation, and the cap prevents an
// unusually large first sibling from retaining an oversized layer arena.
func (l *layer) reserveOwnedKeyBytes(size, reserveBatches int) []byte {
	if size == 0 {
		return nil
	}
	l.ownedKeyArenaMu.Lock()
	remainingBatches := 1
	if reserveBatches > 1 {
		remainingBatches = reserveBatches - l.ownedKeyArenaBatches
		if remainingBatches < 1 {
			remainingBatches = 1
		}
		l.ownedKeyArenaBatches++
	}
	if cap(l.ownedKeyArena)-len(l.ownedKeyArena) < size {
		capacity := size
		if remainingBatches > 1 && size < layerOwnedKeyArenaMaxChunk {
			if size <= layerOwnedKeyArenaMaxChunk/remainingBatches {
				capacity = size * remainingBatches
			} else {
				capacity = layerOwnedKeyArenaMaxChunk
			}
			// Allow 12.5% for sibling batches whose physical keys are slightly
			// longer than the batch that established this arena.
			capacity += (capacity + 7) / 8
			if capacity > layerOwnedKeyArenaMaxChunk {
				capacity = layerOwnedKeyArenaMaxChunk
			}
		}
		l.ownedKeyArena = make([]byte, 0, capacity)
	}
	start := len(l.ownedKeyArena)
	end := start + size
	l.ownedKeyArena = l.ownedKeyArena[:end]
	span := l.ownedKeyArena[start:end:end]
	l.ownedKeyArenaMu.Unlock()
	return span
}

func (l *layer) shardForBytes(key []byte) *layerShard {
	return &l.shards[layerShardIndexBytes(key)]
}

func (l *layer) shardForString(key string) *layerShard {
	return &l.shards[layerShardIndexString(key)]
}

// The middle and tail of hot state keys carry their highest-entropy bytes: a
// commitment path nibble, account/address byte, contract-storage slot, or key
// suffix. Sampling three tail bytes plus one middle byte avoids hashing the full
// 30-100 byte physical key a second time (the Go map will hash it once already).
// Sixteen shards match the maximum commitment root-worker fan-out while avoiding
// four times as many tiny per-layer maps. Include length so short prefixes that
// share a suffix do not systematically collide. The byte/string forms must
// remain identical for write/read routing.
func layerShardIndexBytes(key []byte) uint32 {
	n := len(key)
	if n == 0 {
		return 0
	}
	h := uint32(n) ^ uint32(key[n-1])
	if n > 1 {
		h ^= uint32(key[n-2]) << 2
	}
	if n > 3 {
		h ^= uint32(key[n-4]) << 4
	}
	if n > 8 {
		h ^= uint32(key[n/2]) << 1
	}
	return h & (layerShardCount - 1)
}

func layerShardIndexString(key string) uint32 {
	n := len(key)
	if n == 0 {
		return 0
	}
	h := uint32(n) ^ uint32(key[n-1])
	if n > 1 {
		h ^= uint32(key[n-2]) << 2
	}
	if n > 3 {
		h ^= uint32(key[n-4]) << 4
	}
	if n > 8 {
		h ^= uint32(key[n/2]) << 1
	}
	return h & (layerShardCount - 1)
}

// Buffer is a layered in-memory write-set over a base reader.
//
// Layout (top to bottom on a Get):
//
//	active   — current open layer, if any
//	layers   — committed but not-yet-flushed layers, newest at the end of the slice
//	base     — disk store
//
// Concurrency model:
//
// Foreground mutators (Begin/CommitBlock/DiscardActive/Put/Delete) assume the
// caller serializes them — typically via core.BlockChain's chainmu. The
// internal mu guards the inflight/layers slices and per-shard locks guard layer
// maps, so uncoordinated readers (RPC handlers, metrics, txpool) can call
// Get/Has/PendingBlocks concurrently with a writer holding chainmu without
// triggering a Go race detector report.
//
// Multi-active-layer (async commit): the buffer can hold more than one
// in-flight (begun-but-uncommitted) layer at once — `inflight` is an ordered
// set (oldest→newest). The newest in-flight layer is the foreground's "active"
// layer; Put/Delete/BeginBlock/DiscardActive operate on it exactly as the
// single-active model did. When async commit is enabled, ordered commitment
// lanes hold handles to OLDER in-flight layers and write them via
// ViewLayer/LayerWriter while the foreground writes the newest layer. Lanes may
// work in several block layers concurrently, but each write targets one fixed
// layer and the scheduler promotes or discards layers in FIFO order. Every
// method takes the applicable locks, so the sharded layer maps and slices stay
// race-free. With
// maxInflight==1 (the default), only one layer is ever in flight; this then
// degenerates to the single-active model and is byte-identical to it.
//
// flushMu serializes FlushUpTo/Flush calls against each other so the
// snapshot→disk-I/O→drop phases of two concurrent flushers can't
// interleave (double-flush / double-drop). It is held across the whole
// flush, but mu is released during the disk I/O so readers
// (Get/Has/NewIterator — the LoadDynamicProperties path) proceed
// concurrently. FlushUpTo callers are the async-flush worker, the
// inline fallback, and Close; only one runs the body at a time.
//
// FlushUpTo/Flush/DiscardBlock operate on COMMITTED layers only and never
// touch in-flight layers, so the b.mu-free committed-layer read in FlushUpTo is
// unaffected by the multi-active-layer change: a layer becomes
// committed (and thus flush-eligible) only after its fold completes and
// CommitBlock/CommitInflight promotes it.
type Buffer struct {
	base            ethdb.KeyValueReader
	blockHashReader BlockHashReader
	// baseReadCache is populated through GetNoCopyCached. Overlay layers always
	// win; a successful canonical flush refreshes already-cached keys directly
	// from immutable layer values and invalidates tombstones, while Discard clears
	// the cache before reset/unwind.
	baseReadCache *baseReadCache
	readView      atomic.Pointer[bufferReadView]
	mu            sync.RWMutex
	flushMu       sync.Mutex
	layers        []*layer
	// inflight holds begun-but-uncommitted layers, oldest→newest. The newest
	// is the foreground's active layer. Empty or length 1 under the default
	// maxInflight==1; the async commit worker raises maxInflight to allow a
	// second concurrent layer.
	inflight []*layer
	// maxInflight bounds how many layers may be in flight at once. Zero is
	// treated as 1 (single-active, the default). BeginBlock panics past it,
	// preserving the legacy double-Begin guard in the default configuration.
	maxInflight int
}

// ReadSnapshot pins both halves of one logical Buffer view: the immutable
// layer topology published at construction time and one MVCC sequence of the
// durable base. Layer pointers remain alive even if a concurrent flush drops
// them from Buffer; newer blocks publish new layers that are absent here. A
// caller must construct the snapshot while its external single-writer lock is
// held so no in-flight layer is being advanced at the capture boundary.
//
// Put/Delete intentionally return an error. StateDB's read-only TVM execution
// accepts a read/write-shaped latest-index store, but does not persist its dirty
// overlay; advertising explicit failure is safer than accidentally mutating a
// snapshot if that invariant ever changes.
type ReadSnapshot struct {
	view   *bufferReadView
	base   pointread.KeyValueSnapshot
	closed atomic.Bool
}

var _ ethdb.KeyValueReader = (*ReadSnapshot)(nil)
var _ ethdb.KeyValueWriter = (*ReadSnapshot)(nil)
var _ ethdb.Iteratee = (*ReadSnapshot)(nil)
var _ pointread.PrefixSeeker = (*ReadSnapshot)(nil)
var _ pointread.DurablePrefixSeeker = (*ReadSnapshot)(nil)
var _ pointread.PrefixSeeker = (*Buffer)(nil)

// NewReadSnapshot captures a stable logical read view. flushMu makes the base
// sequence and layer topology atomic with respect to FlushUpTo's
// overlay-to-durable handoff: a row is therefore visible from exactly the
// captured overlay/base combination even after that flush completes.
func (b *Buffer) NewReadSnapshot() (*ReadSnapshot, error) {
	return b.newReadSnapshot(nil)
}

// NewReadSnapshotThrough captures the durable base plus committed buffer
// layers through maxBlock. In-flight layers and committed layers above the
// boundary are deliberately excluded, so an archive reader can use the last
// fully published state while the async commit worker continues promoting
// newer blocks.
//
// The caller must ensure the durable base has not advanced beyond maxBlock.
// BlockChain does this by capping asynchronous flushes at its published archive
// head and holding chainmu while it captures the snapshot.
func (b *Buffer) NewReadSnapshotThrough(maxBlock uint64) (*ReadSnapshot, error) {
	return b.newReadSnapshot(&maxBlock)
}

func (b *Buffer) newReadSnapshot(maxBlock *uint64) (*ReadSnapshot, error) {
	if b == nil || b.base == nil {
		return nil, ErrReadSnapshotUnsupported
	}
	factory, ok := b.base.(pointread.KeyValueSnapshotter)
	if !ok {
		return nil, ErrReadSnapshotUnsupported
	}

	b.flushMu.Lock()
	b.mu.RLock()
	base, err := factory.NewKeyValueSnapshot()
	if err != nil {
		b.mu.RUnlock()
		b.flushMu.Unlock()
		return nil, err
	}
	view := b.readView.Load()
	if maxBlock != nil {
		bounded := &bufferReadView{baseReadCache: b.baseReadCache}
		if view == nil {
			view = &bufferReadView{baseReadCache: b.baseReadCache}
			view.inflight = append(view.inflight, b.inflight...)
			view.layers = append(view.layers, b.layers...)
		}
		for _, l := range view.layers {
			if l != nil && l.number <= *maxBlock {
				bounded.layers = append(bounded.layers, l)
			}
		}
		view = bounded
	} else if view == nil {
		view = &bufferReadView{baseReadCache: b.baseReadCache}
		view.inflight = append(view.inflight, b.inflight...)
		view.layers = append(view.layers, b.layers...)
	}
	b.mu.RUnlock()
	b.flushMu.Unlock()
	return &ReadSnapshot{view: view, base: base}, nil
}

func (s *ReadSnapshot) GetWithPresence(key []byte) ([]byte, bool, error) {
	if s == nil || s.closed.Load() || s.view == nil || s.base == nil {
		return nil, false, nil
	}
	keyHash := layerBloomHashBytes(key)
	if value, found, tomb := lookupLayersNewest(s.view.inflight, key, keyHash); tomb {
		return nil, false, nil
	} else if found {
		return append([]byte(nil), value...), true, nil
	}
	if value, found, tomb := lookupLayersNewest(s.view.layers, key, keyHash); tomb {
		return nil, false, nil
	} else if found {
		return append([]byte(nil), value...), true, nil
	}
	return readBaseWithPresence(s.base, key)
}

func (s *ReadSnapshot) Get(key []byte) ([]byte, error) {
	value, present, err := s.GetWithPresence(key)
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, ErrNotFound
	}
	return value, nil
}

func (s *ReadSnapshot) IsKeyNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

func (s *ReadSnapshot) Has(key []byte) (bool, error) {
	if s == nil || s.closed.Load() || s.view == nil || s.base == nil {
		return false, nil
	}
	keyHash := layerBloomHashBytes(key)
	if _, found, tomb := lookupLayersNewest(s.view.inflight, key, keyHash); tomb {
		return false, nil
	} else if found {
		return true, nil
	}
	if _, found, tomb := lookupLayersNewest(s.view.layers, key, keyHash); tomb {
		return false, nil
	} else if found {
		return true, nil
	}
	return s.base.Has(key)
}

func (*ReadSnapshot) Put([]byte, []byte) error {
	return errors.New("blockbuffer: write through read snapshot")
}

func (*ReadSnapshot) Delete([]byte) error {
	return errors.New("blockbuffer: delete through read snapshot")
}

func (s *ReadSnapshot) NewIterator(prefix, start []byte) ethdb.Iterator {
	if s == nil || s.closed.Load() || s.view == nil || s.base == nil {
		return &bufferIterator{err: ErrNotFound}
	}
	overlay := newOverlayState()
	for i := len(s.view.inflight) - 1; i >= 0; i-- {
		overlay.walk(s.view.inflight[i], prefix, start)
	}
	for i := len(s.view.layers) - 1; i >= 0; i-- {
		overlay.walk(s.view.layers[i], prefix, start)
	}
	return finishIteratorWithBase(s.base, overlay, prefix, start)
}

func (s *ReadSnapshot) SeekPrefix(prefix, start []byte) (key, value []byte, ok bool, err error) {
	if s == nil || s.closed.Load() || s.view == nil || s.base == nil {
		return nil, nil, false, ErrNotFound
	}
	overlay := newOverlayState()
	for i := len(s.view.inflight) - 1; i >= 0; i-- {
		overlay.walk(s.view.inflight[i], prefix, start)
	}
	for i := len(s.view.layers) - 1; i >= 0; i-- {
		overlay.walk(s.view.layers[i], prefix, start)
	}
	return seekPrefixWithBase(s.base, overlay, prefix, start)
}

// SeekDurablePrefix seeks only the pinned durable MVCC snapshot. It is used by
// staged derived indexes after their watermark proves the requested range has
// no overlay-only rows. Keeping this separate from SeekPrefix makes bypassing
// the logical overlay an explicit, auditable choice by the accessor.
func (s *ReadSnapshot) SeekDurablePrefix(prefix, start []byte) (key, value []byte, ok bool, err error) {
	if s == nil || s.closed.Load() || s.base == nil {
		return nil, nil, false, ErrNotFound
	}
	if seeker, ok := s.base.(pointread.PrefixSeeker); ok {
		return seeker.SeekPrefix(prefix, start)
	}
	return seekPrefixWithBase(s.base, newOverlayState(), prefix, start)
}

func (s *ReadSnapshot) NewStateKVLatestIterator(schemaPrefix []byte, accountID common.AccountID, physicalPrefix []byte) ethdb.Iterator {
	if s == nil || s.closed.Load() || s.view == nil || s.base == nil {
		return &bufferIterator{err: ErrNotFound}
	}
	overlay := newOverlayState()
	schema := string(schemaPrefix)
	physical := unsafe.String(unsafe.SliceData(physicalPrefix), len(physicalPrefix))
	for i := len(s.view.inflight) - 1; i >= 0; i-- {
		overlay.walkPrefixBucket(s.view.inflight[i], schema, accountID[0], physical)
	}
	for i := len(s.view.layers) - 1; i >= 0; i-- {
		overlay.walkPrefixBucket(s.view.layers[i], schema, accountID[0], physical)
	}
	return finishIteratorWithBase(s.base, overlay, physicalPrefix, nil)
}

func (s *ReadSnapshot) Close() error {
	if s == nil || !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	var err error
	if s.base != nil {
		err = s.base.Close()
	}
	s.base = nil
	s.view = nil
	return err
}

type bufferBatchOp struct {
	delete           bool
	reservedOwnedPut bool
	key              string
	value            []byte
	target           *layer
}

type bufferBatch struct {
	parent *Buffer
	ops    []bufferBatchOp
	size   int
	closed bool
}

// BlockHashReader is the optional cold-chain lookup carried by buffers used as
// TVM rawdb readers.
type BlockHashReader interface {
	BlockHashByNumber(number uint64) (common.Hash, bool)
}

type BlockHashReaderFunc func(number uint64) (common.Hash, bool)

func (fn BlockHashReaderFunc) BlockHashByNumber(number uint64) (common.Hash, bool) {
	if fn == nil {
		return common.Hash{}, false
	}
	return fn(number)
}

// valueViewReader exposes a value only for the duration of fn. Pebble can use
// this to keep its block handle open while the blockbuffer copies directly
// into cache-owned immutable storage, avoiding Database.Get's intermediate
// defensive copy. Implementations must invoke fn synchronously.
type valueViewReader interface {
	View(key []byte, fn func([]byte) error) error
}

// keyNotFoundClassifier lets a durable backend identify its native not-found
// sentinel without coupling blockbuffer to that backend's error package.
// Confirmed misses may then use the same bounded, versioned cache as values;
// transient I/O errors are never retained.
type keyNotFoundClassifier interface {
	IsKeyNotFound(error) bool
}

// valuePresenceReader couples a point value with its presence in one backend
// view. Pebble implements this directly; layered readers expose the same
// capability so rawdb metadata reads never have to race a Has/Get pair across
// a concurrent reorg topology change.
type valuePresenceReader interface {
	GetWithPresence(key []byte) ([]byte, bool, error)
}

func isKeyNotFound(base ethdb.KeyValueReader, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrNotFound) {
		return true
	}
	classifier, ok := base.(keyNotFoundClassifier)
	return ok && classifier.IsKeyNotFound(err)
}

// readBaseWithPresence normalizes a backend miss without mistaking a real Get
// failure for absence. Engines with an atomic point-read capability take that
// path directly. Generic test/in-memory stores pay a Has verification only
// after Get returned an unclassified error; successful production reads stay
// one point lookup.
func readBaseWithPresence(base ethdb.KeyValueReader, key []byte) ([]byte, bool, error) {
	if base == nil {
		return nil, false, nil
	}
	if reader, ok := base.(valuePresenceReader); ok {
		return reader.GetWithPresence(key)
	}
	value, err := base.Get(key)
	if err == nil {
		return value, true, nil
	}
	if isKeyNotFound(base, err) {
		return nil, false, nil
	}
	exists, hasErr := base.Has(key)
	if hasErr != nil {
		return nil, false, fmt.Errorf("blockbuffer: verify point-read failure: %w", hasErr)
	}
	if !exists {
		return nil, false, nil
	}
	return nil, false, err
}

func readBaseValue(base ethdb.KeyValueReader, key []byte) ([]byte, error) {
	value, present, err := readBaseWithPresence(base, key)
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, ErrNotFound
	}
	return value, nil
}

// IsKeyNotFound exposes the classification already used by the durable-base
// cache. State accessors use it after an optimistic no-copy Get so a confirmed
// negative-cache result does not trigger a redundant Has against Pebble.
func (b *Buffer) IsKeyNotFound(err error) bool {
	if b == nil {
		return errors.Is(err, ErrNotFound)
	}
	return isKeyNotFound(b.base, err)
}

// stringKeyWriter is an optional synchronous writer fast path for layer maps,
// which already own immutable string keys. Implementations must copy key before
// returning. Pebble batches satisfy it with DeferredBatchOp, copying the string
// directly into batch storage instead of allocating a temporary []byte.
type stringKeyWriter interface {
	PutString(key string, value []byte) error
	DeleteString(key string) error
}

// New creates a Buffer that falls through reads to base.
func New(base ethdb.KeyValueReader) *Buffer {
	b := &Buffer{base: base}
	if reader, ok := base.(BlockHashReader); ok {
		b.blockHashReader = reader
	}
	b.publishReadViewLocked()
	return b
}

func (b *Buffer) SetBlockHashReader(reader BlockHashReader) {
	b.mu.Lock()
	b.blockHashReader = reader
	b.mu.Unlock()
}

func (b *Buffer) BlockHashByNumber(number uint64) (common.Hash, bool) {
	if b == nil {
		return common.Hash{}, false
	}
	if hash, ok := rawdb.ReadBlockHashKV(b, number); ok {
		return hash, true
	}
	b.mu.RLock()
	reader := b.blockHashReader
	b.mu.RUnlock()
	if reader == nil {
		return common.Hash{}, false
	}
	return reader.BlockHashByNumber(number)
}

func (b *Buffer) BlockHashByNumberStrict(number uint64) (common.Hash, bool, error) {
	if b == nil {
		return common.Hash{}, false, nil
	}
	if data, ok, err := rawdb.ReadBlockRawStrict(b, number); err != nil {
		return common.Hash{}, ok, err
	} else if ok {
		block, err := types.UnmarshalBlock(data)
		if err != nil {
			return common.Hash{}, true, fmt.Errorf("rawdb: block %d decode: %w", number, err)
		}
		if block.Number() != number {
			return common.Hash{}, true, fmt.Errorf("rawdb: block row %d contains block number %d", number, block.Number())
		}
		return block.Hash(), true, nil
	}
	b.mu.RLock()
	reader := b.blockHashReader
	b.mu.RUnlock()
	if reader == nil {
		return common.Hash{}, false, nil
	}
	if strict, ok := reader.(rawdb.BlockHashReaderStrict); ok {
		return strict.BlockHashByNumberStrict(number)
	}
	hash, found := reader.BlockHashByNumber(number)
	return hash, found, nil
}

// ConcurrentReadWriteSafe is a structural marker for higher-level stores that
// may publish disjoint keys while other workers are still reading. Buffer
// Put/Delete resolve the target under b.mu and protect the actual map mutation
// with the key's shard lock; readers use immutable topology views and the same
// shard locks.
func (*Buffer) ConcurrentReadWriteSafe() {}

// publishReadViewLocked publishes the immutable topology slices. Structural
// mutators append beyond a previously published length or replace/subslice with
// capacity clamped before a future append, so an older view is never rewritten.
// This avoids copying the entire pending-layer topology twice per imported
// block. The layer pointers themselves remain stable and keep dropped layers
// alive until readers of an older view complete.
func (b *Buffer) publishReadViewLocked() {
	view := &bufferReadView{
		baseReadCache: b.baseReadCache,
	}
	if len(b.inflight) != 0 {
		view.inflight = b.inflight[:len(b.inflight):len(b.inflight)]
	}
	if len(b.layers) != 0 {
		view.layers = b.layers[:len(b.layers):len(b.layers)]
	}
	b.readView.Store(view)
}

// loadReadView supports Buffer's zero value for tests and lightweight wrappers:
// New and every structural mutator publish a view, but a never-mutated literal
// may not have one yet. The fallback takes the old read lock once and returns a
// private immutable copy without publishing under a read lock.
func (b *Buffer) loadReadView() *bufferReadView {
	if view := b.readView.Load(); view != nil {
		return view
	}
	b.mu.RLock()
	view := &bufferReadView{baseReadCache: b.baseReadCache}
	view.inflight = append(view.inflight, b.inflight...)
	view.layers = append(view.layers, b.layers...)
	b.mu.RUnlock()
	return view
}

// SetBaseReadCacheSize configures the bounded durable-base read cache used by
// GetNoCopyCached. It must be called before the buffer begins concurrent use;
// passing zero disables the cache. Flat-latest and commitment-branch accessors
// opt into this API because both consume or defensively copy returned bytes.
// An optional schema prefix lets a successful flush count as the second cache
// observation for read-before-write rows in that namespace.
func (b *Buffer) SetBaseReadCacheSize(sizeBytes int, flushAdmissionPrefix ...string) {
	b.setBaseReadCacheSize(sizeBytes, -1, flushAdmissionPrefix...)
}

// SetBaseReadCacheSizeWithTrunk additionally reserves a bounded fixed tier for
// physical keys whose suffix after flushAdmissionPrefix is at most trunkDepth
// bytes. It is intended for commitment tries whose byte suffix is one nibble per
// depth; other durable-cache users retain SetBaseReadCacheSize's CLOCK-only policy.
func (b *Buffer) SetBaseReadCacheSizeWithTrunk(sizeBytes, trunkDepth int, flushAdmissionPrefix string) {
	b.setBaseReadCacheSize(sizeBytes, trunkDepth, flushAdmissionPrefix)
}

func (b *Buffer) setBaseReadCacheSize(sizeBytes, trunkDepth int, flushAdmissionPrefix ...string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	old := b.baseReadCache
	current := newBaseReadCacheWithTrunk(sizeBytes, trunkDepth, flushAdmissionPrefix...)
	b.baseReadCache = current
	b.publishReadViewLocked()
	b.mu.Unlock()
	if old != nil {
		old.clear()
	}
	// old.clear publishes its final empty state. Re-publish the installed owner
	// afterwards so reconfiguration (including disabling the cache) cannot leave
	// process gauges describing the retired cache's capacity and budgets.
	if current != nil {
		publishBaseReadCacheMetrics(current.stats())
	} else {
		publishBaseReadCacheMetrics(baseReadCacheStats{})
	}
}

// NewBatch creates a write batch whose operations are owned by the active layer
// at Put/Delete time. Write applies queued operations under one exclusive lock,
// while each Put/Delete only takes a brief read lock to capture the layer.
func (b *Buffer) NewBatch() ethdb.Batch {
	return &bufferBatch{parent: b}
}

// NewBatchWithSize creates a batch with a small preallocation derived from the
// caller's byte-size hint. The hint is approximate, matching ethdb semantics.
func (b *Buffer) NewBatchWithSize(size int) ethdb.Batch {
	capHint := 0
	if size > 0 {
		capHint = size / 64
		if capHint < 1 {
			capHint = 1
		}
	}
	return &bufferBatch{parent: b, ops: make([]bufferBatchOp, 0, capHint)}
}

func (b *bufferBatch) Put(key, value []byte) error {
	if b.closed {
		return errors.New("blockbuffer: batch closed")
	}
	// The layer ultimately indexes by string. Own that immutable string now so
	// Write can publish it directly instead of first copying []byte here and
	// then allocating the same key again during []byte→string conversion.
	k := string(key)
	v := append([]byte(nil), value...)
	b.ops = append(b.ops, bufferBatchOp{key: k, value: v, target: b.parent.activeLayer()})
	b.size += len(k) + len(v)
	return nil
}

// PutStateKVLatest implements rawdb's structured flat-latest writer path for
// buffered batches. The final immutable string is owned here, while value is
// defensively copied exactly like ordinary Batch.Put.
func (b *bufferBatch) PutStateKVLatest(prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey, value []byte) error {
	if b.closed {
		return errors.New("blockbuffer: batch closed")
	}
	k := joinStateKVLatestKey(prefix, accountID, generation, domain, logicalKey)
	v := append([]byte(nil), value...)
	b.ops = append(b.ops, bufferBatchOp{key: k, value: v, target: b.parent.activeLayer()})
	b.size += len(k) + len(v)
	return nil
}

// PutStateKVLatestOwnedValue retains a freshly encoded immutable value while
// still constructing the structured map key directly.
func (b *bufferBatch) PutStateKVLatestOwnedValue(prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey, value []byte) error {
	if b.closed {
		return errors.New("blockbuffer: batch closed")
	}
	k := joinStateKVLatestKey(prefix, accountID, generation, domain, logicalKey)
	b.ops = append(b.ops, bufferBatchOp{key: k, value: value, target: b.parent.activeLayer()})
	b.size += len(k) + len(value)
	return nil
}

// PutOwnedValue is an optional hot-path extension for freshly encoded values.
// It still owns the key via an immutable string copy, but retains value
// directly. The caller transfers ownership and must never mutate value after
// this call. Ordinary ethdb.Batch.Put keeps its defensive-copy semantics.
func (b *bufferBatch) PutOwnedValue(key, value []byte) error {
	if b.closed {
		return errors.New("blockbuffer: batch closed")
	}
	k := string(key)
	b.ops = append(b.ops, bufferBatchOp{key: k, value: value, target: b.parent.activeLayer()})
	b.size += len(k) + len(value)
	return nil
}

// PutStringOwnedValue retains an immutable string key and freshly encoded
// value directly. This is used by fixed-key metadata writers; ordinary Put
// and PutOwnedValue continue to own caller byte-slice keys defensively.
func (b *bufferBatch) PutStringOwnedValue(key string, value []byte) error {
	if b.closed {
		return errors.New("blockbuffer: batch closed")
	}
	b.ops = append(b.ops, bufferBatchOp{key: key, value: value, target: b.parent.activeLayer()})
	b.size += len(key) + len(value)
	return nil
}

// PutOwnedKeyValue retains a freshly constructed immutable key and value.
// The account-latest commit path builds all physical keys in one exact-size
// arena and transfers that arena to the batch, so converting the slices to
// strings without copying avoids one allocation per updated account. The
// string keeps the arena alive for as long as an operation or layer needs it.
func (b *bufferBatch) PutOwnedKeyValue(key, value []byte) error {
	if b.closed {
		return errors.New("blockbuffer: batch closed")
	}
	var k string
	if len(key) != 0 {
		k = unsafe.String(unsafe.SliceData(key), len(key))
	}
	target := b.parent.activeLayer()
	op := bufferBatchOp{key: k, value: value, target: target}
	if target != nil {
		s := target.shardForString(k)
		s.mu.Lock()
		s.pendingOwnedPuts++
		s.mu.Unlock()
		op.reservedOwnedPut = true
	}
	b.ops = append(b.ops, op)
	b.size += len(k) + len(value)
	return nil
}

func (b *bufferBatch) Delete(key []byte) error {
	if b.closed {
		return errors.New("blockbuffer: batch closed")
	}
	k := string(key)
	b.ops = append(b.ops, bufferBatchOp{delete: true, key: k, target: b.parent.activeLayer()})
	b.size += len(k)
	return nil
}

// DeleteStateKVLatest is the structured flat-latest batch delete counterpart.
func (b *bufferBatch) DeleteStateKVLatest(prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey []byte) error {
	if b.closed {
		return errors.New("blockbuffer: batch closed")
	}
	k := joinStateKVLatestKey(prefix, accountID, generation, domain, logicalKey)
	b.ops = append(b.ops, bufferBatchOp{delete: true, key: k, target: b.parent.activeLayer()})
	b.size += len(k)
	return nil
}

func (b *bufferBatch) DeleteRange(_, _ []byte) error {
	return errors.New("blockbuffer: batch DeleteRange unsupported")
}

func (b *bufferBatch) ValueSize() int { return b.size }

func (b *bufferBatch) Write() error {
	if b.closed {
		return errors.New("blockbuffer: batch closed")
	}
	b.parent.flushMu.Lock()
	defer b.parent.flushMu.Unlock()
	// RLock (not Lock): the slice membership reads need only a shared lock, and
	// RLock still excludes structural writers (BeginBlock/Commit/Discard take
	// b.mu.Lock), so the inflight/layers sets stay stable for the whole call.
	// applyBatchOpToLayer takes the per-target layer lock, so map mutations to
	// different layers (foreground/worker) proceed concurrently.
	b.parent.mu.RLock()
	defer b.parent.mu.RUnlock()
	for i := range b.ops {
		op := &b.ops[i]
		target := op.target
		if target == nil {
			target = b.parent.newestInflightLocked()
		}
		if target == nil {
			panic("blockbuffer: batch Write called with no active layer")
		}
		if !b.parent.layerPendingLocked(target) {
			return errors.New("blockbuffer: batch target layer is no longer pending")
		}
		applyBatchOpToLayer(target, op)
	}
	return nil
}

func applyBatchOpToLayer(target *layer, op *bufferBatchOp) {
	k := op.key
	s := target.shardForString(k)
	s.mu.Lock()
	defer s.mu.Unlock()
	if op.reservedOwnedPut {
		if s.pendingOwnedPuts == 0 {
			panic("blockbuffer: owned batch reservation underflow")
		}
		s.pendingOwnedPuts--
		op.reservedOwnedPut = false
	}
	target.addBloomString(k)
	s.trackPrefixBucketKeyBeforeMutation(k)
	s.trackIteratorKeyBeforeMutation(k)
	if op.delete {
		delete(s.writes, k)
		delete(s.durableWrites, k)
		if s.deletes == nil {
			s.deletes = make(map[string]struct{})
		}
		s.deletes[k] = struct{}{}
		return
	}
	delete(s.deletes, k)
	delete(s.durableWrites, k)
	// Put already copied the caller's value into storage owned by the batch.
	// Batches never mutate those bytes, so the layer can retain that owned
	// slice directly instead of allocating and copying it a second time.
	if s.writes == nil {
		s.writes = make(map[string][]byte)
	}
	s.writes[k] = op.value
}

func releaseBatchOpReservation(op *bufferBatchOp) {
	if !op.reservedOwnedPut {
		return
	}
	s := op.target.shardForString(op.key)
	s.mu.Lock()
	if s.pendingOwnedPuts == 0 {
		s.mu.Unlock()
		panic("blockbuffer: owned batch reservation underflow")
	}
	s.pendingOwnedPuts--
	s.mu.Unlock()
	op.reservedOwnedPut = false
}

func bufferBatchOpSize(op bufferBatchOp) int {
	if op.delete {
		return len(op.key)
	}
	return len(op.key) + len(op.value)
}

// WriteUpTo applies and removes queued operations whose captured committed
// layer belongs to a block at or below cutoff. Operations for newer committed
// layers or the active layer remain queued. Unlike Write, this is intended for
// range-owned batches that must land writes before FlushUpTo drops old layers.
//
// The layer carries its block number (captured at BeginBlock), so this is a
// single integer compare per op — the earlier numberOf callback variant cost a
// pebble Get + key allocation per op, which dominated bulk-sync profiles.
func (b *bufferBatch) WriteUpTo(cutoff uint64) (int, error) {
	if b.closed {
		return 0, errors.New("blockbuffer: batch closed")
	}
	return b.writeFiltered(func(target *layer) bool {
		return target.number <= cutoff
	}, false)
}

// WriteCommitted applies and removes queued operations whose captured target is
// currently a committed layer. If dropStale is true, operations whose captured
// layer has already been discarded are removed instead of causing an error.
func (b *bufferBatch) WriteCommitted(dropStale bool) (int, error) {
	if b.closed {
		return 0, errors.New("blockbuffer: batch closed")
	}
	return b.writeFiltered(func(*layer) bool { return true }, dropStale)
}

// NewestCommittedNumber exposes the parent buffer's newest committed-layer block
// number so a range-owned latest-domain writer can prune its read-your-writes
// overlay down to the highest durable block after a partial flush.
func (b *bufferBatch) NewestCommittedNumber() (uint64, bool) {
	if b.closed || b.parent == nil {
		return 0, false
	}
	return b.parent.NewestCommittedNumber()
}

func (b *bufferBatch) writeFiltered(matchCommitted func(*layer) bool, dropStale bool) (int, error) {
	b.parent.flushMu.Lock()
	defer b.parent.flushMu.Unlock()
	// RLock: see Write. Structural writers are still excluded (they Lock), so the
	// membership classification is stable; applyBatchOpToLayer locks each target
	// layer so committed-layer applies don't block disjoint-layer writers.
	b.parent.mu.RLock()
	defer b.parent.mu.RUnlock()
	if !dropStale {
		// Validate captured targets before compacting b.ops in place. Besides
		// keeping the operation slice intact on error, this guarantees an owned
		// reservation has exactly one live op that will eventually consume or
		// release it.
		for i := range b.ops {
			target := b.ops[i].target
			if target != nil && b.parent.layerStateLocked(target) == layerDetached {
				return len(b.ops), errors.New("blockbuffer: batch target layer is no longer pending")
			}
		}
	}

	kept := b.ops[:0]
	keptSize := 0
	for i := range b.ops {
		op := &b.ops[i]
		target := op.target
		if target == nil {
			target = b.parent.newestInflightLocked()
		}
		if target == nil {
			if dropStale {
				releaseBatchOpReservation(op)
				continue
			}
			kept = append(kept, *op)
			keptSize += bufferBatchOpSize(*op)
			continue
		}
		state := b.parent.layerStateLocked(target)
		if state == layerDetached {
			if dropStale {
				releaseBatchOpReservation(op)
				continue
			}
			return len(b.ops), errors.New("blockbuffer: batch target layer is no longer pending")
		}
		if state != layerCommitted || !matchCommitted(target) {
			kept = append(kept, *op)
			keptSize += bufferBatchOpSize(*op)
			continue
		}
		applyBatchOpToLayer(target, op)
	}
	clear(b.ops[len(kept):])
	b.ops = kept
	b.size = keptSize
	return len(b.ops), nil
}

func (b *bufferBatch) Reset() {
	// Operations may retain caller-transferred key arenas and values. Clear the
	// reusable backing slice so Reset releases them immediately.
	for i := range b.ops {
		releaseBatchOpReservation(&b.ops[i])
	}
	clear(b.ops)
	b.ops = b.ops[:0]
	b.size = 0
}

func (b *bufferBatch) Replay(w ethdb.KeyValueWriter) error {
	for _, op := range b.ops {
		// Replay targets an arbitrary ethdb writer whose ownership contract may
		// retain the key. Give it an owned []byte; the normal blockbuffer Write /
		// WriteUpTo paths publish the batch-owned string without this conversion.
		key := []byte(op.key)
		if op.delete {
			if err := w.Delete(key); err != nil {
				return err
			}
			continue
		}
		if err := w.Put(key, op.value); err != nil {
			return err
		}
	}
	return nil
}

func (b *bufferBatch) Close() {
	for i := range b.ops {
		releaseBatchOpReservation(&b.ops[i])
	}
	b.closed = true
	b.parent = nil
	b.ops = nil
	b.size = 0
}

func (b *Buffer) activeLayer() *layer {
	if b == nil {
		return nil
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.newestInflightLocked()
}

// newestInflightLocked returns the newest in-flight layer (the foreground's
// active layer) or nil if none. Caller holds b.mu (read or write).
func (b *Buffer) newestInflightLocked() *layer {
	if n := len(b.inflight); n > 0 {
		return b.inflight[n-1]
	}
	return nil
}

// effectiveMaxInflight treats the zero value as 1 so a freshly New'd buffer
// keeps the single-active-layer semantics (BeginBlock panics on a second begin).
func (b *Buffer) effectiveMaxInflight() int {
	if b.maxInflight < 1 {
		return 1
	}
	return b.maxInflight
}

// SetMaxInflight raises the number of layers that may be in flight at once.
// The async commit worker sets this to 2 (one committing, one executing). A
// value < 1 restores the single-active default. Must be called before any
// concurrent buffer use (e.g. at BlockChain construction).
func (b *Buffer) SetMaxInflight(n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maxInflight = n
}

// MaxInflight returns the effective in-flight layer cap (the zero value reads as
// 1, the single-active default). Exported for the async-commit depth wiring and
// its tests.
func (b *Buffer) MaxInflight() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.effectiveMaxInflight()
}

// NewestCommittedNumber returns the block number of the newest COMMITTED
// (CommitInflight'd, not-yet-flushed) layer, or (0,false) if none. Committed
// layers are ordered oldest→newest, so the newest is the tail. Used by the
// async-commit deep path (depth>2) to cap the flush cutoff at a fully-committed
// block: the commit worker publishes bc.CurrentBlock() BEFORE CommitInflight, so
// the head block's layer can still be in-flight, and FlushLatestUpTo KEEPS ops
// targeting an in-flight layer (writeFiltered only applies committed targets).
// Capping at currentBlock therefore leaves the head block's latest-domain op
// queued while a later postFlush drops its (by-then committed) layer →
// "batch target layer is no longer pending". The newest committed number is the
// highest block whose layer is guaranteed promoted, so any op ≤ it is flushed
// (not kept) before its layer can be dropped.
func (b *Buffer) NewestCommittedNumber() (uint64, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.layers) == 0 {
		return 0, false
	}
	return b.layers[len(b.layers)-1].number, true
}

func (b *Buffer) layerPendingLocked(target *layer) bool {
	return b.layerStateLocked(target) != layerDetached
}

// layerInflightLocked reports whether target is currently an in-flight layer.
func (b *Buffer) layerInflightLocked(target *layer) bool {
	return b.layerStateLocked(target) == layerInflight
}

// layerStateLocked classifies a queued batch target in O(1) for every
// production layer. The slice walk is retained only for hand-built, unowned
// layers used by low-level tests and diagnostic helpers.
func (b *Buffer) layerStateLocked(target *layer) layerState {
	if b == nil || target == nil {
		return layerDetached
	}
	if target.owner != nil {
		if target.owner != b {
			return layerDetached
		}
		return target.state
	}
	for _, l := range b.inflight {
		if l == target {
			return layerInflight
		}
	}
	for _, l := range b.layers {
		if l == target {
			return layerCommitted
		}
	}
	return layerDetached
}

// BeginBlock opens a fresh active layer for the given block hash and number.
// The number is captured so subsequent FlushUpTo / WriteUpTo cutoffs can be
// evaluated without a per-op block-hash → block-number lookup. Panics if the
// number of in-flight layers would exceed maxInflight (1 by default) — this
// preserves the legacy "double BeginBlock" guard for the single-active case.
func (b *Buffer) BeginBlock(hash common.Hash, number uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.inflight) >= b.effectiveMaxInflight() {
		panic("blockbuffer: BeginBlock would exceed maxInflight in-flight layers")
	}
	l := newLayer(hash, number)
	l.owner = b
	l.state = layerInflight
	b.inflight = append(b.inflight, l)
	b.publishReadViewLocked()
}

// CommitBlock promotes the OLDEST in-flight layer onto the committed stack
// (FIFO). With the default single-active configuration this is the only
// in-flight layer, matching the legacy behaviour. Panics if none is in flight.
// The async commit worker uses CommitInflight(handle) instead so it can assert
// it is committing the specific layer it folded.
func (b *Buffer) CommitBlock() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.inflight) == 0 {
		panic("blockbuffer: CommitBlock called with no active layer")
	}
	b.promoteOldestInflightLocked()
}

// promoteOldestInflightLocked moves inflight[0] onto the committed stack. The
// committed stack stays ordered by block number because in-flight layers are
// begun in block order and committed FIFO. Caller holds b.mu.
func (b *Buffer) promoteOldestInflightLocked() {
	l := b.inflight[0]
	l.buildBloom()
	if len(b.inflight) == 1 {
		b.inflight = nil
	} else {
		// Clamp capacity so the next append cannot overwrite an older view.
		b.inflight = b.inflight[1:len(b.inflight):len(b.inflight)]
	}
	l.state = layerCommitted
	b.layers = append(b.layers, l)
	b.buildNewestLayerBloomSegmentLocked()
	b.publishReadViewLocked()
}

// DiscardActive drops the NEWEST in-flight layer without promoting it (the
// foreground's current block, dropped on an applyBlock error). No-op if no
// layer is in flight.
func (b *Buffer) DiscardActive() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n := len(b.inflight); n > 0 {
		b.inflight[n-1].state = layerDetached
		if n == 1 {
			b.inflight = nil
		} else {
			b.inflight = b.inflight[: n-1 : n-1]
		}
		b.publishReadViewLocked()
	}
}

// InflightHandle is an opaque reference to an in-flight (begun-but-uncommitted)
// layer. The async commit worker obtains one via NewestInflight at handoff and
// uses it with ViewLayer/LayerWriter/CommitInflight/DiscardInflight to operate
// on that specific layer while the foreground writes a newer one. The zero
// handle is invalid (Valid() == false).
type InflightHandle struct {
	l      *layer
	hash   common.Hash
	number uint64
}

// Valid reports whether the handle references a layer.
func (h InflightHandle) Valid() bool { return h.l != nil }

// Number returns the block number captured when the layer was begun.
func (h InflightHandle) Number() uint64 { return h.number }

// Hash returns the block hash captured when the layer was begun.
func (h InflightHandle) Hash() common.Hash { return h.hash }

// NewestInflight returns a handle to the newest in-flight layer (the layer the
// foreground just finished writing, before it begins the next block). ok is
// false if no layer is in flight. The async commit worker calls this at the
// fold handoff point to capture the layer it will own.
func (b *Buffer) NewestInflight() (InflightHandle, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	l := b.newestInflightLocked()
	if l == nil {
		return InflightHandle{}, false
	}
	return InflightHandle{l: l, hash: l.blockHash, number: l.number}, true
}

// MarkActiveWritesDurable records active-layer values already persisted by an
// ordered direct metadata batch. The overlay remains readable and rewindable,
// but its eventual flush skips the identical durable value. Any resident
// durable-base cache entry is refreshed here, while the overlay still masks
// the base, so dropping a coalesced solid layer cannot reveal its stale value.
func (b *Buffer) MarkActiveWritesDurable(keys ...[]byte) error {
	b.mu.RLock()
	target := b.newestInflightLocked()
	b.mu.RUnlock()
	if target == nil {
		return errors.New("blockbuffer: mark durable writes with no active layer")
	}
	return markLayerWritesDurable(target, b.baseReadCache, keys...)
}

func (b *Buffer) MarkInflightWritesDurable(h InflightHandle, keys ...[]byte) error {
	if !h.Valid() {
		return errors.New("blockbuffer: mark durable writes with invalid handle")
	}
	b.mu.RLock()
	inflight := b.layerInflightLocked(h.l)
	b.mu.RUnlock()
	if !inflight {
		return errors.New("blockbuffer: mark durable writes for non-inflight layer")
	}
	return markLayerWritesDurable(h.l, b.baseReadCache, keys...)
}

func markLayerWritesDurable(target *layer, cache *baseReadCache, keys ...[]byte) error {
	for _, key := range keys {
		k := string(key)
		s := target.shardForString(k)
		s.mu.Lock()
		value, ok := s.writes[k]
		_, deleted := s.deletes[k]
		if !ok || deleted {
			s.mu.Unlock()
			return fmt.Errorf("blockbuffer: mark durable write missing or deleted key %x", key)
		}
		if s.durableWrites == nil {
			s.durableWrites = make(map[string][]byte)
		}
		s.durableWrites[k] = value
		s.mu.Unlock()
		// The caller invokes Mark*WritesDurable only after the out-of-band
		// batch has committed. Treat that batch exactly like a successful
		// canonical layer flush for cache-generation purposes. This must not be
		// deferred to FlushUpTo: when several layers are coalesced,
		// mergeLayers intentionally removes already-durable writes from its
		// output map, so the flush observer cannot discover and promote them.
		cache.setFlushedAt(k, value, layerShardIndexString(k))
	}
	return nil
}

// CommitInflight promotes the in-flight layer referenced by h onto the
// committed stack. It asserts h is the OLDEST in-flight layer so the committed
// stack stays block-number ordered (the worker commits FIFO, in fold order).
// Returns an error if h is no longer in flight (e.g. already discarded by a
// reorg drain) or is not the oldest. Used by the async commit worker.
func (b *Buffer) CommitInflight(h InflightHandle) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.layerInflightLocked(h.l) {
		return errors.New("blockbuffer: CommitInflight handle is not in flight")
	}
	if b.inflight[0] != h.l {
		return errors.New("blockbuffer: CommitInflight handle is not the oldest in-flight layer")
	}
	b.promoteOldestInflightLocked()
	return nil
}

// DiscardInflight drops the in-flight layer referenced by h without promoting
// it (the worker's error path, or a reorg discarding an orphan-branch layer
// before it commits). No-op if h is no longer in flight.
func (b *Buffer) DiscardInflight(h InflightHandle) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, l := range b.inflight {
		if l == h.l {
			l.state = layerDetached
			remaining := make([]*layer, 0, len(b.inflight)-1)
			remaining = append(remaining, b.inflight[:i]...)
			remaining = append(remaining, b.inflight[i+1:]...)
			b.inflight = remaining
			b.publishReadViewLocked()
			return
		}
	}
}

// DiscardBlock removes the layer with the given block hash from the layered
// stack. No-op if no such layer exists. Used by switchFork to drop
// orphan-branch buffers.
func (b *Buffer) DiscardBlock(hash common.Hash) {
	b.mu.Lock()
	defer b.mu.Unlock()
	match := -1
	for i, l := range b.layers {
		if l.blockHash == hash {
			match = i
			break
		}
	}
	if match < 0 {
		return
	}
	out := make([]*layer, 0, len(b.layers)-1)
	for _, l := range b.layers {
		if l.blockHash == hash {
			l.state = layerDetached
			continue
		}
		out = append(out, l)
	}
	b.layers = out
	b.publishReadViewLocked()
}

// Discard drops every layer (active and committed). Used as a
// nuclear-option reset; not currently invoked by core.BlockChain.
func (b *Buffer) Discard() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := range b.inflight {
		b.inflight[i].state = layerDetached
	}
	b.inflight = nil
	for i := range b.layers {
		b.layers[i].state = layerDetached
	}
	b.layers = nil
	if b.baseReadCache != nil {
		b.baseReadCache.clear()
	}
	b.publishReadViewLocked()
}

// PendingBlocks returns the block hashes for currently-pending committed
// layers, oldest first. Useful for diagnostics and tests.
func (b *Buffer) PendingBlocks() []common.Hash {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]common.Hash, len(b.layers))
	for i, l := range b.layers {
		out[i] = l.blockHash
	}
	return out
}

// lookup checks one layer for key under the matching shard's read lock. The
// returned value ALIASES the layer's storage (no copy); it stays valid even after a
// concurrent write because writes replace the map entry with a fresh slice and
// never mutate the backing array in place. found and tomb are mutually
// exclusive. Taking key as []byte keeps the `m[string(key)]` map index
// allocation-free (the compiler elides the conversion), so GetNoCopy stays
// alloc-free on a buffer hit.
func (l *layer) lookup(key []byte) (v []byte, found, tomb bool) {
	if l.bloom.Load() == nil {
		if v, found, tomb = l.lookupMap(key); found || tomb {
			return v, found, tomb
		}
		if l.rangeDeletesBytes(key) {
			return nil, false, true
		}
		return nil, false, false
	}
	return l.lookupHash(key, layerBloomHashBytes(key))
}

// lookupHash is lookup with a caller-precomputed filter hash. Stack walks
// compute it once and reuse it across every committed layer.
func (l *layer) lookupHash(key []byte, keyHash uint64) (v []byte, found, tomb bool) {
	if bloom := l.bloom.Load(); bloom == nil || bloom.mayContainHash(keyHash) {
		if v, found, tomb = l.lookupMap(key); found || tomb {
			return v, found, tomb
		}
	}
	if l.rangeDeletesBytes(key) {
		return nil, false, true
	}
	return nil, false, false
}

func (l *layer) lookupMap(key []byte) (v []byte, found, tomb bool) {
	s := l.shardForBytes(key)
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Writes vastly outnumber tombstones during sync. Probe them first so a
	// live overlay hit hashes the physical key once instead of first checking
	// the usually-empty delete map. Put/Delete keep the maps mutually exclusive.
	if val, ok := s.writes[string(key)]; ok {
		return val, true, false
	}
	if len(s.deletes) != 0 {
		if _, t := s.deletes[string(key)]; t {
			return nil, false, true
		}
	}
	return nil, false, false
}

func lookupLayersNewest(layers []*layer, key []byte, keyHash uint64) (v []byte, found, tomb bool) {
	for i := len(layers) - 1; i >= 0; {
		l := layers[i]
		if segment := l.segment.Load(); segment != nil && !segment.hasRangeDeletes &&
			segment.ready.Load() && segment.last == l && segment.size <= i+1 &&
			layers[i-segment.size+1] == segment.first &&
			!segment.bloom.mayContainHash(keyHash) {
			i -= segment.size
			continue
		}
		if v, found, tomb = l.lookupHash(key, keyHash); found || tomb {
			return v, found, tomb
		}
		i--
	}
	return nil, false, false
}

// Get returns the value for key, searching active layer first, then
// layered stack newest-first, then the base reader. Tombstones short-
// circuit and return ErrNotFound. Safe to call concurrently with mutators.
//
// The layer topology comes from an atomically published immutable view; each
// layer's matching map shard is read under its shard lock via lookup. The
// (potentially slow) base read therefore runs without holding Buffer.mu.
func (b *Buffer) Get(key []byte) ([]byte, error) {
	value, present, err := b.GetWithPresence(key)
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, ErrNotFound
	}
	return value, nil
}

// GetWithPresence resolves one immutable topology view and returns absence as
// data rather than an error. This is the atomic point-read capability rawdb
// uses for metadata whose overlay can be replaced by switchFork concurrently.
func (b *Buffer) GetWithPresence(key []byte) ([]byte, bool, error) {
	view := b.loadReadView()
	keyHash := layerBloomHashBytes(key)
	// In-flight layers first, newest-first (the foreground's active layer wins
	// over an older worker-owned layer), then committed layers newest-first.
	if v, found, tomb := lookupLayersNewest(view.inflight, key, keyHash); tomb {
		return nil, false, nil
	} else if found {
		return append([]byte(nil), v...), true, nil
	}
	if v, found, tomb := lookupLayersNewest(view.layers, key, keyHash); tomb {
		return nil, false, nil
	} else if found {
		return append([]byte(nil), v...), true, nil
	}
	return readBaseWithPresence(b.base, key)
}

// GetNoCopy is Get without the defensive value copy: on a buffer hit it returns
// the layer's internal value slice directly (aliasing buffer storage), saving
// the per-Get allocation that dominates the commitment-fold read path. The
// returned slice MUST NOT be mutated. Layer writes replace map values with new
// backing slices, so a retained read remains stable even if the same key is
// subsequently written. The commitment decoder uses that property to borrow
// leaf-key fields for one synchronous fold. Reads that fall through to the
// uncached base reader use the base's own (copying) Get.
func (b *Buffer) GetNoCopy(key []byte) ([]byte, error) {
	return b.getNoCopy(key, false)
}

// GetNoCopyCached is GetNoCopy plus a bounded cache for reads that fall all the
// way through to the durable base. rawdb's flat-latest and commitment-branch
// accessors detect this optional method; ordinary buffer reads remain uncached.
func (b *Buffer) GetNoCopyCached(key []byte) ([]byte, error) {
	return b.getNoCopy(key, true)
}

// Prefetch resolves a key through the current overlays and, on a durable-base
// read, admits the result directly into the bounded base cache. The caller has
// already identified the key as near-future work, so it does not pay the
// ordinary two-observation admission delay. Same-key flush invalidation still
// rejects a late stale fill.
func (b *Buffer) Prefetch(key []byte) ([]byte, error) {
	view := b.loadReadView()
	keyHash := layerBloomHashBytes(key)
	if value, found, tomb := lookupLayersNewest(view.inflight, key, keyHash); tomb {
		return nil, ErrNotFound
	} else if found {
		return value, nil
	}
	if value, found, tomb := lookupLayersNewest(view.layers, key, keyHash); tomb {
		return nil, ErrNotFound
	} else if found {
		return value, nil
	}
	if b.base == nil {
		return nil, ErrNotFound
	}
	cache := view.baseReadCache
	if cache == nil {
		return readBaseValue(b.base, key)
	}
	value, ok, epoch := cache.getForPrefetchWithEpoch(key)
	if ok {
		if value == nil {
			return nil, ErrNotFound
		}
		return value, nil
	}
	value, err := readBaseValue(b.base, key)
	if err != nil {
		if isKeyNotFound(b.base, err) {
			cache.prefetchMissingIfEpoch(key, epoch)
			return nil, ErrNotFound
		}
		return nil, err
	}
	if stored, admitted := cache.prefetchIfEpoch(key, value, epoch); admitted {
		return stored, nil
	}
	return value, nil
}

func (b *Buffer) getNoCopy(key []byte, cacheBase bool) ([]byte, error) {
	// lookup keeps the map index allocation-free (string(key) in the index
	// expression is elided by the compiler), so this read stays alloc-free on a
	// buffer hit — it returns the layer's internal slice directly. The immutable
	// read view keeps topology stable for the walk; lookup locks only the key's
	// matching map shard.
	view := b.loadReadView()
	keyHash := layerBloomHashBytes(key)
	if v, found, tomb := lookupLayersNewest(view.inflight, key, keyHash); tomb {
		return nil, ErrNotFound
	} else if found {
		return v, nil
	}
	if v, found, tomb := lookupLayersNewest(view.layers, key, keyHash); tomb {
		return nil, ErrNotFound
	} else if found {
		return v, nil
	}
	cache := view.baseReadCache
	if b.base == nil {
		return nil, ErrNotFound
	}
	var cacheEpoch baseReadCacheEpoch
	if cacheBase && cache != nil {
		if value, ok, epoch := cache.getWithEpoch(key); ok {
			if value == nil {
				return nil, ErrNotFound
			}
			return value, nil
		} else {
			cacheEpoch = epoch
		}
	}
	if !cacheBase || cache == nil {
		return readBaseValue(b.base, key)
	}
	return readBaseIntoCache(b.base, cache, key, cacheEpoch)
}

// GetNoCopyCachedKeyParts is the split-key counterpart of GetNoCopyCached. It
// avoids materialising the physical key on overlay/cache hits; uncommon keys
// above splitReadKeyStackSize and genuine durable misses use an owned key.
func (b *Buffer) GetNoCopyCachedKeyParts(first, second []byte) ([]byte, error) {
	total := len(first) + len(second)
	if total > splitReadKeyStackSize {
		key := make([]byte, 0, total)
		key = append(key, first...)
		key = append(key, second...)
		return b.getNoCopy(key, true)
	}

	var stack [splitReadKeyStackSize]byte
	key := stack[:total]
	n := copy(key, first)
	copy(key[n:], second)
	return b.getNoCopyCachedStackKey(key)
}

// GetNoCopyCachedStateAccountLatest is the typed account-latest counterpart of
// GetNoCopyCachedKeyParts. Passing AccountID by value keeps the caller's
// address scratch off the heap on overlay/cache hits.
func (b *Buffer) GetNoCopyCachedStateAccountLatest(prefix []byte, accountID common.AccountID) ([]byte, error) {
	total := len(prefix) + common.AccountIDLength
	if total > splitReadKeyStackSize {
		key := make([]byte, 0, total)
		key = append(key, prefix...)
		key = append(key, accountID[:]...)
		return b.getNoCopy(key, true)
	}

	var stack [splitReadKeyStackSize]byte
	key := stack[:total]
	n := copy(key, prefix)
	copy(key[n:], accountID[:])
	return b.getNoCopyCachedStackKey(key)
}

// ViewNoCopyCachedKeyParts resolves a split physical key and invokes fn while
// the value is valid. stable is true for immutable overlay and owned fallback
// Get values. Pebble-view hits and cache hits in the configured
// read-before-write namespace are callback-scoped (stable=false), so fn must
// not retain slices that alias them. Commitment branch decoding uses this
// distinction to copy only leaf keys that outlive the callback while the
// hash-dominated encoded branch remains allocation-free.
func (b *Buffer) ViewNoCopyCachedKeyParts(first, second []byte, fn func(value []byte, stable bool) error) (bool, error) {
	total := len(first) + len(second)
	if total > splitReadKeyStackSize {
		key := make([]byte, 0, total)
		key = append(key, first...)
		key = append(key, second...)
		return b.viewNoCopyCachedKey(key, fn)
	}

	keyBuf := borrowSplitReadKey()
	defer returnSplitReadKey(keyBuf)
	key := keyBuf[:total]
	n := copy(key, first)
	copy(key[n:], second)
	return b.viewNoCopyCachedKey(key, fn)
}

func (b *Buffer) viewNoCopyCachedKey(key []byte, fn func(value []byte, stable bool) error) (bool, error) {
	view := b.loadReadView()
	keyHash := layerBloomHashBytes(key)
	if value, found, tomb := lookupLayersNewest(view.inflight, key, keyHash); tomb {
		return false, nil
	} else if found {
		return true, fn(value, true)
	}
	if value, found, tomb := lookupLayersNewest(view.layers, key, keyHash); tomb {
		return false, nil
	} else if found {
		return true, fn(value, true)
	}
	if b.base == nil {
		return false, nil
	}
	cache := view.baseReadCache
	var cacheEpoch baseReadCacheEpoch
	if cache != nil {
		cached, present, epoch, err := cache.viewWithEpoch(key, fn)
		if cached {
			return present, err
		} else {
			cacheEpoch = epoch
		}
	}
	return viewBaseIntoCache(b.base, cache, key, cacheEpoch, fn)
}

// GetNoCopyCachedStateKVLatest implements rawdb's structured flat-latest read
// path for the synchronous pipeline. Typical storage keys are assembled in
// stack storage and never materialised on overlay/base-cache hits.
func (b *Buffer) GetNoCopyCachedStateKVLatest(prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey []byte) ([]byte, error) {
	total := len(prefix) + common.AccountIDLength + 10 + len(logicalKey)
	if total > splitReadKeyStackSize {
		key := make([]byte, 0, total)
		key = appendStateKVLatestKey(key, prefix, accountID, generation, domain, logicalKey)
		return b.getNoCopy(key, true)
	}

	var stack [splitReadKeyStackSize]byte
	key := appendStateKVLatestKey(stack[:0], prefix, accountID, generation, domain, logicalKey)
	return b.getNoCopyCachedStackKey(key)
}

// getNoCopyCachedStackKey resolves a key backed by caller stack storage. A
// durable miss copies into a pooled scratch key before the interface call/cache
// fill, avoiding both escape of the caller's fixed array and one temporary heap
// object per Pebble read.
func (b *Buffer) getNoCopyCachedStackKey(key []byte) ([]byte, error) {
	view := b.loadReadView()
	keyHash := layerBloomHashBytes(key)
	if value, found, tomb := lookupLayersNewest(view.inflight, key, keyHash); tomb {
		return nil, ErrNotFound
	} else if found {
		return value, nil
	}
	if value, found, tomb := lookupLayersNewest(view.layers, key, keyHash); tomb {
		return nil, ErrNotFound
	} else if found {
		return value, nil
	}
	if b.base == nil {
		return nil, ErrNotFound
	}
	cache := view.baseReadCache
	var cacheEpoch baseReadCacheEpoch
	if cache != nil {
		if value, ok, epoch := cache.getWithEpoch(key); ok {
			if value == nil {
				return nil, ErrNotFound
			}
			return value, nil
		} else {
			cacheEpoch = epoch
		}
	}
	return readBaseIntoCachePooledKey(b.base, cache, key, cacheEpoch)
}

// readBaseViewContext and scopedBaseViewContext keep the callback functions
// passed to Pebble pre-bound. valueViewReader guarantees synchronous callback
// execution, so one borrowed context exclusively owns all invocation state and
// can be returned as soon as View completes. This avoids heap-allocating the
// closure and each captured variable on every durable cache miss.
type readBaseViewContext struct {
	cache    *baseReadCache
	key      []byte
	epoch    baseReadCacheEpoch
	out      []byte
	callback func([]byte) error
}

func newReadBaseViewContext() any {
	ctx := new(readBaseViewContext)
	ctx.callback = ctx.consume
	return ctx
}

func (ctx *readBaseViewContext) consume(value []byte) error {
	if stored, ok := ctx.cache.setIfEpoch(ctx.key, value, ctx.epoch); ok {
		ctx.out = stored
	} else {
		ctx.out = append([]byte(nil), value...)
	}
	return nil
}

var readBaseViewContextPool = sync.Pool{New: newReadBaseViewContext}

func borrowReadBaseViewContext(cache *baseReadCache, key []byte, epoch baseReadCacheEpoch) *readBaseViewContext {
	ctx := readBaseViewContextPool.Get().(*readBaseViewContext)
	ctx.cache = cache
	ctx.key = key
	ctx.epoch = epoch
	ctx.out = nil
	return ctx
}

func returnReadBaseViewContext(ctx *readBaseViewContext) {
	ctx.cache = nil
	ctx.key = nil
	ctx.epoch = baseReadCacheEpoch{}
	ctx.out = nil
	readBaseViewContextPool.Put(ctx)
}

type scopedBaseViewContext struct {
	cache       *baseReadCache
	key         []byte
	epoch       baseReadCacheEpoch
	fn          func(value []byte, stable bool) error
	called      bool
	callbackErr error
	callback    func([]byte) error
}

func newScopedBaseViewContext() any {
	ctx := new(scopedBaseViewContext)
	ctx.callback = ctx.consume
	return ctx
}

func (ctx *scopedBaseViewContext) consume(value []byte) error {
	ctx.called = true
	if ctx.cache != nil {
		if !ctx.cache.scopedRefreshKey(ctx.key) {
			if stored, admitted := ctx.cache.setIfEpoch(ctx.key, value, ctx.epoch); admitted {
				ctx.callbackErr = ctx.fn(stored, true)
				return ctx.callbackErr
			}
		} else {
			ctx.cache.storeIfEpoch(ctx.key, value, ctx.epoch)
		}
	}
	ctx.callbackErr = ctx.fn(value, false)
	return ctx.callbackErr
}

var scopedBaseViewContextPool = sync.Pool{New: newScopedBaseViewContext}

func borrowScopedBaseViewContext(cache *baseReadCache, key []byte, epoch baseReadCacheEpoch, fn func(value []byte, stable bool) error) *scopedBaseViewContext {
	ctx := scopedBaseViewContextPool.Get().(*scopedBaseViewContext)
	ctx.cache = cache
	ctx.key = key
	ctx.epoch = epoch
	ctx.fn = fn
	ctx.called = false
	ctx.callbackErr = nil
	return ctx
}

func returnScopedBaseViewContext(ctx *scopedBaseViewContext) {
	ctx.cache = nil
	ctx.key = nil
	ctx.epoch = baseReadCacheEpoch{}
	ctx.fn = nil
	ctx.called = false
	ctx.callbackErr = nil
	scopedBaseViewContextPool.Put(ctx)
}

// readBaseIntoCache fills cache directly from a callback-style base reader
// when available. If a concurrent flush invalidates the observed epoch, the
// cache rejects the late fill; in that case we make one owned fallback copy
// before View returns so no Pebble-backed slice escapes its valid lifetime.
func readBaseIntoCache(base ethdb.KeyValueReader, cache *baseReadCache, key []byte, epoch baseReadCacheEpoch) ([]byte, error) {
	if viewer, ok := base.(valueViewReader); ok {
		ctx := borrowReadBaseViewContext(cache, key, epoch)
		err := viewer.View(key, ctx.callback)
		out := ctx.out
		returnReadBaseViewContext(ctx)
		if isKeyNotFound(base, err) {
			cache.setMissingIfEpoch(key, epoch)
			return nil, ErrNotFound
		}
		return out, err
	}
	value, present, err := readBaseWithPresence(base, key)
	if err != nil {
		return nil, err
	}
	if !present {
		cache.setMissingIfEpoch(key, epoch)
		return nil, ErrNotFound
	}
	stored, _ := cache.setIfEpoch(key, value, epoch)
	return stored, nil
}

// readBaseIntoCachePooledKey makes caller stack-backed keys safe for interface
// calls without allocating an owned slice for every durable miss. All base and
// cache operations consume the key synchronously; cache admission clones it
// before this scratch buffer is returned to the pool.
func readBaseIntoCachePooledKey(base ethdb.KeyValueReader, cache *baseReadCache, key []byte, epoch baseReadCacheEpoch) ([]byte, error) {
	keyBuf := borrowSplitReadKey()
	pooledKey := keyBuf[:len(key)]
	copy(pooledKey, key)

	var (
		value []byte
		err   error
	)
	if cache == nil {
		value, err = readBaseValue(base, pooledKey)
	} else {
		value, err = readBaseIntoCache(base, cache, pooledKey, epoch)
	}
	returnSplitReadKey(keyBuf)
	return value, err
}

// viewBaseIntoCache is the callback counterpart of readBaseIntoCache. On a
// Pebble cold miss the callback consumes the engine-owned value before its
// closer is released, avoiding the owned fallback copy readBaseIntoCache must
// create. Cache admission retains its own copy but the current callback keeps
// consuming the engine-owned transient value; later cache hits are likewise
// scoped under the cache shard read lock. Generic Get results remain
// caller-owned and stable.
func viewBaseIntoCache(base ethdb.KeyValueReader, cache *baseReadCache, key []byte, epoch baseReadCacheEpoch, fn func(value []byte, stable bool) error) (bool, error) {
	if viewer, ok := base.(valueViewReader); ok {
		ctx := borrowScopedBaseViewContext(cache, key, epoch, fn)
		err := viewer.View(key, ctx.callback)
		called := ctx.called
		callbackErr := ctx.callbackErr
		returnScopedBaseViewContext(ctx)
		if called {
			if callbackErr != nil {
				return true, callbackErr
			}
			return true, err
		}
		// Preserve the commitment accessor's established missing-row contract:
		// backends report absence as an error, while callers receive found=false.
		if isKeyNotFound(base, err) && cache != nil {
			cache.setMissingIfEpoch(key, epoch)
		}
		return false, nil
	}
	value, err := base.Get(key)
	if err != nil {
		if isKeyNotFound(base, err) && cache != nil {
			cache.setMissingIfEpoch(key, epoch)
		}
		return false, nil
	}
	if cache != nil {
		cache.storeIfEpoch(key, value, epoch)
	}
	return true, fn(value, true)
}

// Has reports whether key exists, honoring tombstones. Safe to call
// concurrently with mutators.
func (b *Buffer) Has(key []byte) (bool, error) {
	view := b.loadReadView()
	keyHash := layerBloomHashBytes(key)
	if _, found, tomb := lookupLayersNewest(view.inflight, key, keyHash); tomb {
		return false, nil
	} else if found {
		return true, nil
	}
	if _, found, tomb := lookupLayersNewest(view.layers, key, keyHash); tomb {
		return false, nil
	} else if found {
		return true, nil
	}
	if b.base == nil {
		return false, nil
	}
	// A base-cache entry is authoritative after overlay lookup. This includes
	// negative entries populated by a preceding optimistic Get, which is the
	// common state-hydration miss path. Avoid reissuing the same Pebble point
	// lookup as Has merely to confirm the cached result.
	if cache := view.baseReadCache; cache != nil {
		if value, cached, _ := cache.getWithEpoch(key); cached {
			return value != nil, nil
		}
	}
	return b.base.Has(key)
}

// Put stores a key/value pair in the active layer.
// Panics if no layer is active (writes outside an applyBlock are a bug).
func (b *Buffer) Put(key, value []byte) error {
	b.mu.RLock()
	active := b.newestInflightLocked()
	b.mu.RUnlock()
	if active == nil {
		panic("blockbuffer: Put called with no active layer")
	}
	b.putInto(active, key, value)
	return nil
}

// PutOwnedValue is Put for a freshly encoded immutable value. The caller keeps
// the value immutable after this call; Buffer may retain its backing bytes
// directly. This is used for large staged block payloads and encoded account
// rows that are also consumed read-only by a later publish step.
func (b *Buffer) PutOwnedValue(key, value []byte) error {
	b.mu.RLock()
	active := b.newestInflightLocked()
	b.mu.RUnlock()
	if active == nil {
		panic("blockbuffer: PutOwnedValue called with no active layer")
	}
	b.putIntoStringOwnedValue(active, string(key), value)
	return nil
}

// PutStringOwnedValue is the fixed-key counterpart of PutOwnedValue. The
// caller supplies an immutable string whose backing may be retained by the
// active layer together with value.
func (b *Buffer) PutStringOwnedValue(key string, value []byte) error {
	b.mu.RLock()
	active := b.newestInflightLocked()
	b.mu.RUnlock()
	if active == nil {
		panic("blockbuffer: PutStringOwnedValue called with no active layer")
	}
	b.putIntoStringOwnedValue(active, key, value)
	return nil
}

// PutKeyParts implements the optional rawdb split-key writer path for the
// synchronous commitment pipeline. It joins both key fragments directly into
// the layer's immutable string key, avoiding an intermediate []byte allocation.
func (b *Buffer) PutKeyParts(first, second, value []byte) error {
	b.mu.RLock()
	active := b.newestInflightLocked()
	b.mu.RUnlock()
	if active == nil {
		panic("blockbuffer: PutKeyParts called with no active layer")
	}
	b.putIntoKeyParts(active, first, second, value)
	return nil
}

// PutKeyPartsOwnedValue is PutKeyParts for a freshly encoded immutable value.
// The caller transfers value ownership to the active layer; ordinary Put and
// PutKeyParts continue to copy caller-owned slices defensively.
func (b *Buffer) PutKeyPartsOwnedValue(first, second, value []byte) error {
	b.mu.RLock()
	active := b.newestInflightLocked()
	b.mu.RUnlock()
	if active == nil {
		panic("blockbuffer: PutKeyPartsOwnedValue called with no active layer")
	}
	b.putIntoKeyPartsOwnedValue(active, first, second, value)
	return nil
}

// PutKeyPartsStringOwnedValue is PutKeyPartsOwnedValue with an immutable string
// suffix. Commitment sibling batches already index final branches by string;
// accepting it directly avoids allocating []byte only to copy it into the
// layer's physical string key.
func (b *Buffer) PutKeyPartsStringOwnedValue(first []byte, second string, value []byte) error {
	b.mu.RLock()
	active := b.newestInflightLocked()
	b.mu.RUnlock()
	if active == nil {
		panic("blockbuffer: PutKeyPartsStringOwnedValue called with no active layer")
	}
	b.putIntoKeyPartsStringOwnedValue(active, first, second, value)
	return nil
}

// PutKeyPartsStringsOwnedValues is the active-layer batch form of
// PutKeyPartsStringOwnedValue. It joins all physical keys into one immutable
// arena and retains each caller-owned value directly.
func (b *Buffer) PutKeyPartsStringsOwnedValues(first []byte, seconds []string, values [][]byte) error {
	return b.PutKeyPartsStringsOwnedValuesWithBatchCount(first, seconds, values, 1)
}

// PutKeyPartsStringsOwnedValuesWithBatchCount is the reservation-aware form
// used by commitment sibling folds. See LayerView's equivalent method.
func (b *Buffer) PutKeyPartsStringsOwnedValuesWithBatchCount(first []byte, seconds []string, values [][]byte, batchCount int) error {
	if len(seconds) != len(values) {
		return errors.New("blockbuffer: key/value batch length mismatch")
	}
	b.mu.RLock()
	active := b.newestInflightLocked()
	b.mu.RUnlock()
	if active == nil {
		panic("blockbuffer: PutKeyPartsStringsOwnedValues called with no active layer")
	}
	b.putIntoKeyPartsStringsOwnedValues(active, first, seconds, values, batchCount)
	return nil
}

// PutKeyPartsStringsOwnedValuesInArenaWithBatchCount is the active-layer form
// of LayerView's combined values+keys arena writer.
func (b *Buffer) PutKeyPartsStringsOwnedValuesInArenaWithBatchCount(first []byte, seconds []string, values [][]byte, arena []byte, batchCount int) error {
	if len(seconds) != len(values) {
		return errors.New("blockbuffer: key/value batch length mismatch")
	}
	b.mu.RLock()
	active := b.newestInflightLocked()
	b.mu.RUnlock()
	if active == nil {
		panic("blockbuffer: PutKeyPartsStringsOwnedValuesInArena called with no active layer")
	}
	return b.putIntoKeyPartsStringsOwnedValuesInArena(active, first, seconds, values, arena, batchCount)
}

// PutStateKVLatest implements rawdb's structured flat-latest writer path for
// the synchronous pipeline.
func (b *Buffer) PutStateKVLatest(prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey, value []byte) error {
	b.mu.RLock()
	active := b.newestInflightLocked()
	b.mu.RUnlock()
	if active == nil {
		panic("blockbuffer: PutStateKVLatest called with no active layer")
	}
	b.putIntoStateKVLatest(active, prefix, accountID, generation, domain, logicalKey, value)
	return nil
}

// PutStateKVLatestOwnedValue is the structured ownership-taking write path.
func (b *Buffer) PutStateKVLatestOwnedValue(prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey, value []byte) error {
	b.mu.RLock()
	active := b.newestInflightLocked()
	b.mu.RUnlock()
	if active == nil {
		panic("blockbuffer: PutStateKVLatestOwnedValue called with no active layer")
	}
	b.putIntoStateKVLatestOwnedValue(active, prefix, accountID, generation, domain, logicalKey, value)
	return nil
}

// Delete tombstones a key in the active layer.
// Panics if no layer is active.
func (b *Buffer) Delete(key []byte) error {
	b.mu.RLock()
	active := b.newestInflightLocked()
	b.mu.RUnlock()
	if active == nil {
		panic("blockbuffer: Delete called with no active layer")
	}
	b.deleteInto(active, key)
	return nil
}

// DeleteRange records one half-open range tombstone in the active layer.
// Point writes made after the range deletion remain visible and are flushed
// after the range tombstone, matching ethdb batch ordering. This is especially
// important for commitment rebuilds, which clear and then repopulate the same
// prefix inside one rewindable block layer.
func (b *Buffer) DeleteRange(start, end []byte) error {
	b.mu.RLock()
	active := b.newestInflightLocked()
	b.mu.RUnlock()
	if active == nil {
		panic("blockbuffer: DeleteRange called with no active layer")
	}
	return b.deleteRangeInto(active, start, end)
}

// DeleteKeyParts is the delete counterpart of PutKeyParts.
func (b *Buffer) DeleteKeyParts(first, second []byte) error {
	b.mu.RLock()
	active := b.newestInflightLocked()
	b.mu.RUnlock()
	if active == nil {
		panic("blockbuffer: DeleteKeyParts called with no active layer")
	}
	b.deleteIntoKeyParts(active, first, second)
	return nil
}

// DeleteStateKVLatest is the structured flat-latest delete counterpart.
func (b *Buffer) DeleteStateKVLatest(prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey []byte) error {
	b.mu.RLock()
	active := b.newestInflightLocked()
	b.mu.RUnlock()
	if active == nil {
		panic("blockbuffer: DeleteStateKVLatest called with no active layer")
	}
	b.deleteIntoStateKVLatest(active, prefix, accountID, generation, domain, logicalKey)
	return nil
}

// Flush drains all committed layers (oldest first) into w and clears them.
// The active layer, if any, is left untouched. Returns the first write
// error encountered. Used by callers that want a nuclear "drain everything"
// (e.g. forced shutdown). Slice 2's stable-flush policy uses FlushUpTo
// instead.
func (b *Buffer) Flush(w ethdb.KeyValueWriter) error {
	// Serialize against FlushUpTo via flushMu. This path keeps b.mu for the
	// whole drain (it's the unused nuclear shutdown helper, not the hot
	// async path), but must not interleave with a concurrent FlushUpTo.
	b.flushMu.Lock()
	defer b.flushMu.Unlock()
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, l := range b.layers {
		if err := flushLayer(l, w); err != nil {
			if b.baseReadCache != nil {
				b.baseReadCache.clear()
			}
			return err
		}
		if b.baseReadCache != nil {
			b.baseReadCache.advanceVersion()
		}
		b.promoteBaseReadCacheLayer(l)
	}
	for i := range b.layers {
		b.layers[i].state = layerDetached
	}
	b.layers = nil
	b.publishReadViewLocked()
	return nil
}

// FlushUpTo flushes every committed layer whose block number is <= cutoff
// to w (oldest-first), then drops those layers from the layered slice.
// Layers above the cutoff stay in the slice and remain rewindable via
// DiscardBlock. The active layer (if any) is untouched.
//
// numberOf maps a block hash to its block number; it is the caller's
// hash-to-number lookup, typically backed by rawdb.ReadBlockNumber on the
// disk store. If numberOf returns (_, false) for a layer's blockHash that
// layer is conservatively kept (not flushed) — typically this means the
// block hasn't been written to disk yet and isn't safe to flush.
//
// Iteration stops at the first layer whose number is > cutoff or whose
// numberOf lookup fails. This relies on the slice-1 invariant that
// committed layers are appended in block order; switchFork's DiscardBlock
// preserves that order.
//
// FlushUpTo is idempotent: a second call with the same cutoff (and no new
// blocks added in between) drops zero layers.
//
// Locking: the disk I/O (numberOf lookups + flushLayer writes) runs WITHOUT
// holding b.mu, so concurrent readers — most importantly the
// LoadDynamicProperties(buffer) scan that every applyBlock runs in its
// prologue — are not blocked by an in-flight flush. This is safe because
// FlushUpTo holds flushMu, which excludes writeFiltered (the only path that can
// finish range-owned batch writes into a committed layer); ordinary
// foreground/worker writes target in-flight layers only. Per-shard read locks
// additionally make every map traversal race-free. We therefore:
//
//  1. briefly RLock to snapshot the layer pointers,
//  2. run numberOf + flushLayer without b.mu on that snapshot,
//  3. briefly Lock to drop the flushed prefix.
//
// flushMu serializes flushers against each other; DiscardBlock (the only
// other path that removes front layers) cannot run concurrently because
// switchFork drains the async-flush queue before rewinding. CommitBlock may
// append new layers at the tail during step 2 — they sit after the flushed
// prefix and are preserved by the count-based drop in step 3.
func (b *Buffer) FlushUpTo(
	cutoff uint64,
	w ethdb.KeyValueWriter,
) error {
	b.flushMu.Lock()
	defer b.flushMu.Unlock()

	// Step 1: snapshot the committed-layer pointers under a brief read lock.
	b.mu.RLock()
	snapshot := make([]*layer, len(b.layers))
	copy(snapshot, b.layers)
	b.mu.RUnlock()
	if len(snapshot) == 0 {
		return nil
	}

	// Step 2: disk I/O without b.mu. flushMu excludes committed-layer batch
	// mutations, and writeLayer's shard RLocks protect each map traversal, so
	// readers can continue resolving unrelated shards concurrently.
	eligible := 0
	for _, l := range snapshot {
		if l.number > cutoff {
			break
		}
		eligible++
	}
	var observe flushGroupObserver
	if b.baseReadCache != nil {
		versionAdvanced := false
		observe = func(group []*layer, merged *flushMergedOps) {
			// New sessions cannot start while flushMu is held. Existing sessions
			// keep their older cache version and captured overlay, so advancing
			// immediately after the first durable batch commit makes every
			// replacement in this flush safely newer than their snapshot.
			if !versionAdvanced {
				b.baseReadCache.advanceVersion()
				versionAdvanced = true
			}
			if merged == nil {
				b.promoteBaseReadCacheLayer(group[0])
				return
			}
			b.promoteBaseReadCacheMerged(merged)
		}
	}
	flushed, err := flushLayersObserved(snapshot[:eligible], w, observe)
	if err != nil {
		// A failed batch may have been applied partially by the backend. Clear the
		// whole cache rather than guessing which base values became durable.
		b.clearBaseReadCache()
		// Drop whatever we already flushed before surfacing the error, so a
		// retry doesn't re-write those layers.
		b.dropFlushedPrefix(flushed)
		return err
	}
	if flushed == 0 {
		return nil
	}
	flushCallsCounter.Inc(1)

	// Step 3: drop the flushed prefix under the write lock.
	b.dropFlushedPrefix(flushed)
	return nil
}

// dropFlushedPrefix removes the first n layers under the write lock. n is the
// count of already-flushed front layers; CommitBlock-appended tail layers are
// preserved. Guarded against a shrunk slice defensively, though the flushMu +
// no-concurrent-DiscardBlock invariants make that impossible.
func (b *Buffer) dropFlushedPrefix(n int) {
	if n <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if n > len(b.layers) {
		n = len(b.layers)
	}
	for i := 0; i < n; i++ {
		b.layers[i].state = layerDetached
	}
	if n == len(b.layers) {
		b.layers = nil
	} else {
		// Keep the suffix immutable and force the next append onto fresh storage,
		// preserving any reader that still owns the pre-flush view.
		b.layers = b.layers[n:len(b.layers):len(b.layers)]
	}
	b.publishReadViewLocked()
}

func flushLayer(l *layer, w ethdb.KeyValueWriter) error {
	if batcher, ok := w.(ethdb.Batcher); ok {
		_, encodedSize, _ := layerWriteStats(l)
		batch := batcher.NewBatchWithSize(pebbleBatchHeaderSize + encodedSize + pebbleBatchRecordSlack)
		defer closeBatch(batch)
		if err := writeLayerSorted(l, batch); err != nil {
			return err
		}
		return batch.Write()
	}
	return writeLayer(l, w)
}

func flushLayers(layers []*layer, w ethdb.KeyValueWriter) (int, error) {
	return flushLayersObserved(layers, w, nil)
}

// flushGroupObserver runs after one bounded group is durably committed and
// before its pooled coalescing map is cleared. It lets cache promotion reuse
// the final operations without rescanning and remerging the source layers.
type flushGroupObserver func(group []*layer, merged *flushMergedOps)

func flushLayersObserved(layers []*layer, w ethdb.KeyValueWriter, observe flushGroupObserver) (int, error) {
	if len(layers) == 0 {
		return 0, nil
	}
	batcher, ok := w.(ethdb.Batcher)
	if !ok {
		flushed := 0
		for i, l := range layers {
			if err := writeLayer(l, w); err != nil {
				return flushed, err
			}
			if observe != nil {
				observe(layers[i:i+1], nil)
			}
			flushed++
		}
		return flushed, nil
	}

	sizes := make([]layerBatchSize, len(layers))
	for i, l := range layers {
		valueSize, encodedSize, ops := layerWriteStats(l)
		sizes[i] = layerBatchSize{value: valueSize, encoded: encodedSize, ops: ops}
	}

	flushed := 0
	for start := 0; start < len(layers); {
		group := selectFlushGroup(layers, sizes, start, flushGroupLimits{
			batchValue:   maxFlushBatchValueSize,
			batchEncoded: maxFlushBatchEncodedSize,
			mergeValue:   maxFlushMergeValueSize,
			mergeEncoded: maxFlushMergeEncodedSize,
		})
		end := group.end
		merged := group.merged

		// Pebble deliberately drops buffers larger than batchMaxRetainedSize on
		// Reset. Reusing one large batch therefore made every group after the
		// first grow geometrically from an empty buffer despite our size
		// calculation. Allocate each bounded group at its FINAL encoded size plus
		// the one-record scratch allowance so every batch performs one final
		// allocation and no grow/copy cycle.
		batch := batcher.NewBatchWithSize(group.outputEncoded + pebbleBatchRecordSlack)
		var writeErr error
		if merged == nil {
			writeErr = writeLayerSorted(layers[start], batch)
		} else {
			writeErr = writeMergedLayerOpsSorted(merged.ops, batch)
		}
		if writeErr != nil {
			closeBatch(batch)
			returnFlushMergedOps(merged)
			return flushed, writeErr
		}
		if err := batch.Write(); err != nil {
			closeBatch(batch)
			returnFlushMergedOps(merged)
			return flushed, err
		}
		if flushPhysicalFamilySampleSequence.Add(1)%flushPhysicalFamilySampleInterval == 1 {
			var families flushPhysicalFamilyStats
			if merged == nil {
				families.addLayer(layers[start])
			} else {
				families.addMerged(merged.ops)
			}
			families.publish()
			flushFamilySampledGroupsCounter.Inc(1)
		}
		outputOps := group.inputOps
		if merged != nil {
			outputOps = len(merged.ops)
		}
		flushInputOpsCounter.Inc(int64(group.inputOps))
		flushOutputOpsCounter.Inc(int64(outputOps))
		flushInputBytesCounter.Inc(int64(group.inputValue))
		flushOutputBytesCounter.Inc(int64(group.outputValue))
		flushLayersCounter.Inc(int64(end - start))
		flushGroupsCounter.Inc(1)
		if group.extendedLayers > 0 {
			flushExtendedGroupsCounter.Inc(1)
			flushExtendedLayersCounter.Inc(int64(group.extendedLayers))
		}
		closeBatch(batch)
		if observe != nil {
			observe(layers[start:end], merged)
		}
		returnFlushMergedOps(merged)
		flushed += end - start
		start = end
	}
	return flushed, nil
}

func writeLayer(l *layer, w ethdb.KeyValueWriter) error {
	if err := writeLayerRangeDeletes(l, w); err != nil {
		return err
	}
	stringWriter, writesString := w.(stringKeyWriter)
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.RLock()
		for k, v := range s.writes {
			if durable, ok := s.durableWrites[k]; ok && bytes.Equal(durable, v) {
				continue
			}
			var err error
			if writesString {
				err = stringWriter.PutString(k, v)
			} else {
				err = w.Put([]byte(k), v)
			}
			if err != nil {
				s.mu.RUnlock()
				return err
			}
		}
		for k := range s.deletes {
			var err error
			if writesString {
				err = stringWriter.DeleteString(k)
			} else {
				err = w.Delete([]byte(k))
			}
			if err != nil {
				s.mu.RUnlock()
				return err
			}
		}
		s.mu.RUnlock()
	}
	return nil
}

func writeLayerRangeDeletes(l *layer, w ethdb.KeyValueWriter) error {
	ranges := l.loadRangeDeletes()
	if len(ranges) == 0 {
		return nil
	}
	deleter, ok := w.(ethdb.KeyValueRangeDeleter)
	if !ok {
		return errors.New("blockbuffer: flush target does not support range deletion")
	}
	for _, r := range ranges {
		if err := deleter.DeleteRange([]byte(r.start), []byte(r.end)); err != nil {
			return err
		}
	}
	return nil
}

// Sorting unique final user keys lets Pebble's memtable Inserter reuse the
// preceding skiplist splice instead of searching from the top for every entry
// in map-random order. Values remain in the immutable source layer/map; keeping
// only strings here makes sorting cheaper and bounds retained scratch to 4 MiB.
const maxPooledFlushWriteKeys = 262144

var flushWriteKeysPool = sync.Pool{
	New: func() any {
		keys := make([]string, 0, 4096)
		return &keys
	},
}

func borrowFlushWriteKeys(size int) *[]string {
	keys := flushWriteKeysPool.Get().(*[]string)
	if cap(*keys) < size {
		*keys = make([]string, 0, size)
	} else {
		*keys = (*keys)[:0]
	}
	return keys
}

func returnFlushWriteKeys(keys *[]string) {
	if keys == nil {
		return
	}
	clear(*keys)
	if cap(*keys) > maxPooledFlushWriteKeys {
		return
	}
	*keys = (*keys)[:0]
	flushWriteKeysPool.Put(keys)
}

func writeLayerSorted(l *layer, w ethdb.KeyValueWriter) error {
	// Keep every source map read-locked through gathering, sorting and batch
	// construction. FlushUpTo's flushMu already excludes the only committed-
	// layer writer, but the locks preserve layer's standalone concurrency
	// contract and make this helper safe for direct callers too.
	for i := range l.shards {
		l.shards[i].mu.RLock()
	}
	defer func() {
		for i := len(l.shards) - 1; i >= 0; i-- {
			l.shards[i].mu.RUnlock()
		}
	}()
	if err := writeLayerRangeDeletes(l, w); err != nil {
		return err
	}
	count := 0
	for i := range l.shards {
		s := &l.shards[i]
		for k, v := range s.writes {
			if durable, ok := s.durableWrites[k]; ok && bytes.Equal(durable, v) {
				continue
			}
			count++
		}
		count += len(s.deletes)
	}
	keysPtr := borrowFlushWriteKeys(count)
	defer returnFlushWriteKeys(keysPtr)
	for i := range l.shards {
		s := &l.shards[i]
		for key := range s.writes {
			if durable, ok := s.durableWrites[key]; ok && bytes.Equal(durable, s.writes[key]) {
				continue
			}
			*keysPtr = append(*keysPtr, key)
		}
		for key := range s.deletes {
			*keysPtr = append(*keysPtr, key)
		}
	}
	sort.Strings(*keysPtr)
	stringWriter, writesString := w.(stringKeyWriter)
	for _, key := range *keysPtr {
		s := &l.shards[layerShardIndexString(key)]
		value, put := s.writes[key]
		var err error
		switch {
		case put && writesString:
			err = stringWriter.PutString(key, value)
		case put:
			err = w.Put([]byte(key), value)
		case writesString:
			err = stringWriter.DeleteString(key)
		default:
			err = w.Delete([]byte(key))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func layerWriteSize(l *layer) int {
	size, _, _ := layerWriteStats(l)
	return size
}

const (
	pebbleBatchHeaderSize = 12
	// Pebble v1.1.x's deferred Set/Delete builders temporarily reserve the
	// maximum varint width before shrinking each record to its actual encoding.
	// The first record's init path uses binary.MaxVarintLen64 for both key/value
	// lengths, so an exact final encoded-size hint can still grow on the last
	// record and copy the entire batch. This small one-record scratch allowance
	// prevents that geometric grow without materially overallocating the batch.
	pebbleBatchRecordSlack = 2 * binary.MaxVarintLen64
)

type layerBatchSize struct {
	value   int
	encoded int
	ops     int
}

type flushGroupLimits struct {
	batchValue   int
	batchEncoded int
	mergeValue   int
	mergeEncoded int
}

// flushGroupPlan describes one durable Pebble batch. input* measures all
// source-layer operations consumed by the group; output* measures the final
// last-writer-wins rows that enter Pebble. extendedLayers is the number of
// layers admitted after the legacy final-batch-sized source window filled.
type flushGroupPlan struct {
	end            int
	inputValue     int
	inputEncoded   int
	inputOps       int
	outputValue    int
	outputEncoded  int
	extendedLayers int
	merged         *flushMergedOps
}

// selectFlushGroup first builds the conventional source window bounded by the
// final Pebble batch size. It then probes subsequent layers against the live
// coalesced map and admits them only when both:
//
//   - total source bytes remain inside the larger aggregation bound; and
//   - the resulting final Pebble batch remains inside the original bound.
//
// The second condition is evaluated before mutating the map, so an unrelated
// append-only layer cannot transiently grow the merge map or final WAL batch
// past the established limit. A one-layer result returns nil merged to retain
// the allocation-free direct path. The caller owns and must return merged.
func selectFlushGroup(layers []*layer, sizes []layerBatchSize, start int, limits flushGroupLimits) flushGroupPlan {
	plan := flushGroupPlan{
		end:           start,
		inputEncoded:  pebbleBatchHeaderSize,
		outputEncoded: pebbleBatchHeaderSize,
	}
	// A range tombstone must stay ordered before the point writes in its own
	// layer and must not be collapsed into the point-only merge map. Flush that
	// layer alone; adjacent layers resume normal coalescing in the next group.
	if layers[start].hasRangeDeletes() {
		next := sizes[start]
		plan.end = start + 1
		plan.inputValue = next.value
		plan.inputEncoded += next.encoded
		plan.inputOps = next.ops
		plan.outputValue = next.value
		plan.outputEncoded = pebbleBatchHeaderSize + next.encoded
		return plan
	}
	for plan.end < len(layers) {
		if layers[plan.end].hasRangeDeletes() {
			break
		}
		next := sizes[plan.end]
		if plan.end > start && (plan.inputValue+next.value > limits.batchValue ||
			plan.inputEncoded+next.encoded > limits.batchEncoded) {
			break
		}
		plan.inputValue += next.value
		plan.inputEncoded += next.encoded
		plan.inputOps += next.ops
		plan.end++
		if plan.inputValue >= limits.batchValue || plan.inputEncoded >= limits.batchEncoded {
			break
		}
	}
	baseEnd := plan.end
	canProbeExtension := plan.end < len(layers) && !layers[plan.end].hasRangeDeletes() &&
		plan.inputValue+sizes[plan.end].value <= limits.mergeValue &&
		plan.inputEncoded+sizes[plan.end].encoded <= limits.mergeEncoded
	if plan.end-start > 1 || canProbeExtension {
		plan.merged = borrowFlushMergedOps()
		mergeLayers(layers[start:plan.end], plan.merged)
		outputValue, outputRecords := mergedLayerWriteStats(plan.merged.ops)
		plan.outputValue = outputValue
		plan.outputEncoded = pebbleBatchHeaderSize + outputRecords

		for plan.end < len(layers) {
			next := sizes[plan.end]
			if plan.inputValue+next.value > limits.mergeValue ||
				plan.inputEncoded+next.encoded > limits.mergeEncoded {
				break
			}
			valueDelta, encodedDelta := mergedLayerSizeDelta(layers[plan.end], plan.merged.ops)
			if plan.outputValue+valueDelta > limits.batchValue ||
				plan.outputEncoded+encodedDelta > limits.batchEncoded {
				break
			}
			mergeLayers(layers[plan.end:plan.end+1], plan.merged)
			plan.inputValue += next.value
			plan.inputEncoded += next.encoded
			plan.inputOps += next.ops
			plan.outputValue += valueDelta
			plan.outputEncoded += encodedDelta
			plan.end++
		}
	}
	plan.extendedLayers = plan.end - baseEnd
	if plan.end-start == 1 {
		returnFlushMergedOps(plan.merged)
		plan.merged = nil
		plan.outputValue = plan.inputValue
		plan.outputEncoded = plan.inputEncoded
	}
	return plan
}

// mergedLayerOp is the final operation for one physical key across a bounded
// group of committed layers. Values and keys borrow the immutable layer maps;
// flushLayers keeps every source layer alive until the Pebble batch has copied
// the operation, so no additional key/value ownership is needed here.
type mergedLayerOp struct {
	value  []byte
	delete bool
	// shard is stable because layer maps and the durable-base cache share the
	// same selector. Cache promotion reuses it after the Pebble write instead
	// of hashing every merged key again.
	shard uint8
}

// mergedPromotion carries a map entry into its cache-shard bucket. Keeping the
// operation beside the key avoids hashing and probing the large merged map a
// second time while the grouped promotion pass holds each cache lock.
type mergedPromotion struct {
	key string
	op  mergedLayerOp
}

// flushMergedOpsPool reuses the hash table needed to collapse consecutive
// solidified layers. A flush group has a bounded merge window, so a modest
// entry cap covers normal groups while preventing an exceptional set of tiny
// keys from pinning an oversized map for the process lifetime.
const maxPooledFlushMergedOps = 32768

type flushMergedOps struct {
	ops        map[string]mergedLayerOp
	promotions [layerShardCount][]mergedPromotion
	highWater  int
}

type flushPhysicalFamilyTotal struct {
	ops   int64
	bytes int64
}

type flushPhysicalFamilyStats [rawdb.PhysicalKeyFamilyCount]flushPhysicalFamilyTotal

func (s *flushPhysicalFamilyStats) add(key string, value []byte, deleted bool) {
	family := rawdb.ClassifyPhysicalKeyString(key)
	total := &s[family]
	total.ops++
	total.bytes += int64(len(key))
	if !deleted {
		total.bytes += int64(len(value))
	}
}

func (s *flushPhysicalFamilyStats) addLayer(layer *layer) {
	if layer == nil {
		return
	}
	for i := range layer.shards {
		shard := &layer.shards[i]
		shard.mu.RLock()
		for key, value := range shard.writes {
			if durable, ok := shard.durableWrites[key]; ok && bytes.Equal(durable, value) {
				continue
			}
			s.add(key, value, false)
		}
		for key := range shard.deletes {
			s.add(key, nil, true)
		}
		shard.mu.RUnlock()
	}
}

func (s *flushPhysicalFamilyStats) addMerged(ops map[string]mergedLayerOp) {
	for key, op := range ops {
		s.add(key, op.value, op.delete)
	}
}

func (s *flushPhysicalFamilyStats) publish() {
	for family, total := range s {
		if total.ops == 0 {
			continue
		}
		flushFamilyOpsCounters[family].Inc(total.ops)
		flushFamilyBytesCounters[family].Inc(total.bytes)
	}
}

var flushMergedOpsPool = sync.Pool{
	New: func() any {
		return &flushMergedOps{ops: make(map[string]mergedLayerOp)}
	},
}

func borrowFlushMergedOps() *flushMergedOps {
	merged := flushMergedOpsPool.Get().(*flushMergedOps)
	merged.highWater = 0
	return merged
}

func returnFlushMergedOps(merged *flushMergedOps) {
	if merged == nil {
		return
	}
	clear(merged.ops)
	for i := range merged.promotions {
		clear(merged.promotions[i])
		merged.promotions[i] = merged.promotions[i][:0]
	}
	if merged.highWater > maxPooledFlushMergedOps {
		return
	}
	flushMergedOpsPool.Put(merged)
}

// mergeLayers records the newest operation for each physical key. Layers are
// visited oldest to newest, exactly matching the pre-merge batch order, so a
// later Put/Delete replaces every earlier version of the same key.
func mergeLayers(layers []*layer, merged *flushMergedOps) {
	for _, l := range layers {
		if l == nil {
			continue
		}
		for i := range l.shards {
			s := &l.shards[i]
			s.mu.RLock()
			for k, v := range s.writes {
				if durable, ok := s.durableWrites[k]; ok && bytes.Equal(durable, v) {
					delete(merged.ops, k)
					continue
				}
				merged.ops[k] = mergedLayerOp{value: v, shard: uint8(i)}
			}
			for k := range s.deletes {
				merged.ops[k] = mergedLayerOp{delete: true, shard: uint8(i)}
			}
			s.mu.RUnlock()
		}
	}
	if len(merged.ops) > merged.highWater {
		merged.highWater = len(merged.ops)
	}
}

func mergedLayerWriteStats(ops map[string]mergedLayerOp) (valueSize, encodedSize int) {
	for k, op := range ops {
		value, encoded := mergedLayerOpWriteStats(k, op)
		valueSize += value
		encodedSize += encoded
	}
	return valueSize, encodedSize
}

func mergedLayerOpWriteStats(key string, op mergedLayerOp) (valueSize, encodedSize int) {
	if op.delete {
		return len(key), 1 + uvarintSize(len(key)) + len(key)
	}
	return len(key) + len(op.value),
		1 + uvarintSize(len(key)) + len(key) + uvarintSize(len(op.value)) + len(op.value)
}

// mergedLayerSizeDelta calculates how one layer would change the final
// coalesced representation without mutating it. A layer's write and delete
// maps are disjoint, so every key has exactly one projected transition. This
// lets selectFlushGroup reject a unique/append-only layer before it can enlarge
// the live merge map or the final Pebble batch beyond its established cap.
func mergedLayerSizeDelta(l *layer, existing map[string]mergedLayerOp) (valueDelta, encodedDelta int) {
	if l == nil {
		return 0, 0
	}
	addTransition := func(key string, next mergedLayerOp, keep bool) {
		if previous, ok := existing[key]; ok {
			value, encoded := mergedLayerOpWriteStats(key, previous)
			valueDelta -= value
			encodedDelta -= encoded
		}
		if keep {
			value, encoded := mergedLayerOpWriteStats(key, next)
			valueDelta += value
			encodedDelta += encoded
		}
	}
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.RLock()
		for key, value := range s.writes {
			if durable, ok := s.durableWrites[key]; ok && bytes.Equal(durable, value) {
				addTransition(key, mergedLayerOp{}, false)
				continue
			}
			addTransition(key, mergedLayerOp{value: value, shard: uint8(i)}, true)
		}
		for key := range s.deletes {
			addTransition(key, mergedLayerOp{delete: true, shard: uint8(i)}, true)
		}
		s.mu.RUnlock()
	}
	return valueDelta, encodedDelta
}

func writeMergedLayerOps(ops map[string]mergedLayerOp, w ethdb.KeyValueWriter) error {
	stringWriter, writesString := w.(stringKeyWriter)
	for k, op := range ops {
		var err error
		switch {
		case op.delete && writesString:
			err = stringWriter.DeleteString(k)
		case op.delete:
			err = w.Delete([]byte(k))
		case writesString:
			err = stringWriter.PutString(k, op.value)
		default:
			err = w.Put([]byte(k), op.value)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func writeMergedLayerOpsSorted(ops map[string]mergedLayerOp, w ethdb.KeyValueWriter) error {
	keysPtr := borrowFlushWriteKeys(len(ops))
	defer returnFlushWriteKeys(keysPtr)
	for key := range ops {
		*keysPtr = append(*keysPtr, key)
	}
	sort.Strings(*keysPtr)
	stringWriter, writesString := w.(stringKeyWriter)
	for _, key := range *keysPtr {
		op := ops[key]
		var err error
		switch {
		case op.delete && writesString:
			err = stringWriter.DeleteString(key)
		case op.delete:
			err = w.Delete([]byte(key))
		case writesString:
			err = stringWriter.PutString(key, op.value)
		default:
			err = w.Put([]byte(key), op.value)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// layerWriteStats returns both ethdb's logical ValueSize and the encoded
// Pebble batch record size. Pebble records use one kind byte followed by
// uvarint-framed keys and values; deletes omit the value. Supplying this exact
// encoded size plus Pebble's one-record temporary varint slack up front avoids
// Batch.grow copying a megabyte-scale flush batch. The 12-byte batch header and
// scratch slack are added once by the caller.
func layerWriteStats(l *layer) (valueSize, encodedSize, ops int) {
	if l == nil {
		return 0, 0, 0
	}
	for _, r := range l.loadRangeDeletes() {
		ops++
		valueSize += len(r.start) + len(r.end)
		encodedSize += 1 + uvarintSize(len(r.start)) + len(r.start) + uvarintSize(len(r.end)) + len(r.end)
	}
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.RLock()
		for k, v := range s.writes {
			if durable, ok := s.durableWrites[k]; ok && bytes.Equal(durable, v) {
				continue
			}
			ops++
			valueSize += len(k) + len(v)
			encodedSize += 1 + uvarintSize(len(k)) + len(k) + uvarintSize(len(v)) + len(v)
		}
		for k := range s.deletes {
			ops++
			valueSize += len(k)
			encodedSize += 1 + uvarintSize(len(k)) + len(k)
		}
		s.mu.RUnlock()
	}
	return valueSize, encodedSize, ops
}

func uvarintSize(v int) int {
	size := 1
	for v >= 1<<7 {
		v >>= 7
		size++
	}
	return size
}

func closeBatch(batch ethdb.Batch) {
	if closer, ok := batch.(interface{ Close() }); ok {
		closer.Close()
	}
}

// NewIterator returns an iterator over the buffer view: every key whose bytes
// start with prefix and are >= start, ordered lexicographically. Overlay
// semantics match Get — the active layer wins over committed layers (newest
// first) which in turn override the base reader; tombstones mask base keys.
//
// Implementation snapshots the relevant entries at construction time so the
// returned iterator does not have to hold any locks while iterating. This is
// the right shape for prefix-bounded scans of small key sets (DP map at ~133
// keys); a streaming/merging iterator would only matter for unbounded scans.
//
// Implements ethdb.Iteratee so a *Buffer can be substituted anywhere a disk
// store is expected — most importantly, state.LoadDynamicProperties can
// recognize it and replace its 133 point Gets per applyBlock with one scan.
func (b *Buffer) NewIterator(prefix, start []byte) ethdb.Iterator {
	// Step 1: collect the overlay newest-first (in-flight newest→oldest, then
	// committed newest→oldest) from one immutable topology view. Step 2-4 (base
	// merge + sort) are shared with LayerView via finishIterator.
	view := b.loadReadView()
	overlay := newOverlayState()
	for i := len(view.inflight) - 1; i >= 0; i-- {
		overlay.walk(view.inflight[i], prefix, start)
	}
	for i := len(view.layers) - 1; i >= 0; i-- {
		overlay.walk(view.layers[i], prefix, start)
	}
	return b.finishIterator(overlay, prefix, start)
}

// SeekPrefix is the one-row counterpart of NewIterator. It preserves the same
// newest-layer/tombstone/base semantics without consuming the complete durable
// prefix before the caller can stop.
func (b *Buffer) SeekPrefix(prefix, start []byte) (key, value []byte, ok bool, err error) {
	view := b.loadReadView()
	overlay := newOverlayState()
	for i := len(view.inflight) - 1; i >= 0; i-- {
		overlay.walk(view.inflight[i], prefix, start)
	}
	for i := len(view.layers) - 1; i >= 0; i-- {
		overlay.walk(view.layers[i], prefix, start)
	}
	return seekPrefixWithBase(b.base, overlay, prefix, start)
}

// NewStateKVLatestIterator is rawdb's optional structured iterator path. It
// preserves the same physical-key snapshot semantics as NewIterator while
// narrowing live-layer work to one coarse account bucket. Exact owner,
// generation, domain, and logical-prefix filtering is still applied below.
func (b *Buffer) NewStateKVLatestIterator(schemaPrefix []byte, accountID common.AccountID, physicalPrefix []byte) ethdb.Iterator {
	view := b.loadReadView()
	overlay := newOverlayState()
	schema := string(schemaPrefix)
	physical := unsafe.String(unsafe.SliceData(physicalPrefix), len(physicalPrefix))
	for i := len(view.inflight) - 1; i >= 0; i-- {
		overlay.walkPrefixBucket(view.inflight[i], schema, accountID[0], physical)
	}
	for i := len(view.layers) - 1; i >= 0; i-- {
		overlay.walkPrefixBucket(view.layers[i], schema, accountID[0], physical)
	}
	return b.finishIterator(overlay, physicalPrefix, nil)
}

// overlayOp is one resolved overlay entry: a value write, or a tombstone.
type overlayOp struct {
	value   []byte
	deleted bool
}

// overlayState resolves a newest-first walk of layers into a single overlay map
// (the first time a key is seen wins, so newer layers mask older ones). Shared
// by Buffer.NewIterator and LayerView.NewIterator.
type overlayState struct {
	m      map[string]overlayOp
	ranges []layerRangeDelete
}

func newOverlayState() *overlayState { return &overlayState{m: make(map[string]overlayOp)} }

// walk folds layer l into the overlay, keeping only keys in [prefix+start, …)
// that have the given prefix. The caller's immutable read view keeps the layer
// alive; this takes each layer shard's lock for map iteration so it is race-free
// against a concurrent foreground/worker write to l.
func (o *overlayState) walk(l *layer, prefix, start []byte) {
	if l == nil {
		return
	}
	// prefix/start remain caller-owned and are used only during this synchronous
	// walk. Each shard lazily builds one sorted union of its write/tombstone keys;
	// later prefix scans binary-search that index instead of revisiting every
	// unrelated live key. This is especially important for archive reads, which
	// issue many exact-prefix scans over the same immutable layer snapshot.
	var pfx, relativeStart string
	if len(prefix) != 0 {
		pfx = unsafe.String(unsafe.SliceData(prefix), len(prefix))
	}
	if len(start) != 0 {
		relativeStart = unsafe.String(unsafe.SliceData(start), len(start))
	}
	var lower string
	lowerReady := false
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.RLock()
		if len(s.writes) == 0 && len(s.deletes) == 0 {
			s.mu.RUnlock()
			continue
		}
		if !lowerReady {
			lower = relativeStart
			if pfx != "" {
				lower = pfx
				if relativeStart != "" {
					lower += relativeStart
				}
			}
			lowerReady = true
		}
		index := s.prefixBucketIndex
		if index != nil && index.iteratorKeysBuilt {
			o.walkSortedKeysLocked(s, index.iteratorKeys, pfx, lower)
			s.mu.RUnlock()
			continue
		}
		s.mu.RUnlock()

		// Upgrade only for the one-time index build. Do not wait behind a flush
		// holding a shard read lock across disk I/O: in that uncommon case the
		// current call falls back to the old range-filtered map walk, preserving
		// NewIterator's lock-free-flush guarantee.
		if !s.mu.TryLock() {
			s.mu.RLock()
			o.walkMapsLocked(s, pfx, relativeStart)
			s.mu.RUnlock()
			continue
		}
		index = s.prefixBucketIndex
		if index == nil {
			index = newLayerPrefixBucketIndex("")
			s.prefixBucketIndex = index
		}
		index.ensureIteratorKeys(s.writes, s.deletes)
		o.walkSortedKeysLocked(s, index.iteratorKeys, pfx, lower)
		s.mu.Unlock()
	}
	o.addLayerRanges(l)
}

func (o *overlayState) walkSortedKeysLocked(s *layerShard, keys []string, prefix, lower string) {
	at := sort.Search(len(keys), func(i int) bool { return keys[i] >= lower })
	for ; at < len(keys); at++ {
		key := keys[at]
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			break
		}
		o.addShardKeyLocked(s, key)
	}
}

func (o *overlayState) walkMapsLocked(s *layerShard, prefix, relativeStart string) {
	matches := func(key string) bool {
		if prefix == "" {
			return key >= relativeStart
		}
		return strings.HasPrefix(key, prefix) && key[len(prefix):] >= relativeStart
	}
	for key := range s.writes {
		if matches(key) {
			o.addShardKeyLocked(s, key)
		}
	}
	for key := range s.deletes {
		if matches(key) {
			o.addShardKeyLocked(s, key)
		}
	}
}

func (o *overlayState) addShardKeyLocked(s *layerShard, key string) {
	if _, resolved := o.m[key]; resolved || o.rangeDeletesString(key) {
		return
	}
	if value, exists := s.writes[key]; exists {
		o.m[key] = overlayOp{value: append([]byte(nil), value...)}
		return
	}
	if _, deleted := s.deletes[key]; deleted {
		o.m[key] = overlayOp{deleted: true}
	}
}

func (o *overlayState) addLayerRanges(l *layer) {
	if l == nil {
		return
	}
	// Layers are folded newest-first. Point operations from this layer have
	// already entered o.m and therefore remain authoritative; the appended
	// ranges mask only older layers and the durable base.
	o.ranges = append(o.ranges, l.loadRangeDeletes()...)
}

func (o *overlayState) rangeDeletesString(key string) bool {
	for _, r := range o.ranges {
		if r.containsString(key) {
			return true
		}
	}
	return false
}

// walkPrefixBucket is the account-scoped counterpart of walk. Each shard's
// index is initialized under the same lock that guards its mutation maps, so a
// concurrent writer either appears in the initial build or appends itself
// before publication. If a different schema ever asks to reuse the single
// per-shard index, fall back to the full walk for correctness.
func (o *overlayState) walkPrefixBucket(l *layer, schema string, bucket byte, physical string) {
	if l == nil {
		return
	}
	for shardNum := range l.shards {
		s := &l.shards[shardNum]
		s.mu.Lock()
		if s.prefixBucketIndex == nil {
			s.prefixBucketIndex = newLayerPrefixBucketIndex(schema)
		}
		index := s.prefixBucketIndex
		if index.prefix == "" {
			// A generic iterator may have created the shared key-index holder
			// before the structured account iterator initializes its schema.
			index.prefix = schema
		}
		if index.prefix != schema {
			s.mu.Unlock()
			// All shards in a layer are normally initialized by the same
			// structured call. A second schema is not expected in production;
			// preserve generic iterator semantics with one full-layer fallback.
			o.walk(l, []byte(physical), nil)
			return
		}
		index.ensureBucket(bucket, s.writes, s.deletes)
		for _, key := range index.buckets[bucket] {
			if !strings.HasPrefix(key, physical) {
				continue
			}
			if _, resolved := o.m[key]; resolved || o.rangeDeletesString(key) {
				continue
			}
			if value, exists := s.writes[key]; exists {
				o.m[key] = overlayOp{value: append([]byte(nil), value...)}
				continue
			}
			if _, deleted := s.deletes[key]; deleted {
				o.m[key] = overlayOp{deleted: true}
			}
		}
		s.mu.Unlock()
	}
	o.addLayerRanges(l)
}

// finishIterator merges the resolved overlay with the base keys in the
// [prefix, prefix+start) window and returns a snapshot iterator. Runs lock-free
// (the base store has its own concurrency control; overlay is already a private
// copy). Shared by Buffer.NewIterator and LayerView.NewIterator.
func (b *Buffer) finishIterator(overlay *overlayState, prefix, start []byte) ethdb.Iterator {
	return finishIteratorWithBase(b.base, overlay, prefix, start)
}

func finishIteratorWithBase(base ethdb.KeyValueReader, overlay *overlayState, prefix, start []byte) ethdb.Iterator {
	entries := make([]bufferIteratorEntry, 0, len(overlay.m))
	for key, op := range overlay.m {
		entries = append(entries, bufferIteratorEntry{key: key, value: op.value, deleted: op.deleted})
	}
	slices.SortFunc(entries, func(a, b bufferIteratorEntry) int {
		return strings.Compare(a.key, b.key)
	})

	var baseIt ethdb.Iterator
	if base != nil {
		if iter, ok := base.(ethdb.Iteratee); ok {
			baseIt = iter.NewIterator(prefix, start)
		}
		// If the base does not implement Iteratee, only the overlay is
		// surfaced. This matches the contract that NewIterator on a reader
		// with no iteration support cannot synthesize one.
	}
	// Keep the base iterator lazy. Full latest-domain scans can cover tens of
	// gigabytes, and materialising every base key/value here made a single
	// iterator exceed the service's memory limit before its caller could consume
	// the first row. The overlay is already a private snapshot and is normally
	// tiny, so only it is sorted eagerly; bufferIterator merges both ordered legs
	// on demand.
	return &bufferIterator{base: baseIt, overlay: entries, ranges: append([]layerRangeDelete(nil), overlay.ranges...)}
}

func seekPrefixWithBase(base ethdb.KeyValueReader, overlay *overlayState, prefix, start []byte) (key, value []byte, ok bool, err error) {
	var overlayKey string
	var overlayValue []byte
	overlayOK := false
	for candidate, op := range overlay.m {
		if op.deleted || (overlayOK && candidate >= overlayKey) {
			continue
		}
		overlayKey = candidate
		overlayValue = op.value
		overlayOK = true
	}

	var baseKey, baseValue []byte
	baseOK := false
	if base != nil {
		if iter, iterable := base.(ethdb.Iteratee); iterable {
			it := iter.NewIterator(prefix, start)
			for it.Next() {
				candidate := it.Key()
				candidateString := unsafe.String(unsafe.SliceData(candidate), len(candidate))
				if overlay.rangeDeletesString(candidateString) {
					continue
				}
				if overlayOK && overlayKey < candidateString {
					break
				}
				if op, masked := overlay.m[candidateString]; masked {
					if op.deleted {
						continue
					}
					baseKey = append([]byte(nil), candidate...)
					baseValue = append([]byte(nil), op.value...)
					baseOK = true
					break
				}
				baseKey = append([]byte(nil), candidate...)
				baseValue = append([]byte(nil), it.Value()...)
				baseOK = true
				break
			}
			err = it.Error()
			it.Release()
			if err != nil {
				return nil, nil, false, err
			}
		}
	}

	if overlayOK && (!baseOK || overlayKey < string(baseKey)) {
		return []byte(overlayKey), append([]byte(nil), overlayValue...), true, nil
	}
	if baseOK {
		return baseKey, baseValue, true, nil
	}
	return nil, nil, false, nil
}

// bufferIterator is a snapshot iterator returned by Buffer.NewIterator. It
// holds no blockbuffer locks and lazily merges the base iterator with the
// captured overlay. Key/Value remain valid until the next Next call; callers
// must not mutate them (mirrors the ethdb.Iterator contract).
type bufferIterator struct {
	base        ethdb.Iterator
	overlay     []bufferIteratorEntry
	ranges      []layerRangeDelete
	overlayIdx  int
	baseReady   bool
	advanceBase bool
	key, value  []byte
	err         error
	released    bool
}

func (it *bufferIterator) baseRangeDeleted() bool {
	if !it.baseReady || it.base == nil {
		return false
	}
	key := it.base.Key()
	for _, r := range it.ranges {
		if r.containsBytes(key) {
			return true
		}
	}
	return false
}

type bufferIteratorEntry struct {
	key     string
	value   []byte
	deleted bool
}

func (it *bufferIterator) Next() bool {
	if it.err != nil || it.released {
		return false
	}
	it.key, it.value = nil, nil
	if it.advanceBase {
		it.baseReady = false
		it.advanceBase = false
	}

	for {
		if !it.baseReady && it.base != nil {
			if it.base.Next() {
				it.baseReady = true
			} else {
				it.err = it.base.Error()
				it.base.Release()
				it.base = nil
				if it.err != nil {
					return false
				}
			}
		}
		if it.baseRangeDeleted() {
			it.baseReady = false
			continue
		}

		overlayReady := it.overlayIdx < len(it.overlay)
		if !it.baseReady && !overlayReady {
			return false
		}
		if !it.baseReady {
			entry := &it.overlay[it.overlayIdx]
			it.overlayIdx++
			if entry.deleted {
				continue
			}
			it.key = unsafe.Slice(unsafe.StringData(entry.key), len(entry.key))
			it.value = entry.value
			return true
		}
		if !overlayReady {
			it.key, it.value = it.base.Key(), it.base.Value()
			it.advanceBase = true
			return true
		}

		entry := &it.overlay[it.overlayIdx]
		baseKey := it.base.Key()
		baseKeyString := unsafe.String(unsafe.SliceData(baseKey), len(baseKey))
		switch strings.Compare(baseKeyString, entry.key) {
		case -1:
			it.key, it.value = baseKey, it.base.Value()
			it.advanceBase = true
			return true
		case 0:
			// The overlay masks the matching base row whether it is a write or
			// a tombstone. Advancing the base on the next loop/call preserves
			// the returned base buffers until Next is called again.
			it.overlayIdx++
			if entry.deleted {
				it.baseReady = false
				continue
			}
			it.key = unsafe.Slice(unsafe.StringData(entry.key), len(entry.key))
			it.value = entry.value
			it.advanceBase = true
			return true
		default:
			it.overlayIdx++
			if entry.deleted {
				continue
			}
			it.key = unsafe.Slice(unsafe.StringData(entry.key), len(entry.key))
			it.value = entry.value
			return true
		}
	}
}

func (it *bufferIterator) Error() error { return it.err }

func (it *bufferIterator) Key() []byte {
	return it.key
}

func (it *bufferIterator) Value() []byte {
	return it.value
}

func (it *bufferIterator) Release() {
	if it.released {
		return
	}
	it.released = true
	if it.base != nil {
		it.base.Release()
	}
	it.base = nil
	it.overlay = nil
	it.ranges = nil
	it.key, it.value = nil, nil
	it.baseReady = false
}
