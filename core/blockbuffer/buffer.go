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
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

// ErrNotFound is returned by Get/Has when the key is tombstoned in a layer.
// It is also the sentinel returned by the underlying base reader for
// missing keys (memorydb / pebble both return non-nil errors for misses;
// callers normally check err != nil rather than identity).
var ErrNotFound = errors.New("blockbuffer: not found")

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
	shards    [layerShardCount]layerShard
}

const layerShardCount = 64

// layerShard is padded to one 64-byte cache line on the deployment target
// (amd64). Without the padding, adjacent shard RWMutex counters can still
// false-share under the 16-way commitment fold even though their maps differ.
// The fixed ~4 KiB per live layer is small relative to the layer values and the
// configured 24 GiB Pebble cache, and maps remain lazily allocated.
type layerShard struct {
	mu      sync.RWMutex
	writes  map[string][]byte
	deletes map[string]struct{}
	_       [24]byte
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
	maxFlushBatchValueSize   = 1 << 20
	maxFlushBatchEncodedSize = 1 << 20
)

func newLayer(hash common.Hash, number uint64) *layer {
	return &layer{
		blockHash: hash,
		number:    number,
	}
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
// 30-100 byte physical key a second time (the Go map will hash it once already),
// while two independently distributed commitment nibbles provide all 64 shard
// combinations. Include length so short prefixes that share a suffix do not
// systematically collide. The byte/string forms must remain identical for
// write/read routing.
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
// single-active model did. When async commit is enabled, a serial commit worker
// holds a handle to an OLDER in-flight layer (block N) and writes it via
// ViewLayer/LayerWriter while the foreground writes the newer layer (N+1); the
// worker promotes its layer with CommitInflight or drops it with DiscardInflight.
// The two threads target DISJOINT layers and every method takes the applicable
// locks, so the sharded layer maps and slices stay race-free. With
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
	base ethdb.KeyValueReader
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

type bufferBatchOp struct {
	delete bool
	key    []byte
	value  []byte
	target *layer
}

type bufferBatch struct {
	parent *Buffer
	ops    []bufferBatchOp
	size   int
	closed bool
}

// valueViewReader exposes a value only for the duration of fn. Pebble can use
// this to keep its block handle open while the blockbuffer copies directly
// into cache-owned immutable storage, avoiding Database.Get's intermediate
// defensive copy. Implementations must invoke fn synchronously.
type valueViewReader interface {
	View(key []byte, fn func([]byte) error) error
}

// New creates a Buffer that falls through reads to base.
func New(base ethdb.KeyValueReader) *Buffer {
	b := &Buffer{base: base}
	b.publishReadViewLocked()
	return b
}

// ConcurrentReadWriteSafe is a structural marker for higher-level stores that
// may publish disjoint keys while other workers are still reading. Buffer
// Put/Delete resolve the target under b.mu and protect the actual map mutation
// with the key's shard lock; readers use immutable topology views and the same
// shard locks.
func (*Buffer) ConcurrentReadWriteSafe() {}

// publishReadViewLocked publishes copies of the topology slices. The layer
// pointers themselves are stable: dropping a layer only removes the owning
// slice reference, while a reader that already loaded an older view keeps the
// layer alive until that read completes. Caller holds b.mu for every mutation
// after construction (construction itself is single-threaded).
func (b *Buffer) publishReadViewLocked() {
	view := &bufferReadView{baseReadCache: b.baseReadCache}
	if len(b.inflight) > 0 {
		view.inflight = append([]*layer(nil), b.inflight...)
	}
	if len(b.layers) > 0 {
		view.layers = append([]*layer(nil), b.layers...)
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
func (b *Buffer) SetBaseReadCacheSize(sizeBytes int) {
	if b == nil {
		return
	}
	b.mu.Lock()
	old := b.baseReadCache
	b.baseReadCache = newBaseReadCache(sizeBytes)
	b.publishReadViewLocked()
	b.mu.Unlock()
	if old != nil {
		old.clear()
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
	k := append([]byte(nil), key...)
	v := append([]byte(nil), value...)
	b.ops = append(b.ops, bufferBatchOp{key: k, value: v, target: b.parent.activeLayer()})
	b.size += len(k) + len(v)
	return nil
}

func (b *bufferBatch) Delete(key []byte) error {
	if b.closed {
		return errors.New("blockbuffer: batch closed")
	}
	k := append([]byte(nil), key...)
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
	for _, op := range b.ops {
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

func applyBatchOpToLayer(target *layer, op bufferBatchOp) {
	k := string(op.key)
	s := target.shardForString(k)
	s.mu.Lock()
	defer s.mu.Unlock()
	if op.delete {
		delete(s.writes, k)
		if s.deletes == nil {
			s.deletes = make(map[string]struct{})
		}
		s.deletes[k] = struct{}{}
		return
	}
	delete(s.deletes, k)
	// Put already copied the caller's value into storage owned by the batch.
	// Batches never mutate those bytes, so the layer can retain that owned
	// slice directly instead of allocating and copying it a second time.
	if s.writes == nil {
		s.writes = make(map[string][]byte)
	}
	s.writes[k] = op.value
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

	kept := b.ops[:0]
	keptSize := 0
	for _, op := range b.ops {
		target := op.target
		if target == nil {
			target = b.parent.newestInflightLocked()
		}
		if target == nil {
			if dropStale {
				continue
			}
			kept = append(kept, op)
			keptSize += bufferBatchOpSize(op)
			continue
		}
		if !b.parent.layerPendingLocked(target) {
			if dropStale {
				continue
			}
			return len(b.ops), errors.New("blockbuffer: batch target layer is no longer pending")
		}
		if !b.parent.layerCommittedLocked(target) || !matchCommitted(target) {
			kept = append(kept, op)
			keptSize += bufferBatchOpSize(op)
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
	b.ops = b.ops[:0]
	b.size = 0
}

func (b *bufferBatch) Replay(w ethdb.KeyValueWriter) error {
	for _, op := range b.ops {
		if op.delete {
			if err := w.Delete(op.key); err != nil {
				return err
			}
			continue
		}
		if err := w.Put(op.key, op.value); err != nil {
			return err
		}
	}
	return nil
}

func (b *bufferBatch) Close() {
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
	if b == nil || target == nil {
		return false
	}
	for _, l := range b.inflight {
		if l == target {
			return true
		}
	}
	for _, l := range b.layers {
		if l == target {
			return true
		}
	}
	return false
}

// layerInflightLocked reports whether target is currently an in-flight layer.
func (b *Buffer) layerInflightLocked(target *layer) bool {
	if b == nil || target == nil {
		return false
	}
	for _, l := range b.inflight {
		if l == target {
			return true
		}
	}
	return false
}

func (b *Buffer) layerCommittedLocked(target *layer) bool {
	if b == nil || target == nil {
		return false
	}
	for _, l := range b.layers {
		if l == target {
			return true
		}
	}
	return false
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
	b.inflight = append(b.inflight, newLayer(hash, number))
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
	copy(b.inflight, b.inflight[1:])
	b.inflight[len(b.inflight)-1] = nil
	b.inflight = b.inflight[:len(b.inflight)-1]
	b.layers = append(b.layers, l)
	b.publishReadViewLocked()
}

// DiscardActive drops the NEWEST in-flight layer without promoting it (the
// foreground's current block, dropped on an applyBlock error). No-op if no
// layer is in flight.
func (b *Buffer) DiscardActive() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n := len(b.inflight); n > 0 {
		b.inflight[n-1] = nil
		b.inflight = b.inflight[:n-1]
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
			copy(b.inflight[i:], b.inflight[i+1:])
			b.inflight[len(b.inflight)-1] = nil
			b.inflight = b.inflight[:len(b.inflight)-1]
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
	out := b.layers[:0]
	for _, l := range b.layers {
		if l.blockHash == hash {
			continue
		}
		out = append(out, l)
	}
	// Zero the now-unused tail to avoid retaining dropped layers.
	for i := len(out); i < len(b.layers); i++ {
		b.layers[i] = nil
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
		b.inflight[i] = nil
	}
	b.inflight = b.inflight[:0]
	for i := range b.layers {
		b.layers[i] = nil
	}
	b.layers = b.layers[:0]
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
	s := l.shardForBytes(key)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, t := s.deletes[string(key)]; t {
		return nil, false, true
	}
	if val, ok := s.writes[string(key)]; ok {
		return val, true, false
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
	view := b.loadReadView()
	// In-flight layers first, newest-first (the foreground's active layer wins
	// over an older worker-owned layer), then committed layers newest-first.
	for i := len(view.inflight) - 1; i >= 0; i-- {
		v, found, tomb := view.inflight[i].lookup(key)
		if tomb {
			return nil, ErrNotFound
		}
		if found {
			out := append([]byte(nil), v...)
			return out, nil
		}
	}
	for i := len(view.layers) - 1; i >= 0; i-- {
		v, found, tomb := view.layers[i].lookup(key)
		if tomb {
			return nil, ErrNotFound
		}
		if found {
			out := append([]byte(nil), v...)
			return out, nil
		}
	}
	if b.base == nil {
		return nil, ErrNotFound
	}
	return b.base.Get(key)
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

func (b *Buffer) getNoCopy(key []byte, cacheBase bool) ([]byte, error) {
	// lookup keeps the map index allocation-free (string(key) in the index
	// expression is elided by the compiler), so this read stays alloc-free on a
	// buffer hit — it returns the layer's internal slice directly. The immutable
	// read view keeps topology stable for the walk; lookup locks only the key's
	// matching map shard.
	view := b.loadReadView()
	for i := len(view.inflight) - 1; i >= 0; i-- {
		v, found, tomb := view.inflight[i].lookup(key)
		if tomb {
			return nil, ErrNotFound
		}
		if found {
			return v, nil
		}
	}
	for i := len(view.layers) - 1; i >= 0; i-- {
		v, found, tomb := view.layers[i].lookup(key)
		if tomb {
			return nil, ErrNotFound
		}
		if found {
			return v, nil
		}
	}
	cache := view.baseReadCache
	if b.base == nil {
		return nil, ErrNotFound
	}
	var cacheEpoch uint64
	if cacheBase && cache != nil {
		if value, ok, epoch := cache.getWithEpoch(key); ok {
			return value, nil
		} else {
			cacheEpoch = epoch
		}
	}
	if !cacheBase || cache == nil {
		return b.base.Get(key)
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
	view := b.loadReadView()
	for i := len(view.inflight) - 1; i >= 0; i-- {
		value, found, tomb := view.inflight[i].lookup(key)
		if tomb {
			return nil, ErrNotFound
		}
		if found {
			return value, nil
		}
	}
	for i := len(view.layers) - 1; i >= 0; i-- {
		value, found, tomb := view.layers[i].lookup(key)
		if tomb {
			return nil, ErrNotFound
		}
		if found {
			return value, nil
		}
	}
	if b.base == nil {
		return nil, ErrNotFound
	}
	cache := view.baseReadCache
	var cacheEpoch uint64
	if cache != nil {
		if value, ok, epoch := cache.getWithEpoch(key); ok {
			return value, nil
		} else {
			cacheEpoch = epoch
		}
	}
	owned := append([]byte(nil), key...)
	if cache == nil {
		return b.base.Get(owned)
	}
	return readBaseIntoCache(b.base, cache, owned, cacheEpoch)
}

// readBaseIntoCache fills cache directly from a callback-style base reader
// when available. If a concurrent flush invalidates the observed epoch, the
// cache rejects the late fill; in that case we make one owned fallback copy
// before View returns so no Pebble-backed slice escapes its valid lifetime.
func readBaseIntoCache(base ethdb.KeyValueReader, cache *baseReadCache, key []byte, epoch uint64) ([]byte, error) {
	if viewer, ok := base.(valueViewReader); ok {
		var out []byte
		err := viewer.View(key, func(value []byte) error {
			if stored, ok := cache.setIfEpoch(key, value, epoch); ok {
				out = stored
			} else {
				out = append([]byte(nil), value...)
			}
			return nil
		})
		return out, err
	}
	value, err := base.Get(key)
	if err != nil {
		return nil, err
	}
	stored, _ := cache.setIfEpoch(key, value, epoch)
	return stored, nil
}

// Has reports whether key exists, honoring tombstones. Safe to call
// concurrently with mutators.
func (b *Buffer) Has(key []byte) (bool, error) {
	view := b.loadReadView()
	for i := len(view.inflight) - 1; i >= 0; i-- {
		_, found, tomb := view.inflight[i].lookup(key)
		if tomb {
			return false, nil
		}
		if found {
			return true, nil
		}
	}
	for i := len(view.layers) - 1; i >= 0; i-- {
		_, found, tomb := view.layers[i].lookup(key)
		if tomb {
			return false, nil
		}
		if found {
			return true, nil
		}
	}
	if b.base == nil {
		return false, nil
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
		b.promoteBaseReadCacheLayer(l)
	}
	for i := range b.layers {
		b.layers[i] = nil
	}
	b.layers = b.layers[:0]
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
	flushed, err := flushLayers(snapshot[:eligible], w)
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
	b.promoteBaseReadCacheLayers(snapshot[:flushed])

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
	copy(b.layers, b.layers[n:])
	for i := len(b.layers) - n; i < len(b.layers); i++ {
		b.layers[i] = nil
	}
	b.layers = b.layers[:len(b.layers)-n]
	b.publishReadViewLocked()
}

func flushLayer(l *layer, w ethdb.KeyValueWriter) error {
	if batcher, ok := w.(ethdb.Batcher); ok {
		_, encodedSize := layerWriteStats(l)
		batch := batcher.NewBatchWithSize(pebbleBatchHeaderSize + encodedSize + pebbleBatchRecordSlack)
		defer closeBatch(batch)
		if err := writeLayer(l, batch); err != nil {
			return err
		}
		return batch.Write()
	}
	return writeLayer(l, w)
}

func flushLayers(layers []*layer, w ethdb.KeyValueWriter) (int, error) {
	if len(layers) == 0 {
		return 0, nil
	}
	batcher, ok := w.(ethdb.Batcher)
	if !ok {
		flushed := 0
		for _, l := range layers {
			if err := writeLayer(l, w); err != nil {
				return flushed, err
			}
			flushed++
		}
		return flushed, nil
	}

	sizes := make([]layerBatchSize, len(layers))
	for i, l := range layers {
		valueSize, encodedSize := layerWriteStats(l)
		sizes[i] = layerBatchSize{value: valueSize, encoded: encodedSize}
	}

	flushed := 0
	for start := 0; start < len(layers); {
		end := start
		queuedValueSize := 0
		queuedEncodedSize := pebbleBatchHeaderSize
		for end < len(layers) {
			next := sizes[end]
			if end > start && (queuedValueSize+next.value > maxFlushBatchValueSize ||
				queuedEncodedSize+next.encoded > maxFlushBatchEncodedSize) {
				break
			}
			queuedValueSize += next.value
			queuedEncodedSize += next.encoded
			end++
			if queuedValueSize >= maxFlushBatchValueSize || queuedEncodedSize >= maxFlushBatchEncodedSize {
				break
			}
		}

		// Pebble deliberately drops buffers larger than batchMaxRetainedSize on
		// Reset. Reusing one large batch therefore made every group after the
		// first grow geometrically from an empty buffer despite our size
		// calculation. Allocate each bounded group at its encoded size plus the
		// one-record scratch allowance so every batch performs one final
		// allocation and no grow/copy cycle.
		batch := batcher.NewBatchWithSize(queuedEncodedSize + pebbleBatchRecordSlack)
		for i := start; i < end; i++ {
			if err := writeLayer(layers[i], batch); err != nil {
				closeBatch(batch)
				return flushed, err
			}
		}
		if err := batch.Write(); err != nil {
			closeBatch(batch)
			return flushed, err
		}
		closeBatch(batch)
		flushed += end - start
		start = end
	}
	return flushed, nil
}

func writeLayer(l *layer, w ethdb.KeyValueWriter) error {
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.RLock()
		for k, v := range s.writes {
			if err := w.Put([]byte(k), v); err != nil {
				s.mu.RUnlock()
				return err
			}
		}
		for k := range s.deletes {
			if err := w.Delete([]byte(k)); err != nil {
				s.mu.RUnlock()
				return err
			}
		}
		s.mu.RUnlock()
	}
	return nil
}

func layerWriteSize(l *layer) int {
	size, _ := layerWriteStats(l)
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
}

// layerWriteStats returns both ethdb's logical ValueSize and the encoded
// Pebble batch record size. Pebble records use one kind byte followed by
// uvarint-framed keys and values; deletes omit the value. Supplying this exact
// encoded size plus Pebble's one-record temporary varint slack up front avoids
// Batch.grow copying a megabyte-scale flush batch. The 12-byte batch header and
// scratch slack are added once by the caller.
func layerWriteStats(l *layer) (valueSize, encodedSize int) {
	if l == nil {
		return 0, 0
	}
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.RLock()
		for k, v := range s.writes {
			valueSize += len(k) + len(v)
			encodedSize += 1 + uvarintSize(len(k)) + len(k) + uvarintSize(len(v)) + len(v)
		}
		for k := range s.deletes {
			valueSize += len(k)
			encodedSize += 1 + uvarintSize(len(k)) + len(k)
		}
		s.mu.RUnlock()
	}
	return valueSize, encodedSize
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

// overlayOp is one resolved overlay entry: a value write, or a tombstone.
type overlayOp struct {
	value   []byte
	deleted bool
}

// overlayState resolves a newest-first walk of layers into a single overlay map
// (the first time a key is seen wins, so newer layers mask older ones). Shared
// by Buffer.NewIterator and LayerView.NewIterator.
type overlayState struct {
	m map[string]overlayOp
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
	pfx := string(prefix)
	lo := string(prefix) + string(start)
	matches := func(k string) bool {
		if pfx != "" && !strings.HasPrefix(k, pfx) {
			return false
		}
		return k >= lo
	}
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.RLock()
		for k, v := range s.writes {
			if !matches(k) {
				continue
			}
			if _, set := o.m[k]; set {
				continue
			}
			o.m[k] = overlayOp{value: append([]byte(nil), v...)}
		}
		for k := range s.deletes {
			if !matches(k) {
				continue
			}
			if _, set := o.m[k]; set {
				continue
			}
			o.m[k] = overlayOp{deleted: true}
		}
		s.mu.RUnlock()
	}
}

// finishIterator merges the resolved overlay with the base keys in the
// [prefix, prefix+start) window and returns a snapshot iterator. Runs lock-free
// (the base store has its own concurrency control; overlay is already a private
// copy). Shared by Buffer.NewIterator and LayerView.NewIterator.
func (b *Buffer) finishIterator(overlay *overlayState, prefix, start []byte) ethdb.Iterator {
	type kv struct{ key, value []byte }
	var entries []kv
	if b.base != nil {
		if iter, ok := b.base.(ethdb.Iteratee); ok {
			it := iter.NewIterator(prefix, start)
			for it.Next() {
				k := string(it.Key())
				if op, masked := overlay.m[k]; masked {
					if !op.deleted {
						entries = append(entries, kv{
							key:   append([]byte(nil), it.Key()...),
							value: op.value,
						})
					}
					delete(overlay.m, k)
					continue
				}
				entries = append(entries, kv{
					key:   append([]byte(nil), it.Key()...),
					value: append([]byte(nil), it.Value()...),
				})
			}
			err := it.Error()
			it.Release()
			if err != nil {
				return &bufferIterator{err: err}
			}
		}
		// If the base does not implement Iteratee, only the overlay is
		// surfaced. This matches the contract that NewIterator on a reader
		// with no iteration support cannot synthesize one.
	}
	// Overlay-only keys (no disk hit). Tombstones for non-existent disk keys
	// contribute nothing.
	for k, op := range overlay.m {
		if op.deleted {
			continue
		}
		entries = append(entries, kv{key: []byte(k), value: op.value})
	}

	// Sort ascending by key. The disk leg arrives already sorted; the overlay
	// leg is map-order. One sort over the combined list is cleaner than a
	// merge-cursor for small N.
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})
	out := make([]bufferIteratorEntry, len(entries))
	for i, e := range entries {
		out[i] = bufferIteratorEntry{key: e.key, value: e.value}
	}
	return &bufferIterator{entries: out, idx: -1}
}

// bufferIterator is a snapshot iterator returned by Buffer.NewIterator.
// Holds no locks. Key/Value buffers are owned by the iterator; callers must
// not mutate them (mirrors the ethdb.Iterator contract).
type bufferIterator struct {
	entries []bufferIteratorEntry
	idx     int
	err     error
}

type bufferIteratorEntry struct {
	key, value []byte
}

func (it *bufferIterator) Next() bool {
	if it.err != nil {
		return false
	}
	if it.idx+1 >= len(it.entries) {
		return false
	}
	it.idx++
	return true
}

func (it *bufferIterator) Error() error { return it.err }

func (it *bufferIterator) Key() []byte {
	if it.idx < 0 || it.idx >= len(it.entries) {
		return nil
	}
	return it.entries[it.idx].key
}

func (it *bufferIterator) Value() []byte {
	if it.idx < 0 || it.idx >= len(it.entries) {
		return nil
	}
	return it.entries[it.idx].value
}

func (it *bufferIterator) Release() {
	it.entries = nil
	it.idx = 0
}
