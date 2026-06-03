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
	"errors"
	"sort"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

// ErrNotFound is returned by Get/Has when the key is tombstoned in a layer.
// It is also the sentinel returned by the underlying base reader for
// missing keys (memorydb / pebble both return non-nil errors for misses;
// callers normally check err != nil rather than identity).
var ErrNotFound = errors.New("blockbuffer: not found")

// layer is a single applyBlock's worth of buffered mutations.
type layer struct {
	blockHash common.Hash
	number    uint64
	writes    map[string][]byte
	deletes   map[string]struct{}
}

const maxFlushBatchValueSize = 1 << 20

func newLayer(hash common.Hash, number uint64) *layer {
	return &layer{
		blockHash: hash,
		number:    number,
		writes:    make(map[string][]byte),
		deletes:   make(map[string]struct{}),
	}
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
// internal mu guards the inflight/layers slices and per-layer maps so that
// uncoordinated readers (RPC handlers, metrics, txpool) can call
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
// The two threads target DISJOINT layers and every method takes mu, so the
// per-layer maps and the slices stay race-free. With maxInflight==1 (the
// default) only one layer is ever in flight, so this degenerates to the
// single-active model and is byte-identical to it.
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
// touch in-flight layers, so the lock-free immutable-committed-layer read in
// FlushUpTo is unaffected by the multi-active-layer change: a layer becomes
// committed (and thus flush-eligible) only after its fold completes and
// CommitBlock/CommitInflight promotes it.
type Buffer struct {
	base    ethdb.KeyValueReader
	mu      sync.RWMutex
	flushMu sync.Mutex
	layers  []*layer
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

// New creates a Buffer that falls through reads to base.
func New(base ethdb.KeyValueReader) *Buffer {
	return &Buffer{base: base}
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
	b.parent.mu.Lock()
	defer b.parent.mu.Unlock()
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
	if op.delete {
		delete(target.writes, k)
		target.deletes[k] = struct{}{}
		return
	}
	delete(target.deletes, k)
	target.writes[k] = append([]byte(nil), op.value...)
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

func (b *bufferBatch) writeFiltered(matchCommitted func(*layer) bool, dropStale bool) (int, error) {
	b.parent.flushMu.Lock()
	defer b.parent.flushMu.Unlock()
	b.parent.mu.Lock()
	defer b.parent.mu.Unlock()

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

// Get returns the value for key, searching active layer first, then
// layered stack newest-first, then the base reader. Tombstones short-
// circuit and return ErrNotFound. Safe to call concurrently with mutators.
func (b *Buffer) Get(key []byte) ([]byte, error) {
	k := string(key)
	b.mu.RLock()
	// In-flight layers first, newest-first (the foreground's active layer wins
	// over an older worker-owned layer), then committed layers newest-first.
	for i := len(b.inflight) - 1; i >= 0; i-- {
		l := b.inflight[i]
		if _, tomb := l.deletes[k]; tomb {
			b.mu.RUnlock()
			return nil, ErrNotFound
		}
		if v, ok := l.writes[k]; ok {
			out := append([]byte(nil), v...)
			b.mu.RUnlock()
			return out, nil
		}
	}
	for i := len(b.layers) - 1; i >= 0; i-- {
		l := b.layers[i]
		if _, tomb := l.deletes[k]; tomb {
			b.mu.RUnlock()
			return nil, ErrNotFound
		}
		if v, ok := l.writes[k]; ok {
			out := append([]byte(nil), v...)
			b.mu.RUnlock()
			return out, nil
		}
	}
	b.mu.RUnlock()
	if b.base == nil {
		return nil, ErrNotFound
	}
	return b.base.Get(key)
}

// GetNoCopy is Get without the defensive value copy: on a buffer hit it returns
// the layer's internal value slice directly (aliasing buffer storage), saving
// the per-Get allocation that dominates the commitment-fold read path. The
// returned slice MUST be consumed (decoded, or copied) by the caller and MUST
// NOT be mutated; it is only guaranteed stable until the same key is next
// written. The commitment store satisfies this: DecodeBranchData copies every
// field it retains and never holds the input past the decode. Reads that fall
// through to the base reader use the base's own (copying) Get.
func (b *Buffer) GetNoCopy(key []byte) ([]byte, error) {
	// Index the maps with string(key) inline: the compiler elides the string
	// allocation for map lookup/comma-ok index expressions, so this read is
	// fully allocation-free on a buffer hit (unlike Get, which both allocates
	// the key string and copies the value).
	b.mu.RLock()
	for i := len(b.inflight) - 1; i >= 0; i-- {
		l := b.inflight[i]
		if _, tomb := l.deletes[string(key)]; tomb {
			b.mu.RUnlock()
			return nil, ErrNotFound
		}
		if v, ok := l.writes[string(key)]; ok {
			b.mu.RUnlock()
			return v, nil
		}
	}
	for i := len(b.layers) - 1; i >= 0; i-- {
		l := b.layers[i]
		if _, tomb := l.deletes[string(key)]; tomb {
			b.mu.RUnlock()
			return nil, ErrNotFound
		}
		if v, ok := l.writes[string(key)]; ok {
			b.mu.RUnlock()
			return v, nil
		}
	}
	b.mu.RUnlock()
	if b.base == nil {
		return nil, ErrNotFound
	}
	return b.base.Get(key)
}

// Has reports whether key exists, honoring tombstones. Safe to call
// concurrently with mutators.
func (b *Buffer) Has(key []byte) (bool, error) {
	k := string(key)
	b.mu.RLock()
	for i := len(b.inflight) - 1; i >= 0; i-- {
		l := b.inflight[i]
		if _, tomb := l.deletes[k]; tomb {
			b.mu.RUnlock()
			return false, nil
		}
		if _, ok := l.writes[k]; ok {
			b.mu.RUnlock()
			return true, nil
		}
	}
	for i := len(b.layers) - 1; i >= 0; i-- {
		l := b.layers[i]
		if _, tomb := l.deletes[k]; tomb {
			b.mu.RUnlock()
			return false, nil
		}
		if _, ok := l.writes[k]; ok {
			b.mu.RUnlock()
			return true, nil
		}
	}
	b.mu.RUnlock()
	if b.base == nil {
		return false, nil
	}
	return b.base.Has(key)
}

// Put stores a key/value pair in the active layer.
// Panics if no layer is active (writes outside an applyBlock are a bug).
func (b *Buffer) Put(key, value []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	active := b.newestInflightLocked()
	if active == nil {
		panic("blockbuffer: Put called with no active layer")
	}
	k := string(key)
	delete(active.deletes, k)
	active.writes[k] = append([]byte(nil), value...)
	return nil
}

// Delete tombstones a key in the active layer.
// Panics if no layer is active.
func (b *Buffer) Delete(key []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	active := b.newestInflightLocked()
	if active == nil {
		panic("blockbuffer: Delete called with no active layer")
	}
	k := string(key)
	delete(active.writes, k)
	active.deletes[k] = struct{}{}
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
			return err
		}
	}
	for i := range b.layers {
		b.layers[i] = nil
	}
	b.layers = b.layers[:0]
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
// committed layers are immutable once CommitBlock promotes them: their
// write/delete maps are never mutated again, only the slice that holds them
// is. We therefore:
//
//  1. briefly RLock to snapshot the layer pointers,
//  2. run numberOf + flushLayer lock-free on that snapshot,
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

	// Step 2: lock-free disk I/O. Layers are immutable post-commit, so
	// reading blockHash + number + write/delete maps without b.mu is race-free,
	// and readers can RLock concurrently.
	eligible := 0
	for _, l := range snapshot {
		if l.number > cutoff {
			break
		}
		eligible++
	}
	flushed, err := flushLayers(snapshot[:eligible], w)
	if err != nil {
		// Drop whatever we already flushed before surfacing the error, so a
		// retry doesn't re-write those layers.
		b.dropFlushedPrefix(flushed)
		return err
	}
	if flushed == 0 {
		return nil
	}

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
}

func flushLayer(l *layer, w ethdb.KeyValueWriter) error {
	if batcher, ok := w.(ethdb.Batcher); ok {
		batch := batcher.NewBatch()
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

	batch := batcher.NewBatch()
	defer closeBatch(batch)

	flushed := 0
	queuedLayers := 0
	flushQueued := func() error {
		if queuedLayers == 0 {
			return nil
		}
		if err := batch.Write(); err != nil {
			return err
		}
		flushed += queuedLayers
		queuedLayers = 0
		batch.Reset()
		return nil
	}

	for _, l := range layers {
		if queuedLayers > 0 && batch.ValueSize()+layerWriteSize(l) > maxFlushBatchValueSize {
			if err := flushQueued(); err != nil {
				return flushed, err
			}
		}
		if err := writeLayer(l, batch); err != nil {
			return flushed, err
		}
		queuedLayers++
		if batch.ValueSize() >= maxFlushBatchValueSize {
			if err := flushQueued(); err != nil {
				return flushed, err
			}
		}
	}
	if err := flushQueued(); err != nil {
		return flushed, err
	}
	return flushed, nil
}

func writeLayer(l *layer, w ethdb.KeyValueWriter) error {
	for k, v := range l.writes {
		if err := w.Put([]byte(k), v); err != nil {
			return err
		}
	}
	for k := range l.deletes {
		if err := w.Delete([]byte(k)); err != nil {
			return err
		}
	}
	return nil
}

func layerWriteSize(l *layer) int {
	if l == nil {
		return 0
	}
	size := 0
	for k, v := range l.writes {
		size += len(k) + len(v)
	}
	for k := range l.deletes {
		size += len(k)
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
	// committed newest→oldest) under a brief read lock. Step 2-4 (base merge +
	// sort) are shared with LayerView via finishIterator.
	b.mu.RLock()
	overlay := newOverlayState()
	for i := len(b.inflight) - 1; i >= 0; i-- {
		overlay.walk(b.inflight[i], prefix, start)
	}
	for i := len(b.layers) - 1; i >= 0; i-- {
		overlay.walk(b.layers[i], prefix, start)
	}
	b.mu.RUnlock()
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
// that have the given prefix. Caller holds the buffer read lock.
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
	for k, v := range l.writes {
		if !matches(k) {
			continue
		}
		if _, set := o.m[k]; set {
			continue
		}
		o.m[k] = overlayOp{value: append([]byte(nil), v...)}
	}
	for k := range l.deletes {
		if !matches(k) {
			continue
		}
		if _, set := o.m[k]; set {
			continue
		}
		o.m[k] = overlayOp{deleted: true}
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
