// Package blockbuffer provides an in-memory layered write-set over an
// ethdb.KeyValueReader (typically the on-disk store), used by core.BlockChain
// to defer post-applyBlock rawdb-direct writes so that switchFork can
// discard the layers belonging to orphaned-branch blocks.
//
// One layer is opened per applyBlock via BeginBlock(hash). Writes during the
// block go to the active layer; CommitBlock promotes it onto the layered
// stack (newest at the top). DiscardBlock(hash) removes a specific layer
// (used in switchFork for orphan rewinds). DiscardActive drops the
// in-progress layer (used on applyBlock failure). Reads check the active
// layer first, then layers newest-first, then fall through to the base
// reader. Tombstones for deletes return a not-found error.
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
	writes    map[string][]byte
	deletes   map[string]struct{}
}

func newLayer(hash common.Hash) *layer {
	return &layer{
		blockHash: hash,
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
// Mutators (Begin/Commit/Discard*/Put/Delete/Flush*) assume the caller
// serializes them — typically via core.BlockChain's chainmu. The internal
// mu guards the layers slice and per-layer maps so that uncoordinated
// readers (RPC handlers, metrics, txpool) can call Get/Has/PendingBlocks
// concurrently with a writer holding chainmu without triggering a Go race
// detector report. This matches the slice-1 documented "single-writer"
// model — the lock is added in slice 2 because the buffer is now read by
// callers outside chainmu (BlockChain.DynProps for RPC, etc.).
type Buffer struct {
	base ethdb.KeyValueReader
	mu   sync.RWMutex
	// flushMu serializes FlushUpTo/Flush calls against each other so the
	// snapshot→disk-I/O→drop phases of two concurrent flushers can't
	// interleave (double-flush / double-drop). It is held across the whole
	// flush, but mu is released during the disk I/O so readers
	// (Get/Has/NewIterator — the LoadDynamicProperties path) proceed
	// concurrently. FlushUpTo callers are the async-flush worker, the
	// inline fallback, and Close; only one runs the body at a time.
	flushMu sync.Mutex
	layers  []*layer
	active  *layer
}

// New creates a Buffer that falls through reads to base.
func New(base ethdb.KeyValueReader) *Buffer {
	return &Buffer{base: base}
}

// BeginBlock opens a fresh active layer for the given block hash.
// Panics if a layer is already active.
func (b *Buffer) BeginBlock(hash common.Hash) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active != nil {
		panic("blockbuffer: BeginBlock called while a layer is already active")
	}
	b.active = newLayer(hash)
}

// CommitBlock promotes the active layer onto the layered stack.
// Panics if no layer is active.
func (b *Buffer) CommitBlock() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active == nil {
		panic("blockbuffer: CommitBlock called with no active layer")
	}
	b.layers = append(b.layers, b.active)
	b.active = nil
}

// DiscardActive drops the active layer without promoting it.
// No-op if no layer is active.
func (b *Buffer) DiscardActive() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.active = nil
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
	b.active = nil
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
	if b.active != nil {
		if _, tomb := b.active.deletes[k]; tomb {
			b.mu.RUnlock()
			return nil, ErrNotFound
		}
		if v, ok := b.active.writes[k]; ok {
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

// Has reports whether key exists, honoring tombstones. Safe to call
// concurrently with mutators.
func (b *Buffer) Has(key []byte) (bool, error) {
	k := string(key)
	b.mu.RLock()
	if b.active != nil {
		if _, tomb := b.active.deletes[k]; tomb {
			b.mu.RUnlock()
			return false, nil
		}
		if _, ok := b.active.writes[k]; ok {
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
	if b.active == nil {
		panic("blockbuffer: Put called with no active layer")
	}
	k := string(key)
	delete(b.active.deletes, k)
	b.active.writes[k] = append([]byte(nil), value...)
	return nil
}

// Delete tombstones a key in the active layer.
// Panics if no layer is active.
func (b *Buffer) Delete(key []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active == nil {
		panic("blockbuffer: Delete called with no active layer")
	}
	k := string(key)
	delete(b.active.writes, k)
	b.active.deletes[k] = struct{}{}
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
	numberOf func(common.Hash) (uint64, bool),
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
	// reading blockHash + write/delete maps without b.mu is race-free, and
	// readers can RLock concurrently.
	flushed := 0
	for _, l := range snapshot {
		n, ok := numberOf(l.blockHash)
		if !ok || n > cutoff {
			break
		}
		if err := flushLayer(l, w); err != nil {
			// Drop whatever we already flushed before surfacing the error,
			// so a retry doesn't re-write those layers.
			b.dropFlushedPrefix(flushed)
			return err
		}
		flushed++
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
		for k, v := range l.writes {
			if err := batch.Put([]byte(k), v); err != nil {
				return err
			}
		}
		for k := range l.deletes {
			if err := batch.Delete([]byte(k)); err != nil {
				return err
			}
		}
		return batch.Write()
	}
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
	pfx := string(prefix)
	// ethdb.Iteratee contract: `start` is RELATIVE to `prefix`. The absolute
	// lower bound is therefore `prefix + start`, matching what
	// ethdb/memorydb does:
	//   st := string(append(prefix, start...))
	// Comparing overlay keys against bare `start` would incorrectly drop
	// every overlay entry whose key happens to sort before `start` even
	// though it sits in the `prefix` range — and worse, tombstones in that
	// window would also be skipped, so masked base keys would leak through.
	lo := string(prefix) + string(start)

	b.mu.RLock()
	// Step 1: collect the overlay newest-first. The first time we see a key
	// wins; older layers are masked. `seen` tracks both writes and deletes so
	// a Delete in a newer layer suppresses the value in an older layer (and
	// in the base).
	type overlayOp struct {
		value   []byte
		deleted bool
	}
	overlay := make(map[string]overlayOp)
	matches := func(k string) bool {
		if pfx != "" && !strings.HasPrefix(k, pfx) {
			return false
		}
		if k < lo {
			return false
		}
		return true
	}
	walk := func(l *layer) {
		if l == nil {
			return
		}
		for k, v := range l.writes {
			if !matches(k) {
				continue
			}
			if _, set := overlay[k]; set {
				continue
			}
			overlay[k] = overlayOp{value: append([]byte(nil), v...)}
		}
		for k := range l.deletes {
			if !matches(k) {
				continue
			}
			if _, set := overlay[k]; set {
				continue
			}
			overlay[k] = overlayOp{deleted: true}
		}
	}
	walk(b.active)
	for i := len(b.layers) - 1; i >= 0; i-- {
		walk(b.layers[i])
	}
	b.mu.RUnlock()

	// Step 2: pull base keys that match the prefix/start window. Disk keys
	// that the overlay shadows are dropped here; overlay keys not present on
	// disk are merged in afterwards.
	type kv struct{ key, value []byte }
	var entries []kv
	if b.base != nil {
		if iter, ok := b.base.(ethdb.Iteratee); ok {
			it := iter.NewIterator(prefix, start)
			for it.Next() {
				k := string(it.Key())
				if op, masked := overlay[k]; masked {
					if !op.deleted {
						entries = append(entries, kv{
							key:   append([]byte(nil), it.Key()...),
							value: op.value,
						})
					}
					delete(overlay, k)
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
	// Step 3: overlay-only keys (no disk hit). Tombstones for non-existent
	// disk keys contribute nothing.
	for k, op := range overlay {
		if op.deleted {
			continue
		}
		entries = append(entries, kv{
			key:   []byte(k),
			value: op.value,
		})
	}

	// Step 4: sort ascending by key. The disk leg arrives already sorted; the
	// overlay leg is map-order. One sort.Slice on the combined list is
	// cleaner than maintaining a merge-cursor for small N.
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
