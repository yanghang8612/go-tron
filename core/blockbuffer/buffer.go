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
	"errors"
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
	base   ethdb.KeyValueReader
	mu     sync.RWMutex
	layers []*layer
	active *layer
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
func (b *Buffer) FlushUpTo(
	cutoff uint64,
	numberOf func(common.Hash) (uint64, bool),
	w ethdb.KeyValueWriter,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.layers) == 0 {
		return nil
	}
	flushedThrough := -1
	for i, l := range b.layers {
		n, ok := numberOf(l.blockHash)
		if !ok || n > cutoff {
			break
		}
		if err := flushLayer(l, w); err != nil {
			return err
		}
		flushedThrough = i
	}
	if flushedThrough < 0 {
		return nil
	}
	// Drop layers [0..flushedThrough] inclusive.
	keep := flushedThrough + 1
	if keep >= len(b.layers) {
		for i := range b.layers {
			b.layers[i] = nil
		}
		b.layers = b.layers[:0]
		return nil
	}
	copy(b.layers, b.layers[keep:])
	for i := len(b.layers) - keep; i < len(b.layers); i++ {
		b.layers[i] = nil
	}
	b.layers = b.layers[:len(b.layers)-keep]
	return nil
}

func flushLayer(l *layer, w ethdb.KeyValueWriter) error {
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
