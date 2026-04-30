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
type Buffer struct {
	base   ethdb.KeyValueReader
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
	if b.active != nil {
		panic("blockbuffer: BeginBlock called while a layer is already active")
	}
	b.active = newLayer(hash)
}

// CommitBlock promotes the active layer onto the layered stack.
// Panics if no layer is active.
func (b *Buffer) CommitBlock() {
	if b.active == nil {
		panic("blockbuffer: CommitBlock called with no active layer")
	}
	b.layers = append(b.layers, b.active)
	b.active = nil
}

// DiscardActive drops the active layer without promoting it.
// No-op if no layer is active.
func (b *Buffer) DiscardActive() {
	b.active = nil
}

// DiscardBlock removes the layer with the given block hash from the layered
// stack. No-op if no such layer exists. Used by switchFork to drop
// orphan-branch buffers.
func (b *Buffer) DiscardBlock(hash common.Hash) {
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
	b.active = nil
	for i := range b.layers {
		b.layers[i] = nil
	}
	b.layers = b.layers[:0]
}

// PendingBlocks returns the block hashes for currently-pending committed
// layers, oldest first. Useful for diagnostics and tests.
func (b *Buffer) PendingBlocks() []common.Hash {
	out := make([]common.Hash, len(b.layers))
	for i, l := range b.layers {
		out[i] = l.blockHash
	}
	return out
}

// Get returns the value for key, searching active layer first, then
// layered stack newest-first, then the base reader. Tombstones short-
// circuit and return ErrNotFound.
func (b *Buffer) Get(key []byte) ([]byte, error) {
	k := string(key)
	if b.active != nil {
		if _, tomb := b.active.deletes[k]; tomb {
			return nil, ErrNotFound
		}
		if v, ok := b.active.writes[k]; ok {
			return append([]byte(nil), v...), nil
		}
	}
	for i := len(b.layers) - 1; i >= 0; i-- {
		l := b.layers[i]
		if _, tomb := l.deletes[k]; tomb {
			return nil, ErrNotFound
		}
		if v, ok := l.writes[k]; ok {
			return append([]byte(nil), v...), nil
		}
	}
	if b.base == nil {
		return nil, ErrNotFound
	}
	return b.base.Get(key)
}

// Has reports whether key exists, honoring tombstones.
func (b *Buffer) Has(key []byte) (bool, error) {
	k := string(key)
	if b.active != nil {
		if _, tomb := b.active.deletes[k]; tomb {
			return false, nil
		}
		if _, ok := b.active.writes[k]; ok {
			return true, nil
		}
	}
	for i := len(b.layers) - 1; i >= 0; i-- {
		l := b.layers[i]
		if _, tomb := l.deletes[k]; tomb {
			return false, nil
		}
		if _, ok := l.writes[k]; ok {
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
// error encountered. Slice 1 does not invoke Flush; it is exposed for the
// slice-2 stable-flush policy.
func (b *Buffer) Flush(w ethdb.KeyValueWriter) error {
	for _, l := range b.layers {
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
	}
	for i := range b.layers {
		b.layers[i] = nil
	}
	b.layers = b.layers[:0]
	return nil
}
