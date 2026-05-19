package blockbuffer

import (
	"bytes"
	"errors"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func bufHash(b byte) common.Hash {
	var h common.Hash
	h[31] = b
	return h
}

func mustGet(t *testing.T, b *Buffer, key []byte, want []byte) {
	t.Helper()
	got, err := b.Get(key)
	if err != nil {
		t.Fatalf("Get(%q) err: %v", key, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Get(%q) = %q, want %q", key, got, want)
	}
}

func mustNotFound(t *testing.T, b *Buffer, key []byte) {
	t.Helper()
	_, err := b.Get(key)
	if err == nil {
		t.Fatalf("Get(%q) expected error, got nil", key)
	}
	has, _ := b.Has(key)
	if has {
		t.Fatalf("Has(%q) = true, want false", key)
	}
}

// Read-through: keys not in any layer fall through to base.
func TestBuffer_ReadThroughToBase(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	if err := base.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	b := New(base)
	mustGet(t, b, []byte("k1"), []byte("v1"))
}

// Writes in active layer are immediately visible to Get/Has.
func TestBuffer_WriteThenReadInActiveLayer(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	b := New(base)
	b.BeginBlock(bufHash(1))
	if err := b.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	mustGet(t, b, []byte("k"), []byte("v"))
	has, _ := b.Has([]byte("k"))
	if !has {
		t.Fatal("Has() = false, want true")
	}
}

// Tombstone semantics: Delete makes a key from base appear absent.
func TestBuffer_DeleteTombstonesBaseKey(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	base.Put([]byte("k"), []byte("base-value"))
	b := New(base)
	b.BeginBlock(bufHash(1))
	if err := b.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	mustNotFound(t, b, []byte("k"))
	// Re-Put after Delete clears the tombstone.
	if err := b.Put([]byte("k"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	mustGet(t, b, []byte("k"), []byte("new"))
}

// Multiple committed layers: newer layer's Put overrides older.
func TestBuffer_LayerStacking(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	b := New(base)

	b.BeginBlock(bufHash(1))
	b.Put([]byte("k"), []byte("layer1"))
	b.CommitBlock()
	mustGet(t, b, []byte("k"), []byte("layer1"))

	b.BeginBlock(bufHash(2))
	b.Put([]byte("k"), []byte("layer2"))
	b.CommitBlock()
	mustGet(t, b, []byte("k"), []byte("layer2"))

	// Active layer overrides committed layers.
	b.BeginBlock(bufHash(3))
	b.Put([]byte("k"), []byte("active"))
	mustGet(t, b, []byte("k"), []byte("active"))
}

// DiscardActive drops in-progress writes; subsequent reads see prior state.
func TestBuffer_DiscardActive(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	b := New(base)
	b.BeginBlock(bufHash(1))
	b.Put([]byte("k"), []byte("layer1"))
	b.CommitBlock()

	b.BeginBlock(bufHash(2))
	b.Put([]byte("k"), []byte("active-but-doomed"))
	mustGet(t, b, []byte("k"), []byte("active-but-doomed"))
	b.DiscardActive()

	mustGet(t, b, []byte("k"), []byte("layer1"))
}

// DiscardBlock(hash) removes that specific layer only.
func TestBuffer_DiscardBlockRemovesOnlyTargetLayer(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	b := New(base)
	b.BeginBlock(bufHash(1))
	b.Put([]byte("a"), []byte("1a"))
	b.CommitBlock()

	b.BeginBlock(bufHash(2))
	b.Put([]byte("b"), []byte("2b"))
	b.CommitBlock()

	b.BeginBlock(bufHash(3))
	b.Put([]byte("c"), []byte("3c"))
	b.CommitBlock()

	b.DiscardBlock(bufHash(2))

	// Layer 1 and 3 still present; layer 2 gone.
	mustGet(t, b, []byte("a"), []byte("1a"))
	mustGet(t, b, []byte("c"), []byte("3c"))
	mustNotFound(t, b, []byte("b"))

	// Diagnostics: pending blocks list reflects removal.
	pending := b.PendingBlocks()
	if len(pending) != 2 || pending[0] != bufHash(1) || pending[1] != bufHash(3) {
		t.Fatalf("PendingBlocks after DiscardBlock = %v, want [hash1 hash3]", pending)
	}

	// DiscardBlock for an unknown hash is a no-op.
	b.DiscardBlock(bufHash(99))
	if got := len(b.PendingBlocks()); got != 2 {
		t.Fatalf("DiscardBlock(unknown) altered pending: len = %d, want 2", got)
	}
}

// DiscardBlock with a layer at the most-recent index also drops correctly.
func TestBuffer_DiscardBlockTopLayer(t *testing.T) {
	b := New(rawdb.NewMemoryDatabase())
	b.BeginBlock(bufHash(1))
	b.Put([]byte("k"), []byte("v1"))
	b.CommitBlock()

	b.BeginBlock(bufHash(2))
	b.Put([]byte("k"), []byte("v2"))
	b.CommitBlock()

	b.DiscardBlock(bufHash(2))
	mustGet(t, b, []byte("k"), []byte("v1"))
}

// Discard drops both active and committed layers.
func TestBuffer_DiscardAll(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	base.Put([]byte("k"), []byte("base"))
	b := New(base)
	b.BeginBlock(bufHash(1))
	b.Put([]byte("k"), []byte("layer1"))
	b.CommitBlock()
	b.BeginBlock(bufHash(2))
	b.Put([]byte("k"), []byte("active"))

	b.Discard()

	// All layers gone — read falls through to base.
	mustGet(t, b, []byte("k"), []byte("base"))
	if len(b.PendingBlocks()) != 0 {
		t.Fatalf("PendingBlocks after Discard != empty")
	}
}

// CommitBlock without an active layer panics.
func TestBuffer_CommitBlockWithoutActivePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from CommitBlock without BeginBlock")
		}
	}()
	b := New(rawdb.NewMemoryDatabase())
	b.CommitBlock()
}

// BeginBlock while a layer is already active panics.
func TestBuffer_DoubleBeginBlockPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from double BeginBlock")
		}
	}()
	b := New(rawdb.NewMemoryDatabase())
	b.BeginBlock(bufHash(1))
	b.BeginBlock(bufHash(2))
}

// Put without an active layer panics.
func TestBuffer_PutWithoutActivePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from Put without BeginBlock")
		}
	}()
	b := New(rawdb.NewMemoryDatabase())
	b.Put([]byte("k"), []byte("v"))
}

// Delete without an active layer panics.
func TestBuffer_DeleteWithoutActivePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from Delete without BeginBlock")
		}
	}()
	b := New(rawdb.NewMemoryDatabase())
	b.Delete([]byte("k"))
}

// Flush drains all committed layers oldest-first into the writer, then
// clears them.
func TestBuffer_FlushDrainsOldestFirst(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	b := New(base)

	b.BeginBlock(bufHash(1))
	b.Put([]byte("k"), []byte("oldest"))
	b.CommitBlock()

	b.BeginBlock(bufHash(2))
	b.Put([]byte("k"), []byte("newest"))
	b.CommitBlock()

	dst := rawdb.NewMemoryDatabase()
	if err := b.Flush(dst); err != nil {
		t.Fatal(err)
	}
	got, err := dst.Get([]byte("k"))
	if err != nil {
		t.Fatalf("dst.Get after Flush: %v", err)
	}
	if !bytes.Equal(got, []byte("newest")) {
		t.Fatalf("Flush oldest-first: dst[k] = %q, want %q", got, "newest")
	}
	if len(b.PendingBlocks()) != 0 {
		t.Fatal("Flush did not clear layers")
	}
}

// Flush of tombstones deletes from the writer.
func TestBuffer_FlushTombstones(t *testing.T) {
	dst := rawdb.NewMemoryDatabase()
	dst.Put([]byte("k"), []byte("present"))

	b := New(rawdb.NewMemoryDatabase())
	b.BeginBlock(bufHash(1))
	b.Delete([]byte("k"))
	b.CommitBlock()

	if err := b.Flush(dst); err != nil {
		t.Fatal(err)
	}
	if has, _ := dst.Has([]byte("k")); has {
		t.Fatal("Flush tombstone did not delete from writer")
	}
}

// FlushUpTo flushes only layers ≤ cutoff and keeps higher layers in memory.
func TestBuffer_FlushUpTo_FlushesOnlyMatchingLayers(t *testing.T) {
	b := New(rawdb.NewMemoryDatabase())

	// Three layers at heights 1, 2, 3.
	hashes := []common.Hash{bufHash(1), bufHash(2), bufHash(3)}
	for i, h := range hashes {
		b.BeginBlock(h)
		if err := b.Put([]byte{byte('a' + i)}, []byte{byte('A' + i)}); err != nil {
			t.Fatal(err)
		}
		b.CommitBlock()
	}

	numberOf := func(h common.Hash) (uint64, bool) {
		for i, x := range hashes {
			if x == h {
				return uint64(i + 1), true
			}
		}
		return 0, false
	}

	dst := rawdb.NewMemoryDatabase()
	if err := b.FlushUpTo(2, numberOf, dst); err != nil {
		t.Fatal(err)
	}

	// Layers 1 and 2 flushed, layer 3 still in memory.
	if got, _ := dst.Get([]byte("a")); !bytes.Equal(got, []byte("A")) {
		t.Fatalf("layer 1 not flushed: %q", got)
	}
	if got, _ := dst.Get([]byte("b")); !bytes.Equal(got, []byte("B")) {
		t.Fatalf("layer 2 not flushed: %q", got)
	}
	if has, _ := dst.Has([]byte("c")); has {
		t.Fatalf("layer 3 unexpectedly flushed")
	}

	pending := b.PendingBlocks()
	if len(pending) != 1 || pending[0] != bufHash(3) {
		t.Fatalf("pending = %v, want [hash3]", pending)
	}

	// Buffer still reads layer 3 from memory.
	mustGet(t, b, []byte("c"), []byte("C"))
}

// FlushUpTo with an unknown hash conservatively keeps the layer.
func TestBuffer_FlushUpTo_UnknownHashKeepsLayer(t *testing.T) {
	b := New(rawdb.NewMemoryDatabase())
	b.BeginBlock(bufHash(1))
	b.Put([]byte("a"), []byte("A"))
	b.CommitBlock()
	b.BeginBlock(bufHash(2))
	b.Put([]byte("b"), []byte("B"))
	b.CommitBlock()

	// numberOf returns (_, false) for hash2 — should stop iteration there.
	numberOf := func(h common.Hash) (uint64, bool) {
		if h == bufHash(1) {
			return 1, true
		}
		return 0, false
	}

	dst := rawdb.NewMemoryDatabase()
	if err := b.FlushUpTo(99, numberOf, dst); err != nil {
		t.Fatal(err)
	}
	// Layer 1 flushed, layer 2 kept (unknown number).
	if got, _ := dst.Get([]byte("a")); !bytes.Equal(got, []byte("A")) {
		t.Fatalf("layer 1 not flushed")
	}
	if has, _ := dst.Has([]byte("b")); has {
		t.Fatal("layer 2 unexpectedly flushed (its number is unknown)")
	}
	if pending := b.PendingBlocks(); len(pending) != 1 || pending[0] != bufHash(2) {
		t.Fatalf("pending = %v, want [hash2]", pending)
	}
}

// FlushUpTo is idempotent.
func TestBuffer_FlushUpTo_Idempotent(t *testing.T) {
	b := New(rawdb.NewMemoryDatabase())
	b.BeginBlock(bufHash(1))
	b.Put([]byte("k"), []byte("v"))
	b.CommitBlock()

	numberOf := func(h common.Hash) (uint64, bool) {
		if h == bufHash(1) {
			return 1, true
		}
		return 0, false
	}

	dst := rawdb.NewMemoryDatabase()
	if err := b.FlushUpTo(5, numberOf, dst); err != nil {
		t.Fatal(err)
	}
	// Second call: zero matching layers (already flushed).
	if err := b.FlushUpTo(5, numberOf, dst); err != nil {
		t.Fatal(err)
	}
	if len(b.PendingBlocks()) != 0 {
		t.Fatalf("PendingBlocks not empty after flush")
	}
}

// FlushUpTo keeps higher layers rewindable: a layer above the cutoff can
// still be discarded via DiscardBlock after a partial flush.
func TestBuffer_FlushUpTo_KeepsHigherLayersRewindable(t *testing.T) {
	b := New(rawdb.NewMemoryDatabase())
	b.BeginBlock(bufHash(1))
	b.Put([]byte("a"), []byte("flushed"))
	b.CommitBlock()
	b.BeginBlock(bufHash(2))
	b.Put([]byte("b"), []byte("orphan"))
	b.CommitBlock()

	numberOf := func(h common.Hash) (uint64, bool) {
		switch h {
		case bufHash(1):
			return 1, true
		case bufHash(2):
			return 2, true
		}
		return 0, false
	}

	// Flush up to 1.
	if err := b.FlushUpTo(1, numberOf, rawdb.NewMemoryDatabase()); err != nil {
		t.Fatal(err)
	}
	// Discard layer 2 — orphan rewind.
	b.DiscardBlock(bufHash(2))
	mustNotFound(t, b, []byte("b"))
	if got := len(b.PendingBlocks()); got != 0 {
		t.Fatalf("PendingBlocks = %d, want 0 after flush+discard", got)
	}
}

// drainIterator walks a buffer iterator to completion and returns the
// resulting (key, value) pairs as a slice of two-element string slices. The
// helper exists because every iterator test wants the same compact view of
// the snapshot, and the ethdb.Iterator API is verbose at the use-site.
func drainIterator(t *testing.T, b *Buffer, prefix, start []byte) [][2]string {
	t.Helper()
	it := b.NewIterator(prefix, start)
	defer it.Release()
	var out [][2]string
	for it.Next() {
		out = append(out, [2]string{string(it.Key()), string(it.Value())})
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	return out
}

// NewIterator on an empty buffer with no base writes returns no entries.
func TestBuffer_NewIterator_Empty(t *testing.T) {
	b := New(rawdb.NewMemoryDatabase())
	if got := drainIterator(t, b, nil, nil); len(got) != 0 {
		t.Fatalf("expected 0 entries, got %v", got)
	}
}

// NewIterator surfaces base-only keys sorted, prefix-filtered.
func TestBuffer_NewIterator_BaseOnly(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	base.Put([]byte("dp-allow_pbft"), []byte{0, 0, 0, 0, 0, 0, 0, 1})
	base.Put([]byte("dp-zen_token_id"), []byte{0, 0, 0, 0, 0, 0, 0, 7})
	base.Put([]byte("other-key"), []byte("x"))

	b := New(base)
	got := drainIterator(t, b, []byte("dp-"), nil)
	want := [][2]string{
		{"dp-allow_pbft", "\x00\x00\x00\x00\x00\x00\x00\x01"},
		{"dp-zen_token_id", "\x00\x00\x00\x00\x00\x00\x00\x07"},
	}
	if !equalEntries(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// NewIterator merges overlay-only keys (no disk hit) into the result.
func TestBuffer_NewIterator_OverlayOnly(t *testing.T) {
	b := New(rawdb.NewMemoryDatabase())
	b.BeginBlock(bufHash(1))
	b.Put([]byte("dp-current_cycle_number"), []byte("42"))
	b.CommitBlock()

	got := drainIterator(t, b, []byte("dp-"), nil)
	want := [][2]string{{"dp-current_cycle_number", "42"}}
	if !equalEntries(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// Active-layer writes override committed-layer values, which override base.
// The iterator must reflect Get's priority ordering for overlapping keys.
func TestBuffer_NewIterator_LayerOverride(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	base.Put([]byte("dp-allow_pbft"), []byte("base"))

	b := New(base)
	b.BeginBlock(bufHash(1))
	b.Put([]byte("dp-allow_pbft"), []byte("committed"))
	b.CommitBlock()
	b.BeginBlock(bufHash(2))
	b.Put([]byte("dp-allow_pbft"), []byte("active"))

	got := drainIterator(t, b, []byte("dp-"), nil)
	want := [][2]string{{"dp-allow_pbft", "active"}}
	if !equalEntries(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// A tombstone in any layer suppresses the base value for that key. This is
// the same contract Get/Has have — the iterator must not silently leak
// deleted entries.
func TestBuffer_NewIterator_TombstoneSuppressesBase(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	base.Put([]byte("dp-foo"), []byte("base-foo"))
	base.Put([]byte("dp-bar"), []byte("base-bar"))

	b := New(base)
	b.BeginBlock(bufHash(1))
	b.Delete([]byte("dp-foo"))
	b.CommitBlock()

	got := drainIterator(t, b, []byte("dp-"), nil)
	want := [][2]string{{"dp-bar", "base-bar"}}
	if !equalEntries(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// `start` skips keys lexicographically smaller than it. Verify the iterator
// honors the parameter alongside the prefix filter.
func TestBuffer_NewIterator_StartParameter(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	base.Put([]byte("dp-a"), []byte("a"))
	base.Put([]byte("dp-m"), []byte("m"))
	base.Put([]byte("dp-z"), []byte("z"))

	b := New(base)
	got := drainIterator(t, b, []byte("dp-"), []byte("dp-m"))
	want := [][2]string{
		{"dp-m", "m"},
		{"dp-z", "z"},
	}
	if !equalEntries(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// Release nils the snapshot so subsequent Next/Key calls are no-ops. Mostly
// a smoke check that we don't panic after release.
func TestBuffer_NewIterator_Release(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	base.Put([]byte("dp-a"), []byte("a"))
	b := New(base)
	it := b.NewIterator([]byte("dp-"), nil)
	if !it.Next() {
		t.Fatal("expected at least one entry")
	}
	it.Release()
	if it.Next() {
		t.Fatal("Next after Release should return false")
	}
	if k := it.Key(); k != nil {
		t.Fatalf("Key after Release: got %v, want nil", k)
	}
}

func equalEntries(a, b [][2]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i][0] != b[i][0] || a[i][1] != b[i][1] {
			return false
		}
	}
	return true
}

// Buffer satisfies the ethdb.KeyValueReader and Writer interfaces in shape.
func TestBuffer_SatisfiesEthdbInterfaces(t *testing.T) {
	b := New(rawdb.NewMemoryDatabase())
	// Compile-time assertions inside the test:
	var _ interface {
		Get([]byte) ([]byte, error)
		Has([]byte) (bool, error)
	} = b
	var _ interface {
		Put([]byte, []byte) error
		Delete([]byte) error
	} = b

	// Sanity: ErrNotFound is non-nil.
	if errors.Is(nil, ErrNotFound) {
		t.Fatal("ErrNotFound check broken")
	}
}
