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
