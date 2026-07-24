package blockbuffer

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// blockingWriter is an ethdb.KeyValueWriter whose first Put blocks until a
// release channel is closed, simulating slow disk I/O mid-flush. Used to
// hold FlushUpTo inside its critical section deterministically.
type blockingWriter struct {
	started chan struct{} // closed when the first Put is entered
	release chan struct{} // Put returns once this is closed
	once    sync.Once
	puts    atomic.Int32
}

type stringKeyWriterProbe struct {
	putKeys       []string
	deleteKeys    []string
	genericWrites int
}

func (w *stringKeyWriterProbe) Put([]byte, []byte) error {
	w.genericWrites++
	return nil
}

func (w *stringKeyWriterProbe) Delete([]byte) error {
	w.genericWrites++
	return nil
}

func (w *stringKeyWriterProbe) PutString(key string, _ []byte) error {
	w.putKeys = append(w.putKeys, key)
	return nil
}

func (w *stringKeyWriterProbe) DeleteString(key string) error {
	w.deleteKeys = append(w.deleteKeys, key)
	return nil
}

func newBlockingWriter() *blockingWriter {
	return &blockingWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func TestWriteLayerUsesStringKeyWriter(t *testing.T) {
	buf := New(nil)
	layer := newLayer(bufHash(1), 1)
	buf.putIntoString(layer, "put-key", []byte("value"))
	buf.deleteIntoString(layer, "delete-key")
	probe := new(stringKeyWriterProbe)
	if err := writeLayer(layer, probe); err != nil {
		t.Fatal(err)
	}
	if probe.genericWrites != 0 {
		t.Fatalf("generic []byte writes = %d, want 0", probe.genericWrites)
	}
	if len(probe.putKeys) != 1 || probe.putKeys[0] != "put-key" {
		t.Fatalf("string put keys = %q, want [put-key]", probe.putKeys)
	}
	if len(probe.deleteKeys) != 1 || probe.deleteKeys[0] != "delete-key" {
		t.Fatalf("string delete keys = %q, want [delete-key]", probe.deleteKeys)
	}
}

func (w *blockingWriter) Put(key, value []byte) error {
	w.puts.Add(1)
	w.once.Do(func() {
		close(w.started)
		<-w.release
	})
	return nil
}

func (w *blockingWriter) Delete(key []byte) error { return nil }

type countingBatcher struct {
	ethdb.KeyValueStore
	batches  atomic.Int32
	writes   atomic.Int32
	sizeHint atomic.Int64
}

func (w *countingBatcher) NewBatch() ethdb.Batch {
	w.batches.Add(1)
	return &countingBatch{
		Batch:  w.KeyValueStore.NewBatch(),
		writes: &w.writes,
	}
}

func (w *countingBatcher) NewBatchWithSize(size int) ethdb.Batch {
	w.batches.Add(1)
	w.sizeHint.Store(int64(size))
	return &countingBatch{
		Batch:  w.KeyValueStore.NewBatchWithSize(size),
		writes: &w.writes,
	}
}

type countingBatch struct {
	ethdb.Batch
	writes *atomic.Int32
}

func (b *countingBatch) Write() error {
	b.writes.Add(1)
	return b.Batch.Write()
}

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
	b.BeginBlock(bufHash(1), 1)
	if err := b.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	mustGet(t, b, []byte("k"), []byte("v"))
	has, _ := b.Has([]byte("k"))
	if !has {
		t.Fatal("Has() = false, want true")
	}
}

func TestBufferBatchWritesActiveLayer(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	b := New(base)
	b.BeginBlock(bufHash(1), 1)

	batch := b.NewBatch()
	if err := batch.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := batch.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Get([]byte("k1")); err == nil {
		t.Fatal("batch write became visible before Write")
	}
	if err := batch.Delete([]byte("k1")); err != nil {
		t.Fatal(err)
	}
	if err := batch.Put([]byte("k1"), []byte("v3")); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	mustGet(t, b, []byte("k1"), []byte("v3"))
	mustGet(t, b, []byte("k2"), []byte("v2"))

	batch.Reset()
	if err := batch.Delete([]byte("k2")); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	mustNotFound(t, b, []byte("k2"))
}

func TestBufferBatchOwnsValuesAfterWrite(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	b := New(base)
	b.BeginBlock(bufHash(1), 1)

	value := []byte("original")
	batch := b.NewBatch()
	if err := batch.Put([]byte("key"), value); err != nil {
		t.Fatal(err)
	}
	copy(value, "mutated!")
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	batch.Reset()
	batch.Close()

	mustGet(t, b, []byte("key"), []byte("original"))
}

func TestBufferBatchOwnsKeysBeforeWrite(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	b := New(base)
	b.BeginBlock(bufHash(1), 1)

	key := []byte("original-key")
	batch := b.NewBatch()
	if err := batch.Put(key, []byte("value")); err != nil {
		t.Fatal(err)
	}
	copy(key, "mutated-key!")
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	mustGet(t, b, []byte("original-key"), []byte("value"))
	mustNotFound(t, b, []byte("mutated-key!"))
}

func TestBufferBatchPutOwnedValueRetainsValueAndOwnsKey(t *testing.T) {
	b := New(rawdb.NewMemoryDatabase())
	b.BeginBlock(bufHash(1), 1)
	key := []byte("owned-value-key")
	value := []byte("owned-value")
	batch := b.NewBatch()
	owned := batch.(interface {
		PutOwnedValue(key, value []byte) error
	})
	if err := owned.PutOwnedValue(key, value); err != nil {
		t.Fatal(err)
	}
	key[0] = 'X'
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	got, err := b.GetNoCopy([]byte("owned-value-key"))
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("owned value read = (%q,%v)", got, err)
	}
	if &got[0] != &value[0] {
		t.Fatal("PutOwnedValue copied the transferred value")
	}
	mustNotFound(t, b, []byte("Xwned-value-key"))
}

func TestBufferBatchPutOwnedKeyValueRetainsBothInputs(t *testing.T) {
	b := New(rawdb.NewMemoryDatabase())
	b.BeginBlock(bufHash(1), 1)
	keyArena := []byte("prefix-owned-key-suffix")
	key := keyArena[len("prefix-"):len("prefix-owned-key"):len("prefix-owned-key")]
	value := []byte("owned-value")
	batch := b.NewBatch().(*bufferBatch)
	if err := batch.PutOwnedKeyValue(key, value); err != nil {
		t.Fatal(err)
	}
	if unsafe.StringData(batch.ops[0].key) != unsafe.SliceData(key) {
		t.Fatal("PutOwnedKeyValue copied the transferred key")
	}
	// The string alias, rather than a live caller slice, must keep the arena
	// reachable until the batch publishes the operation.
	keyArena = nil
	key = nil
	runtime.GC()
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	got, err := b.GetNoCopy([]byte("owned-key"))
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("owned key/value read = (%q,%v)", got, err)
	}
	if &got[0] != &value[0] {
		t.Fatal("PutOwnedKeyValue copied the transferred value")
	}
}

func TestBufferBatchResetReleasesOwnedInputs(t *testing.T) {
	b := New(rawdb.NewMemoryDatabase())
	b.BeginBlock(bufHash(1), 1)
	batch := b.NewBatch().(*bufferBatch)
	if err := batch.PutOwnedKeyValue([]byte("owned-key"), []byte("owned-value")); err != nil {
		t.Fatal(err)
	}
	batch.Reset()
	if len(batch.ops) != 0 || batch.size != 0 {
		t.Fatalf("Reset left len/size = %d/%d", len(batch.ops), batch.size)
	}
	if retained := batch.ops[:cap(batch.ops)][0]; retained.key != "" || retained.value != nil || retained.target != nil {
		t.Fatalf("Reset retained operation references: %+v", retained)
	}
}

func TestBufferAndLayerViewPutOwnedValueRetainValueAndOwnKey(t *testing.T) {
	b := New(rawdb.NewMemoryDatabase())
	b.BeginBlock(bufHash(1), 1)
	h, ok := b.NewestInflight()
	if !ok {
		t.Fatal("missing in-flight layer")
	}
	writers := map[string]interface {
		PutOwnedValue(key, value []byte) error
	}{
		"buffer":     b,
		"layer-view": b.ViewLayer(h),
	}
	for name, writer := range writers {
		t.Run(name, func(t *testing.T) {
			key := []byte(name + "-owned-key")
			wantKey := append([]byte(nil), key...)
			value := []byte(name + "-owned-value")
			if err := writer.PutOwnedValue(key, value); err != nil {
				t.Fatal(err)
			}
			key[0] = 'X'
			got, err := b.GetNoCopy(wantKey)
			if err != nil || !bytes.Equal(got, value) {
				t.Fatalf("owned value read = (%q,%v), want %q", got, err, value)
			}
			if &got[0] != &value[0] {
				t.Fatal("PutOwnedValue copied the transferred value")
			}
			mustNotFound(t, b, key)
		})
	}
}

func TestStructuredStateKVLatestOwnedWritersRetainValueAndOwnKey(t *testing.T) {
	type writer interface {
		PutStateKVLatestOwnedValue(prefix []byte, accountID common.AccountID, generation uint64, domain uint16, logicalKey, value []byte) error
	}
	tests := []struct {
		name  string
		write func(t *testing.T, b *Buffer, w writer)
	}{
		{name: "buffer", write: func(t *testing.T, _ *Buffer, w writer) {}},
		{name: "layer-view", write: func(t *testing.T, _ *Buffer, w writer) {}},
		{name: "batch", write: func(t *testing.T, _ *Buffer, w writer) {
			if err := w.(ethdb.Batch).Write(); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := New(rawdb.NewMemoryDatabase())
			b.BeginBlock(bufHash(1), 1)
			h, _ := b.NewestInflight()
			var w writer
			switch tc.name {
			case "buffer":
				w = b
			case "layer-view":
				w = b.ViewLayer(h)
			case "batch":
				w = b.NewBatch().(writer)
			}
			prefix := []byte("state-prefix-")
			var accountID common.AccountID
			accountID[0] = 0x7a
			logicalKey := []byte("owned-logical-key")
			wantKey := []byte(joinStateKVLatestKey(prefix, accountID, 17, 3, logicalKey))
			value := []byte("owned-encoded-value")
			if err := w.PutStateKVLatestOwnedValue(prefix, accountID, 17, 3, logicalKey, value); err != nil {
				t.Fatal(err)
			}
			logicalKey[0] = 'X'
			tc.write(t, b, w)
			got, err := b.GetNoCopy(wantKey)
			if err != nil || !bytes.Equal(got, value) {
				t.Fatalf("owned structured value read = (%q,%v)", got, err)
			}
			if &got[0] != &value[0] {
				t.Fatal("structured owned writer copied value")
			}
		})
	}
}

func BenchmarkBufferBatchWrite(b *testing.B) {
	benchmarkBufferBatchWrite(b, false)
}

func BenchmarkBufferBatchWriteOwnedValues(b *testing.B) {
	benchmarkBufferBatchWrite(b, true)
}

func benchmarkBufferBatchWrite(b *testing.B, ownedValues bool) {
	buffer := New(rawdb.NewMemoryDatabase())
	buffer.BeginBlock(bufHash(1), 1)
	keys := make([][]byte, 128)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("batch-key-%03d", i))
	}
	value := bytes.Repeat([]byte{0xab}, 1024)
	b.ReportAllocs()
	b.SetBytes(int64(len(keys) * len(value)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := buffer.NewBatchWithSize(len(keys) * (len(value) + 16))
		owned, _ := batch.(interface {
			PutOwnedValue(key, value []byte) error
		})
		for _, key := range keys {
			var err error
			if ownedValues {
				err = owned.PutOwnedValue(key, value)
			} else {
				err = batch.Put(key, value)
			}
			if err != nil {
				b.Fatal(err)
			}
		}
		if err := batch.Write(); err != nil {
			b.Fatal(err)
		}
		batch.Close()
	}
}

func BenchmarkPebbleFlushBatchSizing(b *testing.B) {
	db, err := rawdb.NewPebbleDB(b.TempDir(), 16, 16)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })

	puts := newLayer(bufHash(1), 1)
	value := bytes.Repeat([]byte{0xcd}, 1024)
	setup := &Buffer{}
	for i := 0; i < 2048; i++ {
		setup.putIntoString(puts, fmt.Sprintf("flush-key-%04d", i), value)
	}
	deletes := newLayer(bufHash(2), 2)
	for i := 0; i < 32768; i++ {
		setup.deleteIntoString(deletes, fmt.Sprintf("deleted-state-key-%08d", i))
	}

	for _, workload := range []struct {
		name  string
		layer *layer
	}{
		{name: "puts", layer: puts},
		{name: "deletes", layer: deletes},
	} {
		b.Run(workload.name, func(b *testing.B) {
			_, encodedSize := layerWriteStats(workload.layer)
			exactSize := pebbleBatchHeaderSize + encodedSize
			for _, sizing := range []struct {
				name string
				size int
			}{
				{name: "unsized"},
				{name: "exact-final-size", size: exactSize},
				{name: "with-record-slack", size: exactSize + pebbleBatchRecordSlack},
			} {
				b.Run(sizing.name, func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						var batch ethdb.Batch
						if sizing.size == 0 {
							batch = db.NewBatch()
						} else {
							batch = db.NewBatchWithSize(sizing.size)
						}
						if err := writeLayer(workload.layer, batch); err != nil {
							b.Fatal(err)
						}
						closeBatch(batch)
					}
				})
			}
		})
	}
}

func TestBufferBatchWritesToCapturedLayerAfterCommit(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	b := New(base)

	b.BeginBlock(bufHash(1), 1)
	batch := b.NewBatch()
	if err := batch.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()

	b.BeginBlock(bufHash(2), 2)
	if err := batch.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	mustGet(t, b, []byte("k1"), []byte("v1"))
	mustGet(t, b, []byte("k2"), []byte("v2"))
	b.CommitBlock()

	b.DiscardBlock(bufHash(1))
	mustNotFound(t, b, []byte("k1"))
	mustGet(t, b, []byte("k2"), []byte("v2"))
}

func TestBufferBatchRejectsWriteAfterCapturedLayerDropped(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	b := New(base)

	b.BeginBlock(bufHash(1), 1)
	batch := b.NewBatch()
	if err := batch.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()
	b.DiscardBlock(bufHash(1))

	err := batch.Write()
	if err == nil || !strings.Contains(err.Error(), "target layer is no longer pending") {
		t.Fatalf("batch write after captured layer drop err = %v, want target layer rejection", err)
	}
	mustNotFound(t, b, []byte("k1"))
}

func TestBufferBatchWriteUpToAppliesOnlyEligibleCommittedLayers(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	b := New(base)

	b.BeginBlock(bufHash(1), 1)
	batch := b.NewBatch()
	layerBatch, ok := batch.(interface {
		WriteUpTo(uint64) (int, error)
	})
	if !ok {
		t.Fatal("buffer batch missing WriteUpTo")
	}
	if err := batch.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()

	b.BeginBlock(bufHash(2), 2)
	if err := batch.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()

	remaining, err := layerBatch.WriteUpTo(1)
	if err != nil {
		t.Fatal(err)
	}
	if remaining != 1 {
		t.Fatalf("remaining ops = %d, want 1", remaining)
	}
	mustGet(t, b, []byte("k1"), []byte("v1"))
	mustNotFound(t, b, []byte("k2"))

	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	mustGet(t, b, []byte("k2"), []byte("v2"))
}

func TestBufferBatchWriteCommittedDropsStaleActiveLayerOps(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	b := New(base)

	b.BeginBlock(bufHash(1), 1)
	batch := b.NewBatch()
	layerBatch, ok := batch.(interface {
		WriteCommitted(bool) (int, error)
	})
	if !ok {
		t.Fatal("buffer batch missing WriteCommitted")
	}
	if err := batch.Put([]byte("committed"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()

	b.BeginBlock(bufHash(2), 2)
	if err := batch.Put([]byte("discarded"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	b.DiscardActive()

	remaining, err := layerBatch.WriteCommitted(true)
	if err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("remaining ops = %d, want 0", remaining)
	}
	mustGet(t, b, []byte("committed"), []byte("v1"))
	mustNotFound(t, b, []byte("discarded"))
}

// Tombstone semantics: Delete makes a key from base appear absent.
func TestBuffer_DeleteTombstonesBaseKey(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	base.Put([]byte("k"), []byte("base-value"))
	b := New(base)
	b.BeginBlock(bufHash(1), 1)
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

	b.BeginBlock(bufHash(1), 1)
	b.Put([]byte("k"), []byte("layer1"))
	b.CommitBlock()
	mustGet(t, b, []byte("k"), []byte("layer1"))

	b.BeginBlock(bufHash(2), 2)
	b.Put([]byte("k"), []byte("layer2"))
	b.CommitBlock()
	mustGet(t, b, []byte("k"), []byte("layer2"))

	// Active layer overrides committed layers.
	b.BeginBlock(bufHash(3), 3)
	b.Put([]byte("k"), []byte("active"))
	mustGet(t, b, []byte("k"), []byte("active"))
}

// DiscardActive drops in-progress writes; subsequent reads see prior state.
func TestBuffer_DiscardActive(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	b := New(base)
	b.BeginBlock(bufHash(1), 1)
	b.Put([]byte("k"), []byte("layer1"))
	b.CommitBlock()

	b.BeginBlock(bufHash(2), 2)
	b.Put([]byte("k"), []byte("active-but-doomed"))
	mustGet(t, b, []byte("k"), []byte("active-but-doomed"))
	b.DiscardActive()

	mustGet(t, b, []byte("k"), []byte("layer1"))
}

// DiscardBlock(hash) removes that specific layer only.
func TestBuffer_DiscardBlockRemovesOnlyTargetLayer(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	b := New(base)
	b.BeginBlock(bufHash(1), 1)
	b.Put([]byte("a"), []byte("1a"))
	b.CommitBlock()

	b.BeginBlock(bufHash(2), 2)
	b.Put([]byte("b"), []byte("2b"))
	b.CommitBlock()

	b.BeginBlock(bufHash(3), 3)
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
	b.BeginBlock(bufHash(1), 1)
	b.Put([]byte("k"), []byte("v1"))
	b.CommitBlock()

	b.BeginBlock(bufHash(2), 2)
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
	b.BeginBlock(bufHash(1), 1)
	b.Put([]byte("k"), []byte("layer1"))
	b.CommitBlock()
	b.BeginBlock(bufHash(2), 2)
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
	b.BeginBlock(bufHash(1), 1)
	b.BeginBlock(bufHash(2), 2)
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

	b.BeginBlock(bufHash(1), 1)
	b.Put([]byte("k"), []byte("oldest"))
	b.CommitBlock()

	b.BeginBlock(bufHash(2), 2)
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
	b.BeginBlock(bufHash(1), 1)
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
		b.BeginBlock(h, uint64(i+1))
		if err := b.Put([]byte{byte('a' + i)}, []byte{byte('A' + i)}); err != nil {
			t.Fatal(err)
		}
		b.CommitBlock()
	}

	dst := rawdb.NewMemoryDatabase()
	if err := b.FlushUpTo(2, dst); err != nil {
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

// FlushUpTo stops at the first layer whose number exceeds the cutoff and
// keeps later layers intact. Verifies the loop's "stop at first ineligible"
// invariant: with cutoff=1, layer 2 must NOT be flushed even though cutoff=99
// would have included it — the predicate must stop BEFORE evaluating layer 2.
func TestBuffer_FlushUpTo_StopsAtFirstAboveCutoff(t *testing.T) {
	b := New(rawdb.NewMemoryDatabase())
	b.BeginBlock(bufHash(1), 1)
	b.Put([]byte("a"), []byte("A"))
	b.CommitBlock()
	b.BeginBlock(bufHash(2), 2)
	b.Put([]byte("b"), []byte("B"))
	b.CommitBlock()

	dst := rawdb.NewMemoryDatabase()
	if err := b.FlushUpTo(1, dst); err != nil {
		t.Fatal(err)
	}
	// Layer 1 flushed (number=1 ≤ 1), layer 2 kept (number=2 > 1).
	if got, _ := dst.Get([]byte("a")); !bytes.Equal(got, []byte("A")) {
		t.Fatalf("layer 1 not flushed")
	}
	if has, _ := dst.Has([]byte("b")); has {
		t.Fatal("layer 2 unexpectedly flushed (its number exceeds cutoff)")
	}
	if pending := b.PendingBlocks(); len(pending) != 1 || pending[0] != bufHash(2) {
		t.Fatalf("pending = %v, want [hash2]", pending)
	}
}

// FlushUpTo is idempotent.
func TestBuffer_FlushUpTo_Idempotent(t *testing.T) {
	b := New(rawdb.NewMemoryDatabase())
	b.BeginBlock(bufHash(1), 1)
	b.Put([]byte("k"), []byte("v"))
	b.CommitBlock()

	dst := rawdb.NewMemoryDatabase()
	if err := b.FlushUpTo(5, dst); err != nil {
		t.Fatal(err)
	}
	// Second call: zero matching layers (already flushed).
	if err := b.FlushUpTo(5, dst); err != nil {
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
	b.BeginBlock(bufHash(1), 1)
	b.Put([]byte("a"), []byte("flushed"))
	b.CommitBlock()
	b.BeginBlock(bufHash(2), 2)
	b.Put([]byte("b"), []byte("orphan"))
	b.CommitBlock()

	// Flush up to 1.
	if err := b.FlushUpTo(1, rawdb.NewMemoryDatabase()); err != nil {
		t.Fatal(err)
	}
	// Discard layer 2 — orphan rewind.
	b.DiscardBlock(bufHash(2))
	mustNotFound(t, b, []byte("b"))
	if got := len(b.PendingBlocks()); got != 0 {
		t.Fatalf("PendingBlocks = %d, want 0 after flush+discard", got)
	}
}

func TestBuffer_FlushUpToBatchesEligibleLayers(t *testing.T) {
	b := New(rawdb.NewMemoryDatabase())
	hashes := []common.Hash{bufHash(1), bufHash(2), bufHash(3)}
	for i, h := range hashes {
		b.BeginBlock(h, uint64(i+1))
		if err := b.Put([]byte("k"), []byte{byte('A' + i)}); err != nil {
			t.Fatal(err)
		}
		b.CommitBlock()
	}

	dst := &countingBatcher{KeyValueStore: rawdb.NewMemoryDatabase()}
	if err := b.FlushUpTo(3, dst); err != nil {
		t.Fatal(err)
	}
	if got := dst.batches.Load(); got != 1 {
		t.Fatalf("NewBatch calls = %d, want 1", got)
	}
	if got := dst.writes.Load(); got != 1 {
		t.Fatalf("batch Write calls = %d, want 1", got)
	}
	// Each Set record is kind(1), key length(1), key(1), value length(1),
	// value(1), plus the Pebble header and one-record temporary varint slack.
	if got, want := dst.sizeHint.Load(), int64(pebbleBatchHeaderSize+3*5+pebbleBatchRecordSlack); got != want {
		t.Fatalf("batch size hint = %d, want encoded size plus scratch %d", got, want)
	}
	got, err := dst.Get([]byte("k"))
	if err != nil {
		t.Fatalf("dst.Get after FlushUpTo: %v", err)
	}
	if !bytes.Equal(got, []byte("C")) {
		t.Fatalf("batched FlushUpTo order = %q, want %q", got, "C")
	}
}

func TestBuffer_FlushUpToCreatesSizedBatchPerOversizeLayer(t *testing.T) {
	b := New(rawdb.NewMemoryDatabase())
	value := bytes.Repeat([]byte{0x7a}, maxFlushBatchValueSize+1)
	for i := 0; i < 3; i++ {
		b.BeginBlock(bufHash(byte(i+1)), uint64(i+1))
		if err := b.Put([]byte{'a' + byte(i)}, value); err != nil {
			t.Fatal(err)
		}
		b.CommitBlock()
	}

	dst := &countingBatcher{KeyValueStore: rawdb.NewMemoryDatabase()}
	if err := b.FlushUpTo(3, dst); err != nil {
		t.Fatal(err)
	}
	if got := dst.batches.Load(); got != 3 {
		t.Fatalf("NewBatchWithSize calls = %d, want one exact-sized batch per oversized layer", got)
	}
	if got := dst.writes.Load(); got != 3 {
		t.Fatalf("batch Write calls = %d, want 3", got)
	}
	wantHint := int64(pebbleBatchHeaderSize + 1 + uvarintSize(1) + 1 + uvarintSize(len(value)) + len(value) + pebbleBatchRecordSlack)
	if got := dst.sizeHint.Load(); got != wantHint {
		t.Fatalf("last batch size hint = %d, want %d", got, wantHint)
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
	b.BeginBlock(bufHash(1), 1)
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
	b.BeginBlock(bufHash(1), 1)
	b.Put([]byte("dp-allow_pbft"), []byte("committed"))
	b.CommitBlock()
	b.BeginBlock(bufHash(2), 2)
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
	b.BeginBlock(bufHash(1), 1)
	b.Delete([]byte("dp-foo"))
	b.CommitBlock()

	got := drainIterator(t, b, []byte("dp-"), nil)
	want := [][2]string{{"dp-bar", "base-bar"}}
	if !equalEntries(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// `start` skips keys lexicographically smaller than the lower bound. ethdb's
// contract is that `start` is RELATIVE to `prefix` (memorydb computes
// `st := string(append(prefix, start...))`), so calling
// `NewIterator([]byte("dp-"), []byte("m"))` should yield every dp-key >= "dp-m"
// — NOT every key >= "m".
//
// Earlier this test passed `start=[]byte("dp-m")` (caller pre-prepending the
// prefix), which matched a buggy implementation that compared overlay keys
// against bare `start`. That made `NewIterator("dp-", "m")` drop every
// `dp-*` entry because `"dp-..." < "m"`. Codex caught the inversion.
func TestBuffer_NewIterator_StartParameter(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	base.Put([]byte("dp-a"), []byte("a"))
	base.Put([]byte("dp-m"), []byte("m"))
	base.Put([]byte("dp-z"), []byte("z"))

	b := New(base)
	got := drainIterator(t, b, []byte("dp-"), []byte("m"))
	want := [][2]string{
		{"dp-m", "m"},
		{"dp-z", "z"},
	}
	if !equalEntries(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// Regression test for the overlay-start bug: prefix-only entries that sit
// inside the [prefix+start, prefix\xff) range must still surface (writes) or
// mask base (tombstones), even when prefix < start in byte order. With the
// pre-fix implementation `NewIterator([]byte("dp-"), []byte("m"))` saw the
// active layer's overlay key `dp-zen_token_id` compared against bare "m" and
// dropped (because "d" < "m"), and a tombstone on `dp-zen_token_id` likewise
// failed to mask the base value. This test pins the corrected semantics.
func TestBuffer_NewIterator_StartHonoredForOverlay(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	base.Put([]byte("dp-allow_pbft"), []byte("base"))
	base.Put([]byte("dp-zen_token_id"), []byte("base-zen"))

	b := New(base)
	b.BeginBlock(bufHash(1), 1)
	// Overlay write at "dp-zen_token_id" — must override the base value when
	// start=m places "dp-zen_token_id" inside the range.
	b.Put([]byte("dp-zen_token_id"), []byte("overlay-zen"))
	// Overlay tombstone outside the [dp-m,) window — must NOT mask
	// "dp-allow_pbft" because the iterator is bounded.
	b.Delete([]byte("dp-allow_pbft"))
	b.CommitBlock()

	got := drainIterator(t, b, []byte("dp-"), []byte("m"))
	want := [][2]string{
		{"dp-zen_token_id", "overlay-zen"},
	}
	if !equalEntries(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}

	// And a complementary case: with start in the dp-allow_pbft window, the
	// tombstone must mask the base entry, demonstrating overlay & base
	// suppression both still work when the lower bound is inside the prefix.
	gotFull := drainIterator(t, b, []byte("dp-"), nil)
	wantFull := [][2]string{
		{"dp-zen_token_id", "overlay-zen"},
	}
	if !equalEntries(gotFull, wantFull) {
		t.Fatalf("with start=nil expected only overlay-zen (tombstone masks base allow_pbft), got %q", gotFull)
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

// TestBuffer_FlushUpToDoesNotBlockReaders is the regression guard for the
// lock-free flush fix. Before the fix, FlushUpTo held b.mu (write lock) for
// the FULL duration of disk I/O, so a concurrent reader — the
// LoadDynamicProperties(bc.buffer) path every applyBlock runs in its prologue
// — blocked until the flush finished (the ~2x slowdown in the single-SR
// maintenance test). Now FlushUpTo only holds b.mu briefly to snapshot and to
// drop layers; the disk I/O runs lock-free, so a reader must proceed even
// while a flush is parked mid-write.
func TestBuffer_FlushUpToDoesNotBlockReaders(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	b := New(base)
	b.BeginBlock(bufHash(1), 1)
	b.Put([]byte("dp-k1"), []byte("v1"))
	b.CommitBlock()

	w := newBlockingWriter()

	flushDone := make(chan struct{})
	go func() {
		_ = b.FlushUpTo(1, w)
		close(flushDone)
	}()

	// Wait until FlushUpTo is mid-Put. With the lock-free fix this is during
	// the disk I/O phase, holding only flushMu — not b.mu.
	<-w.started

	// A reader on the buffer (same lock the DP scan takes) must NOT block.
	readDone := make(chan struct{})
	go func() {
		_, _ = b.Get([]byte("dp-k1"))
		close(readDone)
	}()

	select {
	case <-readDone:
		// Expected: reader proceeds concurrently with the in-flight flush.
	case <-time.After(2 * time.Second):
		t.Fatal("Get blocked while FlushUpTo was mid-I/O — lock-free flush regressed")
	}

	close(w.release)
	<-flushDone
}

// TestBuffer_FlushUpToDoesNotBlockIterator is the NewIterator analogue — the
// exact call the DP scan makes (LoadDynamicProperties -> IterateDynamicProperties
// -> Buffer.NewIterator). It must proceed concurrently with an in-flight flush.
func TestBuffer_FlushUpToDoesNotBlockIterator(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	b := New(base)
	b.BeginBlock(bufHash(1), 1)
	b.Put([]byte("dp-k1"), []byte("v1"))
	b.CommitBlock()

	w := newBlockingWriter()

	flushDone := make(chan struct{})
	go func() {
		_ = b.FlushUpTo(1, w)
		close(flushDone)
	}()
	<-w.started

	iterDone := make(chan struct{})
	go func() {
		it := b.NewIterator([]byte("dp-"), nil)
		it.Release()
		close(iterDone)
	}()

	select {
	case <-iterDone:
	case <-time.After(2 * time.Second):
		t.Fatal("NewIterator blocked while FlushUpTo was mid-I/O — lock-free flush regressed")
	}

	close(w.release)
	<-flushDone
}

// TestBuffer_FlushUpToPreservesConcurrentCommit verifies the count-based drop
// is correct when CommitBlock appends a new layer during the lock-free I/O
// window: only the flushed prefix is removed, the freshly-committed tail layer
// survives and remains readable.
func TestBuffer_FlushUpToPreservesConcurrentCommit(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	b := New(base)
	b.BeginBlock(bufHash(1), 1)
	b.Put([]byte("dp-old"), []byte("v1"))
	b.CommitBlock()

	w := newBlockingWriter()

	flushDone := make(chan struct{})
	go func() {
		_ = b.FlushUpTo(1, w)
		close(flushDone)
	}()
	<-w.started // flush is mid-I/O on layer 1, holding only flushMu

	// Append a new committed layer while the flush is in flight.
	b.BeginBlock(bufHash(2), 2)
	b.Put([]byte("dp-new"), []byte("v2"))
	b.CommitBlock()

	close(w.release)
	<-flushDone

	// Layer 1 was flushed + dropped; layer 2 (committed mid-flush) survives.
	mustGet(t, b, []byte("dp-new"), []byte("v2"))
	if pending := b.PendingBlocks(); len(pending) != 1 || pending[0] != bufHash(2) {
		t.Fatalf("after flush: pending = %v, want [hash2]", pending)
	}
}
