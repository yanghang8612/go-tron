package blockbuffer

import (
	"bytes"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type countingKeyValueReader struct {
	ethdb.KeyValueReader
	gets  int
	views int
}

type getOnlyReader struct {
	ethdb.KeyValueReader
}

// keyValueWriterOnly intentionally hides optional writer extensions so
// benchmarks can compare rawdb's generic Put fallback with LayerView's
// split-key fast path.
type keyValueWriterOnly struct {
	ethdb.KeyValueWriter
}

type blockingSnapshotReader struct {
	ethdb.KeyValueReader
	started chan struct{}
	release chan struct{}
	once    sync.Once
	gets    int
}

func TestLayerViewCommitmentSplitKeyOwnsInputs(t *testing.T) {
	buf := New(rawdb.NewMemoryDatabase())
	buf.BeginBlock(bufHash(1), 1)
	h, ok := buf.NewestInflight()
	if !ok {
		t.Fatal("missing in-flight layer")
	}
	view := buf.ViewLayer(h)
	prefix := []byte{1, 2, 3, 4}
	wantPrefix := append([]byte(nil), prefix...)
	value := []byte{5, 6, 7, 8}
	wantValue := append([]byte(nil), value...)
	if err := rawdb.WriteCommitmentBranch(view, prefix, value); err != nil {
		t.Fatal(err)
	}
	clear(prefix)
	clear(value)
	got, found, err := rawdb.ReadCommitmentBranch(view, wantPrefix)
	if err != nil || !found || !bytes.Equal(got, wantValue) {
		t.Fatalf("split-key read = (%x,%v,%v), want (%x,true,nil)", got, found, err, wantValue)
	}
	if err := rawdb.DeleteCommitmentBranch(view, wantPrefix); err != nil {
		t.Fatal(err)
	}
	if _, found, err := rawdb.ReadCommitmentBranch(view, wantPrefix); err != nil || found {
		t.Fatalf("split-key read after delete = (found=%v, err=%v), want false/nil", found, err)
	}
}

func (r *blockingSnapshotReader) Get(key []byte) ([]byte, error) {
	r.gets++
	value, err := r.KeyValueReader.Get(key)
	// Preserve the exact base generation observed before the flush, even if the
	// test backend's Get happens to return an internal alias.
	value = append([]byte(nil), value...)
	r.once.Do(func() {
		close(r.started)
		<-r.release
	})
	return value, err
}

func (r *blockingSnapshotReader) View(key []byte, fn func([]byte) error) error {
	r.gets++
	value, err := r.KeyValueReader.Get(key)
	value = append([]byte(nil), value...)
	r.once.Do(func() {
		close(r.started)
		<-r.release
	})
	if err != nil {
		return err
	}
	return fn(value)
}

func (r *countingKeyValueReader) Get(key []byte) ([]byte, error) {
	r.gets++
	return r.KeyValueReader.Get(key)
}

func (r *countingKeyValueReader) View(key []byte, fn func([]byte) error) error {
	r.gets++
	r.views++
	value, err := r.KeyValueReader.Get(key)
	if err != nil {
		return err
	}
	return fn(value)
}

// TestGetNoCopy_MatchesGet is the correctness gate for the commitment-fold
// read-path optimization: GetNoCopy must return byte-identical content to Get
// (so the decoded branch — and thus the state root — is unchanged), the only
// difference being that GetNoCopy aliases internal storage instead of copying.
// Tombstones and the layered stack must behave identically.
func TestGetNoCopy_MatchesGet(t *testing.T) {
	b := New(nil)
	b.BeginBlock(common.Hash{}, 1)

	key := []byte("state-commitment-branch-prefix")
	val := bytes.Repeat([]byte{0xab}, 1500) // ~1.5 KB, a realistic branch row
	if err := b.Put(key, val); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := b.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	ncp, err := b.GetNoCopy(key)
	if err != nil {
		t.Fatalf("GetNoCopy: %v", err)
	}
	if !bytes.Equal(got, val) || !bytes.Equal(ncp, val) {
		t.Fatalf("content mismatch: Get=%x... GetNoCopy=%x...", got[:8], ncp[:8])
	}
	// Get must defensively copy; GetNoCopy must alias buffer storage.
	if len(got) > 0 && len(ncp) > 0 && &got[0] == &ncp[0] {
		t.Fatal("Get returned an alias, expected a copy")
	}

	// Sealed-layer read (after CommitBlock the write lives in b.layers).
	b.CommitBlock()
	if ncp, err = b.GetNoCopy(key); err != nil || !bytes.Equal(ncp, val) {
		t.Fatalf("GetNoCopy from sealed layer: %v / %x", err, ncp)
	}

	// Tombstone short-circuits identically to Get.
	b.BeginBlock(common.Hash{}, 2)
	if err := b.Delete(key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := b.GetNoCopy(key); err != ErrNotFound {
		t.Fatalf("GetNoCopy after tombstone = %v, want ErrNotFound", err)
	}
}

func benchBuffer() (*Buffer, []byte) {
	b := New(nil)
	b.BeginBlock(common.Hash{}, 1)
	key := []byte("state-commitment-branch-prefix-hot")
	_ = b.Put(key, bytes.Repeat([]byte{0xcd}, 1500))
	return b, key
}

// BenchmarkBufferGet / BenchmarkBufferGetNoCopy isolate the per-read copy: Get
// allocates the ~1.5 KB value every call (the fold's dominant read-side alloc),
// GetNoCopy is allocation-free. This is the win the commitment branch reads get.
func BenchmarkBufferGet(b *testing.B) {
	buf, key := benchBuffer()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, _ := buf.Get(key)
		_ = v
	}
}

func BenchmarkBufferGetNoCopy(b *testing.B) {
	buf, key := benchBuffer()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, _ := buf.GetNoCopy(key)
		_ = v
	}
}

// TestLayerViewGetNoCopy_MatchesScopedGet is the async-commit counterpart of
// TestGetNoCopy_MatchesGet. It pins both byte identity and LayerView's scoped
// visibility: own in-flight layer > committed layers > base, while a newer
// foreground in-flight layer remains invisible.
func TestLayerViewGetNoCopy_MatchesScopedGet(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	if err := base.Put([]byte("base"), []byte("base-value")); err != nil {
		t.Fatal(err)
	}
	b := New(base)
	b.SetMaxInflight(2)

	// A committed value visible below the worker-owned layer.
	b.BeginBlock(bufHash(1), 1)
	committedValue := bytes.Repeat([]byte{0xa1}, 1500)
	if err := b.Put([]byte("committed"), committedValue); err != nil {
		t.Fatal(err)
	}
	h1, _ := b.NewestInflight()
	if err := b.CommitInflight(h1); err != nil {
		t.Fatal(err)
	}

	// The view is bound to block 2. A newer foreground block 3 writes the same
	// key, but the view must continue to see block 2.
	b.BeginBlock(bufHash(2), 2)
	workerValue := bytes.Repeat([]byte{0xb2}, 1500)
	if err := b.Put([]byte("scoped"), workerValue); err != nil {
		t.Fatal(err)
	}
	h2, _ := b.NewestInflight()
	b.BeginBlock(bufHash(3), 3)
	if err := b.Put([]byte("scoped"), []byte("newer-foreground")); err != nil {
		t.Fatal(err)
	}
	view := b.ViewLayer(h2)

	got, err := view.Get([]byte("scoped"))
	if err != nil {
		t.Fatal(err)
	}
	ncp, err := view.GetNoCopy([]byte("scoped"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, workerValue) || !bytes.Equal(ncp, workerValue) {
		t.Fatal("LayerView.GetNoCopy changed scoped read visibility or bytes")
	}
	if &got[0] == &ncp[0] {
		t.Fatal("LayerView.Get returned an alias, expected a defensive copy")
	}

	if ncp, err = view.GetNoCopy([]byte("committed")); err != nil || !bytes.Equal(ncp, committedValue) {
		t.Fatalf("committed GetNoCopy = (%x,%v)", ncp, err)
	}
	if ncp, err = view.GetNoCopy([]byte("base")); err != nil || !bytes.Equal(ncp, []byte("base-value")) {
		t.Fatalf("base GetNoCopy = (%q,%v)", ncp, err)
	}

	// A tombstone in the bound layer masks lower committed/base values.
	if err := view.Delete([]byte("committed")); err != nil {
		t.Fatal(err)
	}
	if _, err := view.GetNoCopy([]byte("committed")); err != ErrNotFound {
		t.Fatalf("GetNoCopy after bound tombstone = %v, want ErrNotFound", err)
	}
}

func benchLayerView() (*LayerView, []byte) {
	b := New(nil)
	b.SetMaxInflight(2)
	b.BeginBlock(bufHash(1), 1)
	key := []byte("state-commitment-branch-prefix-hot")
	_ = b.Put(key, bytes.Repeat([]byte{0xcd}, 1500))
	h, _ := b.NewestInflight()
	return b.ViewLayer(h), key
}

func BenchmarkLayerViewGet(b *testing.B) {
	view, key := benchLayerView()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, _ := view.Get(key)
		_ = v
	}
}

func BenchmarkLayerViewGetNoCopy(b *testing.B) {
	view, key := benchLayerView()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, _ := view.GetNoCopy(key)
		_ = v
	}
}

func benchLayerViewBaseRead(cached bool) (*LayerView, []byte) {
	disk := rawdb.NewMemoryDatabase()
	key := []byte("state-commitment-branch-prefix-cold")
	_ = disk.Put(key, bytes.Repeat([]byte{0xef}, 1500))
	buf := New(disk)
	buf.SetMaxInflight(2)
	if cached {
		buf.SetBaseReadCacheSize(1 << 20)
	}
	buf.BeginBlock(bufHash(1), 1)
	h, _ := buf.NewestInflight()
	view := buf.ViewLayer(h)
	if cached {
		_, _ = view.GetNoCopyCached(key) // populate outside the timed region
	}
	return view, key
}

func BenchmarkLayerViewBaseGetNoCopy(b *testing.B) {
	view, key := benchLayerViewBaseRead(false)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, _ := view.GetNoCopy(key)
		_ = v
	}
}

func BenchmarkLayerViewBaseGetNoCopyCached(b *testing.B) {
	view, key := benchLayerViewBaseRead(true)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, _ := view.GetNoCopyCached(key)
		_ = v
	}
}

// TestBaseReadCache_GenerationLifecycle pins the cache's correctness across the
// lifecycle transitions that can otherwise expose stale commitment branches:
// overlay writes, durable flush, fork discard, durable delete, and full reset.
func TestBaseReadCache_GenerationLifecycle(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	key := []byte("state-commitment-branch-cache-key")
	oldValue := bytes.Repeat([]byte{0x11}, 1500)
	if err := disk.Put(key, oldValue); err != nil {
		t.Fatal(err)
	}
	base := &countingKeyValueReader{KeyValueReader: disk}
	b := New(base)
	b.SetBaseReadCacheSize(1 << 20)

	// First durable read populates; second read is served without touching base.
	mustCached := func(want []byte) {
		t.Helper()
		got, err := b.GetNoCopyCached(key)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("GetNoCopyCached = (%x,%v), want %x", got, err, want)
		}
	}
	mustCached(oldValue)
	mustCached(oldValue)
	if base.gets != 1 {
		t.Fatalf("base gets after cache hit = %d, want 1", base.gets)
	}
	if base.views != 1 {
		t.Fatalf("base callback views after cache fill = %d, want 1", base.views)
	}

	// An orphan overlay wins while present; discarding it reveals the still-valid
	// durable cache entry without another base read.
	b.BeginBlock(bufHash(1), 1)
	orphanValue := bytes.Repeat([]byte{0x22}, 1500)
	if err := b.Put(key, orphanValue); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()
	mustCached(orphanValue)
	b.DiscardBlock(bufHash(1))
	mustCached(oldValue)
	if base.gets != 1 {
		t.Fatalf("fork discard invalidated unchanged base: gets=%d, want 1", base.gets)
	}

	// A flushed canonical write changes the durable generation. Because the key
	// was already cached, Flush refreshes it directly from the immutable layer
	// value and the next read does not round-trip through the durable base.
	b.BeginBlock(bufHash(2), 2)
	newValue := bytes.Repeat([]byte{0x33}, 1500)
	if err := b.Put(key, newValue); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()
	if err := b.FlushUpTo(2, disk); err != nil {
		t.Fatal(err)
	}
	mustCached(newValue)
	if base.gets != 1 {
		t.Fatalf("base gets after flushed overwrite = %d, want 1", base.gets)
	}

	// A flushed tombstone also invalidates. Missing values are deliberately not
	// cached, so the durable reader supplies the not-found result.
	b.BeginBlock(bufHash(3), 3)
	if err := b.Delete(key); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()
	if err := b.FlushUpTo(3, disk); err != nil {
		t.Fatal(err)
	}
	if _, err := b.GetNoCopyCached(key); err == nil {
		t.Fatal("flushed delete returned stale cached value")
	}

	// Discard clears cached durable values before callers perform an out-of-band
	// reset/unwind of the underlying database.
	resetOld := bytes.Repeat([]byte{0x44}, 1500)
	if err := disk.Put(key, resetOld); err != nil {
		t.Fatal(err)
	}
	mustCached(resetOld)
	b.Discard()
	resetNew := bytes.Repeat([]byte{0x55}, 1500)
	if err := disk.Put(key, resetNew); err != nil {
		t.Fatal(err)
	}
	mustCached(resetNew)
}

func TestBaseReadCache_FlatLatestLifecycle(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x7a}
	key := []byte("hot-slot")
	oldValue := []byte("old")
	newValue := []byte("new")
	domain := kvdomains.ContractStorage
	if err := rawdb.WriteStateKVLatest(disk, owner, 0, domain, key, oldValue); err != nil {
		t.Fatal(err)
	}
	base := &countingKeyValueReader{KeyValueReader: disk}
	b := New(base)
	b.SetBaseReadCacheSize(1 << 20)

	read := func(want []byte, wantExists bool) []byte {
		t.Helper()
		got, exists, err := rawdb.ReadStateKVLatest(b, owner, 0, domain, key)
		if err != nil || exists != wantExists || !bytes.Equal(got, want) {
			t.Fatalf("ReadStateKVLatest = (%q,%v,%v), want (%q,%v,nil)", got, exists, err, want, wantExists)
		}
		return got
	}

	first := read(oldValue, true)
	first[0] = 'X'
	read(oldValue, true)
	if base.gets != 1 {
		t.Fatalf("base gets after cached flat-latest read = %d, want 1", base.gets)
	}
	if base.views != 1 {
		t.Fatalf("base callback views after flat-latest cache fill = %d, want 1", base.views)
	}

	b.BeginBlock(bufHash(1), 1)
	if err := rawdb.WriteStateKVLatest(b, owner, 0, domain, key, newValue); err != nil {
		t.Fatal(err)
	}
	read(newValue, true)
	b.CommitBlock()
	if err := b.FlushUpTo(1, disk); err != nil {
		t.Fatal(err)
	}
	read(newValue, true)
	if base.gets != 1 {
		t.Fatalf("base gets after flat-latest flush refresh = %d, want 1", base.gets)
	}

	b.BeginBlock(bufHash(2), 2)
	if err := rawdb.DeleteStateKVLatest(b, owner, 0, domain, key); err != nil {
		t.Fatal(err)
	}
	read(nil, false)
	b.CommitBlock()
	if err := b.FlushUpTo(2, disk); err != nil {
		t.Fatal(err)
	}
	read(nil, false)
	if base.gets != 2 {
		t.Fatalf("base gets after flat-latest delete = %d, want 2", base.gets)
	}
}

// TestBaseReadCache_RejectsLateFillAfterFlush exercises the narrow race where
// a durable-base read captures the old value, then a flush invalidates that key
// before the read returns. The old value may satisfy the already-started read,
// but its stale cache fill must be rejected for all subsequent reads.
func TestBaseReadCache_RejectsLateFillAfterFlush(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	key := []byte("state-commitment-branch-racing-fill")
	oldValue := bytes.Repeat([]byte{0x61}, 1500)
	newValue := bytes.Repeat([]byte{0x62}, 1500)
	if err := disk.Put(key, oldValue); err != nil {
		t.Fatal(err)
	}
	base := &blockingSnapshotReader{
		KeyValueReader: disk,
		started:        make(chan struct{}),
		release:        make(chan struct{}),
	}
	b := New(base)
	b.SetBaseReadCacheSize(1 << 20)

	type readResult struct {
		value []byte
		err   error
	}
	result := make(chan readResult, 1)
	go func() {
		value, err := b.GetNoCopyCached(key)
		result <- readResult{value: value, err: err}
	}()
	<-base.started

	b.BeginBlock(bufHash(1), 1)
	if err := b.Put(key, newValue); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()
	if err := b.FlushUpTo(1, disk); err != nil {
		t.Fatal(err)
	}
	close(base.release)
	first := <-result
	if first.err != nil || !bytes.Equal(first.value, oldValue) {
		t.Fatalf("in-flight old-generation read = (%x,%v)", first.value, first.err)
	}

	got, err := b.GetNoCopyCached(key)
	if err != nil || !bytes.Equal(got, newValue) {
		t.Fatalf("read after racing flush = (%x,%v), want new durable value", got, err)
	}
	if base.gets != 2 {
		t.Fatalf("base gets after rejected late fill = %d, want 2", base.gets)
	}
	if _, err := b.GetNoCopyCached(key); err != nil {
		t.Fatal(err)
	}
	if base.gets != 2 {
		t.Fatalf("new generation did not populate cache: gets=%d, want 2", base.gets)
	}
}

func BenchmarkBaseReadCacheColdFill(b *testing.B) {
	disk, err := rawdb.NewPebbleDB(b.TempDir(), 16, 16)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { disk.Close() })
	key := []byte("cold-cache-fill")
	value := bytes.Repeat([]byte{0xa5}, 4096)
	if err := disk.Put(key, value); err != nil {
		b.Fatal(err)
	}

	run := func(b *testing.B, base ethdb.KeyValueReader) {
		buf := New(base)
		buf.SetBaseReadCacheSize(1 << 20)
		b.ReportAllocs()
		b.SetBytes(int64(len(value)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.baseReadCache.del(key)
			got, err := buf.GetNoCopyCached(key)
			if err != nil || len(got) != len(value) {
				b.Fatalf("GetNoCopyCached = %d bytes, %v", len(got), err)
			}
		}
	}
	b.Run("copying-get", func(b *testing.B) {
		run(b, getOnlyReader{KeyValueReader: disk})
	})
	b.Run("callback-view", func(b *testing.B) {
		run(b, disk)
	})
}

func BenchmarkCommitmentBranchLayerWrite(b *testing.B) {
	prefix := bytes.Repeat([]byte{0x0a}, 32)
	value := bytes.Repeat([]byte{0xcd}, 256)
	for _, variant := range []struct {
		name     string
		fallback bool
	}{
		{name: "joined-key-fallback", fallback: true},
		{name: "split-key-fast-path"},
	} {
		b.Run(variant.name, func(b *testing.B) {
			buf := New(rawdb.NewMemoryDatabase())
			buf.BeginBlock(bufHash(1), 1)
			h, ok := buf.NewestInflight()
			if !ok {
				b.Fatal("missing in-flight layer")
			}
			var writer ethdb.KeyValueWriter = buf.ViewLayer(h)
			if variant.fallback {
				writer = keyValueWriterOnly{KeyValueWriter: writer}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := rawdb.WriteCommitmentBranch(writer, prefix, value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
