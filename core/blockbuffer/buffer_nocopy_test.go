package blockbuffer

import (
	"bytes"
	"strconv"
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

var benchmarkCommitmentLayerSink *layer

// cachedLayerReaderOnly preserves rawdb's generic cached no-copy extension but
// intentionally hides the structured state-latest extension. Benchmarks use it
// to isolate only the physical-key construction change.
type cachedLayerReaderOnly struct {
	view *LayerView
}

func (r cachedLayerReaderOnly) Get(key []byte) ([]byte, error) {
	return r.view.Get(key)
}

func (r cachedLayerReaderOnly) Has(key []byte) (bool, error) {
	return r.view.Has(key)
}

func (r cachedLayerReaderOnly) GetNoCopyCached(key []byte) ([]byte, error) {
	return r.view.GetNoCopyCached(key)
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

func TestCommitmentSplitReadLifecycle(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	prefix := bytes.Repeat([]byte{0x0a}, 48)
	oldValue := []byte("old-branch")
	newValue := []byte("new-branch")
	if err := rawdb.WriteCommitmentBranch(disk, prefix, oldValue); err != nil {
		t.Fatal(err)
	}
	base := &countingKeyValueReader{KeyValueReader: disk}
	buf := New(base)
	buf.SetBaseReadCacheSize(1 << 20)

	read := func(reader ethdb.KeyValueReader, want []byte, wantFound bool) {
		t.Helper()
		got, found, err := rawdb.ReadCommitmentBranchNoCopy(reader, prefix)
		if err != nil || found != wantFound || !bytes.Equal(got, want) {
			t.Fatalf("ReadCommitmentBranchNoCopy = (%q,%v,%v), want (%q,%v,nil)", got, found, err, want, wantFound)
		}
	}

	// Two durable reads complete admission and the next split-key read hits it.
	read(buf, oldValue, true)
	read(buf, oldValue, true)
	read(buf, oldValue, true)
	if base.gets != 2 {
		t.Fatalf("base reads after split cache hit = %d, want 2", base.gets)
	}

	// A bound LayerView must prefer its own overlay and tombstone over both the
	// committed topology and the already-populated durable cache.
	buf.BeginBlock(bufHash(1), 1)
	h, _ := buf.NewestInflight()
	view := buf.ViewLayer(h)
	if err := rawdb.WriteCommitmentBranch(view, prefix, newValue); err != nil {
		t.Fatal(err)
	}
	read(view, newValue, true)
	if err := rawdb.DeleteCommitmentBranch(view, prefix); err != nil {
		t.Fatal(err)
	}
	read(view, nil, false)
	if base.gets != 2 {
		t.Fatalf("overlay split reads reached base: %d reads, want 2", base.gets)
	}

	// Replacing the tombstone and flushing refreshes the cached durable value.
	if err := rawdb.WriteCommitmentBranch(view, prefix, newValue); err != nil {
		t.Fatal(err)
	}
	if err := buf.CommitInflight(h); err != nil {
		t.Fatal(err)
	}
	if err := buf.FlushUpTo(1, disk); err != nil {
		t.Fatal(err)
	}
	read(buf, newValue, true)
	if base.gets != 2 {
		t.Fatalf("flushed split read did not use refreshed cache: %d reads, want 2", base.gets)
	}
}

func TestCommitmentSplitViewReportsTransientBaseAndStableCacheOverlay(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	prefix := bytes.Repeat([]byte{0x0b}, 32)
	baseValue := []byte("durable-branch")
	if err := rawdb.WriteCommitmentBranch(disk, prefix, baseValue); err != nil {
		t.Fatal(err)
	}
	base := &countingKeyValueReader{KeyValueReader: disk}
	buf := New(base)
	buf.SetBaseReadCacheSize(1 << 20)

	view := func(reader ethdb.KeyValueReader, want []byte, wantStable bool) {
		t.Helper()
		called := 0
		found, err := rawdb.ViewCommitmentBranchNoCopy(reader, prefix, func(encoded []byte, stable bool) error {
			called++
			if !bytes.Equal(encoded, want) || stable != wantStable {
				t.Fatalf("callback = (%q, stable=%v), want (%q, %v)", encoded, stable, want, wantStable)
			}
			return nil
		})
		if err != nil || !found || called != 1 {
			t.Fatalf("view = found %v called %d err %v, want true/1/nil", found, called, err)
		}
	}

	// First sighting remains on admission probation; the second is copied into
	// the cache and can safely outlive the base View callback. The third is a
	// cache hit and never reaches the base.
	view(buf, baseValue, false)
	view(buf, baseValue, true)
	view(buf, baseValue, true)
	if base.views != 2 {
		t.Fatalf("base views = %d, want 2", base.views)
	}

	buf.BeginBlock(bufHash(1), 1)
	h, _ := buf.NewestInflight()
	layerView := buf.ViewLayer(h)
	overlayValue := []byte("overlay-branch")
	if err := rawdb.WriteCommitmentBranch(layerView, prefix, overlayValue); err != nil {
		t.Fatal(err)
	}
	view(layerView, overlayValue, true)
	if base.views != 2 {
		t.Fatalf("overlay view reached base: %d views, want 2", base.views)
	}
}

func TestLayerViewCommitmentParentViewSkipsOwnLayer(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	prefix := bytes.Repeat([]byte{0x0c}, 32)
	basePrefix := bytes.Repeat([]byte{0x0d}, 32)
	if err := rawdb.WriteCommitmentBranch(disk, basePrefix, []byte("base")); err != nil {
		t.Fatal(err)
	}

	buf := New(disk)
	buf.BeginBlock(bufHash(1), 1)
	parentHandle, _ := buf.NewestInflight()
	parent := buf.ViewLayer(parentHandle)
	if err := rawdb.WriteCommitmentBranch(parent, prefix, []byte("parent")); err != nil {
		t.Fatal(err)
	}
	if err := buf.CommitInflight(parentHandle); err != nil {
		t.Fatal(err)
	}

	buf.BeginBlock(bufHash(2), 2)
	childHandle, _ := buf.NewestInflight()
	child := buf.ViewLayer(childHandle)
	if err := rawdb.WriteCommitmentBranch(child, prefix, []byte("child")); err != nil {
		t.Fatal(err)
	}

	read := func(p []byte) []byte {
		t.Helper()
		var got []byte
		found, err := rawdb.ViewCommitmentParentBranchNoCopy(child, p, func(value []byte, stable bool) error {
			if !stable {
				t.Fatal("memory/overlay parent value reported transient")
			}
			got = append(got, value...)
			return nil
		})
		if err != nil || !found {
			t.Fatalf("parent read %x = found %v err %v", p, found, err)
		}
		return got
	}

	if got := read(prefix); !bytes.Equal(got, []byte("parent")) {
		t.Fatalf("parent branch = %q, want parent", got)
	}
	if got := read(basePrefix); !bytes.Equal(got, []byte("base")) {
		t.Fatalf("durable parent branch = %q, want base", got)
	}
	ordinary, found, err := rawdb.ReadCommitmentBranchNoCopy(child, prefix)
	if err != nil || !found || !bytes.Equal(ordinary, []byte("child")) {
		t.Fatalf("ordinary child read = (%q,%v,%v), want child/true/nil", ordinary, found, err)
	}

	if err := rawdb.DeleteCommitmentBranch(child, prefix); err != nil {
		t.Fatal(err)
	}
	if got := read(prefix); !bytes.Equal(got, []byte("parent")) {
		t.Fatalf("parent branch through child tombstone = %q, want parent", got)
	}
}

func BenchmarkLayerViewCommitmentBranchRead(b *testing.B) {
	disk := rawdb.NewMemoryDatabase()
	buf := New(disk)
	prefix := bytes.Repeat([]byte{0x0e}, 48)
	value := bytes.Repeat([]byte{0x55}, 512)
	buf.BeginBlock(bufHash(1), 1)
	parentHandle, _ := buf.NewestInflight()
	if err := rawdb.WriteCommitmentBranch(buf.ViewLayer(parentHandle), prefix, value); err != nil {
		b.Fatal(err)
	}
	if err := buf.CommitInflight(parentHandle); err != nil {
		b.Fatal(err)
	}
	buf.BeginBlock(bufHash(2), 2)
	childHandle, _ := buf.NewestInflight()
	child := buf.ViewLayer(childHandle)
	consume := func([]byte, bool) error { return nil }

	b.Run("ordinary_own_miss_then_parent", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if found, err := rawdb.ViewCommitmentBranchNoCopy(child, prefix, consume); err != nil || !found {
				b.Fatalf("read = found %v err %v", found, err)
			}
		}
	})
	b.Run("parent_direct", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if found, err := rawdb.ViewCommitmentParentBranchNoCopy(child, prefix, consume); err != nil || !found {
				b.Fatalf("read = found %v err %v", found, err)
			}
		}
	})
}

func TestCommitmentSplitReadOversizedKey(t *testing.T) {
	prefix := bytes.Repeat([]byte{0x7b}, splitReadKeyStackSize)
	want := []byte("oversized-branch")
	disk := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteCommitmentBranch(disk, prefix, want); err != nil {
		t.Fatal(err)
	}
	buf := New(disk)
	buf.SetBaseReadCacheSize(1 << 20)
	buf.BeginBlock(bufHash(1), 1)
	h, _ := buf.NewestInflight()
	for name, reader := range map[string]ethdb.KeyValueReader{
		"buffer": buf,
		"view":   buf.ViewLayer(h),
	} {
		t.Run(name, func(t *testing.T) {
			got, found, err := rawdb.ReadCommitmentBranchNoCopy(reader, prefix)
			if err != nil || !found || !bytes.Equal(got, want) {
				t.Fatalf("oversized split read = (%q,%v,%v)", got, found, err)
			}
		})
	}
}

func TestStateKVLatestStructuredLifecycle(t *testing.T) {
	owner := common.BytesToAddress(bytes.Repeat([]byte{0x42}, common.AddressLength))
	generation := uint64(17)
	domain := kvdomains.ContractStorage
	oversizedKey := bytes.Repeat([]byte{0x7b}, splitReadKeyStackSize)

	for _, tc := range []struct {
		name string
		mode string
	}{
		{name: "buffer", mode: "buffer"},
		{name: "layer-view", mode: "layer-view"},
		{name: "buffer-batch", mode: "buffer-batch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logicalKey := bytes.Repeat([]byte{0x31}, 32)
			disk := rawdb.NewMemoryDatabase()
			if err := rawdb.WriteStateKVLatest(disk, owner, generation, domain, logicalKey, []byte("durable")); err != nil {
				t.Fatal(err)
			}
			if err := rawdb.WriteStateKVLatest(disk, owner, generation, domain, oversizedKey, []byte("oversized")); err != nil {
				t.Fatal(err)
			}
			base := &countingKeyValueReader{KeyValueReader: disk}
			buf := New(base)
			buf.SetBaseReadCacheSize(1 << 20)
			buf.BeginBlock(bufHash(1), 1)
			h, _ := buf.NewestInflight()

			var reader ethdb.KeyValueReader = buf
			var writer ethdb.KeyValueWriter = buf
			publish := func() {}
			switch tc.mode {
			case "layer-view":
				view := buf.ViewLayer(h)
				reader, writer = view, view
			case "buffer-batch":
				batch := buf.NewBatch()
				defer batch.Close()
				writer = batch
				publish = func() {
					t.Helper()
					if err := batch.Write(); err != nil {
						t.Fatal(err)
					}
					batch.Reset()
				}
			}

			read := func(key []byte, want string, wantFound bool) {
				t.Helper()
				got, found, err := rawdb.ReadStateKVLatest(reader, owner, generation, domain, key)
				if err != nil || found != wantFound || string(got) != want {
					t.Fatalf("ReadStateKVLatest = (%q,%v,%v), want (%q,%v,nil)", got, found, err, want, wantFound)
				}
			}

			// Two durable reads complete admission; the third structured read must
			// reconstruct the identical physical key and hit it.
			read(logicalKey, "durable", true)
			read(logicalKey, "durable", true)
			read(logicalKey, "durable", true)
			if base.gets != 2 {
				t.Fatalf("base reads after structured cache hit = %d, want 2", base.gets)
			}

			originalKey := append([]byte(nil), logicalKey...)
			wrapped := rawdb.EncodeStateKVLatestValue([]byte("overlay"))
			if err := rawdb.WriteStateKVLatestEncoded(writer, owner, generation, domain, logicalKey, wrapped); err != nil {
				t.Fatal(err)
			}
			logicalKey[0] ^= 0xff
			clear(wrapped)
			publish()
			read(originalKey, "overlay", true)
			read(logicalKey, "", false)
			if base.gets != 3 { // mutated key is a genuine one-time base miss
				t.Fatalf("base reads after overlay/mutated-key reads = %d, want 3", base.gets)
			}

			if err := rawdb.DeleteStateKVLatest(writer, owner, generation, domain, originalKey); err != nil {
				t.Fatal(err)
			}
			publish()
			read(originalKey, "", false)

			// Long logical keys use the owned fallback and must remain byte-exact.
			read(oversizedKey, "oversized", true)
			if base.gets != 4 {
				t.Fatalf("base reads after oversized key = %d, want 4", base.gets)
			}
		})
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

	// The first durable read records probation, the second admits the key, and
	// the third is served without touching base.
	mustCached := func(want []byte) {
		t.Helper()
		got, err := b.GetNoCopyCached(key)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("GetNoCopyCached = (%x,%v), want %x", got, err, want)
		}
	}
	mustCached(oldValue)
	mustCached(oldValue)
	mustCached(oldValue)
	if base.gets != 2 {
		t.Fatalf("base gets after cache hit = %d, want 2", base.gets)
	}
	if base.views != 2 {
		t.Fatalf("base callback views after cache fill = %d, want 2", base.views)
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
	if base.gets != 2 {
		t.Fatalf("fork discard invalidated unchanged base: gets=%d, want 2", base.gets)
	}

	// A flushed canonical write changes the durable generation. Because the key
	// was already cached, Flush refreshes it directly from the immutable layer
	// value and the next read does not round-trip through the durable base.
	b.BeginBlock(bufHash(2), 2)
	intermediateValue := bytes.Repeat([]byte{0x32}, 1500)
	if err := b.Put(key, intermediateValue); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()
	b.BeginBlock(bufHash(3), 3)
	newValue := bytes.Repeat([]byte{0x33}, 1500)
	if err := b.Put(key, newValue); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()
	if err := b.FlushUpTo(3, disk); err != nil {
		t.Fatal(err)
	}
	mustCached(newValue)
	if base.gets != 2 {
		t.Fatalf("base gets after flushed overwrite = %d, want 2", base.gets)
	}

	// A flushed tombstone also invalidates. Missing values are deliberately not
	// cached, so the durable reader supplies the not-found result.
	b.BeginBlock(bufHash(4), 4)
	if err := b.Put(key, bytes.Repeat([]byte{0x44}, 1500)); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()
	b.BeginBlock(bufHash(5), 5)
	if err := b.Delete(key); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()
	if err := b.FlushUpTo(5, disk); err != nil {
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
	read(oldValue, true)
	if base.gets != 2 {
		t.Fatalf("base gets after cached flat-latest read = %d, want 2", base.gets)
	}
	if base.views != 2 {
		t.Fatalf("base callback views after flat-latest cache fill = %d, want 2", base.views)
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
	if base.gets != 2 {
		t.Fatalf("base gets after flat-latest flush refresh = %d, want 2", base.gets)
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
	if base.gets != 3 {
		t.Fatalf("base gets after flat-latest delete = %d, want 3", base.gets)
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
	if base.gets != 3 {
		t.Fatalf("new generation did not complete admission: gets=%d, want 3", base.gets)
	}
	if _, err := b.GetNoCopyCached(key); err != nil {
		t.Fatal(err)
	}
	if base.gets != 3 {
		t.Fatalf("new generation was not cached after admission: gets=%d, want 3", base.gets)
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
	b.Run("scoped-callback-view", func(b *testing.B) {
		buf := New(disk)
		buf.SetBaseReadCacheSize(1 << 20)
		b.ReportAllocs()
		b.SetBytes(int64(len(value)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.baseReadCache.del(key)
			found, err := buf.ViewNoCopyCachedKeyParts(nil, key, func(got []byte, stable bool) error {
				if len(got) != len(value) || stable {
					b.Fatalf("ViewNoCopyCachedKeyParts = %d bytes stable=%v", len(got), stable)
				}
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
			if !found {
				b.Fatal("ViewNoCopyCachedKeyParts did not find value")
			}
		}
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

func BenchmarkCommitmentBranchLayerOwnedBatches(b *testing.B) {
	const batchCount = 16
	for _, batchSize := range []int{64, 256} {
		b.Run(strconv.Itoa(batchSize), func(b *testing.B) {
			prefix := []byte("state-commitment-branch-v1-")
			seconds := make([][]string, batchCount)
			values := make([][][]byte, batchCount)
			for batch := 0; batch < batchCount; batch++ {
				seconds[batch] = make([]string, batchSize)
				values[batch] = make([][]byte, batchSize)
				for i := 0; i < batchSize; i++ {
					seconds[batch][i] = string([]byte{byte(batch), byte(i >> 8), byte(i)})
					values[batch][i] = bytes.Repeat([]byte{byte(batch + 1)}, 256)
				}
			}
			buf := new(Buffer)

			b.Run("sequential", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					l := newLayer(common.Hash{}, 1)
					for batch := 0; batch < batchCount; batch++ {
						buf.putIntoKeyPartsStringsOwnedValues(l, prefix, seconds[batch], values[batch], batchCount)
					}
					benchmarkCommitmentLayerSink = l
				}
			})

			b.Run("parallel-siblings", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					l := newLayer(common.Hash{}, 1)
					var wg sync.WaitGroup
					wg.Add(batchCount)
					for batch := 0; batch < batchCount; batch++ {
						go func(batch int) {
							defer wg.Done()
							buf.putIntoKeyPartsStringsOwnedValues(l, prefix, seconds[batch], values[batch], batchCount)
						}(batch)
					}
					wg.Wait()
					benchmarkCommitmentLayerSink = l
				}
			})
		})
	}
}

func BenchmarkCommitmentBranchLayerSparseReservation(b *testing.B) {
	const batchSize = 64
	prefix := []byte("state-commitment-branch-v1-")
	seconds := make([]string, batchSize)
	values := make([][]byte, batchSize)
	for i := range seconds {
		seconds[i] = string([]byte{byte(i >> 8), byte(i)})
		values[i] = bytes.Repeat([]byte{byte(i + 1)}, 256)
	}
	buf := new(Buffer)
	for _, hint := range []int{1, 16} {
		b.Run("hint-"+strconv.Itoa(hint), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				l := newLayer(common.Hash{}, 1)
				buf.putIntoKeyPartsStringsOwnedValues(l, prefix, seconds, values, hint)
				benchmarkCommitmentLayerSink = l
			}
		})
	}
}

func TestCommitmentBranchLayerOwnedValueRetainsValueAndOwnsKey(t *testing.T) {
	buf := New(rawdb.NewMemoryDatabase())
	buf.BeginBlock(bufHash(1), 1)
	h, ok := buf.NewestInflight()
	if !ok {
		t.Fatal("missing in-flight layer")
	}
	view := buf.ViewLayer(h)
	prefix := []byte{0x0a, 0x0b, 0x0c}
	wantPrefix := append([]byte(nil), prefix...)
	value := []byte("fresh-branch-encoding")
	if err := rawdb.WriteCommitmentBranchOwned(view, prefix, value); err != nil {
		t.Fatal(err)
	}
	prefix[0] = 0xff
	got, found, err := rawdb.ReadCommitmentBranchNoCopy(view, wantPrefix)
	if err != nil || !found || !bytes.Equal(got, value) {
		t.Fatalf("owned branch read = (%q,%v,%v), want (%q,true,nil)", got, found, err, value)
	}
	if &got[0] != &value[0] {
		t.Fatal("blockbuffer copied the transferred branch value")
	}
	if _, found, err := rawdb.ReadCommitmentBranchNoCopy(view, prefix); err != nil || found {
		t.Fatalf("mutated caller prefix unexpectedly addressed branch: found=%v err=%v", found, err)
	}
}

func TestCommitmentBranchLayerOwnedBatchRetainsValues(t *testing.T) {
	buf := New(rawdb.NewMemoryDatabase())
	buf.BeginBlock(bufHash(1), 1)
	h, ok := buf.NewestInflight()
	if !ok {
		t.Fatal("missing in-flight layer")
	}
	view := buf.ViewLayer(h)
	prefixes := []string{string([]byte{0x01, 0x02}), string([]byte{0x03, 0x04, 0x05})}
	values := [][]byte{[]byte("first-branch"), []byte("second-branch")}
	if err := rawdb.WriteCommitmentBranchesOwnedStrings(view, prefixes, values); err != nil {
		t.Fatal(err)
	}
	for i, prefix := range prefixes {
		got, found, err := rawdb.ReadCommitmentBranchNoCopy(view, []byte(prefix))
		if err != nil || !found || !bytes.Equal(got, values[i]) {
			t.Fatalf("batch branch %d read = (%q,%v,%v), want (%q,true,nil)", i, got, found, err, values[i])
		}
		if &got[0] != &values[i][0] {
			t.Fatalf("batch branch %d copied the transferred value", i)
		}
	}
	if err := view.PutKeyPartsStringsOwnedValues([]byte("prefix"), prefixes, values[:1]); err == nil {
		t.Fatal("mismatched layer batch lengths were accepted")
	}
}

func TestCommitmentBranchLayerOwnedBatchPreservesLastWrite(t *testing.T) {
	buf := New(rawdb.NewMemoryDatabase())
	buf.BeginBlock(bufHash(1), 1)
	h, ok := buf.NewestInflight()
	if !ok {
		t.Fatal("missing in-flight layer")
	}
	view := buf.ViewLayer(h)
	prefixes := []string{"duplicate", "duplicate"}
	values := [][]byte{[]byte("first"), []byte("last")}
	if err := rawdb.WriteCommitmentBranchesOwnedStrings(view, prefixes, values); err != nil {
		t.Fatal(err)
	}
	got, found, err := rawdb.ReadCommitmentBranchNoCopy(view, []byte("duplicate"))
	if err != nil || !found || !bytes.Equal(got, values[1]) {
		t.Fatalf("duplicate branch read = (%q,%v,%v), want (%q,true,nil)", got, found, err, values[1])
	}
}

func TestCommitmentBranchLayerOwnedBatchesPublishConcurrently(t *testing.T) {
	const (
		batchCount = 16
		batchSize  = 128
	)
	buf := New(rawdb.NewMemoryDatabase())
	buf.BeginBlock(bufHash(1), 1)
	h, ok := buf.NewestInflight()
	if !ok {
		t.Fatal("missing in-flight layer")
	}
	view := buf.ViewLayer(h)
	prefixes := make([][]string, batchCount)
	values := make([][][]byte, batchCount)
	for batch := 0; batch < batchCount; batch++ {
		prefixes[batch] = make([]string, batchSize)
		values[batch] = make([][]byte, batchSize)
		for i := 0; i < batchSize; i++ {
			prefixes[batch][i] = string([]byte{byte(batch), byte(i)})
			values[batch][i] = []byte{byte(batch + 1), byte(i)}
		}
	}

	var (
		wg   sync.WaitGroup
		errs [batchCount]error
	)
	wg.Add(batchCount)
	for batch := 0; batch < batchCount; batch++ {
		go func(batch int) {
			defer wg.Done()
			errs[batch] = rawdb.WriteCommitmentBranchesOwnedStrings(view, prefixes[batch], values[batch])
		}(batch)
	}
	wg.Wait()
	for batch, err := range errs {
		if err != nil {
			t.Fatalf("batch %d write: %v", batch, err)
		}
	}
	for batch := 0; batch < batchCount; batch++ {
		for i, prefix := range prefixes[batch] {
			got, found, err := rawdb.ReadCommitmentBranchNoCopy(view, []byte(prefix))
			if err != nil || !found || !bytes.Equal(got, values[batch][i]) {
				t.Fatalf("batch %d branch %d read = (%x,%v,%v), want (%x,true,nil)",
					batch, i, got, found, err, values[batch][i])
			}
		}
	}
}

func BenchmarkCommitmentBranchLayerRead(b *testing.B) {
	prefix := bytes.Repeat([]byte{0x0a}, 48)
	value := bytes.Repeat([]byte{0xcd}, 512)

	b.Run("overlay-hit", func(b *testing.B) {
		buf := New(rawdb.NewMemoryDatabase())
		buf.BeginBlock(bufHash(1), 1)
		h, _ := buf.NewestInflight()
		view := buf.ViewLayer(h)
		if err := rawdb.WriteCommitmentBranch(view, prefix, value); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, ok, err := rawdb.ReadCommitmentBranchNoCopy(view, prefix); err != nil || !ok {
				b.Fatalf("branch read = ok:%v err:%v", ok, err)
			}
		}
	})

	b.Run("base-cache-hit", func(b *testing.B) {
		disk := rawdb.NewMemoryDatabase()
		if err := rawdb.WriteCommitmentBranch(disk, prefix, value); err != nil {
			b.Fatal(err)
		}
		buf := New(disk)
		buf.SetBaseReadCacheSize(1 << 20)
		buf.BeginBlock(bufHash(1), 1)
		h, _ := buf.NewestInflight()
		view := buf.ViewLayer(h)
		if _, ok, err := rawdb.ReadCommitmentBranchNoCopy(view, prefix); err != nil || !ok {
			b.Fatalf("warm branch read = ok:%v err:%v", ok, err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, ok, err := rawdb.ReadCommitmentBranchNoCopy(view, prefix); err != nil || !ok {
				b.Fatalf("branch read = ok:%v err:%v", ok, err)
			}
		}
	})
}

func BenchmarkStateKVLatestLayerRead(b *testing.B) {
	owner := common.BytesToAddress(bytes.Repeat([]byte{0x41}, common.AddressLength))
	logicalKey := bytes.Repeat([]byte{0x5a}, 32)
	value := bytes.Repeat([]byte{0xcd}, 32)
	const generation = uint64(23)
	domain := kvdomains.ContractStorage

	for _, location := range []string{"overlay-hit", "base-cache-hit"} {
		for _, structured := range []bool{false, true} {
			name := location + "/generic-key"
			if structured {
				name = location + "/structured-key"
			}
			b.Run(name, func(b *testing.B) {
				disk := rawdb.NewMemoryDatabase()
				buf := New(disk)
				buf.SetBaseReadCacheSize(1 << 20)
				buf.BeginBlock(bufHash(1), 1)
				h, _ := buf.NewestInflight()
				view := buf.ViewLayer(h)

				if location == "overlay-hit" {
					if err := rawdb.WriteStateKVLatest(view, owner, generation, domain, logicalKey, value); err != nil {
						b.Fatal(err)
					}
				} else if err := rawdb.WriteStateKVLatest(disk, owner, generation, domain, logicalKey, value); err != nil {
					b.Fatal(err)
				}

				var reader ethdb.KeyValueReader = view
				if !structured {
					reader = cachedLayerReaderOnly{view: view}
				}
				if _, ok, err := rawdb.ReadStateKVLatest(reader, owner, generation, domain, logicalKey); err != nil || !ok {
					b.Fatalf("warm latest read = ok:%v err:%v", ok, err)
				}

				b.ReportAllocs()
				b.SetBytes(int64(len(value)))
				b.ResetTimer()
				for range b.N {
					if _, ok, err := rawdb.ReadStateKVLatest(reader, owner, generation, domain, logicalKey); err != nil || !ok {
						b.Fatalf("latest read = ok:%v err:%v", ok, err)
					}
				}
			})
		}
	}
}

func BenchmarkStateKVLatestLayerWrite(b *testing.B) {
	owner := common.BytesToAddress(bytes.Repeat([]byte{0x41}, common.AddressLength))
	logicalKey := bytes.Repeat([]byte{0x5a}, 32)
	wrapped := rawdb.EncodeStateKVLatestValue(bytes.Repeat([]byte{0xcd}, 32))
	const generation = uint64(23)
	domain := kvdomains.ContractStorage

	for _, destination := range []string{"layer", "batch"} {
		for _, structured := range []bool{false, true} {
			name := destination + "/generic-key"
			if structured {
				name = destination + "/structured-key"
			}
			b.Run(name, func(b *testing.B) {
				buf := New(rawdb.NewMemoryDatabase())
				buf.BeginBlock(bufHash(1), 1)
				h, _ := buf.NewestInflight()
				var concrete ethdb.KeyValueWriter = buf.ViewLayer(h)
				reset := func() {}
				if destination == "batch" {
					batch := buf.NewBatch()
					defer batch.Close()
					concrete = batch
					reset = batch.Reset
				}
				var writer ethdb.KeyValueWriter = keyValueWriterOnly{KeyValueWriter: concrete}
				if structured {
					writer = concrete
				}
				b.ReportAllocs()
				b.SetBytes(int64(len(wrapped)))
				b.ResetTimer()
				for range b.N {
					if err := rawdb.WriteStateKVLatestEncoded(writer, owner, generation, domain, logicalKey, wrapped); err != nil {
						b.Fatal(err)
					}
					reset()
				}
			})
		}
	}
}
