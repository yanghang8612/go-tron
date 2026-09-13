package blockbuffer

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type historyChunkAtomicWriter interface {
	StateHistoryChunkWritesAtomic() bool
}

type historyChunkReaderOnly struct{ ethdb.KeyValueReader }

type historyChunkSnapshotFailure struct {
	ethdb.KeyValueReader
	err error
}

func (s historyChunkSnapshotFailure) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	return nil, s.err
}

func TestStateHistoryChunkWritesAtomicRequiresActiveBatchedBase(t *testing.T) {
	var nilBuffer *Buffer
	if nilBuffer.StateHistoryChunkWritesAtomic() || New(nil).StateHistoryChunkWritesAtomic() {
		t.Fatal("nil buffer or base advertised atomic history writes")
	}
	base := newHistoryChunkBaseTest(t)
	b := New(base)
	if b.StateHistoryChunkWritesAtomic() {
		t.Fatal("buffer without an active layer advertised atomic history writes")
	}
	b.BeginBlock(bufHash(1), 1)
	if !b.StateHistoryChunkWritesAtomic() {
		t.Fatal("active layer over a batched base did not advertise atomic history writes")
	}
	h, ok := b.NewestInflight()
	if !ok {
		t.Fatal("missing active layer")
	}
	if _, ok := any(b.ViewLayer(h)).(historyChunkAtomicWriter); ok {
		t.Fatal("bound LayerView must not advertise the canonical write lifetime")
	}
	if _, ok := any(b.NewBatch()).(historyChunkAtomicWriter); ok {
		t.Fatal("ordinary buffer batch must not advertise the canonical write lifetime")
	}
	b.CommitBlock()
	if b.StateHistoryChunkWritesAtomic() {
		t.Fatal("committed-only buffer advertised active history writes")
	}
	b.BeginBlock(bufHash(2), 2)
	b.DiscardActive()
	if b.StateHistoryChunkWritesAtomic() {
		t.Fatal("discarded active layer advertised atomic history writes")
	}
	nonBatching := New(historyChunkReaderOnly{base})
	nonBatching.BeginBlock(bufHash(3), 3)
	if nonBatching.StateHistoryChunkWritesAtomic() {
		t.Fatal("reader-only base advertised atomic history writes")
	}
	memoryBase := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { _ = memoryBase.Close() })
	noSnapshots := New(memoryBase)
	noSnapshots.BeginBlock(bufHash(4), 4)
	if noSnapshots.StateHistoryChunkWritesAtomic() {
		t.Fatal("batched base without snapshots advertised resolvable history chunks")
	}
	for _, tc := range []struct {
		name string
		base ethdb.KeyValueStore
		want bool
	}{
		{"memory chain", rawdb.NewChainDB(memoryBase, rawdb.NoopAncient{}), false},
		{"memory geth wrapper", rawdb.WrapKeyValueStore(memoryBase), false},
		{"nested memory wrappers", rawdb.NewChainDB(rawdb.WrapKeyValueStore(memoryBase), rawdb.NoopAncient{}), false},
		{"pebble chain", rawdb.NewChainDB(base, rawdb.NoopAncient{}), true},
		{"pebble geth wrapper", rawdb.WrapKeyValueStore(base), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := New(tc.base)
			b.BeginBlock(bufHash(5), 5)
			if got := b.StateHistoryChunkWritesAtomic(); got != tc.want {
				t.Fatalf("wrapped base capability = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBufferKeyValueSnapshotPinsHistoryOverlayAcrossFlushAndDelete(t *testing.T) {
	base, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = base.Close() })
	if err := base.Put([]byte("history/base"), []byte("base-before")); err != nil {
		t.Fatal(err)
	}
	b := New(base)
	b.BeginBlock(bufHash(1), 1)
	putHistoryChunkTest(t, b, "history/chunk/1", "first chunk")
	putHistoryChunkTest(t, b, "history/pack/1", "history/chunk/1")
	b.CommitBlock()
	b.BeginBlock(bufHash(2), 2)
	putHistoryChunkTest(t, b, "history/chunk/2", "second chunk")
	putHistoryChunkTest(t, b, "history/pack/2", "history/chunk/2")

	// Capture only after the foreground has published complete immutable packs.
	// No code below mutates either captured layer after this capture boundary.
	var factory pointread.KeyValueSnapshotter = b
	snapshot, err := factory.NewKeyValueSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snapshot.Close() })
	if marker, ok := snapshot.(interface{ IsPinnedKeyValueView() bool }); !ok || !marker.IsPinnedKeyValueView() {
		t.Fatal("acquired snapshot is missing its pinned-view marker")
	}
	if _, ok := any(b).(interface{ IsPinnedKeyValueView() bool }); ok {
		t.Fatal("live buffer must not advertise a pinned view")
	}
	h, _ := b.NewestInflight()
	if _, ok := any(b.ViewLayer(h)).(pointread.KeyValueSnapshotter); ok {
		t.Fatal("LayerView must not silently snapshot a different overlay topology")
	}
	b.CommitBlock()
	if err := b.FlushUpTo(2, base); err != nil {
		t.Fatal(err)
	}
	deleted := base.NewBatch()
	for _, key := range []string{"history/chunk/1", "history/chunk/2", "history/pack/1", "history/pack/2"} {
		if err := deleted.Delete([]byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	if err := deleted.Put([]byte("history/base"), []byte("base-after")); err != nil {
		t.Fatal(err)
	}
	if err := deleted.Write(); err != nil {
		t.Fatal(err)
	}
	b.BeginBlock(bufHash(3), 3)
	putHistoryChunkTest(t, b, "history/pack/1", "future replacement")
	putHistoryChunkTest(t, b, "history/future", "future only")
	for key, want := range map[string]string{
		"history/base": "base-before", "history/chunk/1": "first chunk", "history/chunk/2": "second chunk",
		"history/pack/1": "history/chunk/1", "history/pack/2": "history/chunk/2",
	} {
		got, err := snapshot.Get([]byte(key))
		if err != nil || string(got) != want {
			t.Fatalf("snapshot.Get(%q) = %q, %v; want %q", key, got, err, want)
		}
	}
	if exists, err := snapshot.Has([]byte("history/future")); err != nil || exists {
		t.Fatalf("snapshot future presence = %v, %v", exists, err)
	}
	it := snapshot.NewIterator([]byte("history/"), nil)
	defer it.Release()
	rows := 0
	for it.Next() {
		rows++
		if bytes.Equal(it.Key(), []byte("history/future")) {
			t.Fatal("snapshot iterator included a future layer")
		}
	}
	if err := it.Error(); err != nil || rows != 5 {
		t.Fatalf("snapshot iterator rows = %d, err = %v; want 5", rows, err)
	}
}

func TestBufferKeyValueSnapshotDistinguishesUnsupportedFromFailure(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { _ = base.Close() })
	for name, b := range map[string]*Buffer{"nil": nil, "missing base": New(nil), "no factory": New(base)} {
		t.Run(name, func(t *testing.T) {
			snapshot, err := b.NewKeyValueSnapshot()
			if snapshot != nil || !errors.Is(err, ErrReadSnapshotUnsupported) || !errors.Is(err, pointread.ErrKeyValueSnapshotUnsupported) {
				t.Fatalf("unsupported snapshot = %v, %v", snapshot, err)
			}
		})
	}
	for _, failure := range []error{errors.New("closed snapshot factory"), ErrReadSnapshotUnsupported} {
		b := New(historyChunkSnapshotFailure{KeyValueReader: base, err: failure})
		snapshot, err := b.NewKeyValueSnapshot()
		if snapshot != nil || err != failure || errors.Is(err, pointread.ErrKeyValueSnapshotUnsupported) {
			t.Fatalf("supported factory failure was translated: snapshot = %v, err = %v", snapshot, err)
		}
	}
}

func TestBufferSnapshotsPreserveWrappedMemoryFallback(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { _ = base.Close() })
	for name, wrapped := range map[string]ethdb.KeyValueStore{
		"chain":  rawdb.NewChainDB(base, rawdb.NoopAncient{}),
		"geth":   rawdb.WrapKeyValueStore(base),
		"nested": rawdb.NewChainDB(rawdb.WrapKeyValueStore(base), rawdb.NoopAncient{}),
	} {
		t.Run(name, func(t *testing.T) {
			b := New(wrapped)
			b.BeginBlock(bufHash(1), 1)
			b.CommitBlock()
			for method, capture := range map[string]func() (pointread.KeyValueSnapshot, error){
				"read":    func() (pointread.KeyValueSnapshot, error) { return b.NewReadSnapshot() },
				"through": func() (pointread.KeyValueSnapshot, error) { return b.NewReadSnapshotThrough(1) },
				"factory": b.NewKeyValueSnapshot,
			} {
				t.Run(method, func(t *testing.T) {
					_, err := capture()
					if !errors.Is(err, ErrReadSnapshotUnsupported) || !errors.Is(err, pointread.ErrKeyValueSnapshotUnsupported) {
						t.Fatalf("wrapped unsupported snapshot did not preserve both fallback sentinels: %v", err)
					}
				})
			}
		})
	}
}

func TestBufferSnapshotsDoNotTranslateFactoryFailure(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { _ = base.Close() })
	failure := errors.New("closed snapshot factory")
	b := New(historyChunkSnapshotFailure{KeyValueReader: base, err: failure})
	for method, capture := range map[string]func() (pointread.KeyValueSnapshot, error){
		"read":    func() (pointread.KeyValueSnapshot, error) { return b.NewReadSnapshot() },
		"through": func() (pointread.KeyValueSnapshot, error) { return b.NewReadSnapshotThrough(1) },
		"factory": b.NewKeyValueSnapshot,
	} {
		t.Run(method, func(t *testing.T) {
			_, err := capture()
			if err != failure || errors.Is(err, ErrReadSnapshotUnsupported) || errors.Is(err, pointread.ErrKeyValueSnapshotUnsupported) {
				t.Fatalf("real snapshot failure was translated: %v", err)
			}
		})
	}
}

func TestHistoryChunkScopeReadsAncestorInflightAndFlushesInOrder(t *testing.T) {
	base := newHistoryChunkBaseTest(t)
	b := New(base)
	b.SetMaxInflight(2)
	b.BeginBlock(bufHash(1), 1)
	putHistoryChunkTest(t, b, "history/chunk", "shared bytes")
	putHistoryChunkTest(t, b, "history/pack/1", "history/chunk")
	ancestor, _ := b.NewestInflight()
	b.BeginBlock(bufHash(2), 2)
	if !b.StateHistoryChunkWritesAtomic() {
		t.Fatal("descendant active layer cannot stage history")
	}
	mustGet(t, b, []byte("history/chunk"), []byte("shared bytes"))
	putHistoryChunkTest(t, b, "history/pack/2", "history/chunk")
	descendant, _ := b.NewestInflight()
	if err := b.CommitInflight(descendant); err == nil {
		t.Fatal("descendant promoted ahead of its chunk-owning ancestor")
	}
	if err := b.CommitInflight(ancestor); err != nil {
		t.Fatal(err)
	}
	if err := b.FlushUpTo(1, base); err != nil {
		t.Fatal(err)
	}
	if exists, err := base.Has([]byte("history/pack/2")); err != nil || exists {
		t.Fatalf("uncommitted descendant reached base: %v, %v", exists, err)
	}
	if err := b.CommitInflight(descendant); err != nil {
		t.Fatal(err)
	}
	if err := b.FlushUpTo(2, base); err != nil {
		t.Fatal(err)
	}
	assertHistoryChunkReferenceTest(t, base, "history/pack/2", "shared bytes")
}

func TestHistoryChunkScopeFailureAndRewindDiscardDependentSuffix(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("committed=%v", committed), func(t *testing.T) {
			base := newHistoryChunkBaseTest(t)
			b := New(base)
			b.SetMaxInflight(2)
			b.BeginBlock(bufHash(1), 1)
			putHistoryChunkTest(t, b, "history/chunk/old", "old branch")
			putHistoryChunkTest(t, b, "history/pack/1", "history/chunk/old")
			ancestor, _ := b.NewestInflight()
			b.BeginBlock(bufHash(2), 2)
			putHistoryChunkTest(t, b, "history/pack/2", "history/chunk/old")
			descendant, _ := b.NewestInflight()
			if committed {
				if err := b.CommitInflight(ancestor); err != nil {
					t.Fatal(err)
				}
				if err := b.CommitInflight(descendant); err != nil {
					t.Fatal(err)
				}
				b.DiscardBlock(bufHash(2))
				b.DiscardBlock(bufHash(1))
			} else {
				// Match core's failCommit + ordered finishCommitJob cleanup.
				// Buffer deliberately does not invent this dependency policy.
				b.DiscardInflight(ancestor)
				b.DiscardInflight(descendant)
			}
			if err := b.FlushUpTo(2, base); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"history/chunk/old", "history/pack/1", "history/pack/2"} {
				if exists, err := b.Has([]byte(key)); err != nil || exists {
					t.Fatalf("discarded key %q remains visible: %v, %v", key, exists, err)
				}
			}
			b.BeginBlock(bufHash(3), 1)
			putHistoryChunkTest(t, b, "history/chunk/new", "replacement branch")
			putHistoryChunkTest(t, b, "history/pack/1", "history/chunk/new")
			b.CommitBlock()
			if err := b.FlushUpTo(1, base); err != nil {
				t.Fatal(err)
			}
			assertHistoryChunkReferenceTest(t, base, "history/pack/1", "replacement branch")
		})
	}
}

func TestHistoryChunkScopeFailedGroupRetainsChunksAndReferences(t *testing.T) {
	base := newHistoryChunkBaseTest(t)
	b := New(base)
	b.BeginBlock(bufHash(1), 1)
	putHistoryChunkTest(t, b, "history/chunk", "shared bytes")
	putHistoryChunkTest(t, b, "history/pack/1", "history/chunk")
	b.CommitBlock()
	b.BeginBlock(bufHash(2), 2)
	putHistoryChunkTest(t, b, "history/pack/2", "history/chunk")
	b.CommitBlock()
	if err := b.FlushUpTo(2, &failingBatcher{KeyValueStore: base}); !errors.Is(err, errForcedBatchWrite) {
		t.Fatalf("failed group error = %v", err)
	}
	if len(b.PendingBlocks()) != 2 {
		t.Fatal("failed group lost its retryable layers")
	}
	assertHistoryChunkReferenceTest(t, b, "history/pack/2", "shared bytes")
	for _, key := range []string{"history/chunk", "history/pack/1", "history/pack/2"} {
		if exists, err := base.Has([]byte(key)); err != nil || exists {
			t.Fatalf("failed group partially persisted %q: %v, %v", key, exists, err)
		}
	}
	if err := b.FlushUpTo(2, base); err != nil {
		t.Fatal(err)
	}
	assertHistoryChunkReferenceTest(t, base, "history/pack/1", "shared bytes")
	assertHistoryChunkReferenceTest(t, base, "history/pack/2", "shared bytes")
}

func newHistoryChunkBaseTest(t *testing.T) ethdb.KeyValueStore {
	t.Helper()
	base, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = base.Close() })
	return base
}

func putHistoryChunkTest(t *testing.T, writer ethdb.KeyValueWriter, key, value string) {
	t.Helper()
	if err := writer.Put([]byte(key), []byte(value)); err != nil {
		t.Fatal(err)
	}
}

func assertHistoryChunkReferenceTest(t *testing.T, reader ethdb.KeyValueReader, pack, want string) {
	t.Helper()
	chunkKey, err := reader.Get([]byte(pack))
	if err != nil {
		t.Fatal(err)
	}
	value, err := reader.Get(chunkKey)
	if err != nil || string(value) != want {
		t.Fatalf("resolved %q = %q, %v; want %q", pack, value, err, want)
	}
}
