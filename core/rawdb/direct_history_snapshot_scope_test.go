package rawdb

import (
	"bytes"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
)

type historyScopeFactory struct {
	ethdb.KeyValueStore
	calls  int
	create func() (pointread.KeyValueSnapshot, error)
}

func (d *historyScopeFactory) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	d.calls++
	if d.create != nil {
		return d.create()
	}
	return d.KeyValueStore.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
}

type historyScopeSnapshot struct {
	pointread.KeyValueSnapshot
	pinned   bool
	closed   int
	closeErr error
}

func (s *historyScopeSnapshot) IsPinnedKeyValueView() bool { return s.pinned }
func (s *historyScopeSnapshot) Close() error {
	s.closed++
	return errors.Join(s.KeyValueSnapshot.Close(), s.closeErr)
}

func TestDirectHistoryBuildSnapshotScopeAndIsolation(t *testing.T) {
	dir := t.TempDir()
	hotBase := directTestPebble(t, filepath.Join(dir, "hot"))
	histBase := directTestPebble(t, filepath.Join(dir, "history"))
	hot := &historyScopeFactory{KeyValueStore: hotBase}
	hist := &historyScopeFactory{KeyValueStore: histBase}
	db := NewDirectHistoryStore(hot, hist)
	t.Cleanup(func() { _ = db.Close() })
	var keys [][]byte
	for _, r := range DirectHistoryRanges() {
		key := bytes.Clone(r.Start)
		if r.Name != "state_history_index_progress" {
			key = append(key, 1)
		}
		keys = append(keys, key)
		directTestPut(t, db, key, []byte("before"))
	}
	directTestPut(t, db, headBlockKey, []byte("hot-before"))
	view, release, err := AcquireStateHistoryBuildReadView(WrapKeyValueStore(db))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if release != nil {
			_ = release()
		}
	})
	if hot.calls != 0 || hist.calls != 1 {
		t.Fatal("cold-source acquisition pinned hot or lost history snapshot")
	}
	for _, key := range keys {
		directTestPut(t, db, key, []byte("after"))
		got, e := view.Get(key)
		if e != nil || !bytes.Equal(got, []byte("before")) {
			t.Fatalf("snapshot changed %v", e)
		}
	}
	if err := histBase.Compact(nil, nil); err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		got, e := view.Get(key)
		if e != nil || !bytes.Equal(got, []byte("before")) {
			t.Fatalf("snapshot changed across compaction: %v", e)
		}
	}
	for _, key := range [][]byte{nil, headBlockKey, stateAccountLatestPrefix, append(bytes.Clone(stageProgressKey(StageStateHistoryIndex)), 'x')} {
		if _, err := view.Get(key); !errors.Is(err, ErrStateHistorySnapshotScope) {
			t.Fatalf("hot Get silently accepted: %v", err)
		}
		if _, err := view.Has(key); !errors.Is(err, ErrStateHistorySnapshotScope) {
			t.Fatalf("hot Has silently accepted: %v", err)
		}
	}
	seeker := view.(pointread.PrefixSeeker)
	for _, prefix := range [][]byte{nil, []byte("state-"), stateAccountLatestPrefix, stageProgressKey(StageStateHistoryIndex)} {
		it := view.NewIterator(prefix, nil)
		if it.Next() || !errors.Is(it.Error(), ErrStateHistorySnapshotScope) {
			t.Fatal("mixed/hot iterator accepted")
		}
		it.Release()
		if _, _, _, e := seeker.SeekPrefix(prefix, nil); !errors.Is(e, ErrStateHistorySnapshotScope) {
			t.Fatal("mixed/hot prefix seek accepted")
		}
	}
	for _, r := range DirectHistoryRanges() {
		if r.Name == "state_history_index_progress" {
			continue
		}
		it := view.NewIterator(r.Start, nil)
		if !it.Next() || !bytes.Equal(it.Value(), []byte("before")) {
			t.Fatal("history iterator lost old value")
		}
		if it.Next() || it.Error() != nil {
			t.Fatal("history iterator range changed")
		}
		it.Release()
		key, value, ok, e := seeker.SeekPrefix(r.Start, nil)
		if e != nil || !ok || !bytes.Equal(value, []byte("before")) || !bytes.HasPrefix(key, r.Start) {
			t.Fatal("history seeker changed")
		}
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	release = nil
	// Ordinary callers retain the original dual snapshot and hot-key contract.
	general, e := db.NewKeyValueSnapshot()
	if e != nil {
		t.Fatal(e)
	}
	got, e := general.Get(headBlockKey)
	if e != nil || !bytes.Equal(got, []byte("hot-before")) {
		t.Fatal("general snapshot lost hot read")
	}
	if e = general.Close(); e != nil {
		t.Fatal(e)
	}
	if hot.calls != 1 || hist.calls != 2 {
		t.Fatal("general snapshot no longer pins both stores")
	}
}

func TestDirectHistoryBuildSnapshotErrorsDoNotFallBack(t *testing.T) {
	failure := errors.New("history snapshot failure")
	closeFailure := errors.New("history close failure")
	for _, mode := range []string{"factory", "unsupported", "nil", "unpinned", "close"} {
		t.Run(mode, func(t *testing.T) {
			hot := &historyScopeFactory{KeyValueStore: NewMemoryDatabase()}
			base := directTestPebble(t, filepath.Join(t.TempDir(), "history"))
			hist := &historyScopeFactory{KeyValueStore: base}
			db := NewDirectHistoryStore(hot, hist)
			t.Cleanup(func() { _ = db.Close() })
			var captured *historyScopeSnapshot
			hist.create = func() (pointread.KeyValueSnapshot, error) {
				switch mode {
				case "factory":
					return nil, failure
				case "unsupported":
					return nil, pointread.ErrKeyValueSnapshotUnsupported
				case "nil":
					return nil, nil
				}
				s, e := base.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
				if e != nil {
					return nil, e
				}
				captured = &historyScopeSnapshot{KeyValueSnapshot: s, pinned: mode == "close", closeErr: closeFailure}
				return captured, nil
			}
			view, release, err := AcquireStateHistoryBuildReadView(WrapKeyValueStore(db))
			if hot.calls != 0 || hist.calls != 1 {
				t.Fatal("failed scoped factory fell back to general acquisition")
			}
			switch mode {
			case "close":
				if err != nil || view == nil {
					t.Fatal(err)
				}
				if err := release(); !errors.Is(err, closeFailure) {
					t.Fatal("close error lost")
				}
				if captured.closed != 1 {
					t.Fatal("snapshot cleanup repeated")
				}
			case "factory":
				if !errors.Is(err, failure) {
					t.Fatal(err)
				}
			case "unsupported":
				if !errors.Is(err, pointread.ErrKeyValueSnapshotUnsupported) {
					t.Fatal("unsupported factory became moving source")
				}
			case "nil":
				if err == nil || view != nil || release != nil {
					t.Fatal("nil factory result accepted")
				}
			case "unpinned":
				if err == nil || !errors.Is(err, closeFailure) || captured.closed != 1 {
					t.Fatal("unpinned view or cleanup failure accepted")
				}
			}
		})
	}
}

func TestStateHistoryBuildViewGenericSingleStoreAndBorrow(t *testing.T) {
	base := directTestPebble(t, filepath.Join(t.TempDir(), "single"))
	t.Cleanup(func() { _ = base.Close() })
	directTestPut(t, base, headBlockKey, []byte("old"))
	view, release, err := AcquireStateHistoryBuildReadView(WrapKeyValueStore(base))
	if err != nil {
		t.Fatal(err)
	}
	directTestPut(t, base, headBlockKey, []byte("new"))
	borrowed, closeBorrow, err := AcquireStateHistoryBuildReadView(view)
	if err != nil {
		t.Fatal(err)
	}
	if err := closeBorrow(); err != nil {
		t.Fatal(err)
	}
	got, err := borrowed.Get(headBlockKey)
	if err != nil || !bytes.Equal(got, []byte("old")) {
		t.Fatal("generic/borrowed snapshot lost original semantics")
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
}

func TestDirectHistoryBuildSnapshotWaitsForMixedDurability(t *testing.T) {
	var events []string
	hot := directTestPebble(t, filepath.Join(t.TempDir(), "hot"))
	hist := &directRecordingDB{KeyValueStore: directTestPebble(t, filepath.Join(t.TempDir(), "history")), name: "history", events: &events, writeEntered: make(chan struct{}), writeRelease: make(chan struct{})}
	db := NewDirectHistoryStore(hot, hist)
	defer db.Close()
	var once sync.Once
	unblock := func() { once.Do(func() { close(hist.writeRelease) }) }
	defer unblock()
	directTestPut(t, db, headBlockKey, []byte("old"))
	directTestPut(t, db, stateChangeSetPrefix, []byte("old"))
	old, release, err := AcquireStateHistoryBuildReadView(db)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	batch := db.NewBatch()
	defer batch.Close()
	directTestPut(t, batch, headBlockKey, []byte("new"))
	directTestPut(t, batch, stateChangeSetPrefix, []byte("new"))
	written := make(chan error, 1)
	go func() { written <- batch.Write() }()
	<-hist.writeEntered
	type acquired struct {
		view    StateHistoryReadView
		release func() error
		err     error
	}
	taken := make(chan acquired, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		v, r, e := AcquireStateHistoryBuildReadView(WrapKeyValueStore(db))
		taken <- acquired{v, r, e}
	}()
	<-started
	select {
	case got := <-taken:
		if got.release != nil {
			got.release()
		}
		t.Fatal("history snapshot crossed unfinished mixed durability boundary")
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	current := <-taken
	if current.err != nil {
		t.Fatal(current.err)
	}
	defer current.release()
	directTestRead(t, old, stateChangeSetPrefix, []byte("old"))
	directTestRead(t, current.view, stateChangeSetPrefix, []byte("new"))
	directTestRead(t, hot, headBlockKey, []byte("new"))
}

// Only the history snapshot's explicit flags may reach the restricted view.
// A capable hot store must not supply missing/false history capabilities.
type historyScopeCapabilities struct {
	pointread.KeyValueSnapshot
	owned, concurrent bool
}

func (*historyScopeCapabilities) IsPinnedKeyValueView() bool          { return true }
func (s *historyScopeCapabilities) GetReturnsOwnedBytes() bool        { return s.owned }
func (s *historyScopeCapabilities) ConcurrentOwnedHistoryReads() bool { return s.concurrent }

func TestDirectHistoryBuildSnapshotCapabilityNegatives(t *testing.T) {
	for _, mode := range []string{"unknown", "false", "owned-only"} {
		t.Run(mode, func(t *testing.T) {
			hot := directTestPebble(t, filepath.Join(t.TempDir(), "hot"))
			base := directTestPebble(t, filepath.Join(t.TempDir(), "history"))
			hist := &historyScopeFactory{KeyValueStore: base}
			db := NewDirectHistoryStore(hot, hist)
			defer db.Close()
			hist.create = func() (pointread.KeyValueSnapshot, error) {
				s, e := base.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
				if e != nil {
					return nil, e
				}
				if mode == "unknown" {
					return &historyScopeSnapshot{KeyValueSnapshot: s, pinned: true}, nil
				}
				return &historyScopeCapabilities{KeyValueSnapshot: s, owned: mode == "owned-only"}, nil
			}
			view, release, e := AcquireStateHistoryBuildReadView(WrapKeyValueStore(db))
			if e != nil {
				t.Fatal(e)
			}
			defer release()
			capability, ok := view.(pointread.ConcurrentOwnedKeyValueView)
			if !ok || !capability.IsPinnedKeyValueView() {
				t.Fatal("dedicated snapshot lost pinned marker")
			}
			if capability.GetReturnsOwnedBytes() != (mode == "owned-only") || capability.ConcurrentOwnedHistoryReads() {
				t.Fatal("hot capability or unknown history flags were promoted")
			}
		})
	}
}
