package blockbuffer

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestStorageReadScopeFlushAndMutableLayer(t *testing.T) {
	disk, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	history, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer history.Close()
	db := rawdb.NewDirectHistoryStore(disk, history)
	if err := db.Put([]byte("replaced"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	b := New(db)
	b.SetBaseReadCacheSize(1 << 20)
	b.BeginBlock(bufHash(1), 1)
	for key, value := range map[string][]byte{"replaced": []byte("new"), "empty": {}} {
		if err := b.Put([]byte(key), value); err != nil {
			t.Fatal(err)
		}
	}
	b.CommitBlock()
	b.BeginBlock(bufHash(2), 2)
	s, err := b.TryStorageReadScope(2)
	if err != nil || s == nil {
		t.Fatalf("scope %v/%v", s, err)
	}
	defer s.Close()
	if err := b.FlushUpTo(1, db); err != nil {
		t.Fatal(err)
	}
	if len(b.loadReadView().layers) != 0 {
		t.Fatal("old layer not removed")
	}
	for _, key := range []string{"replaced", "empty", "absent"} {
		want, wantErr := b.Get([]byte(key)) // old live route, independently resolved
		got, gotErr := s.Get([]byte(key))
		if !bytes.Equal(got, want) || !errors.Is(gotErr, wantErr) {
			t.Fatalf("%s: scope %q/%v live %q/%v", key, got, gotErr, want, wantErr)
		}
	}
	if err := b.Put([]byte("replaced"), []byte("current")); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get([]byte("replaced")); err != nil || string(got) != "current" {
		t.Fatalf("current %q/%v", got, err)
	}
	if err := b.Delete([]byte("replaced")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get([]byte("replaced")); err != ErrNotFound {
		t.Fatalf("delete %v", err)
	}
	b.DiscardActive()
	if _, err := s.Get([]byte("absent")); !errors.Is(err, errStorageReadScope) {
		t.Fatalf("discard %v", err)
	}
}

func TestStorageReadScopeBusyDoesNotQueue(t *testing.T) {
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b := New(db)
	b.BeginBlock(bufHash(1), 1)
	b.flushMu.Lock()
	defer b.flushMu.Unlock()
	done := make(chan error, 1)
	go func() {
		s, err := b.TryStorageReadScope(1)
		if s != nil {
			_ = s.Close()
			err = errors.New("busy scope opened")
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("scope queued behind flushMu")
	}
}

type storageFailCursor struct {
	calls   int
	failure error
}

func (c *storageFailCursor) View(_ []byte, fn func([]byte) error) (bool, error) {
	c.calls++
	return true, errors.Join(fn([]byte("borrowed")), c.failure)
}
func (c *storageFailCursor) Close() error { return c.failure }

func TestStorageReadScopeFailedReleaseNeverCaches(t *testing.T) {
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b := New(db)
	b.SetBaseReadCacheSize(1 << 20)
	b.BeginBlock(bufHash(1), 1)
	s, err := b.TryStorageReadScope(1)
	if err != nil || s == nil {
		t.Fatalf("scope %v/%v", s, err)
	}
	if err := s.cursor.Close(); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("release failure")
	cursor := &storageFailCursor{failure: failure}
	s.cursor = cursor
	for i := 0; i < 4; i++ {
		if _, err := s.Get([]byte("key")); !errors.Is(err, failure) {
			t.Fatalf("read %v", err)
		}
	}
	if cursor.calls != 4 {
		t.Fatalf("failed read cached: calls=%d", cursor.calls)
	}
	if err := s.Close(); !errors.Is(err, failure) {
		t.Fatalf("close %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotCacheFillRequiresCapturedVersionAndEpoch(t *testing.T) {
	c := newBaseReadCache(1 << 20)
	key := []byte("key")
	_, _, epoch, _ := c.getAtVersion(key, 0)
	version := c.version.Load()
	c.advanceVersion()
	if _, ok := c.setSnapshotEntry(key, []byte("old"), false, epoch, version); ok {
		t.Fatal("old snapshot filled new cache")
	}
	_, _, epoch, _ = c.getAtVersion(key, c.version.Load())
	c.invalidations[epoch.slot].Add(1)
	if _, ok := c.setSnapshotEntry(key, []byte("old"), false, epoch, c.version.Load()); ok {
		t.Fatal("late fill ignored key epoch")
	}
}

var _ pointread.Cursor = (*storageFailCursor)(nil)

type storageScopeFactoryError struct {
	ethdb.KeyValueStore
	failure error
}

func (s *storageScopeFactoryError) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	return nil, s.failure
}

func TestStorageReadScopeUnsupportedMustNotHideCleanupError(t *testing.T) {
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cleanup := errors.New("failed already-open hot snapshot cleanup")
	for _, failure := range []error{pointread.ErrKeyValueSnapshotUnsupported, errors.Join(pointread.ErrKeyValueSnapshotUnsupported, cleanup)} {
		b := New(&storageScopeFactoryError{KeyValueStore: db, failure: failure})
		b.BeginBlock(bufHash(1), 1)
		s, err := b.TryStorageReadScope(1)
		if s != nil {
			t.Fatal("unexpected scope")
		}
		if errors.Is(failure, cleanup) {
			if !errors.Is(err, cleanup) {
				t.Fatalf("lost cleanup failure: %v", err)
			}
		} else if err != nil {
			t.Fatalf("pure unsupported did not fall back: %v", err)
		}
	}
}
