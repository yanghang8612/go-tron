package pebbledb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/tronprotocol/go-tron/core/pointread"
)

func TestExactPointComparerReopensDefaultDatabase(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "reopen")
	old, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := old.Set([]byte("existing-key"), []byte("existing-value"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := New(dir, 16, 16, "test/reopen/", false, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	value, err := db.Get([]byte("existing-key"))
	if err != nil || !bytes.Equal(value, []byte("existing-value")) {
		t.Fatalf("reopened value = (%q,%v)", value, err)
	}
}

func TestPebbleV1ReopensSharedV2BridgeFormat(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "bridge")
	old, err := pebble.Open(dir, &pebble.Options{Comparer: exactPointComparer})
	if err != nil {
		t.Fatal(err)
	}
	if err := old.Set([]byte("bridge-key"), []byte("bridge-value"), pebble.Sync); err != nil {
		old.Close()
		t.Fatal(err)
	}
	if err := old.RatchetFormatMajorVersion(pebble.FormatFlushableIngest); err != nil {
		old.Close()
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := New(dir, 16, 16, "test/bridge-reopen/", false, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	value, err := db.Get([]byte("bridge-key"))
	if err != nil || !bytes.Equal(value, []byte("bridge-value")) {
		t.Fatalf("bridge value = (%q,%v)", value, err)
	}
}

func TestGetWithPresence(t *testing.T) {
	db, err := New(t.TempDir(), 16, 16, "test/get-with-presence/", false, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("staged-body")
	if value, present, err := db.GetWithPresence(key); err != nil || present || value != nil {
		t.Fatalf("missing value = (%q,%v,%v), want nil/false/nil", value, present, err)
	}
	if err := db.Put(key, []byte("block")); err != nil {
		t.Fatal(err)
	}
	value, present, err := db.GetWithPresence(key)
	if err != nil || !present || !bytes.Equal(value, []byte("block")) {
		t.Fatalf("present value = (%q,%v,%v), want block/true/nil", value, present, err)
	}
	if err := db.Delete(key); err != nil {
		t.Fatal(err)
	}
	if value, present, err := db.GetWithPresence(key); err != nil || present || value != nil {
		t.Fatalf("deleted value = (%q,%v,%v), want nil/false/nil", value, present, err)
	}
}

func TestPointReadSnapshotCursorIsStableAndExact(t *testing.T) {
	db, err := New(t.TempDir(), 16, 16, "test/snapshot/", false, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	key := []byte("branch/a")
	if err := db.Put(key, []byte("before")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.NewPointReadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(key, []byte("after")); err != nil {
		t.Fatal(err)
	}
	cursor, err := snapshot.NewCursor([]byte("branch/"))
	if err != nil {
		t.Fatal(err)
	}
	var got []byte
	found, err := cursor.View(key, func(value []byte) error {
		got = append(got, value...)
		return nil
	})
	if err != nil || !found || !bytes.Equal(got, []byte("before")) {
		t.Fatalf("snapshot value = (%q,%v,%v)", got, found, err)
	}
	if found, err := cursor.View([]byte("branch/missing"), func([]byte) error {
		t.Fatal("callback invoked for missing exact key")
		return nil
	}); err != nil || found {
		t.Fatalf("missing exact key = (%v,%v)", found, err)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestKeyValueSnapshotPinsPointAndIteratorReads(t *testing.T) {
	db, err := New(t.TempDir(), 16, 16, "test/kv-snapshot/", false, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put([]byte("state/a"), []byte("before-a")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("state/b"), []byte("before-b")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.NewKeyValueSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	if err := db.Put([]byte("state/a"), []byte("after-a")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete([]byte("state/b")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("state/c"), []byte("after-c")); err != nil {
		t.Fatal(err)
	}

	if got, err := snapshot.Get([]byte("state/a")); err != nil || !bytes.Equal(got, []byte("before-a")) {
		t.Fatalf("snapshot point value = (%q,%v), want before-a", got, err)
	}
	if ok, err := snapshot.Has([]byte("state/b")); err != nil || !ok {
		t.Fatalf("snapshot deleted-key presence = (%v,%v), want true/nil", ok, err)
	}
	if ok, err := snapshot.Has([]byte("state/c")); err != nil || ok {
		t.Fatalf("snapshot future-key presence = (%v,%v), want false/nil", ok, err)
	}

	it := snapshot.NewIterator([]byte("state/"), nil)
	defer it.Release()
	var keys []string
	var values [][]byte
	for it.Next() {
		keys = append(keys, string(it.Key()))
		values = append(values, append([]byte(nil), it.Value()...))
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if got, want := fmt.Sprint(keys), "[state/a state/b]"; got != want {
		t.Fatalf("snapshot iterator keys = %s, want %s", got, want)
	}
	if !bytes.Equal(values[0], []byte("before-a")) || !bytes.Equal(values[1], []byte("before-b")) {
		t.Fatalf("snapshot iterator values = %q", values)
	}
}

func TestPointReadSnapshotReservedCursorsAreConcurrentAndExact(t *testing.T) {
	db, err := New(t.TempDir(), 16, 16, "test/reserved-snapshot/", false, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const readers = 16
	for i := 0; i < readers; i++ {
		key := []byte{0x71, byte(i)}
		if err := db.Put(key, []byte{byte(i + 1)}); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := db.NewPointReadSnapshotWithCapacity(readers)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	var wg sync.WaitGroup
	errs := make(chan error, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cursor, err := snapshot.NewCursor([]byte{0x71})
			if err != nil {
				errs <- err
				return
			}
			defer cursor.Close()
			found, err := cursor.View([]byte{0x71, byte(i)}, func(value []byte) error {
				if len(value) != 1 || value[0] != byte(i+1) {
					return fmt.Errorf("reader %d value %x", i, value)
				}
				return nil
			})
			if err != nil || !found {
				errs <- fmt.Errorf("reader %d found=%v: %w", i, found, err)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	reserved := snapshot.(*pointReadSnapshot)
	if reserved.nextReserved != readers || len(reserved.reservedBounds) != readers*2 {
		t.Fatalf("reserved cursors/bounds = %d/%d, want %d/%d", reserved.nextReserved, len(reserved.reservedBounds), readers, readers*2)
	}
}

func BenchmarkPebblePointReadSnapshotCursorLifecycle(b *testing.B) {
	db, err := New(b.TempDir(), 16, 16, "bench/snapshot/", false, Options{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	const readers = 16
	prefix := []byte("state-commitment-branch-v1-")
	for _, reserved := range []bool{false, true} {
		name := "unreserved"
		if reserved {
			name = "reserved"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var snapshot pointread.Snapshot
				var err error
				if reserved {
					snapshot, err = db.NewPointReadSnapshotWithCapacity(readers)
				} else {
					snapshot, err = db.NewPointReadSnapshot()
				}
				if err != nil {
					b.Fatal(err)
				}
				for range readers {
					cursor, err := snapshot.NewCursor(prefix)
					if err != nil {
						b.Fatal(err)
					}
					if err := cursor.Close(); err != nil {
						b.Fatal(err)
					}
				}
				if err := snapshot.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestPointReadViewLifecycle(t *testing.T) {
	db, err := New(t.TempDir(), 16, 16, "test/", false, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	view, err := db.NewPointReadView()
	if err != nil {
		t.Fatal(err)
	}
	var got string
	if err := view.Get([]byte("key"), func(value []byte) error {
		got = string(value)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got != "value" {
		t.Fatalf("value = %q, want value", got)
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	if err := view.Get([]byte("key"), func([]byte) error { return nil }); err == nil {
		t.Fatal("closed view accepted a read")
	}
}

func TestPointReadViewBlocksDatabaseClose(t *testing.T) {
	db, err := New(t.TempDir(), 16, 16, "test/", false, Options{})
	if err != nil {
		t.Fatal(err)
	}
	view, err := db.NewPointReadView()
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Database.Close returned with active point view: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Database.Close stayed blocked after point view closed")
	}
}

func BenchmarkPebblePointReadView(b *testing.B) {
	db, err := New(b.TempDir(), 16, 16, "bench/", false, Options{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	const keyCount = 4096
	keys := make([][]byte, keyCount)
	for i := range keys {
		keys[i] = make([]byte, 32)
		binary.BigEndian.PutUint64(keys[i][24:], uint64(i))
		if err := db.Put(keys[i], keys[i]); err != nil {
			b.Fatal(err)
		}
	}
	if err := db.db.Flush(); err != nil {
		b.Fatal(err)
	}
	consume := func([]byte) error { return nil }
	b.Run("per_read_lock", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := db.View(keys[i&(keyCount-1)], consume); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("shared_view", func(b *testing.B) {
		view, err := db.NewPointReadView()
		if err != nil {
			b.Fatal(err)
		}
		defer view.Close()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := view.Get(keys[i&(keyCount-1)], consume); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("parallel_per_read_lock", func(b *testing.B) {
		var next atomic.Uint64
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				i := next.Add(1)
				if err := db.View(keys[i&(keyCount-1)], consume); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
	b.Run("parallel_shared_view", func(b *testing.B) {
		view, err := db.NewPointReadView()
		if err != nil {
			b.Fatal(err)
		}
		defer view.Close()
		var next atomic.Uint64
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				i := next.Add(1)
				if err := view.Get(keys[i&(keyCount-1)], consume); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}
