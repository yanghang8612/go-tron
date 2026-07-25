package pebbledb

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
)

var benchmarkPointReadValue []byte

func TestPointReadSnapshotIsStable(t *testing.T) {
	db, err := New(filepath.Join(t.TempDir(), "snapshot"), 16, 16, "", false, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	prefix := []byte("branch/")
	key := append(append([]byte(nil), prefix...), "a"...)
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
	cursor, err := snapshot.NewCursor(prefix)
	if err != nil {
		t.Fatal(err)
	}
	called := 0
	found, err := cursor.View(key, func(value []byte) error {
		called++
		if string(value) != "before" {
			t.Fatalf("snapshot value = %q, want before", value)
		}
		return nil
	})
	if err != nil || !found || called != 1 {
		t.Fatalf("View = found %v called %d err %v", found, called, err)
	}
	missing := append(append([]byte(nil), prefix...), "missing"...)
	if found, err := cursor.View(missing, func([]byte) error { t.Fatal("called for missing key"); return nil }); err != nil || found {
		t.Fatalf("missing View = found %v err %v", found, err)
	}
	outside := []byte("other/a")
	if err := db.Put(outside, []byte("outside")); err != nil {
		t.Fatal(err)
	}
	if found, err := cursor.View(outside, func([]byte) error { t.Fatal("called outside cursor prefix"); return nil }); err != nil || found {
		t.Fatalf("outside View = found %v err %v", found, err)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkPebblePointReadStrategy(b *testing.B) {
	const entries = 1 << 16
	db, err := pebble.Open(filepath.Join(b.TempDir(), "point-read"), &pebble.Options{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	keys := make([][]byte, entries)
	batch := db.NewBatch()
	for i := range keys {
		key := make([]byte, 64)
		copy(key, "state-commitment-branch-v1-")
		binary.BigEndian.PutUint64(key[len(key)-8:], uint64(i))
		keys[i] = key
		value := bytes.Repeat([]byte{byte(i)}, 530)
		if err := batch.Set(key, value, nil); err != nil {
			b.Fatal(err)
		}
	}
	if err := batch.Commit(nil); err != nil {
		b.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		b.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		b.Fatal(err)
	}

	b.Run("get", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			key := keys[(uint64(i)*2654435761)&(entries-1)]
			value, closer, err := db.Get(key)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkPointReadValue = value
			if err := closer.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("get-sorted", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			key := keys[i&(entries-1)]
			value, closer, err := db.Get(key)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkPointReadValue = value
			if err := closer.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("reused-iterator", func(b *testing.B) {
		iter, err := db.NewIter(nil)
		if err != nil {
			b.Fatal(err)
		}
		defer iter.Close()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			key := keys[(uint64(i)*2654435761)&(entries-1)]
			if !iter.SeekGE(key) || !bytes.Equal(iter.Key(), key) {
				b.Fatal("key not found")
			}
			benchmarkPointReadValue = iter.Value()
		}
	})
	b.Run("reused-iterator-sorted", func(b *testing.B) {
		iter, err := db.NewIter(nil)
		if err != nil {
			b.Fatal(err)
		}
		defer iter.Close()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			key := keys[i&(entries-1)]
			if !iter.SeekGE(key) || !bytes.Equal(iter.Key(), key) {
				b.Fatal("key not found")
			}
			benchmarkPointReadValue = iter.Value()
		}
	})
}
