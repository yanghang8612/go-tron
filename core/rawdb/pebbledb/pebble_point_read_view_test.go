package pebbledb

import (
	"encoding/binary"
	"sync/atomic"
	"testing"
	"time"
)

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
