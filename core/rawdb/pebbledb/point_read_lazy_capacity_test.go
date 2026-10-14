package pebbledb

import (
	"bytes"
	"fmt"
	"testing"
	"time"
	"unsafe"
)

func TestPointReadSnapshotUnusedCapacityHoldsLease(t *testing.T) {
	db, err := New(t.TempDir(), 16, 16, "test/unused-snapshot/", false, Options{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.NewPointReadSnapshotWithCapacity(80)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("closed with unused pinned snapshot: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("unused snapshot leaked its lifecycle lease")
	}
}

func TestPointReadSnapshotCapacityIsLazyAndPinned(t *testing.T) {
	for _, mode := range []string{"get", "iterator"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("GTRON_PEBBLE_BOUNDED_POINT_READ", mode)
			db, err := New(t.TempDir(), 16, 16, "test/lazy-snapshot/", false, Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			key := []byte("branch/key")
			if err := db.Put(key, []byte("before")); err != nil {
				t.Fatal(err)
			}
			snapshot, err := db.NewPointReadSnapshotWithCapacity(80)
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.Close()
			s := snapshot.(*pointReadSnapshot)
			if len(s.reservedCursors) != 0 {
				t.Fatal("unused snapshot allocated cursor storage")
			}
			if err := db.Put(key, []byte("after")); err != nil {
				t.Fatal(err)
			}
			if err := db.db.Flush(); err != nil {
				t.Fatal(err)
			}
			cursor, err := snapshot.NewCursor([]byte("branch/"))
			if err != nil {
				t.Fatal(err)
			}
			defer cursor.Close()
			if len(s.reservedCursors) != 80 {
				t.Fatal("used snapshot lost its capacity reservation")
			}
			found, err := cursor.View(key, func(value []byte) error {
				if !bytes.Equal(value, []byte("before")) {
					return fmt.Errorf("snapshot pinned after first cursor: %q", value)
				}
				return nil
			})
			if err != nil || !found {
				t.Fatalf("pinned read: found=%v err=%v", found, err)
			}
		})
	}
}

// Eager snapshot acquisition/close are included in both used and unused arms.
// This lifecycle control does not measure lookup latency or storage I/O.
func BenchmarkPointReadSnapshotCapacityUse(b *testing.B) {
	b.Setenv("GTRON_PEBBLE_BOUNDED_POINT_READ", "get")
	db, err := New(b.TempDir(), 16, 16, "bench/lazy-snapshot/", false, Options{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	prefix := []byte("state-commitment-branch-v1-")
	for _, used := range []int{0, 1, 80} {
		b.Run(fmt.Sprintf("reserved80/used%d", used), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				snapshot, err := db.NewPointReadSnapshotWithCapacity(80)
				if err != nil {
					b.Fatal(err)
				}
				for range used {
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
			b.ReportMetric(float64(unsafe.Sizeof(pointReadCursor{})), "cursor_B")
		})
	}
}
