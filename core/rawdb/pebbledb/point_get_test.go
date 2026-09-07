package pebbledb

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestPointGetMVCCDifferential(t *testing.T) {
	t.Setenv("GTRON_PEBBLE_BOUNDED_POINT_READ", "get")
	db, err := New(t.TempDir(), 16, 32, "test/point-get/", false, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	keys := make([][]byte, 256)
	for i := range keys {
		keys[i] = []byte{0xff, byte(i)}
	}
	for wave := 0; wave < 3; wave++ {
		for i, key := range keys {
			value := []byte(fmt.Sprintf("value-%d-%d", wave, i))
			if i%7 == 0 {
				value = []byte{}
			}
			if err := db.Put(key, value); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.db.DeleteRange(keys[31], keys[51], pebble.NoSync); err != nil {
			t.Fatal(err)
		}
		if err := db.Delete(keys[91]); err != nil {
			t.Fatal(err)
		}
		if err := db.db.Merge(keys[201], []byte("merged"), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
		if err := db.db.Flush(); err != nil {
			t.Fatal(err)
		}
		s, err := db.NewPointReadSnapshotWithCapacity(2)
		if err != nil {
			t.Fatal(err)
		}
		prefix := []byte{0xff}
		cursor, err := s.NewCursor(prefix)
		if err != nil {
			t.Fatal(err)
		}
		prefix[0] = 0 // Bounds must be privately owned, including all-ff prefixes.
		if cursor.(*pointReadCursor).getSnapshot == nil {
			t.Fatal("Get mode not selected")
		}
		unbounded, err := s.NewCursor(nil)
		if err != nil {
			t.Fatal(err)
		}
		if unbounded.(*pointReadCursor).iter == nil {
			t.Fatal("unbounded prefetch lost iterator")
		}
		if err := unbounded.Close(); err != nil {
			t.Fatal(err)
		}
		oracle, err := s.(*pointReadSnapshot).snapshot.NewIter(nil)
		if err != nil {
			t.Fatal(err)
		}
		// Make the current DB disagree with the pinned snapshot, then flush and
		// compact while both readers keep the old sequence alive.
		if err := db.db.DeleteRange(keys[0], []byte{0xff, 0xff, 0xff}, pebble.NoSync); err != nil {
			t.Fatal(err)
		}
		if err := db.Put(keys[91], []byte("future")); err != nil {
			t.Fatal(err)
		}
		if err := db.db.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := db.db.Compact(keys[0], []byte{0xff, 0xff, 0xff}, true); err != nil {
			t.Fatal(err)
		}
		for j := 0; j < len(keys)*2; j++ {
			key := keys[(j*73)&255]
			wantFound := oracle.SeekPrefixGE(key) && bytes.Equal(oracle.Key(), key)
			var want []byte
			if wantFound {
				want = bytes.Clone(oracle.Value())
			}
			called := false
			found, err := cursor.View(key, func(value []byte) error {
				called = true
				if !bytes.Equal(value, want) {
					t.Fatalf("wave %d key %x: %q != %q", wave, key, value, want)
				}
				return nil
			})
			if err != nil || found != wantFound || called != wantFound {
				t.Fatalf("key %x found=%v called=%v err=%v want=%v", key, found, called, err, wantFound)
			}
		}
		if err := oracle.Error(); err != nil {
			t.Fatal(err)
		}
		for _, key := range [][]byte{nil, {0xfe}, {0xfe, 0xff}, {0xff, 0xff, 1}} {
			found, err := cursor.View(key, func([]byte) error { t.Fatal("callback for absent/out-of-prefix key"); return nil })
			if err != nil || found {
				t.Fatalf("out of prefix/missing %x: %v %v", key, found, err)
			}
		}
		if err := oracle.Close(); err != nil {
			t.Fatal(err)
		}
		if err := cursor.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := cursor.View(keys[0], nil); !errors.Is(err, pebble.ErrClosed) {
			t.Fatalf("closed cursor: %v", err)
		}
		if err := cursor.Close(); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPointGetCallbackLifetime(t *testing.T) {
	t.Setenv("GTRON_PEBBLE_BOUNDED_POINT_READ", "get")
	db, err := New(t.TempDir(), 16, 32, "test/point-get-lifetime/", false, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("p/key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.db.Flush(); err != nil {
		t.Fatal(err)
	}
	s, err := db.NewPointReadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.NewCursor([]byte("p/"))
	if err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("callback failed")
	for _, panics := range []bool{false, true} {
		func() {
			if panics {
				defer func() {
					if recover() != sentinel {
						t.Error("callback panic lost")
					}
				}()
			}
			found, err := c.View([]byte("p/key"), func(value []byte) error {
				if string(value) != "value" {
					t.Fatal("value changed")
				}
				if panics {
					panic(sentinel)
				}
				return sentinel
			})
			if !found || err != sentinel {
				t.Fatalf("callback result: %v %v", found, err)
			}
		}()
		if got := db.db.Metrics().TableIters; got != 0 {
			t.Fatalf("leaked %d table iterators", got)
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("closed with active snapshot: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot leaked lifecycle lease")
	}
}

type pointGetFailFS struct {
	vfs.FS
	fail atomic.Bool
}
type pointGetFailFile struct {
	vfs.File
	fs *pointGetFailFS
}

var errPointGetIO = errors.New("injected point Get I/O failure")

func (fs *pointGetFailFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	return &pointGetFailFile{File: f, fs: fs}, nil
}
func (f *pointGetFailFile) ReadAt(p []byte, off int64) (int, error) {
	if f.fs.fail.Load() {
		return 0, errPointGetIO
	}
	return f.File.ReadAt(p, off)
}

func TestPointGetPropagatesStorageError(t *testing.T) {
	fs := &pointGetFailFS{FS: vfs.NewMem()}
	cache := pebble.NewCache(0)
	pdb, err := pebble.Open("db", &pebble.Options{FS: fs, Cache: cache, Comparer: exactPointComparer})
	cache.Unref()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := pdb.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := pdb.Set([]byte("p/key"), []byte("value"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := pdb.Flush(); err != nil {
		t.Fatal(err)
	}
	db := &Database{db: pdb, boundedPointGet: true}
	s, err := db.NewPointReadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	c, err := s.NewCursor([]byte("p/"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	}()
	fs.fail.Store(true)
	defer fs.fail.Store(false)
	found, err := c.View([]byte("p/key"), func([]byte) error { t.Fatal("callback after I/O error"); return nil })
	if found || !errors.Is(err, errPointGetIO) {
		t.Fatalf("I/O result: %v %v", found, err)
	}
}

func TestPointGetRejectsUnknownModeBeforeOpen(t *testing.T) {
	t.Setenv("GTRON_PEBBLE_BOUNDED_POINT_READ", "typo")
	if db, err := New(t.TempDir(), 16, 16, "test/point-get-mode/", false, Options{}); err == nil {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
		t.Fatal("accepted unknown mode")
	}
}
