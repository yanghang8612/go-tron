package pebbledb

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/tronprotocol/go-tron/core/pointread"
)

var errStorageL6FilterIO = errors.New("storage cursor L6 filter read failure")

type storageL6FilterFS struct {
	vfs.FS
	file     string
	from, to int64
	fail     atomic.Bool
	reads    atomic.Int64
}
type storageL6FilterFile struct {
	vfs.File
	owner *storageL6FilterFS
	name  string
}

func (f *storageL6FilterFS) Open(name string, options ...vfs.OpenOption) (vfs.File, error) {
	file, err := f.FS.Open(name, options...)
	if err == nil && strings.HasSuffix(name, ".sst") {
		return &storageL6FilterFile{file, f, name}, nil
	}
	return file, err
}
func (f *storageL6FilterFile) ReadAt(value []byte, offset int64) (int, error) {
	if f.name == f.owner.file && offset < f.owner.to && offset+int64(len(value)) > f.owner.from {
		f.owner.reads.Add(1)
		if f.owner.fail.Load() {
			return 0, errStorageL6FilterIO
		}
	}
	return f.File.ReadAt(value, offset)
}

func storageL6Fixture(t *testing.T, filtered bool) (*Database, *storageL6FilterFS) {
	t.Helper()
	fs := &storageL6FilterFS{FS: vfs.NewMem()}
	levels := make([]pebble.LevelOptions, 7)
	for i := range levels {
		if filtered {
			levels[i].FilterPolicy = bloom.FilterPolicy(10)
		}
	}
	options := func() *pebble.Options {
		cache := pebble.NewCache(0)
		t.Cleanup(cache.Unref)
		return &pebble.Options{FS: fs, Cache: cache, Comparer: exactPointComparer, Levels: levels, DisableAutomaticCompactions: true, LBaseMaxBytes: 1}
	}
	engine, err := pebble.Open("l6", options())
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "z"} {
		if err := engine.Set([]byte(key), []byte(key+"-value"), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	if err := engine.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := engine.Compact([]byte("a"), []byte("{"), false); err != nil {
		t.Fatal(err)
	}
	tables, err := engine.SSTables(pebble.WithProperties())
	if err != nil {
		t.Fatal(err)
	}
	if len(tables[6]) != 1 {
		t.Fatalf("need one actual L6 table: %+v", engine.Metrics().Levels)
	}
	fs.file = fmt.Sprintf("l6/%06d.sst", tables[6][0].FileNum)
	if filtered != (tables[6][0].Properties.FilterSize > 0) {
		t.Fatal("filter property mismatch")
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := fs.Open(fs.file)
	if err != nil {
		t.Fatal(err)
	}
	readable, err := sstable.NewSimpleReadable(file)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := sstable.NewReader(readable, sstable.ReaderOptions{Comparer: exactPointComparer, Filters: map[string]sstable.FilterPolicy{bloom.FilterPolicy(10).Name(): bloom.FilterPolicy(10)}})
	if err != nil {
		t.Fatal(err)
	}
	layout, err := reader.Layout()
	if err != nil {
		t.Fatal(err)
	}
	fs.from, fs.to = int64(layout.Filter.Offset), int64(layout.Filter.Offset+layout.Filter.Length)
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	engine, err = pebble.Open("l6", options())
	if err != nil {
		t.Fatal(err)
	}
	db := &Database{db: engine}
	t.Cleanup(func() {
		fs.fail.Store(false)
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	fs.reads.Store(0)
	return db, fs
}

func TestStorageCursorFactoryUsesExistingL6FiltersAndLegacyFallback(t *testing.T) {
	for _, filtered := range []bool{false, true} {
		t.Run(fmt.Sprint(filtered), func(t *testing.T) {
			db, fs := storageL6Fixture(t, filtered)
			snapshot, err := db.NewKeyValueSnapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.Close()
			cursor, err := snapshot.(pointread.KeyValueCursorSnapshot).NewPointReadCursor()
			if err != nil {
				t.Fatal(err)
			}
			defer cursor.Close()
			before := db.db.Metrics().Filter
			if found, err := cursor.View([]byte("m"), func([]byte) error { t.Fatal("missing callback"); return nil }); found || err != nil {
				t.Fatalf("missing=%v/%v", found, err)
			}
			after := db.db.Metrics().Filter
			if filtered && (fs.reads.Load() == 0 || after.Hits+after.Misses == before.Hits+before.Misses) {
				t.Fatal("actual canonical factory did not use L6 filter")
			}
			if found, err := cursor.View([]byte("a"), func(v []byte) error {
				if string(v) != "a-value" {
					t.Fatal(string(v))
				}
				return nil
			}); !found || err != nil {
				t.Fatalf("present=%v/%v", found, err)
			}
		})
	}
}

func TestStorageCursorL6FilterReadErrorIsNotAbsence(t *testing.T) {
	db, fs := storageL6Fixture(t, true)
	snapshot, err := db.NewKeyValueSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	cursor, err := snapshot.(pointread.KeyValueCursorSnapshot).NewPointReadCursor()
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	fs.fail.Store(true)
	for i := 0; i < 2; i++ {
		if found, err := cursor.View([]byte("m"), func([]byte) error { t.Fatal("callback after filter failure"); return nil }); found || !errors.Is(err, errStorageL6FilterIO) {
			t.Fatalf("filter failure=%v/%v", found, err)
		}
	}
	if fs.reads.Load() == 0 {
		t.Fatal("failure did not target the actual filter block")
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	fs.fail.Store(false)
	retry, err := snapshot.(pointread.KeyValueCursorSnapshot).NewPointReadCursor()
	if err != nil {
		t.Fatal(err)
	}
	defer retry.Close()
	if found, err := retry.View([]byte("a"), func([]byte) error { return nil }); !found || err != nil {
		t.Fatalf("fresh cursor failed to recover=%v/%v", found, err)
	}
}
