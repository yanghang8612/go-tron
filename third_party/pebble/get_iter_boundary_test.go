package pebble

import (
	"bytes"
	"io"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

// Literal expected values provide an oracle independent of the shared getIter
// used by DB.Get, Snapshot.Get, Batch.Get and GetCursor. Alternate range misses,
// cross-level merges, old views and new values to exercise scratch reset both
// between levels and after each complete Get allocation returns to its pool.
func TestGetBoundaryScratchExpectedVersions(t *testing.T) {
	db, err := Open("boundary", &Options{FS: vfs.NewMem(), DisableAutomaticCompactions: true, L0StopWritesThreshold: 32})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	set := func(key, value string) {
		t.Helper()
		if err := db.Set([]byte(key), []byte(value), NoSync); err != nil {
			t.Fatal(err)
		}
	}
	flush := func() {
		t.Helper()
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	merge := func(value string) {
		t.Helper()
		if err := db.Merge([]byte("a"), []byte(value), NoSync); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []string{"a", "b", "c", "d", "e"} {
		set(key, "base-"+key)
	}
	flush()
	if err := db.Compact([]byte("a"), []byte("z"), false); err != nil {
		t.Fatal(err)
	}
	old := db.NewSnapshot()
	defer old.Close()
	merge("+m0")
	if err := db.DeleteRange([]byte("b"), []byte("d"), NoSync); err != nil {
		t.Fatal(err)
	}
	flush()
	ranged := db.NewSnapshot()
	defer ranged.Close()
	merge("+m1")
	set("b", "new-b")
	set("d", "new-d")
	if err := db.Delete([]byte("e"), NoSync); err != nil {
		t.Fatal(err)
	}
	flush()
	current := db.NewSnapshot()
	defer current.Close()
	// These mutable-memtable writes must stay invisible to all three snapshots,
	// including cursors created below after these newer writes already exist.
	set("a", "future-a")
	set("z", "future-z")
	batch := db.NewIndexedBatch()
	defer batch.Close()
	if err := batch.Merge([]byte("a"), []byte("+batch"), nil); err != nil {
		t.Fatal(err)
	}
	if err := batch.Set([]byte("c"), []byte("batch-c"), nil); err != nil {
		t.Fatal(err)
	}
	if err := batch.Delete([]byte("b"), nil); err != nil {
		t.Fatal(err)
	}
	type view struct {
		name     string
		snapshot *Snapshot
		get      func([]byte) ([]byte, io.Closer, error)
		want     map[string]string
		cursor   *GetCursor
	}
	views := []view{
		{name: "old", snapshot: old, get: old.Get, want: map[string]string{"a": "base-a", "b": "base-b", "c": "base-c", "d": "base-d", "e": "base-e"}},
		{name: "range", snapshot: ranged, get: ranged.Get, want: map[string]string{"a": "base-a+m0", "d": "base-d", "e": "base-e"}},
		{name: "current", snapshot: current, get: current.Get, want: map[string]string{"a": "base-a+m0+m1", "b": "new-b", "d": "new-d"}},
		{name: "db", get: db.Get, want: map[string]string{"a": "future-a", "b": "new-b", "d": "new-d", "z": "future-z"}},
		{name: "batch", get: batch.Get, want: map[string]string{"a": "future-a+batch", "c": "batch-c", "d": "new-d", "z": "future-z"}},
	}
	for i := range views {
		if views[i].snapshot != nil {
			views[i].cursor, err = views[i].snapshot.NewGetCursor()
			if err != nil {
				t.Fatal(err)
			}
			defer func(c *GetCursor) {
				if err := c.Close(); err != nil {
					t.Error(err)
				}
			}(views[i].cursor)
		}
	}
	check := func(name, key string, value []byte, err error, want map[string]string) {
		t.Helper()
		expected, exists := want[key]
		if !exists {
			if err != ErrNotFound {
				t.Fatalf("%s/%s: expected miss, got %q/%v", name, key, value, err)
			}
		} else if err != nil || !bytes.Equal(value, []byte(expected)) {
			t.Fatalf("%s/%s: got %q/%v want %q", name, key, value, err, expected)
		}
	}
	for round := 0; round < 8; round++ {
		if round == 4 {
			// Existing cursors retain their original readState while new Gets
			// and iterators observe the compacted file topology at the same cut.
			if err := db.Compact([]byte("a"), []byte("zz"), false); err != nil {
				t.Fatal(err)
			}
		}
		for _, key := range []string{"b", "a", "z", "e", "c", "d", "a", "missing"} {
			for _, v := range views {
				value, closer, err := v.get([]byte(key))
				check(v.name, key, value, err, v.want)
				if closer != nil {
					if err := closer.Close(); err != nil {
						t.Fatal(err)
					}
				}
				if v.cursor == nil {
					continue
				}
				value, err = v.cursor.Get([]byte(key))
				check(v.name+"/cursor", key, value, err, v.want)
				if err := v.cursor.ReleaseValue(); err != nil {
					t.Fatal(err)
				}
				iter, err := v.snapshot.NewIter(nil)
				if err != nil {
					t.Fatal(err)
				}
				if iter.SeekGE([]byte(key)) && bytes.Equal(iter.Key(), []byte(key)) {
					value, err = iter.ValueAndErr()
				} else if err = iter.Error(); err == nil {
					value, err = nil, ErrNotFound
				}
				check(v.name+"/iterator", key, value, err, v.want)
				if err := iter.Close(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}
