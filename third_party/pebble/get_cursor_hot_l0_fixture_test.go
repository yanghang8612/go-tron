package pebble

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/bloom"
)

// The materialized oracle is independent of GetCursor and iterator merging.
// Every flush spans a..z; actual L0 sublevels are asserted, never inferred from
// flush counts. Base values reside in L6; sparse overlapping L0 tables force
// absent and lower-level-present queries to traverse the full fanout.
type cursorHotL0Fixture struct {
	db        *DB
	snapshot  *Snapshot
	expected  map[string][]byte
	keys      [][]byte
	sublevels int
	tables    int64
}

func makeCursorHotL0Fixture(t testing.TB, sublevels int) *cursorHotL0Fixture {
	t.Helper()
	levels := make([]LevelOptions, numLevels)
	for i := range levels {
		levels[i].FilterPolicy = bloom.FilterPolicy(10)
	}
	cache := NewCache(128 << 20)
	db, e := Open(t.TempDir(), &Options{Cache: cache, Levels: levels, DisableAutomaticCompactions: true, L0StopWritesThreshold: 64})
	cache.Unref()
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := db.Close(); e != nil {
			t.Error(e)
		}
	})
	expected := make(map[string][]byte, 4096)
	b := db.NewBatch()
	for i := 0; i < 4096; i++ {
		k := fmt.Sprintf("m%05d", i)
		v := append([]byte(k+":"), bytes.Repeat([]byte{byte(i % 251)}, 249)...)
		expected[k] = bytes.Clone(v)
		if e = b.Set([]byte(k), v, nil); e != nil {
			t.Fatal(e)
		}
	}
	for _, k := range []string{"a", "z", "merge-key", "point-deleted"} {
		if e = b.Set([]byte(k), []byte("base"), nil); e != nil {
			t.Fatal(e)
		}
	}
	expected["merge-key"] = []byte("base")
	expected["point-deleted"] = []byte("base")
	if e = b.Commit(NoSync); e != nil {
		t.Fatal(e)
	}
	if e = b.Close(); e != nil {
		t.Fatal(e)
	}
	if e = db.Flush(); e != nil {
		t.Fatal(e)
	}
	if e = db.Compact([]byte("a"), []byte("{"), false); e != nil {
		t.Fatal(e)
	}
	if db.Metrics().Levels[6].NumFiles == 0 {
		t.Fatal("fixture requires actual L6 source")
	}
	for i := 0; i < sublevels; i++ {
		b = db.NewBatch()
		for _, k := range []string{"a", "z"} {
			if e = b.Set([]byte(k), []byte(fmt.Sprint(i)), nil); e != nil {
				t.Fatal(e)
			}
		}
		piece := []byte(fmt.Sprintf("+%d", i))
		if e = b.Merge([]byte("merge-key"), piece, nil); e != nil {
			t.Fatal(e)
		}
		expected["merge-key"] = append(expected["merge-key"], piece...)
		if i == sublevels-1 {
			if e = b.Set([]byte("recent"), []byte("recent-value"), nil); e != nil {
				t.Fatal(e)
			}
			if e = b.Set([]byte("empty"), []byte{}, nil); e != nil {
				t.Fatal(e)
			}
			if e = b.Delete([]byte("point-deleted"), nil); e != nil {
				t.Fatal(e)
			}
			expected["recent"] = []byte("recent-value")
			expected["empty"] = []byte{}
			delete(expected, "point-deleted")
		}
		if i == sublevels/2 {
			if e = b.DeleteRange([]byte("m01000"), []byte("m01200"), nil); e != nil {
				t.Fatal(e)
			}
			for j := 1000; j < 1200; j++ {
				delete(expected, fmt.Sprintf("m%05d", j))
			}
		}
		if e = b.Commit(NoSync); e != nil {
			t.Fatal(e)
		}
		if e = b.Close(); e != nil {
			t.Fatal(e)
		}
		if e = db.Flush(); e != nil {
			t.Fatal(e)
		}
	}
	m := db.Metrics()
	if int(m.Levels[0].Sublevels) != sublevels || m.Levels[0].NumFiles != int64(sublevels) {
		t.Fatalf("actual L0 sublevels/tables %d/%d want %d/%d", m.Levels[0].Sublevels, m.Levels[0].NumFiles, sublevels, sublevels)
	}
	// Fixed permutation, alternating lower-level-present and absent keys. No
	// read sorting or seed selected from measurements; all caps use these bytes.
	keys := make([][]byte, 4096)
	for i := range keys {
		j := (i*4051 + 97) & 4095
		prefix := "m"
		if i&1 != 0 {
			prefix = "n"
		}
		keys[i] = []byte(fmt.Sprintf("%s%05d", prefix, j))
	}
	snap := db.NewSnapshot()
	t.Cleanup(func() {
		if e := snap.Close(); e != nil {
			t.Error(e)
		}
	})
	return &cursorHotL0Fixture{db: db, snapshot: snap, expected: expected, keys: keys, sublevels: sublevels, tables: m.Levels[0].NumFiles}
}
func checkCursorHotL0Oracle(t testing.TB, c *GetCursor, f *cursorHotL0Fixture) {
	t.Helper()
	keys := append(append([][]byte{}, f.keys...), []byte("merge-key"), []byte("recent"), []byte("empty"), []byte("point-deleted"))
	for _, k := range keys {
		v, e := c.Get(k)
		want, found := f.expected[string(k)]
		if found {
			if e != nil || !bytes.Equal(v, want) {
				t.Fatalf("%s found=%t error=%v value mismatch", k, found, e)
			}
		} else if e != ErrNotFound {
			t.Fatalf("%s expected missing got %v", k, e)
		}
		// Cross-check the unchanged ordinary Snapshot.Get route, including
		// its borrowed-value closer. Map and original reader are independent
		// oracles for cursor cache/merging and physical empty presence.
		oldValue, closer, oldErr := f.snapshot.Get(k)
		if found {
			if oldErr != nil || !bytes.Equal(oldValue, want) {
				t.Fatalf("ordinary Snapshot.Get %s mismatch: %v", k, oldErr)
			}
		} else if oldErr != ErrNotFound {
			t.Fatalf("ordinary Snapshot.Get %s missing: %v", k, oldErr)
		}
		if closer != nil {
			if closeErr := closer.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
		}
		if e = c.ReleaseValue(); e != nil {
			t.Fatal(e)
		}
	}
}
