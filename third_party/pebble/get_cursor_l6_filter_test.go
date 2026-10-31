package pebble

import (
	"bytes"
	"fmt"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/vfs"
	"testing"
)

func l6FilterTestOptions(fs vfs.FS, filter bool) *Options {
	levels := make([]LevelOptions, numLevels)
	if filter {
		for i := range levels {
			levels[i].FilterPolicy = bloom.FilterPolicy(10)
		}
	}
	return &Options{FS: fs, DisableAutomaticCompactions: true, Levels: levels}
}
func l6FilterTestBase(t *testing.T, d *DB) {
	t.Helper()
	for k, v := range map[string]string{"0": "start", "a": "A", "b": "B", "m": "M", "z": "Z", "zz": "end"} {
		if e := d.Set([]byte(k), []byte(v), NoSync); e != nil {
			t.Fatal(e)
		}
	}
	if e := d.Flush(); e != nil {
		t.Fatal(e)
	}
	if e := d.Compact([]byte("0"), []byte("zzz"), false); e != nil {
		t.Fatal(e)
	}
	if d.Metrics().Levels[6].NumFiles == 0 {
		t.Fatal("test requires actual L6")
	}
}
func l6FilterTestRead(t *testing.T, c *GetCursor, key, want string) {
	t.Helper()
	v, e := c.Get([]byte(key))
	if want == "" {
		if e != ErrNotFound {
			t.Fatalf("%s missing: %v", key, e)
		}
	} else if e != nil || !bytes.Equal(v, []byte(want)) {
		t.Fatalf("%s=%q err%v want%q", key, v, e, want)
	}
	if e = c.ReleaseValue(); e != nil {
		t.Fatal(e)
	}
}
func TestGetCursorL6FiltersPreserveVersionsMergeAndRange(t *testing.T) {
	for _, limit := range []int{4, 8, 16} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) { runTestGetCursorL6FiltersPreserveVersionsMergeAndRange(t, limit) })
	}
}
func runTestGetCursorL6FiltersPreserveVersionsMergeAndRange(t *testing.T, limit int) {
	d, e := Open("db", l6FilterTestOptions(vfs.NewMem(), true))
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	l6FilterTestBase(t, d)
	old := d.NewSnapshot()
	defer old.Close()
	if e = d.DeleteRange([]byte("a"), []byte("c"), NoSync); e != nil {
		t.Fatal(e)
	}
	if e = d.Set([]byte("b"), []byte("new"), NoSync); e != nil {
		t.Fatal(e)
	}
	if e = d.Merge([]byte("m"), []byte("+"), NoSync); e != nil {
		t.Fatal(e)
	}
	if e = d.Flush(); e != nil {
		t.Fatal(e)
	}
	current := d.NewSnapshot()
	defer current.Close()
	var cursors []*GetCursor
	for _, flag := range []bool{false, true} {
		for _, s := range []*Snapshot{old, current} {
			c, e := s.NewGetCursorWithOptions(GetCursorOptions{MaxRetainedTables: limit, UseL6Filters: flag})
			if e != nil {
				t.Fatal(e)
			}
			cursors = append(cursors, c)
		}
	}
	defer func() {
		for _, c := range cursors {
			if e := c.Close(); e != nil {
				t.Error(e)
			}
		}
	}()
	check := func() {
		for i, c := range cursors {
			if i%2 == 0 {
				l6FilterTestRead(t, c, "a", "A")
				l6FilterTestRead(t, c, "b", "B")
				l6FilterTestRead(t, c, "m", "M")
			} else {
				l6FilterTestRead(t, c, "a", "")
				l6FilterTestRead(t, c, "b", "new")
				l6FilterTestRead(t, c, "m", "M+")
			}
			for _, k := range []string{"aa", "c", "x"} {
				l6FilterTestRead(t, c, k, "")
			}
			l6FilterTestRead(t, c, "z", "Z")
		}
	}
	check()
	if e = d.Set([]byte("b"), []byte("future"), NoSync); e != nil {
		t.Fatal(e)
	}
	if e = d.Merge([]byte("m"), []byte("later"), NoSync); e != nil {
		t.Fatal(e)
	}
	if e = d.DeleteRange([]byte("z"), []byte("zz"), NoSync); e != nil {
		t.Fatal(e)
	}
	if e = d.Flush(); e != nil {
		t.Fatal(e)
	}
	if e = d.Compact([]byte("0"), []byte("zzz"), false); e != nil {
		t.Fatal(e)
	}
	check()
	late, e := old.NewGetCursorWithOptions(GetCursorOptions{MaxRetainedTables: limit, UseL6Filters: true})
	if e != nil {
		t.Fatal(e)
	}
	defer late.Close()
	l6FilterTestRead(t, late, "b", "B")
}
func TestGetCursorL6FilterOptionDoesNotLeakThroughGetPool(t *testing.T) {
	d, e := Open("db", l6FilterTestOptions(vfs.NewMem(), true))
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	l6FilterTestBase(t, d)
	s := d.NewSnapshot()
	defer s.Close()
	filtered, e := s.NewGetCursorWithOptions(GetCursorOptions{UseL6Filters: true})
	if e != nil {
		t.Fatal(e)
	}
	defer filtered.Close()
	plain, e := s.NewGetCursor()
	if e != nil {
		t.Fatal(e)
	}
	defer plain.Close()
	total := func() int64 { f := d.Metrics().Filter; return f.Hits + f.Misses }
	for i := 0; i < 20; i++ {
		before := total()
		l6FilterTestRead(t, filtered, "inside-missing", "")
		if total() <= before {
			t.Fatal("opt-in did not check L6 filter")
		}
		before = total()
		_, closer, e := s.Get([]byte("inside-missing"))
		if e != ErrNotFound {
			t.Fatal(e)
		}
		if closer != nil {
			if e = closer.Close(); e != nil {
				t.Fatal(e)
			}
		}
		if total() != before {
			t.Fatal("filter flag leaked into Snapshot.Get")
		}
		l6FilterTestRead(t, plain, "inside-missing", "")
		if total() != before {
			t.Fatal("filter flag leaked into default cursor")
		}
	}
	if filtered.Stats().TableReuses == 0 || filtered.Stats().TableFallbacks != 0 {
		t.Fatalf("opt-in lost compatible table reuse: %+v", filtered.Stats())
	}
}
func TestGetCursorL6FilterOldSSTReopen(t *testing.T) {
	for _, limit := range []int{4, 8, 16} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) { runTestGetCursorL6FilterOldSSTReopen(t, limit) })
	}
}
func runTestGetCursorL6FilterOldSSTReopen(t *testing.T, limit int) {
	fs := vfs.NewMem()
	d, e := Open("db", l6FilterTestOptions(fs, false))
	if e != nil {
		t.Fatal(e)
	}
	l6FilterTestBase(t, d)
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	opts := l6FilterTestOptions(fs, true)
	opts.ReadOnly = true
	d, e = Open("db", opts)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	tables, e := d.SSTables(WithProperties())
	if e != nil {
		t.Fatal(e)
	}
	for _, f := range tables[6] {
		if f.Properties.FilterSize != 0 {
			t.Fatal("legacy SST unexpectedly rewritten")
		}
	}
	s := d.NewSnapshot()
	defer s.Close()
	c, e := s.NewGetCursorWithOptions(GetCursorOptions{MaxRetainedTables: limit, UseL6Filters: true})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	l6FilterTestRead(t, c, "m", "M")
	l6FilterTestRead(t, c, "inside-missing", "")
}
