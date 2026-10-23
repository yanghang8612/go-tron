package pebble

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/vfs"
)

func TestGetCursorRetentionOptionsLazyAndBounded(t *testing.T) {
	db, err := Open("retention", &Options{FS: vfs.NewMem(), DisableAutomaticCompactions: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	snap := db.NewSnapshot()
	defer snap.Close()
	for _, n := range []int{-1, 1, 3, 5, 17, 33} {
		if c, e := snap.NewGetCursorWithOptions(GetCursorOptions{MaxRetainedTables: n}); e == nil {
			c.Close()
			t.Fatalf("accepted unbounded/unsupported limit %d", n)
		}
	}
	for _, n := range []int{0, 4, 8, 16, 32} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			c, e := snap.NewGetCursorWithOptions(GetCursorOptions{MaxRetainedTables: n})
			if e != nil {
				t.Fatal(e)
			}
			if c.extendedTables != nil {
				t.Fatal("scope creation allocated expanded slots")
			}
			if _, e = c.Get([]byte("absent")); e != ErrNotFound {
				t.Fatal(e)
			}
			if c.extendedTables != nil {
				t.Fatal("memtable/missing lookup allocated overflow")
			}
			addr := &c.tables[0]
			for j := 0; j < 3; j++ {
				if _, e = c.Get([]byte("absent")); e != ErrNotFound {
					t.Fatal(e)
				}
				if &c.tables[0] != addr {
					t.Fatal("slot addresses moved")
				}
			}
			if e = c.Close(); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestGetCursorExpandedLeaseAndFallbackReasons(t *testing.T) {
	for _, n := range []int{4, 8, 16, 32} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			var points []*cursorFaultPoint
			db := &DB{}
			db.newIters = func(context.Context, *manifest.FileMetadata, *IterOptions, internalIterOpts) (internalIterator, keyspan.FragmentIterator, error) {
				p := &cursorFaultPoint{internalIterator: newErrorIter(nil)}
				points = append(points, p)
				return p, nil, nil
			}
			c := &GetCursor{db: db, seqNum: 10, tableLimit: n}
			opts := &IterOptions{snapshotForHideObsoletePoints: 10, level: manifest.Level(1)}
			files := make([]manifest.FileMetadata, n+1)
			held := make([]internalIterator, 0, n)
			for i := 0; i < n; i++ {
				p, _, e := c.newIters(context.Background(), &files[i], opts, internalIterOpts{})
				if e != nil {
					t.Fatal(e)
				}
				held = append(held, p)
				if i < 4 && c.extendedTables != nil {
					t.Fatal("first four retained tables allocated overflow")
				}
				if i >= 4 && len(c.extendedTables) != n-4 {
					t.Fatalf("overflow bound %d != %d", len(c.extendedTables), n-4)
				}
				if e = p.Close(); e != nil {
					t.Fatal(e)
				} // logical Close must retain the lease.
			}
			for i := range held {
				var slot *getCursorTable
				if i < 4 {
					slot = &c.tables[i]
				} else {
					slot = &c.extendedTables[i-4]
				}
				if held[i] != &slot.point {
					t.Fatal("borrowed wrapper address moved")
				}
				if points[i].closed != 0 {
					t.Fatal("closed borrowed lease")
				}
			}
			p, _, e := c.newIters(context.Background(), &files[n], opts, internalIterOpts{})
			if e != nil {
				t.Fatal(e)
			}
			if e = p.Close(); e != nil {
				t.Fatal(e)
			}
			stats := c.Stats()
			if stats.TableFallbacks != 1 || stats.TableFallbackAllLeased != 1 || stats.TableFallbackIncompatible != 0 || stats.MaxRetained != uint64(n) {
				t.Fatalf("stats %+v", stats)
			}
			if e = c.Close(); e != nil {
				t.Fatal(e)
			}
			for _, p := range points {
				if p.closed != 1 {
					t.Fatalf("actual close count %d", p.closed)
				}
			}
		})
	}
	// Incompatible construction errors are still classified exactly once.
	want := errors.New("open failure")
	db := &DB{newIters: func(context.Context, *manifest.FileMetadata, *IterOptions, internalIterOpts) (internalIterator, keyspan.FragmentIterator, error) {
		return nil, nil, want
	}}
	c := &GetCursor{db: db, seqNum: 10}
	opts := &IterOptions{snapshotForHideObsoletePoints: 10, LowerBound: []byte{}}
	if _, _, e := c.newIters(context.Background(), &manifest.FileMetadata{}, opts, internalIterOpts{}); !errors.Is(e, want) {
		t.Fatal(e)
	}
	s := c.Stats()
	if s.TableFallbacks != 1 || s.TableFallbackIncompatible != 1 || s.TableFallbackAllLeased != 0 || s.TableOpens != 0 {
		t.Fatalf("failure stats %+v", s)
	}
	if s.TableFallbacks != s.TableFallbackIncompatible+s.TableFallbackAllLeased {
		t.Fatal("fallback classification lost")
	}
}

func TestGetCursorRetentionRealL0SnapshotAndRangeOracle(t *testing.T) {
	f := makeCursorHotL0Fixture(t, 16)
	c, e := f.snapshot.NewGetCursorWithOptions(GetCursorOptions{MaxRetainedTables: 32})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	checkCursorHotL0Oracle(t, c, f)
	// Mutate and compact after capturing the fixed snapshot. Every expected
	// value and absence must remain at the original cut, including range deletes.
	if e = f.db.Set([]byte("m00097"), []byte("later"), NoSync); e != nil {
		t.Fatal(e)
	}
	if e = f.db.DeleteRange([]byte("m"), []byte("o"), NoSync); e != nil {
		t.Fatal(e)
	}
	if e = f.db.Flush(); e != nil {
		t.Fatal(e)
	}
	if e = f.db.Compact([]byte("a"), []byte("{"), false); e != nil {
		t.Fatal(e)
	}
	checkCursorHotL0Oracle(t, c, f)
	if c.Stats().MaxRetained > 32 {
		t.Fatal("exceeded hard bound")
	}
	if e = c.Close(); e != nil {
		t.Fatal(e)
	}
	if c.Stats().TableFallbacks != c.Stats().TableFallbackIncompatible+c.Stats().TableFallbackAllLeased {
		t.Fatal("reason counters do not partition fallback")
	}
}

// A compatible open that falls back because every slot is leased must retain
// its classification even when construction fails. Retained resources remain
// owned until the enclosing Get cleanup; logical wrapper Close cannot unlease.
func TestGetCursorAllLeasedOpenFailure(t *testing.T) {
	want := errors.New("all-leased open failure")
	calls := 0
	var points []*cursorFaultPoint
	db := &DB{newIters: func(context.Context, *manifest.FileMetadata, *IterOptions, internalIterOpts) (internalIterator, keyspan.FragmentIterator, error) {
		calls++
		if calls == 17 {
			return nil, nil, want
		}
		p := &cursorFaultPoint{internalIterator: newErrorIter(nil)}
		points = append(points, p)
		return p, nil, nil
	}}
	c := &GetCursor{db: db, seqNum: 10, tableLimit: 16}
	opts := &IterOptions{snapshotForHideObsoletePoints: 10, level: manifest.Level(1)}
	files := make([]manifest.FileMetadata, 17)
	for i := 0; i < 16; i++ {
		p, _, e := c.newIters(context.Background(), &files[i], opts, internalIterOpts{})
		if e != nil {
			t.Fatal(e)
		}
		if e = p.Close(); e != nil {
			t.Fatal(e)
		}
	}
	if _, _, e := c.newIters(context.Background(), &files[16], opts, internalIterOpts{}); !errors.Is(e, want) {
		t.Fatal(e)
	}
	st := c.Stats()
	if st.TableFallbackAllLeased != 1 || st.TableFallbackIncompatible != 0 || st.TableFallbacks != 1 || st.TableOpens != 16 {
		t.Fatalf("stats %+v", st)
	}
	if e := c.Close(); e != nil {
		t.Fatal(e)
	}
	if c.extendedTables != nil {
		t.Fatal("closed cursor retains expanded backing")
	}
	for _, p := range points {
		if p.closed != 1 {
			t.Fatalf("close count %d", p.closed)
		}
	}
	if e := c.Close(); e != nil {
		t.Fatal(e)
	}
}

// Exercise real Get/ReleaseValue, including range tombstones and merges, with
// all retained state fitting in the original inline capacity.
func TestGetCursorLowFanoutKeepsInlineBacking(t *testing.T) {
	f := makeCursorHotL0Fixture(t, 3)
	c, err := f.snapshot.NewGetCursorWithOptions(GetCursorOptions{MaxRetainedTables: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	addr := &c.tables[0]
	checkCursorHotL0Oracle(t, c, f)
	checkCursorHotL0Oracle(t, c, f)
	if c.extendedTables != nil || &c.tables[0] != addr {
		t.Fatal("low-fanout Get allocated or moved inline backing")
	}
	if st := c.Stats(); st.MaxRetained != 4 || st.TableReuses == 0 || st.TableFallbackAllLeased != 0 {
		t.Fatalf("inline retention %+v", st)
	}
}

func TestGetCursorOverflowClosesBothSegmentsAfterErrors(t *testing.T) {
	pointErr := errors.New("inline point close")
	rangeErr := errors.New("overflow range close")
	var points []*cursorFaultPoint
	var ranges []*cursorFaultRange
	db := &DB{newIters: func(context.Context, *manifest.FileMetadata, *IterOptions, internalIterOpts) (internalIterator, keyspan.FragmentIterator, error) {
		p := &cursorFaultPoint{internalIterator: newErrorIter(nil)}
		r := &cursorFaultRange{FragmentIterator: keyspan.NewIter(DefaultComparer.Compare, nil)}
		if len(points) == 0 {
			p.closeErr = pointErr
		}
		if len(points) == 7 {
			r.closeErr = rangeErr
		}
		points = append(points, p)
		ranges = append(ranges, r)
		return p, r, nil
	}}
	c := &GetCursor{db: db, seqNum: 10, tableLimit: 16}
	opts := &IterOptions{snapshotForHideObsoletePoints: 10, level: manifest.Level(1)}
	files := make([]manifest.FileMetadata, 8)
	var heldPoints []internalIterator
	var heldRanges []keyspan.FragmentIterator
	for i := range files {
		p, r, err := c.newIters(context.Background(), &files[i], opts, internalIterOpts{})
		if err != nil {
			t.Fatal(err)
		}
		heldPoints = append(heldPoints, p)
		heldRanges = append(heldRanges, r)
		// Logical wrapper Close must not release either borrowed component.
		if err := errors.Join(p.Close(), r.Close()); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 4; i++ {
		if heldPoints[i] != &c.tables[i].point || heldRanges[i] != &c.tables[i].rangeDel || !c.tables[i].leased {
			t.Fatal("overflow activation moved/released inline borrowed iterators")
		}
	}
	err := c.Close()
	if !errors.Is(err, pointErr) || !errors.Is(err, rangeErr) {
		t.Fatalf("lost cleanup error: %v", err)
	}
	if c.extendedTables != nil {
		t.Fatal("closed cursor retains overflow")
	}
	for i := range points {
		if points[i].closed != 1 || ranges[i].closed != 1 {
			t.Fatalf("component %d actual closes %d/%d", i, points[i].closed, ranges[i].closed)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

// Across separate Gets, prefer the configured unused capacity over evicting an
// unleased inline table. This also exercises lazy expansion without all-leased
// fanout: five disjoint files are in one actual L0 sublevel.
func TestGetCursorOverflowAfterFourUnleasedTables(t *testing.T) {
	db, err := Open("disjoint-retention", &Options{FS: vfs.NewMem(), DisableAutomaticCompactions: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key-%02d", i))
		if err := db.Set(key, key, NoSync); err != nil {
			t.Fatal(err)
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if st := db.Metrics().Levels[0]; st.Sublevels != 1 || st.NumFiles != 5 {
		t.Fatalf("actual disjoint fixture: %+v", st)
	}
	snap := db.NewSnapshot()
	defer snap.Close()
	c, err := snap.NewGetCursorWithOptions(GetCursorOptions{MaxRetainedTables: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key-%02d", i)
		value, err := c.Get([]byte(key))
		if err != nil || string(value) != key {
			t.Fatalf("Get(%s)=%q, %v", key, value, err)
		}
		if err := c.ReleaseValue(); err != nil {
			t.Fatal(err)
		}
		if i < 4 && c.extendedTables != nil {
			t.Fatal("allocated overflow before fifth distinct file")
		}
	}
	if st := c.Stats(); st.MaxRetained != 5 || st.TableEvictions != 0 || len(c.extendedTables) != 12 {
		t.Fatalf("fifth table should use free retained capacity: %+v, overflow=%d", st, len(c.extendedTables))
	}
}
