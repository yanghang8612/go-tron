package pebbledb

import (
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/metrics"
)

func boundTestRanges() []DiskSpaceRange {
	return []DiskSpaceRange{{Name: "commitment", Start: []byte("c"), End: []byte("d")}}
}
func boundTestTable(small, large string, size uint64) pebble.TableInfo {
	return pebble.TableInfo{Size: size, Smallest: pebble.InternalKey{UserKey: []byte(small)}, Largest: pebble.InternalKey{UserKey: []byte(large)}}
}
func boundTestInfo() pebble.CompactionInfo {
	return pebble.CompactionInfo{Done: true, Reason: "default", Input: []pebble.LevelInfo{{Level: 0, Tables: []pebble.TableInfo{
		boundTestTable("c", "c9", 10), boundTestTable("", "b", 20), boundTestTable("b", "d", 30), boundTestTable("z", "a", 40),
	}}}, Output: pebble.LevelInfo{Level: 1, Tables: []pebble.TableInfo{
		boundTestTable("c", "c1", 11), boundTestTable("d", "z", 21), boundTestTable("c", "d", 31), boundTestTable("z", "a", 41),
	}}}
}
func TestCompactionBoundsExclusiveAndOwnership(t *testing.T) {
	ranges := boundTestRanges()
	o := newCompactionBoundsObserver(t.Name()+"/", ranges)
	ranges[0].Start[0], ranges[0].End[0] = 'x', 'y'
	for _, tc := range []struct {
		small, large string
		class        int
	}{
		{"", "", compactionBoundOutside}, {"", "b", compactionBoundOutside},
		{"b", "c", compactionBoundCross}, {"c", "c", compactionBoundInside},
		{"c", "cz", compactionBoundInside}, {"c", "d", compactionBoundCross},
		{"d", "d", compactionBoundOutside}, {"d", "z", compactionBoundOutside},
		{"a", "z", compactionBoundCross}, {"z", "a", compactionBoundUnknown},
	} {
		if got := o.classify([]byte(tc.small), []byte(tc.large)); got != tc.class {
			t.Fatalf("%q..%q got %d want %d", tc.small, tc.large, got, tc.class)
		}
	}
	for _, r := range [][]DiskSpaceRange{nil, {{Name: "other", Start: []byte("c"), End: []byte("d")}}, {{Name: "commitment", Start: []byte("d"), End: []byte("c")}}} {
		u := newCompactionBoundsObserver(t.Name()+"/unconfigured/", r)
		if u.classify([]byte("c"), []byte("c")) != compactionBoundUnknown {
			t.Fatal("missing/invalid range inferred bounds")
		}
	}
}
func TestCompactionBoundsInputOnlyCohortsAndZeroElision(t *testing.T) {
	o := newCompactionBoundsObserver(t.Name()+"/", boundTestRanges())
	info := boundTestInfo()
	o.observe(info)
	for c := range o.cohorts {
		if o.cohorts[c].jobs.Snapshot().Count() != 1 {
			t.Fatalf("cohort %d", c)
		}
		for direction := range o.cohorts[c].tables {
			for class, m := range o.cohorts[c].tables[direction] {
				if m.tables.Snapshot().Count() != 1 || m.tableSizeBytes.Snapshot().Count() != int64(10*(class+1)+direction) {
					t.Fatalf("wrong aggregate %d/%d/%d", c, direction, class)
				}
			}
		}
	}
	// Output alone cannot select either input-dependent cohort.
	info.Input[0].Tables = []pebble.TableInfo{boundTestTable("c", "c", 1)}
	o.observe(info)
	if o.cohorts[0].jobs.Snapshot().Count() != 2 || o.cohorts[1].jobs.Snapshot().Count() != 1 || o.cohorts[2].jobs.Snapshot().Count() != 1 {
		t.Fatal("output selected input cohort")
	}
	// Empty input and output are a legal successful elision.
	o.observe(pebble.CompactionInfo{Done: true, Reason: "elision-only"})
	if o.cohorts[0].jobs.Snapshot().Count() != 3 {
		t.Fatal("empty elision dropped")
	}
}
func TestCompactionBoundsReasonsAndLifecycle(t *testing.T) {
	for _, reason := range []string{"default", "elision-only", "read", "rewrite", "move", "copy", "delete-only", "flush", "ingested-flushable", "unknown", ""} {
		for _, status := range []string{"success", "failed", "not_done"} {
			t.Run(reason+"/"+status, func(t *testing.T) {
				o := newCompactionBoundsObserver(t.Name()+"/", boundTestRanges())
				d := &Database{compactionBounds: o}
				info := boundTestInfo()
				info.Reason = reason
				if status == "failed" {
					info.Err = errors.New("injected")
				}
				if status == "not_done" {
					info.Done = false
					info.Err = errors.New("ignored reason")
				}
				d.onCompactionBegin(info)
				start := d.compStartTime
				d.onCompactionBegin(info)
				if d.compStartTime != start || d.activeComp != 2 || d.level0Comp.Load() != 2 {
					t.Fatal("begin lifecycle changed")
				}
				d.onCompactionEnd(info)
				if d.activeComp != 1 || d.compTime.Load() != 0 {
					t.Fatal("busy time ended before last compaction")
				}
				d.onCompactionEnd(info)
				if d.activeComp != 0 || d.compTime.Load() <= 0 {
					t.Fatal("end lifecycle changed")
				}
				event := compactionBoundEvent(&info)
				for i, m := range o.events {
					want := int64(0)
					if i == event {
						want = 2
					}
					if m.Snapshot().Count() != want {
						t.Fatalf("event %d", i)
					}
				}
				want := int64(0)
				if status == "success" && (reason == "default" || reason == "elision-only" || reason == "read" || reason == "rewrite") {
					want = 2
				}
				if o.cohorts[0].jobs.Snapshot().Count() != want {
					t.Fatal("nonrewrite counted")
				}
				before := o.events[event].Snapshot().Count()
				func() {
					defer func() {
						if recover() == nil {
							t.Error("missing original underflow panic")
						}
					}()
					d.onCompactionEnd(info)
				}()
				if o.events[event].Snapshot().Count() != before {
					t.Fatal("underflow panic observed as completed")
				}
			})
		}
	}
	// Nil observer follows the old callback, including a failed event.
	d := &Database{activeComp: 1, compStartTime: time.Now()}
	d.onCompactionEnd(pebble.CompactionInfo{Err: errors.New("failure")})
	if d.activeComp != 0 {
		t.Fatal("nil observer changed lifecycle")
	}
}
func TestCompactionBoundsInstancesConcurrencyAndAllocation(t *testing.T) {
	ns := t.Name() + "/"
	o := newCompactionBoundsObserver(ns, boundTestRanges())
	u := newCompactionBoundsObserver(ns, nil)
	if metrics.GetOrRegisterCounter(ns+"compact/table_bounds/configured_instances", nil).Snapshot().Count() != 1 || metrics.GetOrRegisterCounter(ns+"compact/table_bounds/unconfigured_instances", nil).Snapshot().Count() != 1 {
		t.Fatal("constructor metrics overwritten")
	}
	info := boundTestInfo()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				o.observe(info)
				u.observe(info)
			}
		}()
	}
	wg.Wait()
	if o.cohorts[0].jobs.Snapshot().Count() != 1600 {
		t.Fatal("shared-namespace updates lost")
	}
	for direction := 0; direction < 2; direction++ {
		if o.cohorts[0].tables[direction][compactionBoundUnknown].tables.Snapshot().Count() != 4000 {
			t.Fatal("unconfigured tables not unknown")
		}
	}
	if alloc := testing.AllocsPerRun(100, func() { o.observe(info) }); alloc != 0 {
		t.Fatalf("callback allocates %g", alloc)
	}
}
func TestCompactionBoundsSizeOverflow(t *testing.T) {
	o := newCompactionBoundsObserver(t.Name()+"/", boundTestRanges())
	info := pebble.CompactionInfo{Done: true, Reason: "rewrite", Input: []pebble.LevelInfo{{Tables: []pebble.TableInfo{boundTestTable("c", "c", math.MaxUint64), boundTestTable("c", "c", 1)}}}}
	o.observe(info)
	m := o.cohorts[0].tables[0][compactionBoundInside]
	if m.tables.Snapshot().Count() != 2 || m.tableSizeBytes.Snapshot().Count() != math.MaxInt64 || o.overflows.Snapshot().Count() != 1 {
		t.Fatal("size aggregate wrapped")
	}
}
func TestCompactionBoundsNewInitializesBeforeOpen(t *testing.T) {
	ns := t.Name() + "/"
	tune := DefaultOptions()
	tune.DiskSpaceRanges = boundTestRanges()
	d, err := New(t.TempDir(), 16, 16, ns, false, tune)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if d.compactionBounds == nil || d.compactionBounds.classify([]byte("c"), []byte("c")) != compactionBoundInside {
		t.Fatal("constructor missing configured observer")
	}
	tune.DiskSpaceRanges[0].Start[0] = 'x'
	if d.compactionBounds.classify([]byte("c"), []byte("c")) != compactionBoundInside {
		t.Fatal("caller changed observer range")
	}
}
