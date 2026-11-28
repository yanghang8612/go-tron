package pebbledb

import (
	"errors"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/tronprotocol/go-tron/core/pointread"
	"testing"
)

// Exercise the actual production factories rather than duplicating their options.
func TestStorageCursorRetentionRoleIsolation(t *testing.T) {
	engine, err := pebble.Open(t.TempDir(), &pebble.Options{DisableAutomaticCompactions: true, L0StopWritesThreshold: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	for i := 0; i < 20; i++ {
		for _, k := range []string{"a", "z"} {
			if err = engine.Set([]byte(k), []byte(fmt.Sprint(i)), pebble.NoSync); err != nil {
				t.Fatal(err)
			}
		}
		if err = engine.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if got := engine.Metrics().Levels[0].Sublevels; got != 20 {
		t.Fatalf("actual L0=%d", got)
	}
	db := &Database{db: engine, boundedPointGet: true}
	snap, err := db.NewKeyValueSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	cursor, err := snap.(pointread.KeyValueCursorSnapshot).NewPointReadCursor()
	if err != nil {
		t.Fatal(err)
	}
	c := cursor.(*storagePointCursor)
	if found, e := c.View([]byte("m"), func([]byte) error { t.Fatal("unexpected value"); return nil }); found || e != nil {
		t.Fatalf("missing: %v %v", found, e)
	}
	if st := c.inner.Stats(); st.MaxRetained <= 16 || st.MaxRetained > 32 || st.TableFallbackAllLeased != 0 {
		t.Fatalf("canonical retention %+v", st)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	// Bounded commitment cursor keeps four retained pairs.
	p, e := db.NewPointReadSnapshot()
	if e != nil {
		t.Fatal(e)
	}
	r, e := p.NewCursor([]byte("m"))
	if e != nil {
		t.Fatal(e)
	}
	if found, e := r.View([]byte("m"), func([]byte) error { t.Fatal("unexpected value"); return nil }); found || e != nil {
		t.Fatalf("role missing: %v %v", found, e)
	}
	stats := r.(*pointReadCursor).getCursor.Stats()
	if stats.MaxRetained != 4 || stats.TableFallbackAllLeased == 0 {
		t.Fatalf("bounded commitment retention %+v", stats)
	}
	if e = r.Close(); e != nil {
		t.Fatal(e)
	}
	if e = p.Close(); e != nil {
		t.Fatal(e)
	}

	// Flush past 32 overlapping L0 tables; unbounded prefetch retains 32 before falling back.
	for i := 20; i < 36; i++ {
		for _, k := range []string{"a", "z"} {
			if err = engine.Set([]byte(k), []byte(fmt.Sprint(i)), pebble.NoSync); err != nil {
				t.Fatal(err)
			}
		}
		if err = engine.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	p2, e := db.NewPointReadSnapshot()
	if e != nil {
		t.Fatal(e)
	}
	r2, e := p2.NewCursor(nil)
	if e != nil {
		t.Fatal(e)
	}
	if found, e := r2.View([]byte("m"), func([]byte) error { t.Fatal("unexpected value"); return nil }); found || e != nil {
		t.Fatalf("unbounded role missing: %v %v", found, e)
	}
	stats2 := r2.(*pointReadCursor).getCursor.Stats()
	if stats2.MaxRetained != 32 || stats2.TableFallbackAllLeased == 0 {
		t.Fatalf("unbounded prefetch retention %+v", stats2)
	}
	if e = r2.Close(); e != nil {
		t.Fatal(e)
	}
	if e = p2.Close(); e != nil {
		t.Fatal(e)
	}
}

func TestPrefetchRetentionRecoveryKeepsSixteenAndSnapshot(t *testing.T) {
	fs := &pointGetFailFS{FS: vfs.NewMem()}
	cache := pebble.NewCache(0)
	p, e := pebble.Open("recovery-retention", &pebble.Options{FS: fs, Cache: cache, Comparer: exactPointComparer, DisableAutomaticCompactions: true, L0StopWritesThreshold: 64})
	cache.Unref()
	if e != nil {
		t.Fatal(e)
	}
	d := &Database{db: p, boundedPointGet: true}
	defer d.Close()
	for i := 0; i < 36; i++ {
		for _, key := range []string{"a", "z"} {
			if e = p.Set([]byte(key), []byte(fmt.Sprint(i)), pebble.NoSync); e != nil {
				t.Fatal(e)
			}
		}
		if e = p.Flush(); e != nil {
			t.Fatal(e)
		}
	}
	s, e := d.NewPointReadSnapshot()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	r, e := s.NewCursor(nil)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	fs.fail.Store(true)
	_, e = r.View([]byte("m"), func([]byte) error { t.Fatal("missing callback"); return nil })
	fs.fail.Store(false)
	if !errors.Is(e, errPointGetIO) || r.(*pointReadCursor).getCursor != nil {
		t.Fatalf("failed inner was not discarded: %v", e)
	}
	if e = p.Set([]byte("m"), []byte("newer"), pebble.NoSync); e != nil {
		t.Fatal(e)
	}
	if e = p.Flush(); e != nil {
		t.Fatal(e)
	}
	ok, e := r.View([]byte("m"), func([]byte) error { t.Fatal("retry changed pinned cut"); return nil })
	if ok || e != nil {
		t.Fatalf("pinned missing retry: %t/%v", ok, e)
	}
	stats := r.(*pointReadCursor).getCursor.Stats()
	if stats.MaxRetained != 32 || stats.TableFallbackAllLeased == 0 {
		t.Fatalf("recreated prefetch policy: %+v", stats)
	}
}
