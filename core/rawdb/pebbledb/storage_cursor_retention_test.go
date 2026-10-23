package pebbledb

import (
	"fmt"
	"github.com/cockroachdb/pebble"
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
	for i := 0; i < 14; i++ {
		for _, k := range []string{"a", "z"} {
			if err = engine.Set([]byte(k), []byte(fmt.Sprint(i)), pebble.NoSync); err != nil {
				t.Fatal(err)
			}
		}
		if err = engine.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if got := engine.Metrics().Levels[0].Sublevels; got != 14 {
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
	if st := c.inner.Stats(); st.MaxRetained <= 4 || st.MaxRetained > 16 || st.TableFallbackAllLeased != 0 {
		t.Fatalf("canonical retention %+v", st)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	// Both bounded commitment and unbounded prefetch keep the default four.
	for _, prefix := range [][]byte{nil, []byte("m")} {
		p, e := db.NewPointReadSnapshot()
		if e != nil {
			t.Fatal(e)
		}
		r, e := p.NewCursor(prefix)
		if e != nil {
			t.Fatal(e)
		}
		if found, e := r.View([]byte("m"), func([]byte) error { t.Fatal("unexpected value"); return nil }); found || e != nil {
			t.Fatalf("role missing: %v %v", found, e)
		}
		stats := r.(*pointReadCursor).getCursor.Stats()
		if stats.MaxRetained != 4 || stats.TableFallbackAllLeased == 0 {
			t.Fatalf("noncanonical retention %+v", stats)
		}
		if e = r.Close(); e != nil {
			t.Fatal(e)
		}
		if e = p.Close(); e != nil {
			t.Fatal(e)
		}
	}
}
