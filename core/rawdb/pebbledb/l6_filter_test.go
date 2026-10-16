package pebbledb

import (
	"bytes"
	"github.com/cockroachdb/pebble"
	"testing"
)

func TestL6FilterWriterPolicyAndUnboundedReadSelection(t *testing.T) {
	t.Setenv("GTRON_PEBBLE_BOUNDED_POINT_READ", "get")
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "enabled"}[enabled], func(t *testing.T) {
			opts := DefaultOptions()
			opts.EnableL6Filter = enabled
			opts.DisableAutomaticCompactions = true
			db, e := New(t.TempDir(), 16, 32, "l6-policy/", false, opts)
			if e != nil {
				t.Fatal(e)
			}
			defer db.Close()
			// First flush can move trivially. An overlapping second wave forces the L6
			// writer to emit new bytes under the selected policy.
			for _, value := range []string{"old", "current"} {
				if e = db.Put([]byte("a"), []byte(value)); e != nil {
					t.Fatal(e)
				}
				if e = db.Put([]byte("z"), []byte("end")); e != nil {
					t.Fatal(e)
				}
				if e = db.db.Flush(); e != nil {
					t.Fatal(e)
				}
				if e = db.db.Compact([]byte("a"), []byte("zz"), false); e != nil {
					t.Fatal(e)
				}
			}
			tables, e := db.db.SSTables(pebble.WithProperties())
			if e != nil {
				t.Fatal(e)
			}
			if len(tables[6]) == 0 {
				t.Fatal("requires actual L6")
			}
			for _, table := range tables[6] {
				if (table.Properties.FilterSize > 0) != enabled {
					t.Fatalf("L6 filter policy mismatch: %+v", table.Properties)
				}
			}
			snapshot, e := db.NewPointReadSnapshot()
			if e != nil {
				t.Fatal(e)
			}
			defer snapshot.Close()
			bounded, e := snapshot.NewCursor([]byte("a"))
			if e != nil {
				t.Fatal(e)
			}
			defer bounded.Close()
			unbounded, e := snapshot.NewCursor(nil)
			if e != nil {
				t.Fatal(e)
			}
			defer unbounded.Close()
			total := func() int64 { f := db.db.Metrics().Filter; return f.Hits + f.Misses }
			before := total()
			if found, e := bounded.View([]byte("absent"), func([]byte) error { t.Fatal("missing callback"); return nil }); e != nil || found {
				t.Fatalf("bounded miss: %v %v", found, e)
			}
			if total() != before {
				t.Fatal("bounded commitment policy checked L6 filter")
			}
			before = total()
			if found, e := unbounded.View([]byte("absent"), func([]byte) error { t.Fatal("missing callback"); return nil }); e != nil || found {
				t.Fatalf("unbounded miss: %v %v", found, e)
			}
			if enabled && total() <= before {
				t.Fatal("unbounded reader did not opt in")
			}
			if !enabled && total() != before {
				t.Fatal("legacy SST fabricated filter work")
			}
			if found, e := unbounded.View([]byte("a"), func(v []byte) error {
				if !bytes.Equal(v, []byte("current")) {
					t.Fatalf("bad borrowed value %q", v)
				}
				return nil
			}); e != nil || !found {
				t.Fatalf("present: %v %v", found, e)
			}
		})
	}
}
