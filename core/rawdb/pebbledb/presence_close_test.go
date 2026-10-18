package pebbledb

import (
	"errors"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestPresenceSuccessfulGetRetainsValueCloseError(t *testing.T) {
	for _, kind := range []string{"Has", "GetWithPresence", "Snapshot.Has"} {
		t.Run(kind, func(t *testing.T) {
			fail, calls := true, 0
			want := errors.New("merge value close failed")
			merger := &pebble.Merger{Name: "presence-close", Merge: func(k, v []byte) (pebble.ValueMerger, error) {
				m, e := pebble.DefaultMerger.Merge(k, v)
				return &unboundedMerger{ValueMerger: m, failure: &fail, calls: &calls, err: want}, e
			}}
			p, err := pebble.Open("presence", &pebble.Options{FS: vfs.NewMem(), Merger: merger})
			if err != nil {
				t.Fatal(err)
			}
			d := &Database{db: p}
			defer d.Close()
			if err := p.Merge([]byte("k"), []byte("value"), pebble.NoSync); err != nil {
				t.Fatal(err)
			}
			s, err := d.NewKeyValueSnapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			var present bool
			switch kind {
			case "Has":
				present, err = d.Has([]byte("k"))
			case "GetWithPresence":
				_, present, err = d.GetWithPresence([]byte("k"))
			case "Snapshot.Has":
				present, err = s.Has([]byte("k"))
			}
			if present || !errors.Is(err, want) || calls != 1 {
				t.Fatalf("present=%v err=%v closeCalls=%d", present, err, calls)
			}
		})
	}
}
