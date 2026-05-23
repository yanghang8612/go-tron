package state

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestForkStatsStoreRoundTripAtRoot(t *testing.T) {
	sdb := newTestStateDB(t)
	stats := []byte{1, 0, 1}

	if got := sdb.ReadForkStats(35); got != nil {
		t.Fatalf("fork stats should be absent before write, got %x", got)
	}
	sdb.WriteForkStats(35, stats)

	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.ReadForkStats(35); !bytes.Equal(got, stats) {
		t.Fatalf("fork stats = %x, want %x", got, stats)
	}
}

func TestForkStatsStoreIgnoresFutureFlatMirror(t *testing.T) {
	sdb := newTestStateDB(t)
	sdb.WriteForkStats(35, []byte{1, 0, 1})
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	rawdb.WriteForkStats(sdb.db.DiskDB(), 35, []byte{0, 0, 0})

	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.ReadForkStats(35); !bytes.Equal(got, []byte{1, 0, 1}) {
		t.Fatalf("historical root loaded future flat fork stats: %x", got)
	}
}
