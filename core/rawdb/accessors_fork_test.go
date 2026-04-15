package rawdb

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/core/rawdb"
)

func TestForkStats_WriteReadRoundTrip(t *testing.T) {
	db := rawdb.NewMemoryDatabase()

	stats := make([]byte, 27)
	stats[0] = 0x01
	stats[5] = 0x01
	stats[26] = 0x01
	WriteForkStats(db, 35, stats)

	got := ReadForkStats(db, 35)
	if !bytes.Equal(got, stats) {
		t.Errorf("round trip: got %v, want %v", got, stats)
	}
}

func TestForkStats_MissingVersionReturnsNil(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if got := ReadForkStats(db, 99); got != nil {
		t.Errorf("missing version: got %v, want nil", got)
	}
}

func TestForkStats_DifferentVersionsIsolated(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	WriteForkStats(db, 35, []byte{1, 0, 1})
	WriteForkStats(db, 36, []byte{0, 0, 1})

	if got := ReadForkStats(db, 35); !bytes.Equal(got, []byte{1, 0, 1}) {
		t.Errorf("v35: got %v", got)
	}
	if got := ReadForkStats(db, 36); !bytes.Equal(got, []byte{0, 0, 1}) {
		t.Errorf("v36: got %v", got)
	}
}

func TestForkStats_OverwriteReplacesValue(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	WriteForkStats(db, 35, []byte{1, 1, 1})
	WriteForkStats(db, 35, []byte{0, 0, 0})

	if got := ReadForkStats(db, 35); !bytes.Equal(got, []byte{0, 0, 0}) {
		t.Errorf("overwrite: got %v", got)
	}
}
