package rawdb

import (
	"testing"

	"github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
)

func TestWitnessLatestBlock_RoundTrip(t *testing.T) {
	db := rawdb.NewMemoryDatabase()

	var addr tcommon.Address
	for i := range addr {
		addr[i] = 0xab
	}

	// Default: returns 0 when absent.
	if got := ReadWitnessLatestBlock(db, addr); got != 0 {
		t.Fatalf("absent: got %d, want 0", got)
	}

	// Write and read back.
	WriteWitnessLatestBlock(db, addr, 42)
	if got := ReadWitnessLatestBlock(db, addr); got != 42 {
		t.Fatalf("after write: got %d, want 42", got)
	}

	// Update to a larger value.
	WriteWitnessLatestBlock(db, addr, 1_000_000)
	if got := ReadWitnessLatestBlock(db, addr); got != 1_000_000 {
		t.Fatalf("after update: got %d, want 1000000", got)
	}
}

func TestWitnessLatestBlock_MultipleWitnesses(t *testing.T) {
	db := rawdb.NewMemoryDatabase()

	var a, b tcommon.Address
	for i := range a {
		a[i] = 0xaa
		b[i] = 0xbb
	}

	WriteWitnessLatestBlock(db, a, 100)
	WriteWitnessLatestBlock(db, b, 200)

	if got := ReadWitnessLatestBlock(db, a); got != 100 {
		t.Fatalf("addr a: got %d, want 100", got)
	}
	if got := ReadWitnessLatestBlock(db, b); got != 200 {
		t.Fatalf("addr b: got %d, want 200", got)
	}
}
