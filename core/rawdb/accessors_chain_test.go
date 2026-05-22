package rawdb

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
)

func TestTotalTransactionCount(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()

	// Initial read returns 0.
	if n := ReadTotalTransactionCount(db); n != 0 {
		t.Fatalf("initial count: want 0, got %d", n)
	}

	WriteTotalTransactionCount(db, 42)
	if n := ReadTotalTransactionCount(db); n != 42 {
		t.Fatalf("after write 42: want 42, got %d", n)
	}

	// Overwrite with a larger value.
	WriteTotalTransactionCount(db, 1_000_000)
	if n := ReadTotalTransactionCount(db); n != 1_000_000 {
		t.Fatalf("after write 1000000: want 1000000, got %d", n)
	}

	// Increment simulation.
	prev := ReadTotalTransactionCount(db)
	WriteTotalTransactionCount(db, prev+5)
	if n := ReadTotalTransactionCount(db); n != 1_000_005 {
		t.Fatalf("after +5: want 1000005, got %d", n)
	}
}
