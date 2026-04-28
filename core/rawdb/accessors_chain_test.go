package rawdb

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
)

func testAddr(b byte) common.Address {
	var addr common.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

func TestActiveWitnesses(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()

	got := ReadActiveWitnesses(db)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}

	witnesses := []common.Address{testAddr(1), testAddr(2), testAddr(3)}
	WriteActiveWitnesses(db, witnesses)

	got = ReadActiveWitnesses(db)
	if len(got) != 3 {
		t.Fatalf("expected 3 witnesses, got %d", len(got))
	}
	for i, w := range got {
		if w != witnesses[i] {
			t.Fatalf("witness %d: want %x, got %x", i, witnesses[i], w)
		}
	}

	witnesses2 := []common.Address{testAddr(4)}
	WriteActiveWitnesses(db, witnesses2)
	got = ReadActiveWitnesses(db)
	if len(got) != 1 || got[0] != testAddr(4) {
		t.Fatalf("overwrite failed: got %v", got)
	}
}

func TestWitnessIndex(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()

	got := ReadWitnessIndex(db)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}

	AppendWitnessIndex(db, testAddr(1))
	AppendWitnessIndex(db, testAddr(2))
	AppendWitnessIndex(db, testAddr(3))

	got = ReadWitnessIndex(db)
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
	if got[0] != testAddr(1) || got[1] != testAddr(2) || got[2] != testAddr(3) {
		t.Fatalf("unexpected witnesses: %v", got)
	}

	AppendWitnessIndex(db, testAddr(2))
	got = ReadWitnessIndex(db)
	if len(got) != 3 {
		t.Fatalf("duplicate added: got %d", len(got))
	}
}

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
