package rawdb

import (
	"testing"

	"github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
)

func TestShuffledWitnesses_RoundTrip(t *testing.T) {
	db := rawdb.NewMemoryDatabase()

	if got := ReadShuffledWitnesses(db); got != nil {
		t.Fatalf("absent: got %v, want nil", got)
	}

	var a, b, c tcommon.Address
	for i := range a {
		a[i] = 0xaa
		b[i] = 0xbb
		c[i] = 0xcc
	}
	want := []tcommon.Address{a, b, c}
	if err := WriteShuffledWitnesses(db, want); err != nil {
		t.Fatal(err)
	}

	got := ReadShuffledWitnesses(db)
	if len(got) != len(want) {
		t.Fatalf("len: got %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("idx %d: got %x, want %x", i, got[i], want[i])
		}
	}

	if err := DeleteShuffledWitnesses(db); err != nil {
		t.Fatal(err)
	}
	if ReadShuffledWitnesses(db) != nil {
		t.Fatal("after delete: still present")
	}
}

func TestShuffledWitnesses_CorruptedLength(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	// Write a raw value that's not a multiple of AddressLength.
	_ = db.Put(shuffledWitnessesKey, []byte{0x41, 0xaa, 0xbb})
	if got := ReadShuffledWitnesses(db); got != nil {
		t.Fatalf("corrupted value must return nil, got %v", got)
	}
}
