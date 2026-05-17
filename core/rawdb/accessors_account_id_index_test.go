package rawdb

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/core/rawdb"
)

func TestAccountIdIndex_RoundTrip(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	id := []byte("alice-tron")
	owner := []byte{0x41, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa}

	if HasAccountIdIndex(db, id) {
		t.Fatal("absent id reports present")
	}
	if got := ReadAccountIdIndex(db, id); got != nil {
		t.Fatalf("absent: got %x, want nil", got)
	}

	if err := WriteAccountIdIndex(db, id, owner); err != nil {
		t.Fatal(err)
	}
	if !HasAccountIdIndex(db, id) {
		t.Fatal("after write: Has returned false")
	}
	got := ReadAccountIdIndex(db, id)
	if !bytes.Equal(got, owner) {
		t.Fatalf("read: got %x, want %x", got, owner)
	}

	if err := DeleteAccountIdIndex(db, id); err != nil {
		t.Fatal(err)
	}
	if HasAccountIdIndex(db, id) {
		t.Fatal("after delete: Has returned true")
	}
}

func TestAccountIdIndex_RejectsEmptyInputs(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := []byte{0x41}
	if err := WriteAccountIdIndex(db, nil, owner); err == nil {
		t.Fatal("expected error for empty accountID")
	}
	if err := WriteAccountIdIndex(db, []byte("x"), nil); err == nil {
		t.Fatal("expected error for empty owner")
	}
}

func TestAccountIdIndex_DistinctIds(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	ownerA := make([]byte, 21)
	for i := range ownerA {
		ownerA[i] = 0xaa
	}
	ownerB := make([]byte, 21)
	for i := range ownerB {
		ownerB[i] = 0xbb
	}
	_ = WriteAccountIdIndex(db, []byte("alice"), ownerA)
	_ = WriteAccountIdIndex(db, []byte("bob"), ownerB)
	if !bytes.Equal(ReadAccountIdIndex(db, []byte("alice")), ownerA) {
		t.Fatal("alice lookup wrong")
	}
	if !bytes.Equal(ReadAccountIdIndex(db, []byte("bob")), ownerB) {
		t.Fatal("bob lookup wrong")
	}
}

func TestAccountIdIndex_CaseInsensitive(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := bytes.Repeat([]byte{0xaa}, 21)
	if err := WriteAccountIdIndex(db, []byte("AliceID1"), owner); err != nil {
		t.Fatal(err)
	}
	if !HasAccountIdIndex(db, []byte("aliceid1")) {
		t.Fatal("lower-case lookup missing")
	}
	if got := ReadAccountIdIndex(db, []byte("ALICEID1")); !bytes.Equal(got, owner) {
		t.Fatalf("upper-case lookup: got %x, want %x", got, owner)
	}
}
