package rawdb

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/core/rawdb"
)

func TestAccountNameIndex_RoundTrip(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	name := []byte("alice")
	owner := []byte{0x41, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa}

	if HasAccountNameIndex(db, name) {
		t.Fatal("absent name reports present")
	}
	if got := ReadAccountNameIndex(db, name); got != nil {
		t.Fatalf("absent: got %x, want nil", got)
	}

	if err := WriteAccountNameIndex(db, name, owner); err != nil {
		t.Fatal(err)
	}
	if !HasAccountNameIndex(db, name) {
		t.Fatal("after write: Has returned false")
	}
	if got := ReadAccountNameIndex(db, name); !bytes.Equal(got, owner) {
		t.Fatalf("read: got %x, want %x", got, owner)
	}

	if err := DeleteAccountNameIndex(db, name); err != nil {
		t.Fatal(err)
	}
	if HasAccountNameIndex(db, name) {
		t.Fatal("after delete: Has returned true")
	}
}
