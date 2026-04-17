package rawdb

import (
	"testing"

	"github.com/ethereum/go-ethereum/core/rawdb"
)

func TestAccountAsset_RoundTrip(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := []byte{0x41, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa}

	if got := ReadAccountAsset(db, owner, 1001); got != 0 {
		t.Fatalf("absent: got %d, want 0", got)
	}
	if err := WriteAccountAsset(db, owner, 1001, 12345); err != nil {
		t.Fatal(err)
	}
	if got := ReadAccountAsset(db, owner, 1001); got != 12345 {
		t.Fatalf("roundtrip: got %d, want 12345", got)
	}
	if err := WriteAccountAsset(db, owner, 1001, 67890); err != nil {
		t.Fatal(err)
	}
	if got := ReadAccountAsset(db, owner, 1001); got != 67890 {
		t.Fatalf("overwrite: got %d, want 67890", got)
	}
}

func TestAccountAsset_SeparatedByTokenID(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := []byte{0x41, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb}
	_ = WriteAccountAsset(db, owner, 1001, 100)
	_ = WriteAccountAsset(db, owner, 2002, 200)
	if ReadAccountAsset(db, owner, 1001) != 100 || ReadAccountAsset(db, owner, 2002) != 200 {
		t.Fatal("token IDs collided")
	}
}

func TestAccountAsset_SeparatedByOwner(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	a := []byte{0x41, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa}
	b := []byte{0x41, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb}
	_ = WriteAccountAsset(db, a, 1001, 1)
	_ = WriteAccountAsset(db, b, 1001, 2)
	if ReadAccountAsset(db, a, 1001) != 1 || ReadAccountAsset(db, b, 1001) != 2 {
		t.Fatal("owners collided")
	}
}

func TestAccountAsset_Delete(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := []byte{0x41, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc}
	_ = WriteAccountAsset(db, owner, 42, 999)
	if err := DeleteAccountAsset(db, owner, 42); err != nil {
		t.Fatal(err)
	}
	if got := ReadAccountAsset(db, owner, 42); got != 0 {
		t.Fatalf("after delete: got %d, want 0", got)
	}
}

func TestAccountAsset_EmptyOwnerRejected(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if err := WriteAccountAsset(db, nil, 1, 1); err == nil {
		t.Fatal("expected error for empty owner")
	}
}
