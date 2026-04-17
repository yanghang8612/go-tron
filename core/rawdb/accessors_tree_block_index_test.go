package rawdb

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/core/rawdb"
)

func TestTreeBlockIndex_RoundTrip(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	treeKey := []byte("merkle-root-bytes")

	if ReadTreeBlockIndex(db, 12345) != nil {
		t.Fatal("absent: read returned non-nil")
	}
	if err := WriteTreeBlockIndex(db, 12345, treeKey); err != nil {
		t.Fatal(err)
	}
	got := ReadTreeBlockIndex(db, 12345)
	if !bytes.Equal(got, treeKey) {
		t.Fatalf("roundtrip: got %q, want %q", got, treeKey)
	}
	if err := DeleteTreeBlockIndex(db, 12345); err != nil {
		t.Fatal(err)
	}
	if ReadTreeBlockIndex(db, 12345) != nil {
		t.Fatal("after delete: still present")
	}
}

func TestTreeBlockIndex_DistinctBlocks(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	_ = WriteTreeBlockIndex(db, 1, []byte("a"))
	_ = WriteTreeBlockIndex(db, 2, []byte("b"))
	if !bytes.Equal(ReadTreeBlockIndex(db, 1), []byte("a")) {
		t.Fatal("block 1 wrong")
	}
	if !bytes.Equal(ReadTreeBlockIndex(db, 2), []byte("b")) {
		t.Fatal("block 2 wrong")
	}
}
