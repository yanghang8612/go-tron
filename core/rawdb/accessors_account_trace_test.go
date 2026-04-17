package rawdb

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/core/rawdb"
)

func mustAddr(v byte) []byte {
	out := make([]byte, 21)
	out[0] = 0x41
	for i := 1; i < 21; i++ {
		out[i] = v
	}
	return out
}

func TestAccountTrace_RoundTrip(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := mustAddr(0xaa)

	if got, ok := ReadAccountTrace(db, owner, 100); ok || got != 0 {
		t.Fatalf("absent: got (%d, %v), want (0, false)", got, ok)
	}

	if err := WriteAccountTrace(db, owner, 100, 9_000_000); err != nil {
		t.Fatal(err)
	}
	got, ok := ReadAccountTrace(db, owner, 100)
	if !ok || got != 9_000_000 {
		t.Fatalf("round-trip: got (%d, %v), want (9000000, true)", got, ok)
	}
}

func TestAccountTrace_DistinctBlocksAndOwners(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	a := mustAddr(0xaa)
	b := mustAddr(0xbb)

	_ = WriteAccountTrace(db, a, 100, 100)
	_ = WriteAccountTrace(db, a, 200, 200)
	_ = WriteAccountTrace(db, b, 100, 999)

	for _, tc := range []struct {
		owner    []byte
		blockNum int64
		want     int64
	}{
		{a, 100, 100},
		{a, 200, 200},
		{b, 100, 999},
	} {
		got, ok := ReadAccountTrace(db, tc.owner, tc.blockNum)
		if !ok || got != tc.want {
			t.Fatalf("owner=%x block=%d: got (%d, %v), want (%d, true)", tc.owner, tc.blockNum, got, ok, tc.want)
		}
	}
}

func TestAccountTrace_XOROrdering(t *testing.T) {
	// The XOR-encoded suffix must make newer block numbers sort earlier
	// lexicographically. Verify by comparing two keys for the same owner.
	owner := mustAddr(0xcc)
	keyNewer := accountTraceKey(owner, 2_000_000_000)
	keyOlder := accountTraceKey(owner, 1_000_000_000)
	if bytes.Compare(keyNewer, keyOlder) >= 0 {
		t.Fatalf("XOR ordering broken: newer should sort before older\nnewer %x\nolder %x", keyNewer, keyOlder)
	}
}

func TestAccountTrace_Delete(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := mustAddr(0xdd)
	_ = WriteAccountTrace(db, owner, 42, 777)
	if err := DeleteAccountTrace(db, owner, 42); err != nil {
		t.Fatal(err)
	}
	if _, ok := ReadAccountTrace(db, owner, 42); ok {
		t.Fatal("after delete: trace still present")
	}
}

func TestAccountTrace_RejectsEmptyOwner(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if err := WriteAccountTrace(db, nil, 1, 1); err == nil {
		t.Fatal("expected error for empty owner")
	}
}
