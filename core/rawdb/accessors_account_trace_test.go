package rawdb

import (
	"bytes"
	"strings"
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

func TestAccountTraceStrictReportsMalformedHotRow(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := mustAddr(0xab)
	if err := db.Put(accountTraceKey(owner, 100), []byte{0xff, 0xfe}); err != nil {
		t.Fatalf("put malformed account trace: %v", err)
	}
	if got, ok := ReadAccountTrace(db, owner, 100); ok || got != 0 {
		t.Fatalf("ReadAccountTrace malformed hot row = %d/%v, want compatibility miss", got, ok)
	}
	if got, ok, err := ReadAccountTraceStrict(db, owner, 100); err == nil || ok || got != 0 || !strings.Contains(err.Error(), "unmarshal") {
		t.Fatalf("ReadAccountTraceStrict malformed hot row = %d/%v/%v, want unmarshal error", got, ok, err)
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

func TestAccountTrace_ReadAtOrBefore(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	a := mustAddr(0xaa)
	b := mustAddr(0xbb)
	_ = WriteAccountTrace(db, a, 100, 100)
	_ = WriteAccountTrace(db, a, 200, 200)
	_ = WriteAccountTrace(db, a, 350, 350)
	_ = WriteAccountTrace(db, b, 250, 999)

	for _, tc := range []struct {
		name      string
		owner     []byte
		blockNum  int64
		wantBlock int64
		wantBal   int64
		wantOK    bool
	}{
		{name: "exact", owner: a, blockNum: 200, wantBlock: 200, wantBal: 200, wantOK: true},
		{name: "previous", owner: a, blockNum: 300, wantBlock: 200, wantBal: 200, wantOK: true},
		{name: "newest below", owner: a, blockNum: 400, wantBlock: 350, wantBal: 350, wantOK: true},
		{name: "before first", owner: a, blockNum: 50, wantOK: false},
		{name: "owner isolated", owner: b, blockNum: 300, wantBlock: 250, wantBal: 999, wantOK: true},
	} {
		gotBlock, gotBal, ok, err := ReadAccountTraceAtOrBefore(db, tc.owner, tc.blockNum)
		if err != nil {
			t.Fatalf("%s: ReadAccountTraceAtOrBefore error: %v", tc.name, err)
		}
		if ok != tc.wantOK || gotBlock != tc.wantBlock || gotBal != tc.wantBal {
			t.Fatalf("%s: got block=%d balance=%d ok=%v, want block=%d balance=%d ok=%v",
				tc.name, gotBlock, gotBal, ok, tc.wantBlock, tc.wantBal, tc.wantOK)
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

func TestAccountTraceRejectsNegativeBlockNumbers(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := mustAddr(0xee)
	if err := WriteAccountTrace(db, owner, -1, 1); err == nil {
		t.Fatal("WriteAccountTrace accepted negative block")
	}
	if got, ok := ReadAccountTrace(db, owner, -1); got != 0 || ok {
		t.Fatalf("ReadAccountTrace negative block = %d/%v, want 0/false", got, ok)
	}
	if got, ok, err := ReadAccountTraceStrict(db, owner, -1); err == nil || got != 0 || ok {
		t.Fatalf("ReadAccountTraceStrict negative block = %d/%v/%v, want error", got, ok, err)
	}
	if err := DeleteAccountTrace(db, owner, -1); err == nil {
		t.Fatal("DeleteAccountTrace accepted negative block")
	}
}

func TestIterateAccountTraceRowsRejectsNegativeEncodedRow(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := mustAddr(0xef)
	data := []byte{0x08, 0x01}
	if err := db.Put(accountTraceKey(owner, -1), data); err != nil {
		t.Fatalf("put negative account trace: %v", err)
	}
	if err := IterateAccountTraceRows(db, 0, 1, func(_ []byte, _ int64, _ int64) (bool, error) {
		t.Fatal("IterateAccountTraceRows called callback for negative row")
		return false, nil
	}); err == nil || !strings.Contains(err.Error(), "negative block") {
		t.Fatalf("IterateAccountTraceRows negative row error = %v, want negative-block error", err)
	}
	if block, balance, ok, err := ReadAccountTraceAtOrBefore(db, owner, 0); err == nil || block != 0 || balance != 0 || ok || !strings.Contains(err.Error(), "negative block") {
		t.Fatalf("ReadAccountTraceAtOrBefore negative row = %d/%d/%v/%v, want negative-block error", block, balance, ok, err)
	}
}
