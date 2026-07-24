package vm

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
)

func TestInternalTransactionArenaReusesAndClearsRecords(t *testing.T) {
	root := tcommon.HexToHash("010203040506070809")
	caller := tcommon.BytesToAddress([]byte{0x41, 0x11})
	target := tcommon.BytesToAddress([]byte{0x41, 0x22})
	tvm := &TVM{RootTxID: root}
	arena := new(InternalTransactionArena)

	arena.Reset()
	tvm.SetInternalTransactionArena(arena)
	first := tvm.addInternalTransactionWithTokenInfo(caller, target, 7, []byte{1}, "call", map[string]int64{"1000001": 9})
	first.Rejected = true
	firstHashStorage := &first.Hash[0]
	if len(first.CallValueInfo) != 2 {
		t.Fatalf("first call values = %d, want 2", len(first.CallValueInfo))
	}

	arena.Reset()
	tvm.SetInternalTransactionArena(arena)
	second := tvm.addInternalTransaction(caller, target, 3, []byte{2}, "call", 0, 0)
	if second != first {
		t.Fatal("arena did not reuse the protobuf record")
	}
	if &second.Hash[0] != firstHashStorage {
		t.Fatal("arena did not reuse identity byte storage")
	}
	if second.Rejected {
		t.Fatal("reused record retained rejected flag")
	}
	if len(second.CallValueInfo) != 1 || second.CallValueInfo[0].TokenId != "" || second.CallValueInfo[0].CallValue != 3 {
		t.Fatalf("reused call values = %+v, want one base value", second.CallValueInfo)
	}
	if len(tvm.InternalTransactions) != 1 || tvm.InternalTransactions[0] != second {
		t.Fatalf("internal transaction list was not reset: %+v", tvm.InternalTransactions)
	}
}

func TestInternalTransactionArenaDropsPathologicalHighWater(t *testing.T) {
	arena := new(InternalTransactionArena)
	for range maxRetainedInternalTransactionArenaEntries + 1 {
		record, _ := arena.acquire(80)
		arena.transactions = append(arena.transactions, &record.tx)
	}
	arena.Reset()
	if arena.entries != nil || arena.transactions != nil || arena.used != 0 {
		t.Fatalf("oversized arena retained entries=%d transaction-cap=%d used=%d",
			len(arena.entries), cap(arena.transactions), arena.used)
	}
}
