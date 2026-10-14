package state

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type retainingVersionedKVReader struct {
	keys      []TransactionAccessKey
	txIndexes []int
	value     TransactionWriteValue
	present   bool
}

func (r *retainingVersionedKVReader) ReadTransactionVersionedValue(key TransactionAccessKey, txIndex int) (TransactionWriteValue, int, bool) {
	r.keys = append(r.keys, key)
	r.txIndexes = append(r.txIndexes, txIndex)
	return r.value, txIndex - 1, r.present
}

func TestTransactionVersionedAccountKVEnabledOwnsLogicalKey(t *testing.T) {
	addr := testAddr(0xf5)
	key := []byte("reused storage key")
	reader := &retainingVersionedKVReader{value: TransactionWriteValue{Exists: true, Value: []byte("versioned")}, present: true}
	s := newTestStateDB(t)
	s.SetTransactionVersionedValueReader(reader, 7)
	value, exists, versioned := s.readTransactionVersionedAccountKV(addr, kvdomains.ContractStorage, key)
	if string(value) != "versioned" || !exists || !versioned {
		t.Fatalf("versioned read=%q/%v/%v", value, exists, versioned)
	}
	clear(key)
	if len(reader.keys) != 1 || reader.keys[0] != (TransactionAccessKey{Kind: TransactionAccessAccountKV, Address: addr, KVDomain: kvdomains.ContractStorage, LogicalKey: "reused storage key"}) || reader.txIndexes[0] != 7 {
		t.Fatalf("reader identity/ownership changed: %+v indexes=%v", reader.keys, reader.txIndexes)
	}
	reader.value = TransactionWriteValue{}
	value, exists, versioned = s.readTransactionVersionedAccountKV(addr, kvdomains.ContractStorage, key)
	if value != nil || exists || !versioned {
		t.Fatalf("versioned tombstone=%q/%v/%v", value, exists, versioned)
	}
	reader.present = false
	value, exists, versioned = s.readTransactionVersionedAccountKV(addr, kvdomains.ContractStorage, key)
	if value != nil || exists || versioned {
		t.Fatalf("version miss=%q/%v/%v", value, exists, versioned)
	}
	s.SetTransactionVersionedValueReader(nil, 0)
	calls := len(reader.keys)
	value, exists, versioned = s.readTransactionVersionedAccountKV(addr, kvdomains.ContractStorage, key)
	if value != nil || exists || versioned || len(reader.keys) != calls {
		t.Fatal("disabled reader retained versioned semantics")
	}
}

func TestTransactionVersionedAccountKVDisabledReadAllocations(t *testing.T) {
	var s StateDB
	key := make([]byte, tcommon.HashLength)
	key[31] = 1
	addr := testAddr(0xf6)
	allocations := testing.AllocsPerRun(1000, func() {
		value, exists, versioned := s.readTransactionVersionedAccountKV(addr, kvdomains.ContractStorage, key)
		if value != nil || exists || versioned {
			t.Fatal("disabled reader returned a versioned value")
		}
	})
	if allocations != 0 {
		t.Fatalf("disabled versioned KV read allocated %.0f times, want zero", allocations)
	}
}
