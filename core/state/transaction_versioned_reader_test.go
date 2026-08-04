package state

import (
	"encoding/binary"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type testTransactionVersion struct {
	txIndex int
	value   TransactionWriteValue
}

type testTransactionVersionedReader map[TransactionAccessKey][]testTransactionVersion

func (reader testTransactionVersionedReader) ReadTransactionVersionedValue(key TransactionAccessKey, txIndex int) (TransactionWriteValue, int, bool) {
	versions := reader[key]
	for index := len(versions) - 1; index >= 0; index-- {
		if versions[index].txIndex < txIndex {
			return versions[index].value, versions[index].txIndex, true
		}
	}
	return TransactionWriteValue{}, 0, false
}

func testVersionedInt64(value int64) TransactionWriteValue {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(value))
	return TransactionWriteValue{Exists: true, Value: encoded[:]}
}

func TestTransactionVersionedReaderOverlaysAccountAndKV(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(41)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 10)
	sdb.SetAllowance(addr, 1)
	sdb.SetAccountKV(addr, kvdomains.SystemReward, []byte("reward"), []byte("durable"))
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}

	reader := testTransactionVersionedReader{
		{Kind: TransactionAccessAccountField, Address: addr, AccountField: TransactionAccountFieldBalance}: {
			{txIndex: 1, value: testVersionedInt64(20)},
			{txIndex: 7, value: testVersionedInt64(99)},
		},
		{Kind: TransactionAccessAccountField, Address: addr, AccountField: TransactionAccountFieldAllowance}: {
			{txIndex: 2, value: testVersionedInt64(5)},
		},
		{Kind: TransactionAccessAccountKV, Address: addr, KVDomain: kvdomains.SystemReward, LogicalKey: "reward"}: {
			{txIndex: 3, value: TransactionWriteValue{Exists: true, Value: []byte("shared")}},
		},
		{Kind: TransactionAccessAccountKV, Address: addr, KVDomain: kvdomains.SystemReward, LogicalKey: "deleted"}: {
			{txIndex: 4, value: TransactionWriteValue{}},
		},
	}
	sdb.SetTransactionVersionedValueReader(reader, 6)
	if got := sdb.GetBalance(addr); got != 20 {
		t.Fatalf("versioned balance = %d, want 20", got)
	}
	if got := sdb.GetAllowance(addr); got != 5 {
		t.Fatalf("versioned allowance = %d, want 5", got)
	}
	if got, exists, err := sdb.GetAccountKV(addr, kvdomains.SystemReward, []byte("reward")); err != nil || !exists || string(got) != "shared" {
		t.Fatalf("versioned account KV = %q exists=%v err=%v", got, exists, err)
	}
	if got, exists, err := sdb.GetAccountKV(addr, kvdomains.SystemReward, []byte("deleted")); err != nil || exists || got != nil {
		t.Fatalf("versioned tombstone = %q exists=%v err=%v", got, exists, err)
	}

	// Once hydrated, task-local writes take precedence over the immutable
	// boundary reader and remain visible to the sender suffix.
	sdb.AddBalance(addr, 7)
	if got := sdb.GetBalance(addr); got != 27 {
		t.Fatalf("local balance = %d, want 27", got)
	}
}

func TestTransactionVersionedReaderHydratesFullAccountAndDeletion(t *testing.T) {
	source := newTestStateDB(t)
	addr := testAddr(42)
	source.CreateAccount(addr, corepb.AccountType_Contract)
	source.AddBalance(addr, 33)
	full, err := source.transactionWriteValue(TransactionAccessKey{Kind: TransactionAccessAccount, Address: addr}, source.DynamicProperties())
	if err != nil {
		t.Fatal(err)
	}

	worker := newTestStateDB(t)
	worker.SetTransactionVersionedValueReader(testTransactionVersionedReader{
		{Kind: TransactionAccessAccount, Address: addr}: {
			{txIndex: 1, value: full},
		},
		{Kind: TransactionAccessAccountField, Address: addr, AccountField: TransactionAccountFieldBalance}: {
			{txIndex: 2, value: testVersionedInt64(44)},
		},
	}, 3)
	if accountType, exists := worker.GetAccountType(addr); !exists || accountType != corepb.AccountType_Contract {
		t.Fatalf("versioned full account type = %v exists=%v", accountType, exists)
	}
	if got := worker.GetBalance(addr); got != 44 {
		t.Fatalf("versioned full account balance = %d, want 44", got)
	}

	deleted := newTestStateDB(t)
	deleted.SetTransactionVersionedValueReader(testTransactionVersionedReader{
		{Kind: TransactionAccessAccount, Address: addr}: {
			{txIndex: 1, value: full},
		},
		{Kind: TransactionAccessAccountField, Address: addr, AccountField: TransactionAccountFieldExistence}: {
			{txIndex: 2, value: TransactionWriteValue{}},
		},
	}, 3)
	if deleted.AccountExists(addr) || deleted.GetBalance(addr) != 0 {
		t.Fatal("later versioned deletion did not hide the full account")
	}
}

func TestTransactionVersionedReaderOverlaysStorage(t *testing.T) {
	source := newTestStateDB(t)
	contract := testAddr(43)
	slot := tcommon.BytesToHash([]byte{1})
	durable := tcommon.BytesToHash([]byte{2})
	shared := tcommon.BytesToHash([]byte{3})
	local := tcommon.BytesToHash([]byte{4})
	source.CreateAccount(contract, corepb.AccountType_Contract)
	source.SetState(contract, slot, durable)
	if _, err := source.Commit(); err != nil {
		t.Fatal(err)
	}

	worker, err := source.CopyBlockExecutionBase()
	if err != nil {
		t.Fatal(err)
	}
	worker.SetTransactionVersionedValueReader(testTransactionVersionedReader{
		{Kind: TransactionAccessStorage, Address: contract, StorageKey: slot}: {
			{txIndex: 2, value: TransactionWriteValue{Exists: true, Value: shared.Bytes()}},
		},
	}, 3)
	if got, exists := worker.GetStateWithExist(contract, slot); !exists || got != shared {
		t.Fatalf("versioned storage = %x exists=%v, want %x", got, exists, shared)
	}
	worker.SetState(contract, slot, local)
	if got, exists := worker.GetStateWithExist(contract, slot); !exists || got != local {
		t.Fatalf("task-local storage = %x exists=%v, want %x", got, exists, local)
	}

	deleted, err := source.CopyBlockExecutionBase()
	if err != nil {
		t.Fatal(err)
	}
	deleted.SetTransactionVersionedValueReader(testTransactionVersionedReader{
		{Kind: TransactionAccessStorage, Address: contract, StorageKey: slot}: {
			{txIndex: 2, value: TransactionWriteValue{}},
		},
	}, 3)
	if got, exists := deleted.GetStateWithExist(contract, slot); exists || got != (tcommon.Hash{}) {
		t.Fatalf("versioned storage tombstone = %x exists=%v", got, exists)
	}
}
