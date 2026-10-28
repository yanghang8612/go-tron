package state

import (
	"encoding/binary"
	"fmt"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

type testTransactionVersion struct {
	txIndex int
	value   TransactionWriteValue
}

func TestTransactionVersionedMetadataAliasesChooseLatestWriter(t *testing.T) {
	addr := testAddr(0x78)
	typedKey := TransactionAccessKey{Kind: TransactionAccessContractMetadata, Address: addr}
	genericKey := TransactionAccessKey{Kind: TransactionAccessAccountKV, Address: addr, KVDomain: kvdomains.ContractMetadata, LogicalKey: string(contractMetaKVKey)}
	typed, err := proto.Marshal(&contractpb.SmartContract{Name: "typed"})
	if err != nil {
		t.Fatal(err)
	}
	generic, err := statecodec.Marshal(&contractpb.SmartContract{Name: "generic"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name                       string
		typedWriter, genericWriter int
		typedValue, genericValue   TransactionWriteValue
		want                       string
		wantError                  bool
	}{
		{"generic newer", 1, 2, TransactionWriteValue{Exists: true, Value: typed}, TransactionWriteValue{Exists: true, Value: generic}, "generic", false},
		{"typed newer", 2, 1, TransactionWriteValue{Exists: true, Value: typed}, TransactionWriteValue{Exists: true, Value: generic}, "typed", false},
		{"generic deletion newer", 1, 2, TransactionWriteValue{Exists: true, Value: typed}, TransactionWriteValue{}, "", false},
		{"typed deletion newer", 2, 1, TransactionWriteValue{}, TransactionWriteValue{Exists: true, Value: generic}, "", false},
		{"obsolete malformed typed", 1, 2, TransactionWriteValue{Exists: true, Value: []byte{0xff}}, TransactionWriteValue{Exists: true, Value: generic}, "generic", false},
		{"same writer aliases", 1, 1, TransactionWriteValue{Exists: true, Value: typed}, TransactionWriteValue{Exists: true, Value: generic}, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStateDB(t)
			s.CreateAccount(addr, corepb.AccountType_Contract)
			if _, err := s.Commit(); err != nil {
				t.Fatal(err)
			}
			s.SetTransactionVersionedValueReader(testTransactionVersionedReader{
				typedKey:   {{txIndex: tc.typedWriter, value: tc.typedValue}},
				genericKey: {{txIndex: tc.genericWriter, value: tc.genericValue}},
			}, 3)
			var recorder TransactionAccessRecorder
			s.SetTransactionAccessRecorder(&recorder)
			value, exists, err := s.GetAccountKV(addr, kvdomains.ContractMetadata, contractMetaKVKey)
			if tc.wantError {
				if err == nil && s.Error() == nil {
					t.Fatal("conflicting same-writer aliases accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if exists != (tc.want != "") {
				t.Fatalf("exists=%v want=%q", exists, tc.want)
			}
			if exists {
				var metadata contractpb.SmartContract
				if err := statecodec.Unmarshal(value, &metadata); err != nil {
					t.Fatal(err)
				}
				if metadata.Name != tc.want {
					t.Fatalf("name=%q want=%q", metadata.Name, tc.want)
				}
			}
			seen := map[TransactionAccessKey]bool{}
			for _, read := range recorder.CaptureReadSet().Reads {
				seen[read.Key] = true
			}
			if !seen[typedKey] || !seen[genericKey] {
				t.Fatalf("missing metadata alias read dependencies: %v", seen)
			}
		})
	}
}

func TestTransactionVersionedMetadataRecordsAbsentTypedDependency(t *testing.T) {
	s := newTestStateDB(t)
	addr := testAddr(0x79)
	s.CreateAccount(addr, corepb.AccountType_Contract)
	if _, err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	s.SetTransactionVersionedValueReader(testTransactionVersionedReader{}, 3)
	var recorder TransactionAccessRecorder
	s.SetTransactionAccessRecorder(&recorder)
	if _, _, err := s.GetAccountKV(addr, kvdomains.ContractMetadata, contractMetaKVKey); err != nil {
		t.Fatal(err)
	}
	for _, read := range recorder.CaptureReadSet().Reads {
		if read.Key == (TransactionAccessKey{Kind: TransactionAccessContractMetadata, Address: addr}) {
			return
		}
	}
	t.Fatal("missing absent typed metadata dependency")
}

func TestTransactionVersionedMetadataApplyRejectsReadFailure(t *testing.T) {
	s := newTestStateDB(t)
	addr := testAddr(0x7a)
	s.CreateAccount(addr, corepb.AccountType_Contract)
	s.AddBalance(addr, 100)
	if _, err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	value, err := proto.Marshal(&contractpb.SmartContract{Name: "next", ContractAddress: addr.Bytes()})
	if err != nil {
		t.Fatal(err)
	}
	s.SetTransactionVersionedValueReader(testTransactionVersionedReader{
		{Kind: TransactionAccessContractMetadata, Address: addr}:                                                                       {{txIndex: 1, value: TransactionWriteValue{Exists: true, Value: value}}},
		{Kind: TransactionAccessAccountKV, Address: addr, KVDomain: kvdomains.ContractMetadata, LogicalKey: string(contractMetaKVKey)}: {{txIndex: 1, value: TransactionWriteValue{}}},
	}, 2)
	writes := TransactionWriteSet{{Kind: TransactionAccessContractMetadata, Address: addr}: {Exists: true, Value: value}}
	if err := s.ApplyTransactionWriteSet(writes, NewDynamicProperties(), nil); err == nil || s.Error() == nil {
		t.Fatalf("metadata preimage failure accepted: apply=%v state=%v", err, s.Error())
	}
	// Once a read fails, even a later otherwise-valid write set must be
	// rejected before changing any more state.
	writes = TransactionWriteSet{{Kind: TransactionAccessAccountField, Address: addr, AccountField: TransactionAccountFieldBalance}: testVersionedInt64(200)}
	if err := s.ApplyTransactionWriteSet(writes, NewDynamicProperties(), nil); err == nil {
		t.Fatal("pre-existing sticky state error accepted")
	}
	if got := s.GetBalance(addr); got != 100 {
		t.Fatalf("balance changed after sticky failure: %d", got)
	}
}

func TestTransactionVersionedMetadataSurvivesLocalRollback(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		t.Run(fmt.Sprint("deleted=", deleted), func(t *testing.T) {
			s := newTestStateDB(t)
			addr := testAddr(0x77)
			s.CreateAccount(addr, corepb.AccountType_Contract)
			s.SetContract(addr, &contractpb.SmartContract{ContractAddress: addr.Bytes(), Name: "durable"})
			if _, err := s.Commit(); err != nil {
				t.Fatal(err)
			}
			// A prewarmed clean cache belongs to the old transaction boundary.
			if got := s.GetContract(addr); got == nil || got.Name != "durable" {
				t.Fatal(got)
			}
			encoded, err := proto.Marshal(&contractpb.SmartContract{ContractAddress: addr.Bytes(), Name: "versioned"})
			if err != nil {
				t.Fatal(err)
			}
			value := TransactionWriteValue{Exists: !deleted, Value: encoded}
			if deleted {
				value.Value = nil
			}
			s.SetTransactionVersionedValueReader(testTransactionVersionedReader{{Kind: TransactionAccessContractMetadata, Address: addr}: {{txIndex: 1, value: value}}}, 2)
			check := func() {
				t.Helper()
				got := s.GetContract(addr)
				if deleted {
					if got != nil {
						t.Fatal("versioned deletion", got)
					}
				} else if got == nil || got.Name != "versioned" {
					t.Fatal("versioned metadata", got)
				}
			}
			check()
			snapshot := s.Snapshot()
			s.SetContract(addr, &contractpb.SmartContract{ContractAddress: addr.Bytes(), Name: "local"})
			s.RevertToSnapshot(snapshot)
			check()
		})
	}
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
	owned, exists, err := sdb.GetAccountKV(addr, kvdomains.SystemReward, []byte("reward"))
	if err != nil || !exists {
		t.Fatalf("owned versioned account KV exists=%v err=%v", exists, err)
	}
	owned[0] = 'X'
	if got, _, _ := sdb.GetAccountKV(addr, kvdomains.SystemReward, []byte("reward")); string(got) != "shared" {
		t.Fatalf("public GetAccountKV mutation changed shared version: %q", got)
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

func TestTransactionVersionedReaderOverridesRevertedStorageCache(t *testing.T) {
	source := newTestStateDB(t)
	contract := testAddr(44)
	slot := tcommon.BytesToHash([]byte{1})
	durable := tcommon.BytesToHash([]byte{2})
	shared := tcommon.BytesToHash([]byte{3})
	local := tcommon.BytesToHash([]byte{4})
	source.CreateAccount(contract, corepb.AccountType_Contract)
	source.SetState(contract, slot, durable)
	if _, err := source.Commit(); err != nil {
		t.Fatal(err)
	}

	// Sender-chain workers are reverted to block start before their StateDB is
	// retained as an async retry template. Journal replay restores the slot but
	// intentionally leaves the object dirty, so CopyBlockExecutionBase carries
	// this clean, cached durable value into the retry job.
	template, err := source.CopyBlockExecutionBase()
	if err != nil {
		t.Fatal(err)
	}
	snapshot := template.Snapshot()
	template.SetState(contract, slot, local)
	template.RevertToSnapshot(snapshot)
	obj := template.stateObjects[contract]
	if obj == nil || !obj.dirty {
		t.Fatal("reverted template did not retain the dirty object")
	}
	if _, dirty := obj.dirtyStorage[slot]; dirty {
		t.Fatal("reverted slot is still task-local dirty storage")
	}

	worker, err := template.CopyBlockExecutionBase()
	if err != nil {
		t.Fatal(err)
	}
	worker.SetTransactionVersionedValueReader(testTransactionVersionedReader{
		{Kind: TransactionAccessStorage, Address: contract, StorageKey: slot}: {
			{txIndex: 2, value: TransactionWriteValue{Exists: true, Value: shared.Bytes()}},
		},
	}, 3)
	if got, exists := worker.GetStateWithExist(contract, slot); !exists || got != shared {
		t.Fatalf("versioned storage behind reverted cache = %x exists=%v, want %x", got, exists, shared)
	}

	// A write performed by this retry (including a forwarded sender ancestor)
	// remains authoritative over the immutable canonical prefix value.
	worker.SetState(contract, slot, local)
	if got, exists := worker.GetStateWithExist(contract, slot); !exists || got != local {
		t.Fatalf("task-local storage = %x exists=%v, want %x", got, exists, local)
	}
}
