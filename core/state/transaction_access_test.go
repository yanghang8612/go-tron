package state

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type unknownTransactionJournalChange struct{}

func (unknownTransactionJournalChange) revert(map[tcommon.Address]*stateObject, map[tcommon.Address]*types.Witness) {
}

func TestVisitTransactionWritesSinceClassifiesJournal(t *testing.T) {
	var account, created, witness, contract tcommon.Address
	account[0], created[0], witness[0], contract[0] = 0x41, 0x41, 0x41, 0x41
	account[20], created[20], witness[20], contract[20] = 1, 2, 3, 4
	var slot tcommon.Hash
	slot[31] = 9
	kvKey := kvCompositeKeyString(kvdomains.AccountPermissionAux, []byte("owner"))

	s := &StateDB{journal: newJournal()}
	s.journal.entries = []journalChange{
		accountChange{address: account, prev: []byte{1}},
		accountChange{address: created},
		&accountScalarChange{address: account},
		witnessChange{address: witness},
		&storageChange{address: contract, key: slot},
		codeChange{address: contract},
		contractMetaChange{address: contract},
		kvChange{address: account, mapKey: kvKey},
		kvResetChange{address: account},
		selfDestructChange{address: contract},
		transientStorageChange{tk: transientStorageKey{addr: contract, key: slot}},
		resourceWeightChange{resource: corepb.ResourceCode_BANDWIDTH},
		unknownTransactionJournalChange{},
	}

	var got []TransactionWrite
	s.VisitTransactionWritesSince(0, func(write TransactionWrite) bool {
		got = append(got, write)
		return true
	})
	wantKinds := []TransactionWriteKind{
		TransactionWriteAccount,
		TransactionWriteAccountCreate,
		TransactionWriteAccount,
		TransactionWriteWitness,
		TransactionWriteStorage,
		TransactionWriteCode,
		TransactionWriteContractMetadata,
		TransactionWriteAccountKV,
		TransactionWriteAccountKVReset,
		TransactionWriteSelfDestruct,
		TransactionWriteTransientStorage,
		TransactionWriteDynamicProperties,
		TransactionWriteUnknown,
	}
	if len(got) != len(wantKinds) {
		t.Fatalf("writes = %d, want %d", len(got), len(wantKinds))
	}
	for i, want := range wantKinds {
		if got[i].Kind != want {
			t.Fatalf("write %d kind = %d, want %d", i, got[i].Kind, want)
		}
	}
	if got[4].Address != contract || got[4].StorageKey != slot {
		t.Fatalf("storage write = %+v", got[4])
	}
	if got[7].Address != account || got[7].KVDomain != kvdomains.AccountPermissionAux || got[7].KVKey != kvKey {
		t.Fatalf("account KV write = %+v", got[7])
	}
	if got[11].PropertyKey != "total_net_weight" {
		t.Fatalf("dynamic-property write = %+v", got[11])
	}
	accessWrites := 0
	known := s.VisitTransactionAccessWritesSince(0, func(TransactionAccessKey) bool {
		accessWrites++
		return true
	})
	if known || accessWrites != len(wantKinds)-1 {
		t.Fatalf("versioned writes known=%v count=%d, want false/%d", known, accessWrites, len(wantKinds)-1)
	}
}

func TestVisitTransactionWritesSinceFollowsRollback(t *testing.T) {
	var address tcommon.Address
	address[0], address[20] = 0x41, 1
	s := &StateDB{
		journal:      newJournal(),
		stateObjects: make(map[tcommon.Address]*stateObject),
		witnesses:    make(map[tcommon.Address]*types.Witness),
	}
	snapshot := s.Snapshot()
	mark := s.journal.length()
	s.journal.append(accountChange{address: address})

	count := 0
	s.VisitTransactionWritesSince(mark, func(TransactionWrite) bool {
		count++
		return true
	})
	if count != 1 {
		t.Fatalf("writes before rollback = %d, want 1", count)
	}
	s.RevertToSnapshot(snapshot)
	count = 0
	s.VisitTransactionWritesSince(mark, func(TransactionWrite) bool {
		count++
		return true
	})
	if count != 0 {
		t.Fatalf("writes after rollback = %d, want 0", count)
	}
}

func TestDynamicPropertiesSnapshotChangedBeforeNestedCommit(t *testing.T) {
	dp := NewDynamicProperties()
	blockSnapshot := dp.Snapshot()
	firstTx := dp.Snapshot()
	if dp.SnapshotChanged(firstTx) {
		t.Fatal("fresh transaction snapshot reported a change")
	}
	dp.SetPublicNetUsage(1)
	if !dp.SnapshotChanged(firstTx) {
		t.Fatal("transaction dynamic-property write was not observed")
	}
	dp.CommitSnapshot(firstTx)

	secondTx := dp.Snapshot()
	dp.SetPublicNetUsage(2)
	if !dp.SnapshotChanged(secondTx) {
		t.Fatal("repeated key write in a later transaction was not observed")
	}
	dp.CommitSnapshot(secondTx)
	dp.RevertToSnapshot(blockSnapshot)
	if got := dp.PublicNetUsage(); got != 0 {
		t.Fatalf("public net usage after outer rollback = %d, want 0", got)
	}
}

func TestTransactionAccessRecorderCapturesLogicalCellsWithoutMutation(t *testing.T) {
	s := newTestStateDB(t)
	account := testAddr(0x31)
	contract := testAddr(0x32)
	s.CreateAccount(account, corepb.AccountType_Normal)
	s.AddBalance(account, 100)
	s.CreateAccount(contract, corepb.AccountType_Contract)

	journalMark := s.DomainChangeJournalMark()
	balance := s.GetBalance(account)
	var recorder TransactionAccessRecorder
	recorder.Reset(16)
	s.SetTransactionAccessRecorder(&recorder)

	var slot tcommon.Hash
	slot[31] = 7
	_ = s.GetBalance(account)
	_, _, _ = s.GetAccountKV(account, kvdomains.AccountPermissionAux, []byte("owner"))
	_ = s.GetState(contract, slot)
	_ = s.GetCode(contract)
	_ = s.GetContract(contract)
	_ = s.GetWitness(account)
	_ = s.GetTransientState(contract, slot)

	dp := NewDynamicProperties()
	dp.SetTransactionAccessRecorder(&recorder)
	_ = dp.TransactionFee()
	dp.Set("shadow_counter", 1)
	dp.SetString("shadow_label", "one")
	_ = dp.LatestBlockHeaderHash()

	want := map[TransactionAccessKey]TransactionAccessMode{
		{Kind: TransactionAccessAccount, Address: account}:                                                                  TransactionAccessRead,
		{Kind: TransactionAccessAccountKVGeneration, Address: account}:                                                      TransactionAccessRead,
		{Kind: TransactionAccessAccountKV, Address: account, KVDomain: kvdomains.AccountPermissionAux, LogicalKey: "owner"}: TransactionAccessRead,
		{Kind: TransactionAccessStorage, Address: contract, StorageKey: slot}:                                               TransactionAccessRead,
		{Kind: TransactionAccessCode, Address: contract}:                                                                    TransactionAccessRead,
		{Kind: TransactionAccessContractMetadata, Address: contract}:                                                        TransactionAccessRead,
		{Kind: TransactionAccessWitness, Address: account}:                                                                  TransactionAccessRead,
		{Kind: TransactionAccessTransientStorage, Address: contract, StorageKey: slot}:                                      TransactionAccessRead,
		{Kind: TransactionAccessDynamicInt, LogicalKey: "transaction_fee"}:                                                  TransactionAccessRead,
		{Kind: TransactionAccessDynamicInt, LogicalKey: "shadow_counter"}:                                                   TransactionAccessRead | TransactionAccessWrite,
		{Kind: TransactionAccessDynamicString, LogicalKey: "shadow_label"}:                                                  TransactionAccessRead | TransactionAccessWrite,
		{Kind: TransactionAccessDynamicHash, LogicalKey: "latest_block_header_hash"}:                                        TransactionAccessRead,
	}
	got := make(map[TransactionAccessKey]TransactionAccessMode)
	recorder.Visit(func(key TransactionAccessKey, mode TransactionAccessMode) bool {
		got[key] = mode
		return true
	})
	for key, mode := range want {
		if got[key]&mode != mode {
			t.Fatalf("access %v mode = %d, want bits %d (all=%v)", key, got[key], mode, got)
		}
	}
	if recorder.Unsupported() {
		t.Fatal("point reads unexpectedly marked unsupported")
	}
	if got := s.DomainChangeJournalMark(); got != journalMark {
		t.Fatalf("access capture changed StateDB journal: got %d, want %d", got, journalMark)
	}
	if got := s.GetBalance(account); got != balance {
		t.Fatalf("access capture changed balance: got %d, want %d", got, balance)
	}
}

func TestTransactionAccessRecorderRejectsPrefixReads(t *testing.T) {
	s := newTestStateDB(t)
	account := testAddr(0x41)
	s.CreateAccount(account, corepb.AccountType_Normal)
	var recorder TransactionAccessRecorder
	recorder.Reset(16)
	s.SetTransactionAccessRecorder(&recorder)
	if err := s.IterateAccountKV(account, kvdomains.AccountPermissionAux, nil, func(_, _ []byte) (bool, error) {
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if !recorder.Unsupported() {
		t.Fatal("prefix read was not marked unsupported")
	}
}
