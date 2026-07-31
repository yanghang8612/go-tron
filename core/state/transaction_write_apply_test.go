package state

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestApplyTransactionWriteSetPublishesTypedPostImagesAndDeltas(t *testing.T) {
	base := newTestStateDB(t)
	account := testAddr(0xd1)
	blackhole := testAddr(0xd2)
	contract := testAddr(0xd3)
	base.CreateAccount(account, corepb.AccountType_Normal)
	base.AddBalance(account, 100)
	base.CreateAccount(blackhole, corepb.AccountType_Normal)
	base.AddBalance(blackhole, 1_000)
	base.CreateAccount(contract, corepb.AccountType_Contract)
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}

	worker, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	workerDP := NewDynamicProperties()
	workerDP.SetTransactionFeePool(10_000)
	publisherDP := workerDP.Copy()

	var recorder TransactionAccessRecorder
	recorder.Reset(32)
	worker.SetTransactionAccessRecorder(&recorder)
	workerDP.SetTransactionAccessRecorder(&recorder)
	mark := worker.DomainChangeJournalMark()
	worker.AddBalance(account, 7)
	worker.SetNetUsage(account, 12)
	worker.AddSettlementBalance(blackhole, 9)
	workerDP.AddTransactionFeePool(11)
	var slot, slotValue tcommon.Hash
	slot[31] = 1
	slotValue[31] = 2
	worker.SetState(contract, slot, slotValue)
	if err := worker.SetAccountKV(account, kvdomains.SystemReward, []byte("reward"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	recorder.RecordRawKVPut([]byte("raw-key"), []byte("raw-value"))
	worker.FinalizeTransaction()
	worker.SetTransactionAccessRecorder(nil)
	workerDP.SetTransactionAccessRecorder(nil)
	writes, known, err := worker.CaptureTransactionWriteSet(mark, &recorder, workerDP)
	if err != nil || !known {
		t.Fatalf("capture worker writes: known=%v err=%v", known, err)
	}

	// Ordered publication sees unrelated earlier settlement increments and must
	// add worker deltas to that newer baseline rather than overwrite it.
	publisher.AddBalance(blackhole, 5)
	publisherDP.AddTransactionFeePool(7)
	raw := ethrawdb.NewMemoryDatabase()
	if err := publisher.ApplyTransactionWriteSet(writes, publisherDP, raw); err != nil {
		t.Fatal(err)
	}
	publisher.FinalizeTransaction()

	if got := publisher.GetBalance(account); got != 107 {
		t.Fatalf("account balance = %d, want 107", got)
	}
	if got := publisher.GetNetUsage(account); got != 12 {
		t.Fatalf("account net usage = %d, want 12", got)
	}
	if got := publisher.GetBalance(blackhole); got != 1_014 {
		t.Fatalf("blackhole balance = %d, want 1014", got)
	}
	if got := publisherDP.TransactionFeePool(); got != 10_018 {
		t.Fatalf("transaction fee pool = %d, want 10018", got)
	}
	if got := publisher.GetState(contract, slot); got != slotValue {
		t.Fatalf("storage = %x, want %x", got, slotValue)
	}
	if got, exists, err := publisher.GetAccountKV(account, kvdomains.SystemReward, []byte("reward")); err != nil || !exists || string(got) != "value" {
		t.Fatalf("account KV = %q exists=%v err=%v", got, exists, err)
	}
	if got, err := raw.Get([]byte("raw-key")); err != nil || string(got) != "raw-value" {
		t.Fatalf("raw KV = %q err=%v", got, err)
	}
}

func TestApplyTransactionWriteSetRecordedPreservesTypedAccountType(t *testing.T) {
	sdb := newTestStateDB(t)
	account := testAddr(0xd4)
	sdb.CreateAccount(account, corepb.AccountType_Normal)
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}

	key := TransactionAccessKey{
		Kind:         TransactionAccessAccountField,
		Address:      account,
		AccountField: TransactionAccountFieldAccountType,
	}
	writes := TransactionWriteSet{
		key: int64TransactionWriteValue(int64(corepb.AccountType_Contract)),
	}
	mark := sdb.DomainChangeJournalMark()
	var recorder TransactionAccessRecorder
	recorder.Reset(16)
	if err := sdb.ApplyTransactionWriteSetRecorded(writes, NewDynamicProperties(), ethrawdb.NewMemoryDatabase(), &recorder); err != nil {
		t.Fatal(err)
	}
	sdb.FinalizeTransaction()
	applied, known, err := sdb.CaptureTransactionWriteSet(mark, &recorder, NewDynamicProperties())
	if err != nil || !known {
		t.Fatalf("capture applied writes: known=%v err=%v", known, err)
	}
	if !EqualTransactionWriteSets(applied, writes) {
		t.Fatalf("applied writes = %#v, want %#v", applied, writes)
	}
}

func TestApplyTransactionWriteSetPreservesDeletionPostImages(t *testing.T) {
	base := newTestStateDB(t)
	contract := testAddr(0xd5)
	base.CreateAccount(contract, corepb.AccountType_Contract)
	var slot, slotValue tcommon.Hash
	slot[31] = 3
	slotValue[31] = 4
	base.SetState(contract, slot, slotValue)
	if err := base.SetAccountKV(contract, kvdomains.SystemReward, []byte("gone"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	base.SetContract(contract, &contractpb.SmartContract{ContractAddress: contract.Bytes()})
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}
	worker, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}

	var recorder TransactionAccessRecorder
	recorder.Reset(16)
	worker.SetTransactionAccessRecorder(&recorder)
	mark := worker.DomainChangeJournalMark()
	worker.SetState(contract, slot, tcommon.Hash{})
	if err := worker.DeleteAccountKV(contract, kvdomains.SystemReward, []byte("gone")); err != nil {
		t.Fatal(err)
	}
	worker.SetContract(contract, nil)
	recorder.RecordRawKVDelete([]byte("raw-gone"))
	worker.FinalizeTransaction()
	worker.SetTransactionAccessRecorder(nil)
	writes, known, err := worker.CaptureTransactionWriteSet(mark, &recorder, NewDynamicProperties())
	if err != nil || !known {
		t.Fatalf("capture deletions: known=%v err=%v", known, err)
	}

	raw := ethrawdb.NewMemoryDatabase()
	if err := raw.Put([]byte("raw-gone"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := publisher.ApplyTransactionWriteSet(writes, NewDynamicProperties(), raw); err != nil {
		t.Fatal(err)
	}
	publisher.FinalizeTransaction()
	if _, exists := publisher.GetStateWithExist(contract, slot); exists {
		t.Fatal("storage deletion was not published")
	}
	if _, exists, err := publisher.GetAccountKV(contract, kvdomains.SystemReward, []byte("gone")); err != nil || exists {
		t.Fatalf("account KV deletion: exists=%v err=%v", exists, err)
	}
	if metadata, exists, err := publisher.GetContractMetadataBytes(contract); err != nil || exists || metadata != nil {
		t.Fatalf("contract metadata deletion: data=%x exists=%v err=%v", metadata, exists, err)
	}
	if exists, err := raw.Has([]byte("raw-gone")); err != nil || exists {
		t.Fatalf("raw deletion: exists=%v err=%v", exists, err)
	}
}

func TestApplyTransactionWriteSetRejectsUnsupportedBeforeMutation(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0xd4)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 100)
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	writes := TransactionWriteSet{
		{Kind: TransactionAccessAccountField, Address: addr, AccountField: TransactionAccountFieldBalance}: int64TransactionWriteValue(200),
		{Kind: TransactionAccessAccount, Address: addr}:                                                    ownedTransactionWriteValue(true, []byte("unsupported")),
	}
	if err := sdb.ApplyTransactionWriteSet(writes, NewDynamicProperties(), nil); err == nil {
		t.Fatal("full account write unexpectedly accepted")
	}
	if got := sdb.GetBalance(addr); got != 100 {
		t.Fatalf("balance changed after preflight failure: %d", got)
	}
}
