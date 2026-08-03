package state

import (
	"encoding/binary"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestCaptureTransactionWriteSetUsesFinalLogicalValues(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0xc1)
	sdb.CreateAccount(addr, corepb.AccountType_Contract)
	sdb.AddBalance(addr, 10)
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}

	dp := NewDynamicProperties()
	var recorder TransactionAccessRecorder
	recorder.Reset(16)
	sdb.SetTransactionAccessRecorder(&recorder)
	dp.SetTransactionAccessRecorder(&recorder)
	mark := sdb.DomainChangeJournalMark()

	sdb.AddBalance(addr, 7)
	var slot tcommon.Hash
	slot[31] = 1
	var storageValue tcommon.Hash
	storageValue[31] = 9
	sdb.SetState(addr, slot, storageValue)
	if err := sdb.SetAccountKV(addr, kvdomains.SystemReward, []byte("reward"), []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := sdb.SetAccountKV(addr, kvdomains.SystemReward, []byte("reward"), []byte("final")); err != nil {
		t.Fatal(err)
	}
	dp.Set("energy_fee", 321)

	sdb.SetTransactionAccessRecorder(nil)
	dp.SetTransactionAccessRecorder(nil)
	writes, known, err := sdb.CaptureTransactionWriteSet(mark, &recorder, dp)
	if err != nil || !known {
		t.Fatalf("capture writes: known=%v err=%v", known, err)
	}

	accountKey := TransactionAccessKey{Kind: TransactionAccessAccountField, Address: addr, AccountField: TransactionAccountFieldBalance}
	if value, ok := writes[accountKey]; !ok || !value.Exists || len(value.Value) != 8 || int64(binary.BigEndian.Uint64(value.Value)) != 17 {
		t.Fatalf("account balance path = %+v ok=%v", value, ok)
	}
	coarseAccountKey := TransactionAccessKey{Kind: TransactionAccessAccount, Address: addr}
	if _, ok := writes[coarseAccountKey]; ok {
		t.Fatal("typed scalar write set retained a coarse account post-image")
	}
	storageKey := TransactionAccessKey{Kind: TransactionAccessStorage, Address: addr, StorageKey: slot}
	if value, ok := writes[storageKey]; !ok || !value.Exists || string(value.Value) != string(storageValue[:]) {
		t.Fatalf("storage path = %x exists=%v ok=%v", value.Value, value.Exists, ok)
	}
	kvKey := TransactionAccessKey{Kind: TransactionAccessAccountKV, Address: addr, KVDomain: kvdomains.SystemReward, LogicalKey: "reward"}
	if value, ok := writes[kvKey]; !ok || !value.Exists || string(value.Value) != "final" {
		t.Fatalf("account kv path = %q exists=%v ok=%v", value.Value, value.Exists, ok)
	}
	dynamicKey := TransactionAccessKey{Kind: TransactionAccessDynamicInt, LogicalKey: "energy_fee"}
	if value, ok := writes[dynamicKey]; !ok || !value.Exists || len(value.Value) != 8 || int64(binary.BigEndian.Uint64(value.Value)) != 321 {
		t.Fatalf("dynamic path = %x exists=%v ok=%v", value.Value, value.Exists, ok)
	}
}

func TestCaptureTransactionWriteSetFilteredProjectsKnownPostImages(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0xc5)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 10)
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}

	dp := NewDynamicProperties()
	var recorder TransactionAccessRecorder
	recorder.Reset(8)
	sdb.SetTransactionAccessRecorder(&recorder)
	dp.SetTransactionAccessRecorder(&recorder)
	mark := sdb.DomainChangeJournalMark()
	sdb.AddBalance(addr, 7)
	dp.Set("energy_fee", 321)
	sdb.SetTransactionAccessRecorder(nil)
	dp.SetTransactionAccessRecorder(nil)

	balanceKey := TransactionAccessKey{Kind: TransactionAccessAccountField, Address: addr, AccountField: TransactionAccountFieldBalance}
	writes, known, err := sdb.CaptureTransactionWriteSetFiltered(mark, &recorder, dp, func(key TransactionAccessKey) bool {
		return key == balanceKey
	})
	if err != nil || !known {
		t.Fatalf("capture filtered writes: known=%v err=%v", known, err)
	}
	if len(writes) != 1 {
		t.Fatalf("filtered writes = %v, want one balance cell", writes)
	}
	value, ok := writes[balanceKey]
	if !ok || !value.Exists || len(value.Value) != 8 || int64(binary.BigEndian.Uint64(value.Value)) != 17 {
		t.Fatalf("filtered balance = %+v ok=%v", value, ok)
	}
}

func TestCaptureTransactionWriteSetIgnoresUnrelatedPriorAccountFields(t *testing.T) {
	base := newTestStateDB(t)
	addr := testAddr(0xc4)
	base.CreateAccount(addr, corepb.AccountType_Normal)
	base.AddBalance(addr, 100)
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}

	captureBalance := func(sdb *StateDB, priorNetUsage int64) TransactionWriteSet {
		t.Helper()
		if priorNetUsage != 0 {
			sdb.SetNetUsage(addr, priorNetUsage)
		}
		var recorder TransactionAccessRecorder
		recorder.Reset(8)
		sdb.SetTransactionAccessRecorder(&recorder)
		mark := sdb.DomainChangeJournalMark()
		sdb.AddBalance(addr, 7)
		sdb.SetTransactionAccessRecorder(nil)
		writes, known, err := sdb.CaptureTransactionWriteSet(mark, &recorder, NewDynamicProperties())
		if err != nil || !known {
			t.Fatalf("capture balance: known=%v err=%v", known, err)
		}
		return writes
	}

	canonical, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	worker, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	canonicalWrites := captureBalance(canonical, 19)
	workerWrites := captureBalance(worker, 0)
	if !EqualTransactionWriteSets(canonicalWrites, workerWrites) {
		t.Fatalf("typed balance write sets differ: canonical=%v worker=%v", canonicalWrites, workerWrites)
	}
}

func TestCaptureTransactionWriteSetPreservesDeletion(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(0xc2)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	if err := sdb.SetAccountKV(addr, kvdomains.SystemReward, []byte("gone"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}

	var recorder TransactionAccessRecorder
	recorder.Reset(8)
	sdb.SetTransactionAccessRecorder(&recorder)
	mark := sdb.DomainChangeJournalMark()
	if err := sdb.DeleteAccountKV(addr, kvdomains.SystemReward, []byte("gone")); err != nil {
		t.Fatal(err)
	}
	sdb.SetTransactionAccessRecorder(nil)
	writes, known, err := sdb.CaptureTransactionWriteSet(mark, &recorder, NewDynamicProperties())
	if err != nil || !known {
		t.Fatalf("capture deletion: known=%v err=%v", known, err)
	}
	key := TransactionAccessKey{Kind: TransactionAccessAccountKV, Address: addr, KVDomain: kvdomains.SystemReward, LogicalKey: "gone"}
	if value, ok := writes[key]; !ok || value.Exists || value.Value != nil {
		t.Fatalf("deleted value = %+v ok=%v", value, ok)
	}
}

func TestCaptureTransactionWriteSetNormalizesSettlementDeltas(t *testing.T) {
	sdb := newTestStateDB(t)
	blackhole := sdb.BlackholeAddress()
	sdb.CreateAccount(blackhole, corepb.AccountType_Normal)
	sdb.AddBalance(blackhole, 100)
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	dp := NewDynamicProperties()
	dp.SetTransactionFeePool(1_000)

	var recorder TransactionAccessRecorder
	recorder.Reset(8)
	sdb.SetTransactionAccessRecorder(&recorder)
	dp.SetTransactionAccessRecorder(&recorder)
	mark := sdb.DomainChangeJournalMark()
	sdb.AddSettlementBalance(blackhole, 9)
	dp.AddTransactionFeePool(11)
	sdb.SetTransactionAccessRecorder(nil)
	dp.SetTransactionAccessRecorder(nil)

	writes, known, err := sdb.CaptureTransactionWriteSet(mark, &recorder, dp)
	if err != nil || !known {
		t.Fatalf("capture settlement deltas: known=%v err=%v", known, err)
	}
	accountKey := TransactionAccessKey{Kind: TransactionAccessAccountField, Address: blackhole, AccountField: TransactionAccountFieldBalance}
	if value, ok := writes[accountKey]; !ok || !value.Exists || !value.Commutative || int64(binary.BigEndian.Uint64(value.Value)) != 9 {
		t.Fatalf("blackhole settlement write = %+v ok=%v, want delta 9", value, ok)
	}
	feePoolKey := TransactionAccessKey{Kind: TransactionAccessDynamicInt, LogicalKey: "transaction_fee_pool"}
	if value, ok := writes[feePoolKey]; !ok || !value.Exists || !value.Commutative || int64(binary.BigEndian.Uint64(value.Value)) != 11 {
		t.Fatalf("fee-pool settlement write = %+v ok=%v, want delta 11", value, ok)
	}
}

func TestEqualTransactionWriteSetsOwnsAndComparesValues(t *testing.T) {
	key := TransactionAccessKey{Kind: TransactionAccessDynamicString, LogicalKey: "k"}
	input := []byte("value")
	left := TransactionWriteSet{key: ownedTransactionWriteValue(true, input)}
	right := TransactionWriteSet{key: ownedTransactionWriteValue(true, []byte("value"))}
	input[0] = 'X'
	if !EqualTransactionWriteSets(left, right) {
		t.Fatal("equal owned write sets did not compare equal")
	}
	right[key] = ownedTransactionWriteValue(false, nil)
	if EqualTransactionWriteSets(left, right) {
		t.Fatal("presence mismatch compared equal")
	}
}

func TestCaptureTransactionWriteSetIncludesRawKVPostImages(t *testing.T) {
	sdb := newTestStateDB(t)
	var recorder TransactionAccessRecorder
	recorder.Reset(8)
	mark := sdb.DomainChangeJournalMark()
	putKey := []byte("raw-put")
	putValue := []byte("value")
	recorder.RecordRawKVRead(putKey)
	recorder.RecordRawKVPut(putKey, putValue)
	recorder.RecordRawKVDelete([]byte("raw-delete"))
	putKey[0] = 'X'
	putValue[0] = 'X'

	writes, known, err := sdb.CaptureTransactionWriteSet(mark, &recorder, NewDynamicProperties())
	if err != nil || !known {
		t.Fatalf("capture raw writes: known=%v err=%v", known, err)
	}
	put := TransactionAccessKey{Kind: TransactionAccessRawKV, LogicalKey: "raw-put"}
	if value, ok := writes[put]; !ok || !value.Exists || string(value.Value) != "value" {
		t.Fatalf("raw put = %+v ok=%v", value, ok)
	}
	deleted := TransactionAccessKey{Kind: TransactionAccessRawKV, LogicalKey: "raw-delete"}
	if value, ok := writes[deleted]; !ok || value.Exists || value.Value != nil {
		t.Fatalf("raw delete = %+v ok=%v", value, ok)
	}
	if len(recorder.rawKVKeys) != 2 {
		t.Fatalf("raw key interner size = %d, want 2", len(recorder.rawKVKeys))
	}
	recorder.Reset(8)
	recorder.RecordRawKVRead([]byte("raw-put"))
	if len(recorder.rawKVKeys) != 2 {
		t.Fatalf("raw key interner grew across transaction reset: %d", len(recorder.rawKVKeys))
	}
}
