package state

import (
	"fmt"
	"reflect"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

type orderedTransactionRawWriter struct {
	keys []string
}

func (writer *orderedTransactionRawWriter) Put(key, _ []byte) error {
	writer.keys = append(writer.keys, string(key))
	return nil
}

func (writer *orderedTransactionRawWriter) Delete(key []byte) error {
	writer.keys = append(writer.keys, string(key))
	return nil
}

func TestTransactionAccessPublicationPoliciesAreExhaustive(t *testing.T) {
	// true means the ordered applier implements the family; false means capture
	// and versioning know the family but publication must fall back to serial.
	kindPolicy := map[TransactionAccessKind]bool{
		TransactionAccessAccount:             true,
		TransactionAccessWitness:             true,
		TransactionAccessStorage:             true,
		TransactionAccessCode:                true,
		TransactionAccessContractMetadata:    true,
		TransactionAccessAccountKV:           true,
		TransactionAccessAccountKVGeneration: false,
		TransactionAccessSelfDestruct:        false,
		TransactionAccessTransientStorage:    true,
		TransactionAccessDynamicInt:          true,
		TransactionAccessDynamicString:       true,
		TransactionAccessDynamicHash:         true,
		TransactionAccessAccountField:        true,
		TransactionAccessRawKV:               true,
	}
	if len(kindPolicy) != int(TransactionAccessKindCount)-1 {
		t.Fatalf("publication policy covers %d kinds, enum has %d", len(kindPolicy), TransactionAccessKindCount-1)
	}
	address := testAddr(0xc0)
	for kind := TransactionAccessKind(1); kind < TransactionAccessKindCount; kind++ {
		if _, ok := kindPolicy[kind]; !ok {
			t.Fatalf("transaction access kind %d has no publication policy", kind)
		}
		if !TransactionAccessRecorderCoversWrites(kind) {
			t.Fatalf("transaction access kind %d is missing inline/journal write coverage", kind)
		}
		key := TransactionAccessKey{Kind: kind}
		switch kind {
		case TransactionAccessAccount,
			TransactionAccessWitness,
			TransactionAccessCode,
			TransactionAccessContractMetadata,
			TransactionAccessAccountKVGeneration,
			TransactionAccessSelfDestruct:
			key.Address = address
		case TransactionAccessAccountField:
			key.Address = address
			key.AccountField = TransactionAccountFieldBalance
		case TransactionAccessStorage, TransactionAccessTransientStorage:
			key.Address = address
			key.StorageKey = tcommon.Hash{31: 1}
		case TransactionAccessAccountKV:
			key.Address = address
			key.KVDomain = kvdomains.SystemReward
			key.LogicalKey = "k"
		case TransactionAccessDynamicInt, TransactionAccessDynamicString, TransactionAccessDynamicHash, TransactionAccessRawKV:
			key.LogicalKey = "k"
		}
		if err := validateTransactionWriteKeyShape(key); err != nil {
			t.Fatalf("transaction access kind %d canonical key rejected: %v", kind, err)
		}
	}
	for _, kind := range []TransactionAccessKind{TransactionAccessUnknown, TransactionAccessKindCount, 0xff} {
		if TransactionAccessRecorderCoversWrites(kind) {
			t.Fatalf("non-kind %d unexpectedly has recorder coverage", kind)
		}
		if err := validateTransactionWriteKeyShape(TransactionAccessKey{Kind: kind}); err == nil {
			t.Fatalf("non-kind %d canonical shape unexpectedly accepted", kind)
		}
	}

	// Existence and FrozenResource are dependency-only paths. Every other
	// declared scalar has a schema validator and deterministic applier.
	fieldPolicy := map[TransactionAccountField]bool{
		TransactionAccountFieldExistence:             false,
		TransactionAccountFieldAccountType:           true,
		TransactionAccountFieldBalance:               true,
		TransactionAccountFieldAllowance:             true,
		TransactionAccountFieldLatestWithdrawTime:    true,
		TransactionAccountFieldNetUsage:              true,
		TransactionAccountFieldLatestOperationTime:   true,
		TransactionAccountFieldLatestConsumeTime:     true,
		TransactionAccountFieldFreeNetUsage:          true,
		TransactionAccountFieldLatestConsumeFreeTime: true,
		TransactionAccountFieldNetWindow:             true,
		TransactionAccountFieldFrozenResource:        false,
	}
	if len(fieldPolicy) != int(TransactionAccountFieldCount)-1 {
		t.Fatalf("account-field policy covers %d fields, enum has %d", len(fieldPolicy), TransactionAccountFieldCount-1)
	}
	sdb := newTestStateDB(t)
	sdb.CreateAccount(address, corepb.AccountType_Normal)
	for field := TransactionAccountField(1); field < TransactionAccountFieldCount; field++ {
		publishable, ok := fieldPolicy[field]
		if !ok {
			t.Fatalf("account field %d has no publication policy", field)
		}
		value := int64TransactionWriteValue(1)
		if field == TransactionAccountFieldAccountType {
			value = int64TransactionWriteValue(int64(corepb.AccountType_Normal))
		}
		if field == TransactionAccountFieldNetWindow {
			value.Value = append(value.Value, 1)
		}
		key := TransactionAccessKey{Kind: TransactionAccessAccountField, Address: address, AccountField: field}
		err := validateTransactionWriteApply(key, value, NewDynamicProperties(), &orderedTransactionRawWriter{})
		if !publishable {
			if err == nil {
				t.Fatalf("read-only account field %d unexpectedly passed publication schema", field)
			}
			continue
		}
		if err != nil {
			t.Fatalf("publishable account field %d failed schema: %v", field, err)
		}
		if err := sdb.applyTransactionAccountField(key, value); err != nil {
			t.Fatalf("publishable account field %d has no applier: %v", field, err)
		}
	}
}

func TestApplyTransactionWriteSetPublishesTypedPostImagesAndDeltas(t *testing.T) {
	base := newTestStateDB(t)
	account := testAddr(0xd1)
	blackhole := base.BlackholeAddress()
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
	workerDP.AddBurnTrx(13)
	workerDP.AddTotalTransactionCost(17)
	workerDP.AddTotalCreateAccountCost(19)
	workerDP.AddTotalCreateWitnessCost(23)
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
	publisherDP.AddBurnTrx(2)
	publisherDP.AddTotalTransactionCost(3)
	publisherDP.AddTotalCreateAccountCost(5)
	publisherDP.AddTotalCreateWitnessCost(7)
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
	if got := publisherDP.BurnTrxAmount(); got != 15 {
		t.Fatalf("burn amount = %d, want 15", got)
	}
	if got := publisherDP.TotalTransactionCost(); got != 20 {
		t.Fatalf("total transaction cost = %d, want 20", got)
	}
	if got := publisherDP.TotalCreateAccountCost(); got != 24 {
		t.Fatalf("total create-account cost = %d, want 24", got)
	}
	if got := publisherDP.TotalCreateWitnessCost(); got != 30 {
		t.Fatalf("total create-witness cost = %d, want 30", got)
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

func TestApplyTransactionWriteSetAllowsChainSpecificBlackholeDelta(t *testing.T) {
	sdb := newTestStateDB(t)
	nileBlackhole := testAddr(0xd2)
	if err := sdb.WriteAccountNameIndex([]byte("Blackhole"), nileBlackhole); err != nil {
		t.Fatal(err)
	}
	sdb.CreateAccount(nileBlackhole, corepb.AccountType_Normal)
	sdb.AddBalance(nileBlackhole, 100)
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	writes := TransactionWriteSet{
		{Kind: TransactionAccessAccountField, Address: nileBlackhole, AccountField: TransactionAccountFieldBalance}: {
			Exists: true, Commutative: true, Value: int64TransactionWriteValue(7).Value,
		},
	}
	if err := sdb.ApplyTransactionWriteSet(writes, NewDynamicProperties(), ethrawdb.NewMemoryDatabase()); err != nil {
		t.Fatalf("chain-specific blackhole delta rejected: %v", err)
	}
	if got := sdb.GetBalance(nileBlackhole); got != 107 {
		t.Fatalf("chain-specific blackhole balance = %d, want 107", got)
	}
}

func TestApplyTransactionWriteSetRejectsMalformedBlackholeIndexBeforeMutation(t *testing.T) {
	invalidPrefix := params.BlackholeAddress.Bytes()
	invalidPrefix[0] = 0x42
	for _, test := range []struct {
		name string
		row  []byte
	}{
		{name: "short", row: []byte{1}},
		{name: "invalid_prefix", row: invalidPrefix},
	} {
		t.Run(test.name, func(t *testing.T) {
			sdb := newTestStateDB(t)
			sdb.CreateAccount(params.BlackholeAddress, corepb.AccountType_Normal)
			sdb.AddBalance(params.BlackholeAddress, 100)
			if err := sdb.SystemKVPut(kvdomains.SystemAccountIndex, blackholeAccountNameIndexKey[:], test.row); err != nil {
				t.Fatal(err)
			}
			if _, err := sdb.Commit(); err != nil {
				t.Fatal(err)
			}
			writes := TransactionWriteSet{
				{Kind: TransactionAccessAccountField, Address: params.BlackholeAddress, AccountField: TransactionAccountFieldBalance}: {
					Exists: true, Commutative: true, Value: int64TransactionWriteValue(7).Value,
				},
			}
			if err := sdb.ApplyTransactionWriteSet(writes, NewDynamicProperties(), ethrawdb.NewMemoryDatabase()); err == nil {
				t.Fatal("malformed blackhole index unexpectedly authorized a delta")
			}
			if got := sdb.GetBalance(params.BlackholeAddress); got != 100 {
				t.Fatalf("balance changed before blackhole-index failure: %d", got)
			}
		})
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

func TestApplyTransactionWriteSetRecordedPreservesNoopIntent(t *testing.T) {
	sdb := newTestStateDB(t)
	account := testAddr(0xd5)
	sdb.CreateAccount(account, corepb.AccountType_Normal)
	if err := sdb.SetAccountKV(account, kvdomains.SystemReward, []byte("same"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}

	sameKey := TransactionAccessKey{Kind: TransactionAccessAccountKV, Address: account, KVDomain: kvdomains.SystemReward, LogicalKey: "same"}
	absentKey := TransactionAccessKey{Kind: TransactionAccessAccountKV, Address: account, KVDomain: kvdomains.SystemReward, LogicalKey: "absent"}
	transientKey := TransactionAccessKey{Kind: TransactionAccessTransientStorage, Address: account, StorageKey: tcommon.Hash{31: 1}}
	writes := TransactionWriteSet{
		sameKey:      ownedTransactionWriteValue(true, []byte("value")),
		absentKey:    {},
		transientKey: ownedTransactionWriteValue(false, make([]byte, tcommon.HashLength)),
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

func TestApplyTransactionWriteSetCreatesFreshAccount(t *testing.T) {
	worker := newTestStateDB(t)
	publisher := newTestStateDB(t)
	account := testAddr(0xd6)
	workerDP := NewDynamicProperties()
	publisherDP := NewDynamicProperties()

	var workerRecorder TransactionAccessRecorder
	workerRecorder.Reset(16)
	worker.SetTransactionAccessRecorder(&workerRecorder)
	mark := worker.DomainChangeJournalMark()
	worker.CreateAccountWithTime(account, corepb.AccountType_Normal, 123)
	worker.AddBalance(account, 456)
	if err := worker.SetAccountKV(account, kvdomains.SystemReward, []byte("created"), []byte("row")); err != nil {
		t.Fatal(err)
	}
	worker.FinalizeTransaction()
	worker.SetTransactionAccessRecorder(nil)
	writes, known, err := worker.CaptureTransactionWriteSet(mark, &workerRecorder, workerDP)
	if err != nil || !known {
		t.Fatalf("capture creation writes: known=%v err=%v", known, err)
	}
	accountKey := TransactionAccessKey{Kind: TransactionAccessAccount, Address: account}
	if value, ok := writes[accountKey]; !ok || !value.Exists {
		t.Fatalf("creation account post-image = %+v ok=%v", value, ok)
	}
	balanceKey := TransactionAccessKey{Kind: TransactionAccessAccountField, Address: account, AccountField: TransactionAccountFieldBalance}
	if value, ok := writes[balanceKey]; !ok || transactionWriteInt64(value) != 456 {
		t.Fatalf("creation balance post-image = %+v ok=%v", value, ok)
	}
	conflicting := make(TransactionWriteSet, len(writes))
	for key, value := range writes {
		conflicting[key] = value
	}
	conflicting[balanceKey] = int64TransactionWriteValue(999)
	rejectedPublisher := newTestStateDB(t)
	if err := rejectedPublisher.ApplyTransactionWriteSet(conflicting, NewDynamicProperties(), ethrawdb.NewMemoryDatabase()); err == nil {
		t.Fatal("conflicting full-account and balance post-images unexpectedly accepted")
	}
	if rejectedPublisher.GetAccount(account) != nil {
		t.Fatal("account was created before full/field consistency failure")
	}

	applyMark := publisher.DomainChangeJournalMark()
	var applyRecorder TransactionAccessRecorder
	applyRecorder.Reset(16)
	if err := publisher.ApplyTransactionWriteSetRecorded(writes, publisherDP, ethrawdb.NewMemoryDatabase(), &applyRecorder); err != nil {
		t.Fatal(err)
	}
	publisher.FinalizeTransaction()
	applied, appliedKnown, err := publisher.CaptureTransactionWriteSet(applyMark, &applyRecorder, publisherDP)
	if err != nil || !appliedKnown {
		t.Fatalf("capture applied creation: known=%v err=%v", appliedKnown, err)
	}
	if !EqualTransactionWriteSets(applied, writes) {
		t.Fatalf("applied creation writes = %#v, want %#v", applied, writes)
	}
	created := publisher.GetAccount(account)
	if created == nil || created.Balance() != 456 || created.CreateTime() != 123 {
		t.Fatalf("created account = %+v", created)
	}
	if value, exists, err := publisher.GetAccountKV(account, kvdomains.SystemReward, []byte("created")); err != nil || !exists || string(value) != "row" {
		t.Fatalf("created account KV = %q exists=%v err=%v", value, exists, err)
	}

	if err := publisher.ApplyTransactionWriteSet(writes, publisherDP, ethrawdb.NewMemoryDatabase()); err == nil {
		t.Fatal("full account replacement unexpectedly accepted")
	}
}

func TestApplyTransactionWriteSetRejectsConflictingFullAccountFieldsBeforeMutation(t *testing.T) {
	worker := newTestStateDB(t)
	account := testAddr(0xd7)
	var recorder TransactionAccessRecorder
	recorder.Reset(32)
	worker.SetTransactionAccessRecorder(&recorder)
	mark := worker.DomainChangeJournalMark()
	worker.CreateAccountWithTime(account, corepb.AccountType_Normal, 123)
	worker.SetAccountType(account, corepb.AccountType_Contract)
	worker.AddBalance(account, 101)
	worker.SetAllowance(account, 102)
	worker.SetLatestWithdrawTime(account, 103)
	worker.SetNetUsage(account, 104)
	worker.SetLatestOperationTime(account, 105)
	worker.SetLatestConsumeTime(account, 106)
	worker.SetFreeNetUsage(account, 107)
	worker.SetLatestConsumeFreeTime(account, 108)
	worker.SetNetWindow(account, 109, true)
	worker.FinalizeTransaction()
	worker.SetTransactionAccessRecorder(nil)
	writes, known, err := worker.CaptureTransactionWriteSet(mark, &recorder, NewDynamicProperties())
	if err != nil || !known {
		t.Fatalf("capture full account fields: known=%v err=%v", known, err)
	}

	validPublisher := newTestStateDB(t)
	if err := validPublisher.ApplyTransactionWriteSet(writes, NewDynamicProperties(), ethrawdb.NewMemoryDatabase()); err != nil {
		t.Fatalf("canonical full account fields rejected: %v", err)
	}
	validPublisher.FinalizeTransaction()
	created := validPublisher.GetAccount(account)
	if created == nil || created.Balance() != 101 || created.RawNetWindowSize() != 109 || !created.NetWindowOptimized() {
		t.Fatalf("created account fields = %+v", created)
	}

	fields := [...]TransactionAccountField{
		TransactionAccountFieldAccountType,
		TransactionAccountFieldBalance,
		TransactionAccountFieldAllowance,
		TransactionAccountFieldLatestWithdrawTime,
		TransactionAccountFieldNetUsage,
		TransactionAccountFieldLatestOperationTime,
		TransactionAccountFieldLatestConsumeTime,
		TransactionAccountFieldFreeNetUsage,
		TransactionAccountFieldLatestConsumeFreeTime,
		TransactionAccountFieldNetWindow,
	}
	for _, field := range fields {
		t.Run(fmt.Sprintf("field_%d", field), func(t *testing.T) {
			conflicting := make(TransactionWriteSet, len(writes))
			for key, value := range writes {
				conflicting[key] = value
			}
			key := TransactionAccessKey{Kind: TransactionAccessAccountField, Address: account, AccountField: field}
			value, ok := conflicting[key]
			if !ok {
				t.Fatalf("captured writes omit account field %d", field)
			}
			if field == TransactionAccountFieldNetWindow {
				value.Value = append([]byte(nil), value.Value...)
				value.Value[8] ^= 1
			} else {
				value = int64TransactionWriteValue(transactionWriteInt64(value) + 1)
			}
			conflicting[key] = value
			publisher := newTestStateDB(t)
			if err := publisher.ApplyTransactionWriteSet(conflicting, NewDynamicProperties(), ethrawdb.NewMemoryDatabase()); err == nil {
				t.Fatalf("conflicting full-account field %d unexpectedly accepted", field)
			}
			if publisher.GetAccount(account) != nil {
				t.Fatalf("account created before field %d consistency failure", field)
			}
		})
	}

	commutative := make(TransactionWriteSet, len(writes))
	for key, value := range writes {
		commutative[key] = value
	}
	balanceKey := TransactionAccessKey{Kind: TransactionAccessAccountField, Address: account, AccountField: TransactionAccountFieldBalance}
	balance := commutative[balanceKey]
	balance.Commutative = true
	commutative[balanceKey] = balance
	publisher := newTestStateDB(t)
	if err := publisher.ApplyTransactionWriteSet(commutative, NewDynamicProperties(), ethrawdb.NewMemoryDatabase()); err == nil {
		t.Fatal("commutative field alongside full account unexpectedly accepted")
	}
	if publisher.GetAccount(account) != nil {
		t.Fatal("account created before commutative/full consistency failure")
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
	if exists, err := raw.Has([]byte("raw-gone")); err != nil || exists {
		t.Fatalf("raw deletion: exists=%v err=%v", exists, err)
	}
}

func TestApplyTransactionWriteSetPreservesContractMetadataDeletion(t *testing.T) {
	base := newTestStateDB(t)
	contract := testAddr(0xd7)
	base.CreateAccount(contract, corepb.AccountType_Contract)
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
	recorder.Reset(8)
	worker.SetTransactionAccessRecorder(&recorder)
	mark := worker.DomainChangeJournalMark()
	worker.SetContract(contract, nil)
	worker.FinalizeTransaction()
	worker.SetTransactionAccessRecorder(nil)
	writes, known, err := worker.CaptureTransactionWriteSet(mark, &recorder, NewDynamicProperties())
	if err != nil || !known {
		t.Fatalf("capture metadata deletion: known=%v err=%v", known, err)
	}
	if err := publisher.ApplyTransactionWriteSet(writes, NewDynamicProperties(), ethrawdb.NewMemoryDatabase()); err != nil {
		t.Fatal(err)
	}
	publisher.FinalizeTransaction()
	if metadata, exists, err := publisher.GetContractMetadataBytes(contract); err != nil || exists || metadata != nil {
		t.Fatalf("contract metadata deletion: data=%x exists=%v err=%v", metadata, exists, err)
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

func TestApplyTransactionWriteSetRejectsOrderSensitivePhysicalAliases(t *testing.T) {
	contract := testAddr(0xe1)
	other := testAddr(0xe2)
	invalidPrefix := contract
	invalidPrefix[0] = 0x42
	dynamicKey := TransactionAccessKey{Kind: TransactionAccessDynamicInt, LogicalKey: "energy_fee"}
	metadataKey := TransactionAccessKey{Kind: TransactionAccessContractMetadata, Address: contract}
	storageKey := TransactionAccessKey{Kind: TransactionAccessStorage, Address: contract, StorageKey: tcommon.Hash{31: 1}}
	witnessKey := TransactionAccessKey{Kind: TransactionAccessWitness, Address: other}
	metadataValue, err := proto.Marshal(&contractpb.SmartContract{ContractAddress: contract.Bytes()})
	if err != nil {
		t.Fatal(err)
	}
	witnessValue, err := proto.Marshal(&corepb.Witness{Address: other.Bytes()})
	if err != nil {
		t.Fatal(err)
	}
	validHash := make([]byte, tcommon.HashLength)
	for _, test := range []struct {
		name   string
		writes TransactionWriteSet
	}{
		{
			name: "metadata_and_storage",
			writes: TransactionWriteSet{
				metadataKey: {Exists: true, Value: metadataValue},
				storageKey:  {Exists: true, Value: validHash},
			},
		},
		{
			name: "metadata_and_metadata_kv",
			writes: TransactionWriteSet{
				metadataKey: {Exists: true, Value: metadataValue},
				{Kind: TransactionAccessAccountKV, Address: contract, KVDomain: kvdomains.ContractMetadata, LogicalKey: "meta"}: {Exists: true, Value: metadataValue},
			},
		},
		{
			name: "storage_and_storage_kv",
			writes: TransactionWriteSet{
				storageKey: {Exists: true, Value: validHash},
				{Kind: TransactionAccessAccountKV, Address: contract, KVDomain: kvdomains.ContractStorage, LogicalKey: "row"}: {Exists: true, Value: validHash},
			},
		},
		{
			name: "witness_and_witness_kv",
			writes: TransactionWriteSet{
				witnessKey: {Exists: true, Value: witnessValue},
				{Kind: TransactionAccessAccountKV, Address: other, KVDomain: kvdomains.WitnessCapsule, LogicalKey: "witness"}: {Exists: true, Value: witnessValue},
			},
		},
		{
			name: "dynamic_and_system_dynamic_kv",
			writes: TransactionWriteSet{
				dynamicKey: int64TransactionWriteValue(1),
				{Kind: TransactionAccessAccountKV, Address: tcommon.SystemAccountAddress, KVDomain: kvdomains.SystemDynamicProperty, LogicalKey: "energy_fee"}: int64TransactionWriteValue(1),
			},
		},
		{
			name: "metadata_kv_without_typed_peer",
			writes: TransactionWriteSet{
				{Kind: TransactionAccessAccountKV, Address: contract, KVDomain: kvdomains.ContractMetadata, LogicalKey: "meta"}: {Exists: true, Value: metadataValue},
			},
		},
		{
			name: "storage_kv_without_typed_peer",
			writes: TransactionWriteSet{
				{Kind: TransactionAccessAccountKV, Address: contract, KVDomain: kvdomains.ContractStorage, LogicalKey: "row"}: {Exists: true, Value: validHash},
			},
		},
		{
			name: "witness_kv_without_typed_peer",
			writes: TransactionWriteSet{
				{Kind: TransactionAccessAccountKV, Address: other, KVDomain: kvdomains.WitnessCapsule, LogicalKey: "witness"}: {Exists: true, Value: witnessValue},
			},
		},
		{
			name: "system_dynamic_kv_without_typed_peer",
			writes: TransactionWriteSet{
				{Kind: TransactionAccessAccountKV, Address: tcommon.SystemAccountAddress, KVDomain: kvdomains.SystemDynamicProperty, LogicalKey: "energy_fee"}: int64TransactionWriteValue(1),
			},
		},
		{
			name: "dynamic_same_name_conflicting_types",
			writes: TransactionWriteSet{
				{Kind: TransactionAccessDynamicInt, LogicalKey: "future_property"}:    int64TransactionWriteValue(1),
				{Kind: TransactionAccessDynamicString, LogicalKey: "future_property"}: {Exists: true, Value: []byte("one")},
			},
		},
		{
			name: "known_string_as_int",
			writes: TransactionWriteSet{
				{Kind: TransactionAccessDynamicInt, LogicalKey: "energy_price_history"}: int64TransactionWriteValue(1),
			},
		},
		{
			name: "known_int_as_string",
			writes: TransactionWriteSet{
				{Kind: TransactionAccessDynamicString, LogicalKey: "energy_fee"}: {Exists: true, Value: []byte("one")},
			},
		},
		{
			name: "hash_as_string",
			writes: TransactionWriteSet{
				{Kind: TransactionAccessDynamicString, LogicalKey: "latest_block_header_hash"}: {Exists: true, Value: []byte("wrong")},
			},
		},
		{
			name: "noncanonical_address_prefix",
			writes: TransactionWriteSet{
				{Kind: TransactionAccessAccountField, Address: invalidPrefix, AccountField: TransactionAccountFieldBalance}: int64TransactionWriteValue(1),
			},
		},
		{
			name: "account_field_with_ignored_logical_key",
			writes: TransactionWriteSet{
				{Kind: TransactionAccessAccountField, Address: contract, AccountField: TransactionAccountFieldBalance, LogicalKey: "alias"}: int64TransactionWriteValue(1),
			},
		},
		{
			name: "storage_with_ignored_logical_key",
			writes: TransactionWriteSet{
				{Kind: TransactionAccessStorage, Address: contract, StorageKey: tcommon.Hash{31: 1}, LogicalKey: "alias"}: {Exists: true, Value: validHash},
			},
		},
		{
			name: "account_kv_with_ignored_storage_key",
			writes: TransactionWriteSet{
				{Kind: TransactionAccessAccountKV, Address: contract, KVDomain: kvdomains.SystemReward, StorageKey: tcommon.Hash{31: 1}, LogicalKey: "reward"}: {Exists: true, Value: []byte("value")},
			},
		},
		{
			name: "dynamic_with_ignored_address",
			writes: TransactionWriteSet{
				{Kind: TransactionAccessDynamicInt, Address: contract, LogicalKey: "energy_fee"}: int64TransactionWriteValue(1),
			},
		},
		{
			name: "raw_with_ignored_account_field",
			writes: TransactionWriteSet{
				{Kind: TransactionAccessRawKV, AccountField: TransactionAccountFieldBalance, LogicalKey: "application-owned"}: {Exists: true, Value: []byte("value")},
			},
		},
		{
			name: "protected_raw_and_typed",
			writes: TransactionWriteSet{
				dynamicKey: int64TransactionWriteValue(1),
				{Kind: TransactionAccessRawKV, LogicalKey: "state-code-v1-protected"}: {Exists: true, Value: []byte("code")},
			},
		},
		{
			name: "protected_raw_only",
			writes: TransactionWriteSet{
				{Kind: TransactionAccessRawKV, LogicalKey: "state-code-v1-protected"}: {Exists: true, Value: []byte("code")},
			},
		},
		{
			name: "protected_state_tx_range_raw",
			writes: TransactionWriteSet{
				{Kind: TransactionAccessRawKV, LogicalKey: "state-tx-range-v1-protected"}: {Exists: true, Value: []byte("range")},
			},
		},
		{
			name: "protected_chain_metadata_raw",
			writes: TransactionWriteSet{
				{Kind: TransactionAccessRawKV, LogicalKey: "execution-safety-incident-v1"}: {Exists: true, Value: []byte("erase")},
			},
		},
		{
			name: "protected_block_body_raw",
			writes: TransactionWriteSet{
				{Kind: TransactionAccessRawKV, LogicalKey: "b-protected"}: {Exists: true, Value: []byte("block")},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateTransactionWriteSetApply(test.writes, NewDynamicProperties(), &orderedTransactionRawWriter{}); err == nil {
				t.Fatal("order-sensitive physical alias was accepted")
			}
		})
	}
}

func TestValidateTransactionWriteKeyShapeAllowsEmptyAccountKVLogicalKey(t *testing.T) {
	key := TransactionAccessKey{Kind: TransactionAccessAccountKV, Address: testAddr(0xe6), KVDomain: kvdomains.SystemReward}
	writes := TransactionWriteSet{key: {Exists: true, Value: []byte("value")}}
	if err := ValidateTransactionWriteSetApply(writes, NewDynamicProperties(), &orderedTransactionRawWriter{}); err != nil {
		t.Fatalf("empty account-KV logical key rejected: %v", err)
	}
}

func TestSortedTransactionWriteKeysBreaksSharedRankTiesByKind(t *testing.T) {
	intKey := TransactionAccessKey{Kind: TransactionAccessDynamicInt, LogicalKey: "same"}
	stringKey := TransactionAccessKey{Kind: TransactionAccessDynamicString, LogicalKey: "same"}
	for range 100 {
		ordered := sortedTransactionWriteKeys(TransactionWriteSet{
			stringKey: {Exists: true, Value: []byte("one")},
			intKey:    int64TransactionWriteValue(1),
		})
		if len(ordered) != 2 || ordered[0] != intKey || ordered[1] != stringKey {
			t.Fatalf("shared-rank order = %+v, want int then string", ordered)
		}
	}
}

func TestApplyTransactionWriteSetPreflightsTypedKeySemanticsBeforeMutation(t *testing.T) {
	account := testAddr(0xe3)
	other := testAddr(0xe7)
	sdb := newTestStateDB(t)
	sdb.CreateAccount(account, corepb.AccountType_Normal)
	sdb.AddBalance(account, 100)
	sdb.CreateAccount(other, corepb.AccountType_Normal)
	sdb.AddBalance(other, 50)
	if _, err := sdb.Commit(); err != nil {
		t.Fatal(err)
	}
	balanceKey := TransactionAccessKey{
		Kind:         TransactionAccessAccountField,
		Address:      account,
		AccountField: TransactionAccountFieldBalance,
	}
	for _, test := range []struct {
		name string
		key  TransactionAccessKey
		val  TransactionWriteValue
	}{
		{
			name: "unregistered_account_kv_domain",
			key:  TransactionAccessKey{Kind: TransactionAccessAccountKV, Address: account, KVDomain: kvdomains.KVDomain(0x0099), LogicalKey: "invalid"},
			val:  ownedTransactionWriteValue(true, []byte("value")),
		},
		{
			name: "unsupported_dynamic_hash_name",
			key:  TransactionAccessKey{Kind: TransactionAccessDynamicHash, LogicalKey: "not_the_latest_block_hash"},
			val:  ownedTransactionWriteValue(true, make([]byte, tcommon.HashLength)),
		},
		{
			name: "storage_present_zero",
			key:  TransactionAccessKey{Kind: TransactionAccessStorage, Address: account, StorageKey: tcommon.Hash{31: 1}},
			val:  ownedTransactionWriteValue(true, make([]byte, tcommon.HashLength)),
		},
		{
			name: "storage_absent_nonzero",
			key:  TransactionAccessKey{Kind: TransactionAccessStorage, Address: account, StorageKey: tcommon.Hash{31: 1}},
			val:  ownedTransactionWriteValue(false, append(make([]byte, tcommon.HashLength-1), 1)),
		},
		{
			name: "transient_present_nonzero",
			key:  TransactionAccessKey{Kind: TransactionAccessTransientStorage, Address: account, StorageKey: tcommon.Hash{31: 1}},
			val:  ownedTransactionWriteValue(true, append(make([]byte, tcommon.HashLength-1), 1)),
		},
		{
			name: "transient_absent_nonzero",
			key:  TransactionAccessKey{Kind: TransactionAccessTransientStorage, Address: account, StorageKey: tcommon.Hash{31: 1}},
			val:  ownedTransactionWriteValue(false, append(make([]byte, tcommon.HashLength-1), 1)),
		},
		{
			name: "code_present_empty",
			key:  TransactionAccessKey{Kind: TransactionAccessCode, Address: account},
			val:  ownedTransactionWriteValue(true, nil),
		},
		{
			name: "code_absent_nonempty",
			key:  TransactionAccessKey{Kind: TransactionAccessCode, Address: account},
			val:  ownedTransactionWriteValue(false, []byte{1}),
		},
		{
			name: "metadata_absent_nonempty",
			key:  TransactionAccessKey{Kind: TransactionAccessContractMetadata, Address: account},
			val:  ownedTransactionWriteValue(false, []byte{1}),
		},
		{
			name: "account_kv_absent_nonempty",
			key:  TransactionAccessKey{Kind: TransactionAccessAccountKV, Address: account, KVDomain: kvdomains.SystemReward, LogicalKey: "reward"},
			val:  ownedTransactionWriteValue(false, []byte{1}),
		},
		{
			name: "raw_kv_absent_nonempty",
			key:  TransactionAccessKey{Kind: TransactionAccessRawKV, LogicalKey: "application-owned"},
			val:  ownedTransactionWriteValue(false, []byte{1}),
		},
		{
			name: "nonsettlement_dynamic_marked_commutative",
			key:  TransactionAccessKey{Kind: TransactionAccessDynamicInt, LogicalKey: "energy_fee"},
			val:  TransactionWriteValue{Exists: true, Commutative: true, Value: make([]byte, 8)},
		},
		{
			name: "zero_settlement_dynamic_delta",
			key:  TransactionAccessKey{Kind: TransactionAccessDynamicInt, LogicalKey: "transaction_fee_pool"},
			val:  TransactionWriteValue{Exists: true, Commutative: true, Value: make([]byte, 8)},
		},
		{
			name: "nonblackhole_balance_marked_commutative",
			key:  TransactionAccessKey{Kind: TransactionAccessAccountField, Address: other, AccountField: TransactionAccountFieldBalance},
			val:  TransactionWriteValue{Exists: true, Commutative: true, Value: make([]byte, 8)},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			writes := TransactionWriteSet{
				balanceKey: int64TransactionWriteValue(200),
				test.key:   test.val,
			}
			if err := sdb.ApplyTransactionWriteSet(writes, NewDynamicProperties(), &orderedTransactionRawWriter{}); err == nil {
				t.Fatal("invalid typed key unexpectedly accepted")
			}
			if got := sdb.GetBalance(account); got != 100 {
				t.Fatalf("balance changed before complete preflight: %d", got)
			}
			if got := sdb.GetBalance(other); got != 50 {
				t.Fatalf("other balance changed before complete preflight: %d", got)
			}
		})
	}
}

func TestApplyTransactionWriteSetRejectsMismatchedContractAddress(t *testing.T) {
	contract := testAddr(0xe4)
	other := testAddr(0xe5)
	metadata, err := proto.Marshal(&contractpb.SmartContract{ContractAddress: other.Bytes()})
	if err != nil {
		t.Fatal(err)
	}
	writes := TransactionWriteSet{
		{Kind: TransactionAccessContractMetadata, Address: contract}: ownedTransactionWriteValue(true, metadata),
	}
	if err := ValidateTransactionWriteSetApply(writes, NewDynamicProperties(), &orderedTransactionRawWriter{}); err == nil {
		t.Fatal("mismatched contract address unexpectedly accepted")
	}
}

func TestApplyTransactionWriteSetUsesStableTopologicalOrder(t *testing.T) {
	sdb := newTestStateDB(t)
	raw := &orderedTransactionRawWriter{}
	writes := TransactionWriteSet{
		{Kind: TransactionAccessRawKV, LogicalKey: "z"}: {Exists: true, Value: []byte("z")},
		{Kind: TransactionAccessRawKV, LogicalKey: "a"}: {Exists: true, Value: []byte("a")},
		{Kind: TransactionAccessRawKV, LogicalKey: "m"}: {},
	}
	if err := sdb.ApplyTransactionWriteSet(writes, NewDynamicProperties(), raw); err != nil {
		t.Fatal(err)
	}
	if want := []string{"a", "m", "z"}; !reflect.DeepEqual(raw.keys, want) {
		t.Fatalf("raw apply order = %v, want %v", raw.keys, want)
	}
}
