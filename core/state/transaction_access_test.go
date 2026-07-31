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

func TestTransactionAccessRecorderCaptureReadSetSurvivesReset(t *testing.T) {
	var address tcommon.Address
	address[0], address[20] = 0x41, 7
	var recorder TransactionAccessRecorder
	recorder.Reset(4)
	recorder.recordAccountKV(address, kvdomains.AccountPermissionAux, []byte("owner"), TransactionAccessRead)
	commutative := TransactionAccessKey{Kind: TransactionAccessDynamicInt, LogicalKey: "transaction_fee_pool"}
	recorder.record(commutative, TransactionAccessCommutativeRead|TransactionAccessCommutativeWrite)
	recorder.markUnsupported()

	captured := recorder.CaptureReadSet()
	recorder.Reset(4)
	if !captured.Unsupported || len(captured.Reads) != 2 {
		t.Fatalf("captured read set = %+v", captured)
	}
	seenKV, seenDelta := false, false
	for _, read := range captured.Reads {
		switch {
		case read.Key.Kind == TransactionAccessAccountKV:
			seenKV = read.Key.Address == address && read.Key.LogicalKey == "owner" && read.Mode == TransactionAccessRead
		case read.Key == commutative:
			seenDelta = read.Mode == TransactionAccessCommutativeRead
		}
	}
	if !seenKV || !seenDelta {
		t.Fatalf("captured reads lost after reset: %+v", captured.Reads)
	}
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
	_, _ = s.GetAccountType(account)
	_, _, _ = s.GetAccountKV(account, kvdomains.AccountPermissionAux, []byte("owner"))
	_ = s.GetState(contract, slot)
	_ = s.GetCode(contract)
	_ = s.GetContract(contract)
	_, _ = s.ContractRuntime(contract)
	_ = s.GetWitness(account)
	_ = s.GetTransientState(contract, slot)

	dp := NewDynamicProperties()
	dp.SetTransactionAccessRecorder(&recorder)
	_ = dp.TransactionFee()
	dp.Set("shadow_counter", 1)
	dp.SetString("shadow_label", "one")
	_ = dp.LatestBlockHeaderHash()

	want := map[TransactionAccessKey]TransactionAccessMode{
		{Kind: TransactionAccessAccountField, Address: account, AccountField: TransactionAccountFieldBalance}:               TransactionAccessRead,
		{Kind: TransactionAccessAccountField, Address: account, AccountField: TransactionAccountFieldAccountType}:           TransactionAccessRead,
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
	for key := range got {
		if key.Kind == TransactionAccessAccount {
			t.Fatalf("typed point-read path captured coarse account dependency: %v", key)
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

func TestTransactionAccessRecorderReplaysCachedResourcePointReads(t *testing.T) {
	s := newTestStateDB(t)
	account := testAddr(0x35)
	s.CreateAccount(account, corepb.AccountType_Normal)

	// Warm every resource cache without a transaction recorder. The following
	// access must still describe the physical rows it consumes.
	if _, _, err := s.GetAccountFrozenResourceTotals(account); err != nil {
		t.Fatalf("warm resource totals: %v", err)
	}

	var recorder TransactionAccessRecorder
	recorder.Reset(16)
	s.SetTransactionAccessRecorder(&recorder)
	if _, _, err := s.GetAccountFrozenResourceTotals(account); err != nil {
		t.Fatalf("cached resource totals: %v", err)
	}

	want := []TransactionAccessKey{
		{Kind: TransactionAccessAccountField, Address: account, AccountField: TransactionAccountFieldFrozenResource},
		{Kind: TransactionAccessAccountKVGeneration, Address: account},
		{Kind: TransactionAccessAccountKV, Address: account, KVDomain: kvdomains.AccountFrozenBandwidthAux, LogicalKey: string(accountFrozenBandwidthKey(0))},
		{Kind: TransactionAccessAccountKV, Address: account, KVDomain: kvdomains.AccountFrozenBandwidthAux, LogicalKey: string(accountFrozenBandwidthKey(1))},
		{Kind: TransactionAccessAccountKV, Address: account, KVDomain: kvdomains.AccountFrozenV2Aux, LogicalKey: string(accountFrozenV2Key(corepb.ResourceCode_BANDWIDTH))},
		{Kind: TransactionAccessAccountKV, Address: account, KVDomain: kvdomains.AccountResourceAux, LogicalKey: string(accountResourceKey)},
		{Kind: TransactionAccessAccountKV, Address: account, KVDomain: kvdomains.AccountFrozenV2Aux, LogicalKey: string(accountFrozenV2Key(corepb.ResourceCode_ENERGY))},
	}
	got := make(map[TransactionAccessKey]TransactionAccessMode)
	recorder.Visit(func(key TransactionAccessKey, mode TransactionAccessMode) bool {
		got[key] = mode
		return true
	})
	for _, key := range want {
		if mode := got[key]; mode&TransactionAccessRead == 0 {
			t.Fatalf("cached resource access %v mode = %d, want read (all=%v)", key, mode, got)
		}
	}
	if recorder.Unsupported() {
		t.Fatal("cached point reads unexpectedly marked unsupported")
	}
}

func TestTransactionAccessRecorderSeparatesSettlementAndOrdinaryReads(t *testing.T) {
	s := newTestStateDB(t)
	blackhole := testAddr(0x39)
	s.CreateAccount(blackhole, corepb.AccountType_Normal)
	s.AddBalance(blackhole, 100)
	dp := NewDynamicProperties()

	var recorder TransactionAccessRecorder
	recorder.Reset(16)
	s.SetTransactionAccessRecorder(&recorder)
	dp.SetTransactionAccessRecorder(&recorder)

	s.AddSettlementBalance(blackhole, 7)
	dp.AddBurnTrx(3)
	dp.AddTransactionFeePool(5)
	dp.AddTotalTransactionCost(7)
	dp.AddTotalCreateAccountCost(11)
	dp.AddTotalCreateWitnessCost(13)

	accountKey := TransactionAccessKey{Kind: TransactionAccessAccountField, Address: blackhole, AccountField: TransactionAccountFieldBalance}
	burnKey := TransactionAccessKey{Kind: TransactionAccessDynamicInt, LogicalKey: "burn_trx_amount"}
	got := make(map[TransactionAccessKey]TransactionAccessMode)
	recorder.Visit(func(key TransactionAccessKey, mode TransactionAccessMode) bool {
		got[key] = mode
		return true
	})
	wantSettlementAccount := TransactionAccessCommutativeRead | TransactionAccessCommutativeWrite
	if mode := got[accountKey]; mode != wantSettlementAccount {
		t.Fatalf("settlement account mode = %d, want %d", mode, wantSettlementAccount)
	}
	wantBurn := TransactionAccessCommutativeRead | TransactionAccessCommutativeWrite
	if mode := got[burnKey]; mode != wantBurn {
		t.Fatalf("burn accumulator mode = %d, want %d", mode, wantBurn)
	}
	if full, fields := recorder.AccountWriteCoverage(blackhole); full || !fields {
		t.Fatalf("settlement account coverage = full:%t fields:%t, want false/true", full, fields)
	}
	for _, key := range []string{
		"transaction_fee_pool",
		"total_transaction_cost",
		"total_create_account_cost",
		"total_create_witness_cost",
	} {
		accessKey := TransactionAccessKey{Kind: TransactionAccessDynamicInt, LogicalKey: key}
		if mode := got[accessKey]; mode != wantBurn {
			t.Fatalf("%s accumulator mode = %d, want %d", key, mode, wantBurn)
		}
	}

	// A real read of the same cell in the same transaction must survive the
	// settlement label; normalized validation may ignore only the helper's
	// internal read-modify-write dependency.
	_ = s.GetBalance(blackhole)
	_ = dp.BurnTrxAmount()
	got = make(map[TransactionAccessKey]TransactionAccessMode)
	recorder.Visit(func(key TransactionAccessKey, mode TransactionAccessMode) bool {
		got[key] = mode
		return true
	})
	if mode := got[accountKey]; mode != TransactionAccessRead|wantSettlementAccount {
		t.Fatalf("mixed account mode = %d", mode)
	}
	if mode := got[burnKey]; mode != TransactionAccessRead|wantBurn {
		t.Fatalf("mixed burn mode = %d", mode)
	}
	if got := s.GetBalance(blackhole); got != 107 {
		t.Fatalf("settlement balance = %d, want 107", got)
	}
	if got := dp.BurnTrxAmount(); got != 3 {
		t.Fatalf("burn amount = %d, want 3", got)
	}
	s.SetAccountName(blackhole, "full-write")
	if full, fields := recorder.AccountWriteCoverage(blackhole); !full || !fields {
		t.Fatalf("mixed account coverage = full:%t fields:%t, want true/true", full, fields)
	}
}

func TestSettlementMutationsRetainSnapshotRollback(t *testing.T) {
	s := newTestStateDB(t)
	blackhole := testAddr(0x3a)
	s.CreateAccount(blackhole, corepb.AccountType_Normal)
	s.AddBalance(blackhole, 100)
	dp := NewDynamicProperties()

	stateSnapshot := s.Snapshot()
	dynamicSnapshot := dp.Snapshot()
	s.AddSettlementBalance(blackhole, 9)
	dp.AddTransactionFeePool(11)
	dp.AddTotalTransactionCost(13)
	if got := s.GetBalance(blackhole); got != 109 {
		t.Fatalf("settlement balance = %d, want 109", got)
	}
	if got := dp.TransactionFeePool(); got != 11 {
		t.Fatalf("fee pool = %d, want 11", got)
	}
	if got := dp.TotalTransactionCost(); got != 13 {
		t.Fatalf("transaction cost = %d, want 13", got)
	}

	s.RevertToSnapshot(stateSnapshot)
	dp.RevertToSnapshot(dynamicSnapshot)
	if got := s.GetBalance(blackhole); got != 100 {
		t.Fatalf("balance after rollback = %d, want 100", got)
	}
	if got := dp.TransactionFeePool(); got != 0 {
		t.Fatalf("fee pool after rollback = %d, want 0", got)
	}
	if got := dp.TotalTransactionCost(); got != 0 {
		t.Fatalf("transaction cost after rollback = %d, want 0", got)
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
