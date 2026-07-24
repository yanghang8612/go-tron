package core

import (
	"strconv"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	testBandwidthBlockTime = int64(3000)
	testBandwidthHeadSlot  = int64(1)
)

func makeBandwidthTransferAssetTx(ownerByte, toByte byte, assetName []byte, amount int64) *types.Transaction {
	c := &contractpb.TransferAssetContract{
		OwnerAddress: testProcessorAddr(ownerByte).Bytes(),
		ToAddress:    testProcessorAddr(toByte).Bytes(),
		AssetName:    assetName,
		Amount:       amount,
	}
	anyParam, _ := anypb.New(c)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{Type: corepb.Transaction_Contract_TransferAssetContract, Parameter: anyParam},
			},
		},
	})
}

func makeBandwidthShieldedTransferTx(fromByte byte, amount int64) *types.Transaction {
	c := &contractpb.ShieldedTransferContract{
		TransparentFromAddress: testProcessorAddr(fromByte).Bytes(),
		FromAmount:             amount,
	}
	anyParam, _ := anypb.New(c)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{Type: corepb.Transaction_Contract_ShieldedTransferContract, Parameter: anyParam},
			},
		},
	})
}

// TestTxBandwidthSize_SupportVMFormula locks the java-tron BandwidthProcessor
// asymmetry: pre-supportVM, the bandwidth size is the full tx serialized
// size (including any `ret` field). Post-supportVM (allow_creation_of_contracts
// = 1), `ret` is stripped and `MAX_RESULT_SIZE_IN_TX (= 64)` is added per
// non-shielded contract.
//
// Empirical pinning case: Nile h=55000 / h=550000 VoteWitnessContract txs
// reported `net_usage = 299` on java-tron vs `239` on pre-fix gtron. The
// 60-byte gap = -4 (empty ret slot stripped) + 64 (constant added).
func TestTxBandwidthSize_SupportVMFormula(t *testing.T) {
	// 1-contract TransferContract; no Ret pre-populated. With ret empty,
	// clearRet is a no-op for size, so supportVM size = legacy + 64.
	tx := makeTestTransferTx(1, 2, 100)
	legacy := txBandwidthSize(tx, false)
	withVM := txBandwidthSize(tx, true)
	if delta := withVM - legacy; delta != maxResultSizeInTx {
		t.Fatalf("supportVM bandwidth delta: got +%d, want +%d (MAX_RESULT_SIZE_IN_TX)",
			delta, maxResultSizeInTx)
	}

	// With a populated Ret entry, supportVM strips it before sizing then
	// re-adds 64 — net delta from legacy is (64 − retEntrySize).
	txWithRet := makeTestTransferTx(1, 2, 100)
	txWithRet.Proto().Ret = []*corepb.Transaction_Result{{
		Fee:         0,
		Ret:         corepb.Transaction_Result_SUCESS,
		ContractRet: corepb.Transaction_Result_SUCCESS,
	}}
	legacyWithRet := txBandwidthSize(txWithRet, false)
	withVMRet := txBandwidthSize(txWithRet, true)
	if withVMRet >= legacyWithRet+maxResultSizeInTx {
		t.Fatalf("ret should be stripped under supportVM; legacy=%d, withVM=%d (delta should be < +64)",
			legacyWithRet, withVMRet)
	}
	// Both VM-mode sizes (with vs without ret) must agree — clearRet
	// neutralizes the populated ret entry.
	if withVMRet != withVM {
		t.Fatalf("clearRet did not neutralize populated ret: empty-ret=%d, populated-ret=%d",
			withVM, withVMRet)
	}
}

func TestTransactionSizeWithoutRetDoesNotMutate(t *testing.T) {
	tx := makeTestTransferTx(1, 2, 100)
	tx.Proto().Signature = [][]byte{make([]byte, 65), make([]byte, 65)}
	tx.Proto().Ret = []*corepb.Transaction_Result{
		{Fee: 1, ContractRet: corepb.Transaction_Result_SUCCESS},
		nil,
		{Fee: 2, ContractRet: corepb.Transaction_Result_REVERT},
	}

	wantPB := proto.Clone(tx.Proto()).(*corepb.Transaction)
	wantPB.Ret = nil
	want := proto.Size(wantPB)
	if got := transactionSizeWithoutRet(tx); got != want {
		t.Fatalf("size without ret: got %d, want %d", got, want)
	}
	if got := len(tx.Proto().Ret); got != 3 {
		t.Fatalf("transaction ret mutated: got %d entries, want 3", got)
	}
}

var transactionSizeSink int

func BenchmarkTransactionSizeWithoutRet(b *testing.B) {
	tx := makeTestTransferTx(1, 2, 100)
	tx.Proto().RawData.Data = make([]byte, 512)
	tx.Proto().Signature = [][]byte{make([]byte, 65), make([]byte, 65)}
	tx.Proto().Ret = []*corepb.Transaction_Result{
		{Fee: 1, ContractRet: corepb.Transaction_Result_SUCCESS},
		{Fee: 2, ContractRet: corepb.Transaction_Result_REVERT},
	}

	b.Run("deep-clone", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			stripped := proto.Clone(tx.Proto()).(*corepb.Transaction)
			stripped.Ret = nil
			transactionSizeSink = proto.Size(stripped)
		}
	})
	b.Run("field-size-subtraction", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			transactionSizeSink = transactionSizeWithoutRet(tx)
		}
	})
}

func TestConsumeBandwidth_ShieldedTransferSkipped(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	tx := makeBandwidthShieldedTransferTx(1, 100)
	res, err := consumeBandwidthWithResourceTime(statedb, dynProps, tx, testBandwidthBlockTime, testBandwidthHeadSlot)
	if err != nil {
		t.Fatalf("consumeBandwidth failed: %v", err)
	}
	if res.NetUsage != 0 || res.NetFee != 0 || res.NetFeeForBandwidth {
		t.Fatalf("bandwidth result: got usage=%d fee=%d feeForBandwidth=%v, want zero bill",
			res.NetUsage, res.NetFee, res.NetFeeForBandwidth)
	}
	if got := statedb.GetFreeNetUsage(testProcessorAddr(1)); got != 0 {
		t.Fatalf("shielded transfer should not consume free net, got %d", got)
	}
	if got := dynProps.PublicNetUsage(); got != 0 {
		t.Fatalf("shielded transfer should not consume public net, got %d", got)
	}
}

func TestConsumeBandwidth_FreeBandwidth(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	sender := testProcessorAddr(1)
	statedb.CreateAccount(sender, corepb.AccountType_Normal)
	statedb.AddBalance(sender, 10_000_000)
	// Pre-create the recipient so this exercises the regular bandwidth path
	// (the create-new-account branch is covered separately).
	statedb.CreateAccount(testProcessorAddr(2), corepb.AccountType_Normal)

	tx := makeTestTransferTx(1, 2, 100)
	txSize := int64(tx.Size())

	_, err := consumeBandwidthWithResourceTime(statedb, dynProps, tx, testBandwidthBlockTime, testBandwidthHeadSlot)
	if err != nil {
		t.Fatalf("consumeBandwidth failed: %v", err)
	}

	if statedb.GetFreeNetUsage(sender) != txSize {
		t.Fatalf("free net usage: want %d, got %d", txSize, statedb.GetFreeNetUsage(sender))
	}
	if statedb.GetLatestConsumeFreeTime(sender) != testBandwidthHeadSlot {
		t.Fatalf("latest consume free time: want %d, got %d", testBandwidthHeadSlot, statedb.GetLatestConsumeFreeTime(sender))
	}
	if statedb.GetLatestOperationTime(sender) != testBandwidthBlockTime {
		t.Fatalf("latest operation time: want %d, got %d", testBandwidthBlockTime, statedb.GetLatestOperationTime(sender))
	}
	if dynProps.PublicNetUsage() != txSize {
		t.Fatalf("public net usage: want %d, got %d", txSize, dynProps.PublicNetUsage())
	}
	if dynProps.PublicNetTime() != testBandwidthHeadSlot {
		t.Fatalf("public net time: want %d, got %d", testBandwidthHeadSlot, dynProps.PublicNetTime())
	}
}

func TestConsumeBandwidth_FrozenBandwidth(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()
	// Single-account world: total_net_weight mirrors this account's freeze
	// so availableAccountNet == total_net_limit.
	dynProps.SetTotalNetWeight(1)
	dynProps.Set("unfreeze_delay_days", 14)

	sender := testProcessorAddr(1)
	statedb.CreateAccount(sender, corepb.AccountType_Normal)
	statedb.AddBalance(sender, 10_000_000)
	statedb.AddFreezeV2(sender, corepb.ResourceCode_BANDWIDTH, 1_000_000)
	statedb.CreateAccount(testProcessorAddr(2), corepb.AccountType_Normal)

	tx := makeTestTransferTx(1, 2, 100)
	txSize := int64(tx.Size())

	_, err := consumeBandwidthWithResourceTime(statedb, dynProps, tx, testBandwidthBlockTime, testBandwidthHeadSlot)
	if err != nil {
		t.Fatalf("consumeBandwidth failed: %v", err)
	}

	if statedb.GetNetUsage(sender) != txSize {
		t.Fatalf("net usage: want %d, got %d", txSize, statedb.GetNetUsage(sender))
	}
	if statedb.GetLatestOperationTime(sender) != testBandwidthBlockTime {
		t.Fatalf("latest operation time: want %d, got %d", testBandwidthBlockTime, statedb.GetLatestOperationTime(sender))
	}
	if statedb.GetFreeNetUsage(sender) != 0 {
		t.Fatalf("free net usage should be 0, got %d", statedb.GetFreeNetUsage(sender))
	}
}

func TestConsumeBandwidth_TransferAssetUsesAssetAccountNet(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()
	dynProps.SetTotalNetWeight(1)
	dynProps.Set("unfreeze_delay_days", 14)

	sender := testProcessorAddr(1)
	receiver := testProcessorAddr(2)
	issuer := testProcessorAddr(9)
	statedb.CreateAccount(sender, corepb.AccountType_Normal)
	statedb.CreateAccount(receiver, corepb.AccountType_Normal)
	statedb.CreateAccount(issuer, corepb.AccountType_Normal)
	statedb.AddFreezeV2(issuer, corepb.ResourceCode_BANDWIDTH, params.TRXPrecision)

	const tokenID = int64(1_000_001)
	tokenName := []byte("TOK")
	asset := &contractpb.AssetIssueContract{
		Id:                      strconv.FormatInt(tokenID, 10),
		Name:                    tokenName,
		OwnerAddress:            issuer.Bytes(),
		FreeAssetNetLimit:       100_000,
		PublicFreeAssetNetLimit: 100_000,
	}
	if err := statedb.WriteAssetIssueByName(tokenName, asset); err != nil {
		t.Fatal(err)
	}
	if err := statedb.WriteAssetIssue(tokenID, asset); err != nil {
		t.Fatal(err)
	}
	if err := statedb.WriteAssetNameIndex(tokenName, tokenID); err != nil {
		t.Fatal(err)
	}

	tx := makeBandwidthTransferAssetTx(1, 2, tokenName, 100)
	txSize := int64(tx.Size())

	res, err := consumeBandwidthWithResourceTime(statedb, dynProps, tx, testBandwidthBlockTime, testBandwidthHeadSlot)
	if err != nil {
		t.Fatalf("consumeBandwidth failed: %v", err)
	}
	if res.NetUsage != txSize || res.NetFee != 0 {
		t.Fatalf("bandwidth result: got usage=%d fee=%d, want usage=%d fee=0", res.NetUsage, res.NetFee, txSize)
	}
	if got := statedb.GetFreeAssetNetUsage(sender, string(tokenName)); got != txSize {
		t.Fatalf("free_asset_net_usage: want %d, got %d", txSize, got)
	}
	if got := statedb.GetFreeAssetNetUsageV2(sender, strconv.FormatInt(tokenID, 10)); got != txSize {
		t.Fatalf("free_asset_net_usageV2: want %d, got %d", txSize, got)
	}
	if got := statedb.GetLatestAssetOperationTime(sender, string(tokenName)); got != testBandwidthHeadSlot {
		t.Fatalf("latest_asset_operation_time: want %d, got %d", testBandwidthHeadSlot, got)
	}
	if got := statedb.GetLatestOperationTime(sender); got != testBandwidthBlockTime {
		t.Fatalf("latest operation time: want %d, got %d", testBandwidthBlockTime, got)
	}
	if got := statedb.GetNetUsage(issuer); got != txSize {
		t.Fatalf("issuer net usage: want %d, got %d", txSize, got)
	}
	if got := statedb.GetLatestConsumeTime(issuer); got != testBandwidthHeadSlot {
		t.Fatalf("issuer latest consume time: want %d, got %d", testBandwidthHeadSlot, got)
	}
	legacyAsset := statedb.ReadAssetIssueByName(tokenName)
	if legacyAsset == nil {
		t.Fatal("legacy asset missing")
	}
	if got := legacyAsset.PublicFreeAssetNetUsage; got != txSize {
		t.Fatalf("legacy public free asset usage: want %d, got %d", txSize, got)
	}
	if got := legacyAsset.PublicLatestFreeNetTime; got != testBandwidthHeadSlot {
		t.Fatalf("legacy public latest free net time: want %d, got %d", testBandwidthHeadSlot, got)
	}
	v2Asset := statedb.ReadAssetIssue(tokenID)
	if v2Asset == nil {
		t.Fatal("v2 asset missing")
	}
	if got := v2Asset.PublicFreeAssetNetUsage; got != txSize {
		t.Fatalf("v2 public free asset usage: want %d, got %d", txSize, got)
	}
	if got := statedb.GetFreeNetUsage(sender); got != 0 {
		t.Fatalf("sender free net usage should not be used, got %d", got)
	}
	if got := dynProps.TotalTransactionCost(); got != 0 {
		t.Fatalf("total_transaction_cost should not change, got %d", got)
	}
}

func TestConsumeBandwidth_BurnTRX(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	sender := testProcessorAddr(1)
	statedb.CreateAccount(sender, corepb.AccountType_Normal)
	statedb.AddBalance(sender, 10_000_000)
	statedb.CreateAccount(testProcessorAddr(2), corepb.AccountType_Normal)

	statedb.SetFreeNetUsage(sender, dynProps.FreeNetLimit())
	statedb.SetLatestConsumeFreeTime(sender, testBandwidthHeadSlot)

	tx := makeTestTransferTx(1, 2, 100)
	txSize := int64(tx.Size())

	balBefore := statedb.GetBalance(sender)
	blackholeBefore := statedb.GetBalance(params.BlackholeAddress)
	res, err := consumeBandwidthWithResourceTime(statedb, dynProps, tx, testBandwidthBlockTime, testBandwidthHeadSlot)
	if err != nil {
		t.Fatalf("consumeBandwidth failed: %v", err)
	}

	expectedCost := txSize * dynProps.TransactionFee()
	balAfter := statedb.GetBalance(sender)
	if balBefore-balAfter != expectedCost {
		t.Fatalf("TRX burn: want %d, got %d", expectedCost, balBefore-balAfter)
	}
	if !res.NetFeeForBandwidth {
		t.Fatal("NetFeeForBandwidth: want true")
	}
	if got := statedb.GetBalance(params.BlackholeAddress) - blackholeBefore; got != expectedCost {
		t.Fatalf("blackhole balance delta: want %d, got %d", expectedCost, got)
	}
	if got := dynProps.TotalTransactionCost(); got != expectedCost {
		t.Fatalf("total_transaction_cost: want %d, got %d", expectedCost, got)
	}
	if got := statedb.GetLatestOperationTime(sender); got != testBandwidthBlockTime {
		t.Fatalf("latest operation time: want %d, got %d", testBandwidthBlockTime, got)
	}
}

func TestConsumeBandwidth_TransactionFeePool(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()
	dynProps.SetAllowTransactionFeePool(true)

	sender := testProcessorAddr(1)
	statedb.CreateAccount(sender, corepb.AccountType_Normal)
	statedb.AddBalance(sender, 10_000_000)
	statedb.CreateAccount(testProcessorAddr(2), corepb.AccountType_Normal)

	statedb.SetFreeNetUsage(sender, dynProps.FreeNetLimit())
	statedb.SetLatestConsumeFreeTime(sender, testBandwidthHeadSlot)

	tx := makeTestTransferTx(1, 2, 100)
	txSize := int64(tx.Size())

	res, err := consumeBandwidthWithResourceTime(statedb, dynProps, tx, testBandwidthBlockTime, testBandwidthHeadSlot)
	if err != nil {
		t.Fatalf("consumeBandwidth failed: %v", err)
	}

	expectedCost := txSize * dynProps.TransactionFee()
	if got := dynProps.TransactionFeePool(); got != expectedCost {
		t.Fatalf("transaction_fee_pool: want %d, got %d", expectedCost, got)
	}
	if got := dynProps.BurnTrxAmount(); got != 0 {
		t.Fatalf("burn_trx_amount: want 0, got %d", got)
	}
	if got := statedb.GetBalance(params.BlackholeAddress); got != 0 {
		t.Fatalf("blackhole balance: want 0, got %d", got)
	}
	if got := dynProps.TotalTransactionCost(); got != expectedCost {
		t.Fatalf("total_transaction_cost: want %d, got %d", expectedCost, got)
	}
	if !res.NetFeeForBandwidth {
		t.Fatal("NetFeeForBandwidth: want true")
	}
}

func TestConsumeBandwidth_BlackholeOptimization(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()
	dynProps.SetAllowBlackHoleOptimization(true)

	sender := testProcessorAddr(1)
	statedb.CreateAccount(sender, corepb.AccountType_Normal)
	statedb.AddBalance(sender, 10_000_000)
	statedb.CreateAccount(testProcessorAddr(2), corepb.AccountType_Normal)

	statedb.SetFreeNetUsage(sender, dynProps.FreeNetLimit())
	statedb.SetLatestConsumeFreeTime(sender, testBandwidthHeadSlot)

	tx := makeTestTransferTx(1, 2, 100)
	txSize := int64(tx.Size())

	_, err := consumeBandwidthWithResourceTime(statedb, dynProps, tx, testBandwidthBlockTime, testBandwidthHeadSlot)
	if err != nil {
		t.Fatalf("consumeBandwidth failed: %v", err)
	}

	expectedCost := txSize * dynProps.TransactionFee()
	if got := dynProps.BurnTrxAmount(); got != expectedCost {
		t.Fatalf("burn_trx_amount: want %d, got %d", expectedCost, got)
	}
	if got := statedb.GetBalance(params.BlackholeAddress); got != 0 {
		t.Fatalf("blackhole balance: want 0, got %d", got)
	}
	if got := dynProps.TransactionFeePool(); got != 0 {
		t.Fatalf("transaction_fee_pool: want 0, got %d", got)
	}
}

func TestConsumeBandwidth_InsufficientBalance(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	sender := testProcessorAddr(1)
	statedb.CreateAccount(sender, corepb.AccountType_Normal)
	statedb.AddBalance(sender, 1)
	statedb.CreateAccount(testProcessorAddr(2), corepb.AccountType_Normal)

	statedb.SetFreeNetUsage(sender, dynProps.FreeNetLimit())
	statedb.SetLatestConsumeFreeTime(sender, testBandwidthHeadSlot)

	tx := makeTestTransferTx(1, 2, 0)
	_, err := consumeBandwidthWithResourceTime(statedb, dynProps, tx, testBandwidthBlockTime, testBandwidthHeadSlot)
	if err == nil {
		t.Fatal("expected error for insufficient balance to pay bandwidth")
	}
}

func BenchmarkChargeStakedNetAccountRead(b *testing.B) {
	db := state.NewDatabase(ethrawdb.NewMemoryDatabase())
	sdb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		b.Fatal(err)
	}
	owner := testProcessorAddr(1)
	sdb.CreateAccount(owner, corepb.AccountType_Normal)
	sdb.FreezeV1Bandwidth(owner, 1_000_000, 200)
	sdb.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 1_000_000)
	for tokenID := int64(1_000_000); tokenID < 1_000_256; tokenID++ {
		sdb.SetTRC10Balance(owner, tokenID, tokenID)
	}
	root, err := sdb.Commit()
	if err != nil {
		b.Fatal(err)
	}
	dynProps := state.NewDynamicProperties()
	dynProps.SetTotalNetWeight(2)
	dynProps.Set("unfreeze_delay_days", 14)

	b.Run("targeted-bandwidth-read", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			view, err := state.New(root, db)
			if err != nil {
				b.Fatal(err)
			}
			if !chargeStakedNet(view, dynProps, owner, 100, 1) {
				b.Fatal("targeted bandwidth charge failed")
			}
		}
	})
	b.Run("legacy-full-account-read", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			view, err := state.New(root, db)
			if err != nil {
				b.Fatal(err)
			}
			if view.GetAccount(owner) == nil {
				b.Fatal("full account read failed")
			}
			if !chargeStakedNet(view, dynProps, owner, 100, 1) {
				b.Fatal("legacy bandwidth charge failed")
			}
		}
	})
}

// TestConsumeBandwidth_CreateNewAccount_Fee verifies the create_account_fee
// path: tx whose recipient does not yet exist, no staked bandwidth → owner
// pays create_account_fee (default 100k) and total_create_account_cost
// is incremented. Free bandwidth is intentionally bypassed.
func TestConsumeBandwidth_CreateNewAccount_Fee(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	sender := testProcessorAddr(1)
	statedb.CreateAccount(sender, corepb.AccountType_Normal)
	statedb.AddBalance(sender, 10_000_000)
	// Recipient does NOT exist → triggers create-new-account branch.

	tx := makeTestTransferTx(1, 2, 100)

	balBefore := statedb.GetBalance(sender)
	res, err := consumeBandwidthWithResourceTime(statedb, dynProps, tx, testBandwidthBlockTime, testBandwidthHeadSlot)
	if err != nil {
		t.Fatalf("consumeBandwidth failed: %v", err)
	}
	if res.NetFee != 100_000 {
		t.Fatalf("NetFee: want 100000, got %d", res.NetFee)
	}
	if res.NetFeeForBandwidth {
		t.Fatal("NetFeeForBandwidth: want false for create-account fee")
	}
	if balBefore-statedb.GetBalance(sender) != 100_000 {
		t.Fatalf("owner balance change: want 100000, got %d",
			balBefore-statedb.GetBalance(sender))
	}
	if dynProps.TotalCreateAccountCost() != 100_000 {
		t.Fatalf("total_create_account_cost: want 100000, got %d",
			dynProps.TotalCreateAccountCost())
	}
	if got := statedb.GetLatestOperationTime(sender); got != testBandwidthBlockTime {
		t.Fatalf("latest operation time: want %d, got %d", testBandwidthBlockTime, got)
	}
}

// A zero create-new-account bandwidth rate is legal governance state. Java
// accepts the staked-bandwidth branch at zero cost even when the account has no
// stake, and still persists its consume/operation timestamps.
func TestConsumeBandwidth_CreateNewAccount_ZeroBandwidthRate(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()
	dynProps.Set("create_new_account_bandwidth_rate", 0)

	sender := testProcessorAddr(1)
	statedb.CreateAccount(sender, corepb.AccountType_Normal)
	statedb.AddBalance(sender, 10_000_000)

	tx := makeTestTransferTx(1, 2, 100)
	balBefore := statedb.GetBalance(sender)
	res, err := consumeBandwidthWithResourceTime(statedb, dynProps, tx, testBandwidthBlockTime, testBandwidthHeadSlot)
	if err != nil {
		t.Fatalf("consumeBandwidth failed: %v", err)
	}
	if res.NetUsage != 0 || res.NetFee != 0 {
		t.Fatalf("bill: got usage=%d fee=%d, want both zero", res.NetUsage, res.NetFee)
	}
	if got := statedb.GetBalance(sender); got != balBefore {
		t.Fatalf("balance: got %d, want unchanged %d", got, balBefore)
	}
	if got := statedb.GetLatestConsumeTime(sender); got != testBandwidthHeadSlot {
		t.Fatalf("latest consume time: got %d, want %d", got, testBandwidthHeadSlot)
	}
	if got := statedb.GetLatestOperationTime(sender); got != testBandwidthBlockTime {
		t.Fatalf("latest operation time: got %d, want %d", got, testBandwidthBlockTime)
	}
}

// When staked bandwidth cannot cover creation and create_account_fee is zero,
// Java still executes the fee path and refreshes latest_operation_time.
func TestConsumeBandwidth_CreateNewAccount_ZeroFeeRefreshesOperationTime(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()
	dynProps.Set("create_account_fee", 0)

	sender := testProcessorAddr(1)
	statedb.CreateAccount(sender, corepb.AccountType_Normal)
	statedb.AddBalance(sender, 10_000_000)

	tx := makeTestTransferTx(1, 2, 100)
	balBefore := statedb.GetBalance(sender)
	res, err := consumeBandwidthWithResourceTime(statedb, dynProps, tx, testBandwidthBlockTime, testBandwidthHeadSlot)
	if err != nil {
		t.Fatalf("consumeBandwidth failed: %v", err)
	}
	if res.NetUsage != 0 || res.NetFee != 0 {
		t.Fatalf("bill: got usage=%d fee=%d, want both zero", res.NetUsage, res.NetFee)
	}
	if got := statedb.GetBalance(sender); got != balBefore {
		t.Fatalf("balance: got %d, want unchanged %d", got, balBefore)
	}
	if got := statedb.GetLatestOperationTime(sender); got != testBandwidthBlockTime {
		t.Fatalf("latest operation time: got %d, want %d", got, testBandwidthBlockTime)
	}
}
