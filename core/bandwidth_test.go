package core

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

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

	_, err := consumeBandwidth(statedb, dynProps, tx, 3000)
	if err != nil {
		t.Fatalf("consumeBandwidth failed: %v", err)
	}

	if statedb.GetFreeNetUsage(sender) != txSize {
		t.Fatalf("free net usage: want %d, got %d", txSize, statedb.GetFreeNetUsage(sender))
	}
	if statedb.GetLatestConsumeFreeTime(sender) != 3000 {
		t.Fatalf("latest consume free time: want 3000, got %d", statedb.GetLatestConsumeFreeTime(sender))
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

	_, err := consumeBandwidth(statedb, dynProps, tx, 3000)
	if err != nil {
		t.Fatalf("consumeBandwidth failed: %v", err)
	}

	if statedb.GetNetUsage(sender) != txSize {
		t.Fatalf("net usage: want %d, got %d", txSize, statedb.GetNetUsage(sender))
	}
	if statedb.GetFreeNetUsage(sender) != 0 {
		t.Fatalf("free net usage should be 0, got %d", statedb.GetFreeNetUsage(sender))
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
	statedb.SetLatestConsumeFreeTime(sender, 3000)

	tx := makeTestTransferTx(1, 2, 100)
	txSize := int64(tx.Size())

	balBefore := statedb.GetBalance(sender)
	_, err := consumeBandwidth(statedb, dynProps, tx, 3000)
	if err != nil {
		t.Fatalf("consumeBandwidth failed: %v", err)
	}

	expectedCost := txSize * dynProps.TransactionFee()
	balAfter := statedb.GetBalance(sender)
	if balBefore-balAfter != expectedCost {
		t.Fatalf("TRX burn: want %d, got %d", expectedCost, balBefore-balAfter)
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
	statedb.SetLatestConsumeFreeTime(sender, 3000)

	tx := makeTestTransferTx(1, 2, 0)
	_, err := consumeBandwidth(statedb, dynProps, tx, 3000)
	if err == nil {
		t.Fatal("expected error for insufficient balance to pay bandwidth")
	}
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
	res, err := consumeBandwidth(statedb, dynProps, tx, 3000)
	if err != nil {
		t.Fatalf("consumeBandwidth failed: %v", err)
	}
	if res.NetFee != 100_000 {
		t.Fatalf("NetFee: want 100000, got %d", res.NetFee)
	}
	if balBefore-statedb.GetBalance(sender) != 100_000 {
		t.Fatalf("owner balance change: want 100000, got %d",
			balBefore-statedb.GetBalance(sender))
	}
	if dynProps.TotalCreateAccountCost() != 100_000 {
		t.Fatalf("total_create_account_cost: want 100000, got %d",
			dynProps.TotalCreateAccountCost())
	}
}
