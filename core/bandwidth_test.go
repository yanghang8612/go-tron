package core

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

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
