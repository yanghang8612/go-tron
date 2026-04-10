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

	sender := testProcessorAddr(1)
	statedb.CreateAccount(sender, corepb.AccountType_Normal)
	statedb.AddBalance(sender, 10_000_000)
	statedb.AddFreezeV2(sender, corepb.ResourceCode_BANDWIDTH, 1_000_000)

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

	statedb.SetFreeNetUsage(sender, dynProps.FreeNetLimit())
	statedb.SetLatestConsumeFreeTime(sender, 3000)

	tx := makeTestTransferTx(1, 2, 0)
	_, err := consumeBandwidth(statedb, dynProps, tx, 3000)
	if err == nil {
		t.Fatal("expected error for insufficient balance to pay bandwidth")
	}
}
