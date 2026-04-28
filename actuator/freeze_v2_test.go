package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeFreezeV2Tx(ownerByte byte, amount int64, resource corepb.ResourceCode) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	fc := &contractpb.FreezeBalanceV2Contract{
		OwnerAddress:  owner.Bytes(),
		FrozenBalance: amount,
		Resource:      resource,
	}
	any, _ := anypb.New(fc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{Type: corepb.Transaction_Contract_FreezeBalanceV2Contract, Parameter: any},
			},
		},
	})
}

func TestFreezeV2Validate(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)

	// Missing account
	tx := makeFreezeV2Tx(1, 100, corepb.ResourceCode_BANDWIDTH)
	act := &FreezeBalanceV2Actuator{}
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowStakingV2(true)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing account")
	}

	// Create account with balance
	seedAccount(statedb, owner, 1000)

	// Zero amount
	tx = makeFreezeV2Tx(1, 0, corepb.ResourceCode_BANDWIDTH)
	ctx = setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowStakingV2(true)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for zero amount")
	}

	// Insufficient balance
	tx = makeFreezeV2Tx(1, 5000, corepb.ResourceCode_BANDWIDTH)
	ctx = setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowStakingV2(true)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for insufficient balance")
	}

	// Success
	tx = makeFreezeV2Tx(1, 500, corepb.ResourceCode_BANDWIDTH)
	ctx = setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowStakingV2(true)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFreezeV2Execute(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 1000)

	tx := makeFreezeV2Tx(1, 500, corepb.ResourceCode_BANDWIDTH)
	act := &FreezeBalanceV2Actuator{}
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowStakingV2(true)

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}

	if statedb.GetBalance(owner) != 500 {
		t.Fatalf("balance: want 500, got %d", statedb.GetBalance(owner))
	}
	if got := statedb.GetFrozenV2Amount(owner, corepb.ResourceCode_BANDWIDTH); got != 500 {
		t.Fatalf("frozen BW: want 500, got %d", got)
	}
}

// TestFreezeV2_TronPower_Validate: TRON_POWER resource is allowed once StakingV2
// (= AllowNewResourceModel, same proposal #62) is active.
func TestFreezeV2_TronPower_Validate(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(5)
	seedAccount(statedb, owner, 1000)
	act := &FreezeBalanceV2Actuator{}

	tx := makeFreezeV2Tx(5, 100, corepb.ResourceCode_TRON_POWER)

	// Fork inactive: all V2 operations fail at the StakingV2 gate.
	ctx := setupContext(t, statedb, tx)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: staking v2 not yet enabled")
	}

	// Fork active: TRON_POWER freeze is accepted.
	ctx = setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowStakingV2(true)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error with fork active: %v", err)
	}
}

// TestFreezeV2_InvalidResource: an unknown resource code is rejected whether
// the fork is inactive (StakingV2 gate) or active (resource-type guard).
func TestFreezeV2_InvalidResource(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(6)
	seedAccount(statedb, owner, 1000)
	act := &FreezeBalanceV2Actuator{}

	tx := makeFreezeV2Tx(6, 100, corepb.ResourceCode(99))

	// Fork inactive: rejected at the StakingV2 gate.
	ctx := setupContext(t, statedb, tx)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: staking v2 not yet enabled")
	}

	// Fork active: rejected by the resource-type default branch.
	ctx = setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowStakingV2(true)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for unknown resource code")
	}
}

// TestFreezeV2_Execute_InitializesOldTronPower: Execute snapshots LegacyTronPower into
// old_tron_power before the freeze when AllowNewResourceModel is active.
func TestFreezeV2_Execute_InitializesOldTronPower(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(7)
	seedAccount(statedb, owner, 1000)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 300)

	tx := makeFreezeV2Tx(7, 200, corepb.ResourceCode_BANDWIDTH)
	act := &FreezeBalanceV2Actuator{}
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowStakingV2(true)
	ctx.DynProps.SetAllowNewResourceModel(true)

	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute: %v", err)
	}

	acc := statedb.GetAccount(owner)
	if got := acc.OldTronPower(); got != 300 {
		t.Errorf("old_tron_power: want 300 (pre-freeze snapshot), got %d", got)
	}
}
