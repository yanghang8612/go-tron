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
	tx := makeFreezeV2Tx(1, 1_000_000, corepb.ResourceCode_BANDWIDTH)
	act := &FreezeBalanceV2Actuator{}
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing account")
	}

	// Create account with balance
	seedAccount(statedb, owner, 10_000_000)

	// Zero amount
	tx = makeFreezeV2Tx(1, 0, corepb.ResourceCode_BANDWIDTH)
	ctx = setupContext(t, statedb, tx)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for zero amount")
	}

	// Sub-TRX amount
	tx = makeFreezeV2Tx(1, 999_999, corepb.ResourceCode_BANDWIDTH)
	ctx = setupContext(t, statedb, tx)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for sub-TRX amount")
	}

	// Insufficient balance
	tx = makeFreezeV2Tx(1, 50_000_000, corepb.ResourceCode_BANDWIDTH)
	ctx = setupContext(t, statedb, tx)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for insufficient balance")
	}

	// Success
	tx = makeFreezeV2Tx(1, 5_000_000, corepb.ResourceCode_BANDWIDTH)
	ctx = setupContext(t, statedb, tx)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFreezeV2Execute(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 10_000_000)

	tx := makeFreezeV2Tx(1, 5_000_000, corepb.ResourceCode_BANDWIDTH)
	act := &FreezeBalanceV2Actuator{}
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetUnfreezeDelayDays(14)

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}

	if statedb.GetBalance(owner) != 5_000_000 {
		t.Fatalf("balance: want 5000000, got %d", statedb.GetBalance(owner))
	}
	if got := statedb.GetFrozenV2Amount(owner, corepb.ResourceCode_BANDWIDTH); got != 5_000_000 {
		t.Fatalf("frozen BW: want 5000000, got %d", got)
	}
}

// TestFreezeV2_TronPower_Validate: TRON_POWER resource is allowed once StakingV2
// (= AllowNewResourceModel, same proposal #62) is active.
func TestFreezeV2_TronPower_Validate(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(5)
	seedAccount(statedb, owner, 10_000_000)
	act := &FreezeBalanceV2Actuator{}

	tx := makeFreezeV2Tx(5, 1_000_000, corepb.ResourceCode_TRON_POWER)

	// Fork inactive: all V2 operations fail at the StakingV2 gate.
	ctx := setupContext(t, statedb, tx)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: staking v2 not yet enabled")
	}

	// Fork active: TRON_POWER freeze is accepted; needs both the top-level
	// StakingV2 gate (unfreeze_delay_days > 0) and AllowNewResourceModel for
	// the TRON_POWER resource switch branch.
	ctx = setupContext(t, statedb, tx)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	ctx.DynProps.SetAllowNewResourceModel(true)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error with fork active: %v", err)
	}
}

// TestFreezeV2_InvalidResource: an unknown resource code is rejected whether
// the fork is inactive (StakingV2 gate) or active (resource-type guard).
func TestFreezeV2_InvalidResource(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(6)
	seedAccount(statedb, owner, 10_000_000)
	act := &FreezeBalanceV2Actuator{}

	tx := makeFreezeV2Tx(6, 1_000_000, corepb.ResourceCode(99))

	// Fork inactive: rejected at the StakingV2 gate.
	ctx := setupContext(t, statedb, tx)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: staking v2 not yet enabled")
	}

	// Fork active: rejected by the resource-type default branch.
	ctx = setupContext(t, statedb, tx)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for unknown resource code")
	}
}

// TestFreezeV2_Execute_InitializesOldTronPower: Execute snapshots LegacyTronPower into
// old_tron_power before the freeze when AllowNewResourceModel is active.
func TestFreezeV2_Execute_InitializesOldTronPower(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(7)
	seedAccount(statedb, owner, 10_000_000)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 3_000_000)

	tx := makeFreezeV2Tx(7, 2_000_000, corepb.ResourceCode_BANDWIDTH)
	act := &FreezeBalanceV2Actuator{}
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowStakingV2(true)
	ctx.DynProps.SetAllowNewResourceModel(true)

	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute: %v", err)
	}

	acc := statedb.GetAccount(owner)
	if got := acc.OldTronPower(); got != 3_000_000 {
		t.Errorf("old_tron_power: want 3000000 (pre-freeze snapshot), got %d", got)
	}
}

// TestFreezeV2_Execute_UpdatesTotalWeight: V2 freeze must update
// total_{net,energy,tron_power}_weight to mirror java-tron's
// FreezeBalanceV2Actuator.execute switch block. Without this, gtron's
// availableAccountNet returns 0 for owner and falls to the create-account
// fee / free-net path instead of consuming staked bandwidth — visible as a
// cross-impl drift in SR balance over time.
func TestFreezeV2_Execute_UpdatesTotalWeight(t *testing.T) {
	cases := []struct {
		name     string
		resource corepb.ResourceCode
		amount   int64
		check    func(t *testing.T, ctx *Context)
	}{
		{
			name:     "BANDWIDTH",
			resource: corepb.ResourceCode_BANDWIDTH,
			amount:   5_000_000, // 5 TRX → weight 5
			check: func(t *testing.T, ctx *Context) {
				if got := ctx.DynProps.TotalNetWeight(); got != 5 {
					t.Errorf("total_net_weight: want 5, got %d", got)
				}
			},
		},
		{
			name:     "ENERGY",
			resource: corepb.ResourceCode_ENERGY,
			amount:   3_000_000, // 3 TRX → weight 3
			check: func(t *testing.T, ctx *Context) {
				if got := ctx.DynProps.TotalEnergyWeight(); got != 3 {
					t.Errorf("total_energy_weight: want 3, got %d", got)
				}
			},
		},
		{
			name:     "TRON_POWER",
			resource: corepb.ResourceCode_TRON_POWER,
			amount:   7_000_000, // 7 TRX → weight 7
			check: func(t *testing.T, ctx *Context) {
				if got := ctx.DynProps.TotalTronPowerWeight(); got != 7 {
					t.Errorf("total_tron_power_weight: want 7, got %d", got)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			statedb := setupStateDB(t)
			owner := makeTestAddr(8)
			seedAccount(statedb, owner, 100_000_000)

			tx := makeFreezeV2Tx(8, tc.amount, tc.resource)
			ctx := setupContext(t, statedb, tx)
			ctx.DynProps.SetUnfreezeDelayDays(14)
			ctx.DynProps.SetAllowNewResourceModel(true)

			if _, err := (&FreezeBalanceV2Actuator{}).Execute(ctx); err != nil {
				t.Fatalf("execute: %v", err)
			}
			tc.check(t, ctx)
		})
	}
}

// TestFreezeV2_Execute_WeightDelta: a freeze that crosses an integer-TRX
// boundary increments the total weight by the *difference* in TRX count,
// not by frozenBalance/TRX_PRECISION. Mirrors java-tron's
// (newFrozenWithDelegated/TRX - oldFrozenWithDelegated/TRX) formula.
func TestFreezeV2_Execute_WeightDelta(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(9)
	seedAccount(statedb, owner, 100_000_000)

	// Pre-existing 999_999 sun (0.999... TRX → weight 0).
	statedb.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 999_999)

	tx := makeFreezeV2Tx(9, 1_000_001, corepb.ResourceCode_BANDWIDTH)
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetUnfreezeDelayDays(14)

	if _, err := (&FreezeBalanceV2Actuator{}).Execute(ctx); err != nil {
		t.Fatalf("execute: %v", err)
	}
	// Old: 999_999 sun → 0 TRX. New: 2_000_000 sun → 2 TRX. Delta = 2.
	if got := ctx.DynProps.TotalNetWeight(); got != 2 {
		t.Errorf("total_net_weight: want 2 (boundary crossed twice), got %d", got)
	}
}
