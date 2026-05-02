package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeUnfreezeV2Tx(ownerByte byte, amount int64, resource corepb.ResourceCode) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	uc := &contractpb.UnfreezeBalanceV2Contract{
		OwnerAddress:    owner.Bytes(),
		UnfreezeBalance: amount,
		Resource:        resource,
	}
	any, _ := anypb.New(uc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{Type: corepb.Transaction_Contract_UnfreezeBalanceV2Contract, Parameter: any},
			},
		},
	})
}

func TestUnfreezeV2Validate(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(3)

	// Missing account
	tx := makeUnfreezeV2Tx(3, 100, corepb.ResourceCode_BANDWIDTH)
	act := &UnfreezeBalanceV2Actuator{}
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing account")
	}

	// Create account and freeze some balance
	seedAccount(statedb, owner, 1000)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 500)

	// Insufficient frozen
	tx = makeUnfreezeV2Tx(3, 1000, corepb.ResourceCode_BANDWIDTH)
	ctx = setupContext(t, statedb, tx)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for insufficient frozen")
	}

	// Success
	tx = makeUnfreezeV2Tx(3, 200, corepb.ResourceCode_BANDWIDTH)
	ctx = setupContext(t, statedb, tx)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUnfreezeV2Execute(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(3)
	seedAccount(statedb, owner, 1000)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 500)

	blockTime := int64(100000)
	tx := makeUnfreezeV2Tx(3, 200, corepb.ResourceCode_BANDWIDTH)
	act := &UnfreezeBalanceV2Actuator{}
	dp := state.NewDynamicProperties()
	// Java-tron's mainnet default is 0 days (immediate unfreeze). Set 14
	// explicitly here so the test exercises the delayed-unfreeze path.
	dp.Set("unfreeze_delay_days", 14)
	ctx := &Context{
		State:       statedb,
		DynProps:    dp,
		Tx:          tx,
		BlockTime:   blockTime,
		BlockNumber: 1,
	}

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}

	if got := statedb.GetFrozenV2Amount(owner, corepb.ResourceCode_BANDWIDTH); got != 300 {
		t.Fatalf("frozen BW: want 300, got %d", got)
	}
	acc := statedb.GetAccount(owner)
	unfrozen := acc.UnfrozenV2()
	if len(unfrozen) != 1 {
		t.Fatalf("unfrozen count: want 1, got %d", len(unfrozen))
	}
	if unfrozen[0].UnfreezeAmount != 200 {
		t.Fatalf("unfreeze amount: want 200, got %d", unfrozen[0].UnfreezeAmount)
	}
	expectedExpire := blockTime + 14*86400000
	if unfrozen[0].UnfreezeExpireTime != expectedExpire {
		t.Fatalf("expire time: want %d, got %d", expectedExpire, unfrozen[0].UnfreezeExpireTime)
	}
	if statedb.GetBalance(owner) != 1000 {
		t.Fatalf("balance: want 1000, got %d", statedb.GetBalance(owner))
	}
}

// TestUnfreezeV2_TronPower_Validate: TRON_POWER unfreeze is accepted once StakingV2
// (= AllowNewResourceModel, same proposal #62) is active.
func TestUnfreezeV2_TronPower_Validate(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(4)
	seedAccount(statedb, owner, 1000)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_TRON_POWER, 300)
	act := &UnfreezeBalanceV2Actuator{}

	tx := makeUnfreezeV2Tx(4, 200, corepb.ResourceCode_TRON_POWER)

	// Fork inactive: all V2 operations fail at the StakingV2 gate.
	ctx := setupContext(t, statedb, tx)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: staking v2 not yet enabled")
	}

	// Fork active: TRON_POWER unfreeze is accepted; needs both the top-level
	// StakingV2 gate (unfreeze_delay_days > 0) and AllowNewResourceModel.
	ctx = setupContext(t, statedb, tx)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	ctx.DynProps.SetAllowNewResourceModel(true)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error with fork active: %v", err)
	}
}

// TestUnfreezeV2_Execute_DecrementsTotalWeight: V2 unfreeze must mirror
// java-tron's UnfreezeBalanceV2Actuator.updateTotalResourceWeight by
// decrementing total_{net,energy,tron_power}_weight. Without this, gtron
// retains stale weight after unfreeze and continues to grant staked
// bandwidth that is no longer backed.
func TestUnfreezeV2_Execute_DecrementsTotalWeight(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(5)
	seedAccount(statedb, owner, 100_000_000)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 10_000_000)

	tx := makeUnfreezeV2Tx(5, 4_000_000, corepb.ResourceCode_BANDWIDTH)
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	ctx.DynProps.SetTotalNetWeight(10) // pre-existing weight from prior freeze

	if _, err := (&UnfreezeBalanceV2Actuator{}).Execute(ctx); err != nil {
		t.Fatalf("execute: %v", err)
	}
	// 10_000_000 → 6_000_000 sun → 10 → 6 TRX → delta -4.
	if got := ctx.DynProps.TotalNetWeight(); got != 6 {
		t.Errorf("total_net_weight: want 6 (10 - 4), got %d", got)
	}
}
