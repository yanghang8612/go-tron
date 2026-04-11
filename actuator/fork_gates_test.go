package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// ---------- helpers --------------------------------------------------------
//
// Each test creates two Contexts: one with the fork flag OFF (all flags default
// to 0 in NewDynamicProperties) and one with the flag ON (setter called).
// For the flag-OFF case we assert the exact fork error message.
// For the flag-ON case we assert the error is NOT the fork error — the
// actuator may still fail on other preconditions (no account etc.) which is fine.

func dynOff(t *testing.T) *state.DynamicProperties {
	t.Helper()
	return state.NewDynamicProperties() // all flags default to 0
}

func dynOn(t *testing.T, setter func(*state.DynamicProperties)) *state.DynamicProperties {
	t.Helper()
	dp := state.NewDynamicProperties()
	setter(dp)
	return dp
}

func assertForkError(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error %q but got nil", want)
	}
	if err.Error() != want {
		t.Fatalf("expected error %q, got %q", want, err.Error())
	}
}

func assertNotForkError(t *testing.T, err error, forkMsg string) {
	t.Helper()
	if err != nil && err.Error() == forkMsg {
		t.Fatalf("fork gate should be open but got fork error: %q", forkMsg)
	}
	// err may be non-nil for other reasons (no account, bad params, etc.) — that is fine.
}

// ---------- 1. AccountPermissionUpdateActuator / AllowMultiSign -----------

func TestForkGate_AccountPermission_FlagOff(t *testing.T) {
	owner := makeTestAddr(1)
	c := &contractpb.AccountPermissionUpdateContract{
		OwnerAddress: owner[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountPermissionUpdateContract, c, 0)
	ctx.DynProps = dynOff(t) // flag off (already default, but explicit)

	act := &AccountPermissionUpdateActuator{}
	assertForkError(t, act.Validate(ctx), "multi-sign not yet enabled")
}

func TestForkGate_AccountPermission_FlagOn(t *testing.T) {
	owner := makeTestAddr(1)
	c := &contractpb.AccountPermissionUpdateContract{
		OwnerAddress: owner[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountPermissionUpdateContract, c, 0)
	ctx.DynProps = dynOn(t, func(dp *state.DynamicProperties) { dp.SetAllowMultiSign(true) })

	act := &AccountPermissionUpdateActuator{}
	assertNotForkError(t, act.Validate(ctx), "multi-sign not yet enabled")
}

// ---------- 2. DelegateResourceActuator / AllowDelegateResource -----------

func TestForkGate_DelegateResource_FlagOff(t *testing.T) {
	owner := makeTestAddr(1)
	c := &contractpb.DelegateResourceContract{
		OwnerAddress: owner[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_DelegateResourceContract, c, 0)
	ctx.DynProps = dynOff(t)

	act := &DelegateResourceActuator{}
	assertForkError(t, act.Validate(ctx), "resource delegation not yet enabled")
}

func TestForkGate_DelegateResource_FlagOn(t *testing.T) {
	owner := makeTestAddr(1)
	c := &contractpb.DelegateResourceContract{
		OwnerAddress: owner[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_DelegateResourceContract, c, 0)
	ctx.DynProps = dynOn(t, func(dp *state.DynamicProperties) { dp.SetAllowDelegateResource(true) })

	act := &DelegateResourceActuator{}
	assertNotForkError(t, act.Validate(ctx), "resource delegation not yet enabled")
}

// ---------- 3. UnDelegateResourceActuator / AllowDelegateResource ---------

func TestForkGate_UnDelegateResource_FlagOff(t *testing.T) {
	owner := makeTestAddr(1)
	c := &contractpb.UnDelegateResourceContract{
		OwnerAddress: owner[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UnDelegateResourceContract, c, 0)
	ctx.DynProps = dynOff(t)

	act := &UnDelegateResourceActuator{}
	assertForkError(t, act.Validate(ctx), "resource delegation not yet enabled")
}

func TestForkGate_UnDelegateResource_FlagOn(t *testing.T) {
	owner := makeTestAddr(1)
	c := &contractpb.UnDelegateResourceContract{
		OwnerAddress: owner[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UnDelegateResourceContract, c, 0)
	ctx.DynProps = dynOn(t, func(dp *state.DynamicProperties) { dp.SetAllowDelegateResource(true) })

	act := &UnDelegateResourceActuator{}
	assertNotForkError(t, act.Validate(ctx), "resource delegation not yet enabled")
}

// ---------- 4. FreezeBalanceV2Actuator / AllowStakingV2 -------------------

func TestForkGate_FreezeBalanceV2_FlagOff(t *testing.T) {
	owner := makeTestAddr(1)
	c := &contractpb.FreezeBalanceV2Contract{
		OwnerAddress:  owner[:],
		FrozenBalance: 1000,
		Resource:      corepb.ResourceCode_BANDWIDTH,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_FreezeBalanceV2Contract, c, 0)
	ctx.DynProps = dynOff(t)

	act := &FreezeBalanceV2Actuator{}
	assertForkError(t, act.Validate(ctx), "staking v2 not yet enabled")
}

func TestForkGate_FreezeBalanceV2_FlagOn(t *testing.T) {
	owner := makeTestAddr(1)
	c := &contractpb.FreezeBalanceV2Contract{
		OwnerAddress:  owner[:],
		FrozenBalance: 1000,
		Resource:      corepb.ResourceCode_BANDWIDTH,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_FreezeBalanceV2Contract, c, 0)
	ctx.DynProps = dynOn(t, func(dp *state.DynamicProperties) { dp.SetAllowStakingV2(true) })

	act := &FreezeBalanceV2Actuator{}
	assertNotForkError(t, act.Validate(ctx), "staking v2 not yet enabled")
}

// ---------- 5. UnfreezeBalanceV2Actuator / AllowStakingV2 -----------------

func TestForkGate_UnfreezeBalanceV2_FlagOff(t *testing.T) {
	owner := makeTestAddr(1)
	c := &contractpb.UnfreezeBalanceV2Contract{
		OwnerAddress:    owner[:],
		UnfreezeBalance: 1000,
		Resource:        corepb.ResourceCode_BANDWIDTH,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UnfreezeBalanceV2Contract, c, 0)
	ctx.DynProps = dynOff(t)

	act := &UnfreezeBalanceV2Actuator{}
	assertForkError(t, act.Validate(ctx), "staking v2 not yet enabled")
}

func TestForkGate_UnfreezeBalanceV2_FlagOn(t *testing.T) {
	owner := makeTestAddr(1)
	c := &contractpb.UnfreezeBalanceV2Contract{
		OwnerAddress:    owner[:],
		UnfreezeBalance: 1000,
		Resource:        corepb.ResourceCode_BANDWIDTH,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UnfreezeBalanceV2Contract, c, 0)
	ctx.DynProps = dynOn(t, func(dp *state.DynamicProperties) { dp.SetAllowStakingV2(true) })

	act := &UnfreezeBalanceV2Actuator{}
	assertNotForkError(t, act.Validate(ctx), "staking v2 not yet enabled")
}

// ---------- 6. WithdrawExpireUnfreezeActuator / AllowStakingV2 ------------

func TestForkGate_WithdrawExpireUnfreeze_FlagOff(t *testing.T) {
	owner := makeTestAddr(1)
	c := &contractpb.WithdrawExpireUnfreezeContract{
		OwnerAddress: owner[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_WithdrawExpireUnfreezeContract, c, 0)
	ctx.DynProps = dynOff(t)

	act := &WithdrawExpireUnfreezeActuator{}
	assertForkError(t, act.Validate(ctx), "staking v2 not yet enabled")
}

func TestForkGate_WithdrawExpireUnfreeze_FlagOn(t *testing.T) {
	owner := makeTestAddr(1)
	c := &contractpb.WithdrawExpireUnfreezeContract{
		OwnerAddress: owner[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_WithdrawExpireUnfreezeContract, c, 0)
	ctx.DynProps = dynOn(t, func(dp *state.DynamicProperties) { dp.SetAllowStakingV2(true) })

	act := &WithdrawExpireUnfreezeActuator{}
	assertNotForkError(t, act.Validate(ctx), "staking v2 not yet enabled")
}

// ---------- 7. CancelAllUnfreezeV2Actuator / AllowStakingV2 ---------------

func TestForkGate_CancelAllUnfreezeV2_FlagOff(t *testing.T) {
	owner := makeTestAddr(1)
	c := &contractpb.CancelAllUnfreezeV2Contract{
		OwnerAddress: owner[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_CancelAllUnfreezeV2Contract, c, 0)
	ctx.DynProps = dynOff(t)

	act := &CancelAllUnfreezeV2Actuator{}
	assertForkError(t, act.Validate(ctx), "staking v2 not yet enabled")
}

func TestForkGate_CancelAllUnfreezeV2_FlagOn(t *testing.T) {
	owner := makeTestAddr(1)
	c := &contractpb.CancelAllUnfreezeV2Contract{
		OwnerAddress: owner[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_CancelAllUnfreezeV2Contract, c, 0)
	ctx.DynProps = dynOn(t, func(dp *state.DynamicProperties) { dp.SetAllowStakingV2(true) })

	act := &CancelAllUnfreezeV2Actuator{}
	assertNotForkError(t, act.Validate(ctx), "staking v2 not yet enabled")
}

// ---------- 8. MarketSellAssetActuator / AllowMarketTransaction -----------

func TestForkGate_MarketSellAsset_FlagOff(t *testing.T) {
	owner := makeTestAddr(1)
	c := &contractpb.MarketSellAssetContract{
		OwnerAddress: owner[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_MarketSellAssetContract, c, 0)
	ctx.DynProps = dynOff(t)

	act := &MarketSellAssetActuator{}
	assertForkError(t, act.Validate(ctx), "market transactions not yet enabled")
}

func TestForkGate_MarketSellAsset_FlagOn(t *testing.T) {
	owner := makeTestAddr(1)
	c := &contractpb.MarketSellAssetContract{
		OwnerAddress: owner[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_MarketSellAssetContract, c, 0)
	ctx.DynProps = dynOn(t, func(dp *state.DynamicProperties) { dp.SetAllowMarketTransaction(true) })

	act := &MarketSellAssetActuator{}
	assertNotForkError(t, act.Validate(ctx), "market transactions not yet enabled")
}

// ---------- 9. MarketCancelOrderActuator / AllowMarketTransaction ---------

func TestForkGate_MarketCancelOrder_FlagOff(t *testing.T) {
	owner := makeTestAddr(1)
	c := &contractpb.MarketCancelOrderContract{
		OwnerAddress: owner[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_MarketCancelOrderContract, c, 0)
	ctx.DynProps = dynOff(t)

	act := &MarketCancelOrderActuator{}
	assertForkError(t, act.Validate(ctx), "market transactions not yet enabled")
}

func TestForkGate_MarketCancelOrder_FlagOn(t *testing.T) {
	owner := makeTestAddr(1)
	c := &contractpb.MarketCancelOrderContract{
		OwnerAddress: owner[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_MarketCancelOrderContract, c, 0)
	ctx.DynProps = dynOn(t, func(dp *state.DynamicProperties) { dp.SetAllowMarketTransaction(true) })

	act := &MarketCancelOrderActuator{}
	assertNotForkError(t, act.Validate(ctx), "market transactions not yet enabled")
}

// ---------- 10. UpdateBrokerageActuator / AllowChangeDelegation -----------

func TestForkGate_UpdateBrokerage_FlagOff(t *testing.T) {
	owner := makeTestAddr(1)
	c := &contractpb.UpdateBrokerageContract{
		OwnerAddress: owner[:],
		Brokerage:    20,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateBrokerageContract, c, 0)
	ctx.DynProps = dynOff(t)

	act := &UpdateBrokerageActuator{}
	assertForkError(t, act.Validate(ctx), "brokerage update not yet enabled")
}

func TestForkGate_UpdateBrokerage_FlagOn(t *testing.T) {
	owner := makeTestAddr(1)
	c := &contractpb.UpdateBrokerageContract{
		OwnerAddress: owner[:],
		Brokerage:    20,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateBrokerageContract, c, 0)
	ctx.DynProps = dynOn(t, func(dp *state.DynamicProperties) { dp.SetAllowChangeDelegation(true) })

	act := &UpdateBrokerageActuator{}
	assertNotForkError(t, act.Validate(ctx), "brokerage update not yet enabled")
}
