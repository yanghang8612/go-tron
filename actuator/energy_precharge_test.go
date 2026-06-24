package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// TestAccountEnergyLimitWithFixRatio_PreChargesCallerEnergyUsage pins java-tron's
// pre-VM energy pre-charge (VMActuator.getAccountEnergyLimitWithFixRatio, inside
// `if (allowTvmFreezeV2())`): BEFORE VM.play, java recovers the caller's
// energy_usage to now, sets latestConsumeTimeForEnergy=now, and `increase`s the
// usage by min(leftFrozenEnergy, energyFromFeeLimit), persisting it into the
// VM-visible repository. A contract that reads the caller's OWN energy usage
// mid-VM (the staking-query precompiles) must observe this charged value. go was
// settling the whole bill only AFTER the VM, so the intra-VM read saw the stale,
// un-charged value — the Nile 34,621,377/34,621,401 STRX energy-rental divergence.
func TestAccountEnergyLimitWithFixRatio_PreChargesCallerEnergyUsage(t *testing.T) {
	owner := tcommon.Address{0x41, 0x55, 0x01}
	ctx := newEnergyBillCtx(t, owner)
	ctx.DynProps.SetLatestBlockHeaderNumber(blockNumForEnergyLimit) // energyLimitHardFork active → FixRatio
	ctx.DynProps.SetUnfreezeDelayDays(14)                           // SupportUnfreezeDelay → allowTvmFreezeV2 (pre-charge gate)

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	const frozen = int64(10_000) * params.TRXPrecision
	ctx.State.GetAccount(owner).AddFrozenEnergy(frozen, ctx.BlockTime+10_000_000)
	ctx.DynProps.SetTotalEnergyWeight(10_000)
	ctx.DynProps.SetTotalEnergyCurrentLimit(50_000)
	// Fresh usage (0) + lastConsumeTime=now so recovery is a no-op and the merged
	// pre-charge value equals exactly the charged amount (= the full energy limit).
	ctx.State.SetEnergyUsage(owner, 0)
	ctx.State.SetLatestConsumeTimeForEnergy(owner, ctx.HeadSlot)

	limit := calcAccountEnergyLimit(ctx.State.GetAccount(owner), ctx.DynProps)
	if limit != 50_000 {
		t.Fatalf("test setup: account energy limit = %d, want 50000", limit)
	}
	usageBefore := ctx.State.GetEnergyUsage(owner)

	result := &Result{}
	accountEnergyLimitWithFixRatio(ctx, owner, ctx.Tx.FeeLimit(), 0, result)

	usageAfter := ctx.State.GetEnergyUsage(owner)
	if usageAfter <= usageBefore {
		t.Fatalf("caller energy_usage was not pre-charged before VM: before=%d after=%d "+
			"(java increases it by min(leftFrozen,feelimitEnergy) so intra-VM reads see the charged value)",
			usageBefore, usageAfter)
	}
	// usage 0 + lastConsumeTime=now ⇒ leftFrozenEnergy == limit and feeLimit huge
	// ⇒ merged usage == the full energy limit.
	if usageAfter != limit {
		t.Fatalf("caller energy_usage pre-charge = %d, want full limit %d", usageAfter, limit)
	}
	if got := ctx.State.GetLatestConsumeTimeForEnergy(owner); got != ctx.HeadSlot {
		t.Fatalf("caller latestConsumeTimeForEnergy = %d, want now=%d", got, ctx.HeadSlot)
	}
}

// TestEnergyPreCharge_RoundTripMatchesNoPreChargeBill is the consensus-safety
// invariant: pre-charge → (VM makes no change to the account) → restore(success)
// → PayEnergyBill must leave the final on-chain energy_usage / window /
// latestConsumeTime BYTE-IDENTICAL to the existing no-pre-charge bill path (which
// is chain-validated to 34.6M). Only the intra-VM read changes; the persisted
// state does not. Run for both cancelAllV2 settings (the live Nile path is true).
func TestEnergyPreCharge_RoundTripMatchesNoPreChargeBill(t *testing.T) {
	for _, cancelAllV2 := range []bool{false, true} {
		owner := tcommon.Address{0x41, 0x55, 0x02}
		const billed = int64(7_000_000) // energy the tx ultimately consumes (stake-funded)

		setup := func() *Context {
			ctx := newEnergyBillCtx(t, owner)
			ctx.DynProps.SetLatestBlockHeaderNumber(blockNumForEnergyLimit)
			ctx.DynProps.SetUnfreezeDelayDays(14)
			if cancelAllV2 {
				ctx.DynProps.SetAllowCancelAllUnfreezeV2(true)
			}
			ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
			ctx.State.GetAccount(owner).AddFrozenEnergy(int64(20_000)*params.TRXPrecision, ctx.BlockTime+10_000_000)
			ctx.DynProps.SetTotalEnergyWeight(20_000)
			ctx.DynProps.SetTotalEnergyCurrentLimit(100_000_000)
			// Pre-existing usage with an OLDER lastConsumeTime so recovery (decay)
			// is non-trivial — exercises the recover step in both paths.
			ctx.State.SetEnergyUsage(owner, 3_000_000)
			ctx.State.SetLatestConsumeTimeForEnergy(owner, ctx.HeadSlot-100)
			return ctx
		}

		// Path A — no pre-charge: settle directly (chain-validated behaviour).
		ctxA := setup()
		useEnergyForBill(ctxA, owner, billed, true)
		wantUsage := ctxA.State.GetEnergyUsage(owner)
		wantWindow := ctxA.State.GetAccount(owner).RawEnergyWindowSize()
		wantOpt := ctxA.State.GetAccount(owner).EnergyWindowOptimized()
		wantTime := ctxA.State.GetLatestConsumeTimeForEnergy(owner)

		// Path B — pre-charge → (no VM change) → restore(success) → settle.
		ctxB := setup()
		resultB := &Result{ContractRet: int32(corepb.Transaction_Result_SUCCESS)}
		leftFrozen := availableAccountEnergyForBill(ctxB.State, ctxB.DynProps, owner, ctxB.ResourceTime())
		feelimitEnergy := ctxB.Tx.FeeLimit() / vmEnergyFee(ctxB)
		preChargeEnergyUsage(ctxB, owner, minInt64(leftFrozen, feelimitEnergy), resultB)
		if got := ctxB.State.GetEnergyUsage(owner); got <= 3_000_000 {
			t.Fatalf("cancelAllV2=%v: pre-charge did not raise usage (got %d)", cancelAllV2, got)
		}
		restoreEnergyPreCharges(ctxB, resultB)
		useEnergyForBill(ctxB, owner, billed, true)
		gotUsage := ctxB.State.GetEnergyUsage(owner)
		gotWindow := ctxB.State.GetAccount(owner).RawEnergyWindowSize()
		gotOpt := ctxB.State.GetAccount(owner).EnergyWindowOptimized()
		gotTime := ctxB.State.GetLatestConsumeTimeForEnergy(owner)

		if gotUsage != wantUsage {
			t.Fatalf("cancelAllV2=%v: final energy_usage changed by pre-charge: want %d got %d", cancelAllV2, wantUsage, gotUsage)
		}
		if gotWindow != wantWindow || gotOpt != wantOpt {
			t.Fatalf("cancelAllV2=%v: final energy window changed by pre-charge: want (%d,%v) got (%d,%v)", cancelAllV2, wantWindow, wantOpt, gotWindow, gotOpt)
		}
		if gotTime != wantTime {
			t.Fatalf("cancelAllV2=%v: final latestConsumeTime changed by pre-charge: want %d got %d", cancelAllV2, wantTime, gotTime)
		}
	}
}

// TestEnergyPreCharge_RevertDiscardsCharge verifies the revert path: when the VM
// reverts, java never commits rootRepository and skips resetAccountUsage, so the
// pre-charge is discarded outright — the account returns to its pristine usage /
// window / consume-time, and the subsequent bill settles over the original state.
func TestEnergyPreCharge_RevertDiscardsCharge(t *testing.T) {
	owner := tcommon.Address{0x41, 0x55, 0x03}
	ctx := newEnergyBillCtx(t, owner)
	ctx.DynProps.SetLatestBlockHeaderNumber(blockNumForEnergyLimit)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.GetAccount(owner).AddFrozenEnergy(int64(20_000)*params.TRXPrecision, ctx.BlockTime+10_000_000)
	ctx.DynProps.SetTotalEnergyWeight(20_000)
	ctx.DynProps.SetTotalEnergyCurrentLimit(100_000_000)
	ctx.State.SetEnergyUsage(owner, 3_000_000)
	ctx.State.SetLatestConsumeTimeForEnergy(owner, ctx.HeadSlot-100)

	wantUsage := ctx.State.GetEnergyUsage(owner)
	wantWindow := ctx.State.GetAccount(owner).RawEnergyWindowSize()
	wantOpt := ctx.State.GetAccount(owner).EnergyWindowOptimized()
	wantTime := ctx.State.GetLatestConsumeTimeForEnergy(owner)

	result := &Result{ContractRet: int32(corepb.Transaction_Result_REVERT)}
	leftFrozen := availableAccountEnergyForBill(ctx.State, ctx.DynProps, owner, ctx.ResourceTime())
	feelimitEnergy := ctx.Tx.FeeLimit() / vmEnergyFee(ctx)
	preChargeEnergyUsage(ctx, owner, minInt64(leftFrozen, feelimitEnergy), result)
	restoreEnergyPreCharges(ctx, result)

	if got := ctx.State.GetEnergyUsage(owner); got != wantUsage {
		t.Fatalf("revert: energy_usage not discarded to pristine: want %d got %d", wantUsage, got)
	}
	if got := ctx.State.GetAccount(owner).RawEnergyWindowSize(); got != wantWindow {
		t.Fatalf("revert: window not discarded to pristine: want %d got %d", wantWindow, got)
	}
	if got := ctx.State.GetAccount(owner).EnergyWindowOptimized(); got != wantOpt {
		t.Fatalf("revert: window-optimized not discarded: want %v got %v", wantOpt, got)
	}
	if got := ctx.State.GetLatestConsumeTimeForEnergy(owner); got != wantTime {
		t.Fatalf("revert: latestConsumeTime not discarded to pristine: want %d got %d", wantTime, got)
	}
}

// TestTotalEnergyLimitWithFixRatio_PreChargesOriginAtPercent100 pins java-tron's
// behaviour that getTotalEnergyLimitWithFixRatio runs the allowTvmFreezeV2 origin
// pre-charge block UNCONDITIONALLY — even when consume_user_resource_percent == 100
// (creatorEnergyLimit == 0). java still calls updateUsage(creator) +
// setLatestConsumeTimeForEnergy(now) + increase(creator, ..., 0, now, now), so a
// contract reading the ORIGIN's own energy usage mid-VM sees the recovered value.
// go must do the same instead of returning before the origin pre-charge.
func TestTotalEnergyLimitWithFixRatio_PreChargesOriginAtPercent100(t *testing.T) {
	caller := tcommon.Address{0x41, 0x55, 0x10}
	origin := tcommon.Address{0x41, 0x55, 0x11}
	contractAddr := tcommon.Address{0x41, 0x55, 0x12}
	ctx := newEnergyBillCtx(t, caller)
	ctx.DynProps.SetLatestBlockHeaderNumber(blockNumForEnergyLimit)
	ctx.DynProps.SetUnfreezeDelayDays(14)

	ctx.State.CreateAccount(caller, corepb.AccountType_Normal)
	ctx.State.GetAccount(caller).AddFrozenEnergy(int64(10_000)*params.TRXPrecision, ctx.BlockTime+10_000_000)
	ctx.State.CreateAccount(origin, corepb.AccountType_Normal)
	ctx.State.GetAccount(origin).AddFrozenEnergy(int64(5_000)*params.TRXPrecision, ctx.BlockTime+10_000_000)
	// Origin has pre-existing usage and an OLDER lastConsumeTime than now.
	ctx.State.SetEnergyUsage(origin, 500_000)
	ctx.State.SetLatestConsumeTimeForEnergy(origin, ctx.HeadSlot-50)
	ctx.DynProps.SetTotalEnergyWeight(15_000)
	ctx.DynProps.SetTotalEnergyCurrentLimit(60_000_000)
	// consume_user_resource_percent == 100: caller pays all, origin's share is 0.
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:              origin[:],
		ConsumeUserResourcePercent: 100,
		OriginEnergyLimit:          1_000_000,
	})

	if ctx.State.GetLatestConsumeTimeForEnergy(origin) == ctx.HeadSlot {
		t.Fatal("test setup: origin lastConsumeTime should start older than now")
	}

	result := &Result{}
	if _, err := totalEnergyLimitWithFixRatio(ctx, origin, caller, contractAddr, ctx.Tx.FeeLimit(), 0, result); err != nil {
		t.Fatalf("totalEnergyLimitWithFixRatio returned error: %v", err)
	}

	if got := ctx.State.GetLatestConsumeTimeForEnergy(origin); got != ctx.HeadSlot {
		t.Fatalf("origin lastConsumeTimeForEnergy = %d, want now=%d "+
			"(java pre-charges the origin even at percent==100)", got, ctx.HeadSlot)
	}
}
