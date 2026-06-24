package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// These tests pin go-tron to java-tron's
// VMActuator.getTotalEnergyLimitWithFixRatio guard: a triggered contract whose
// origin_energy_limit < 0 must be rejected for ALL consume_user_resource_percent
// values, before the percent branches. java throws
// ContractValidateException("originEnergyLimit can't be < 0"), which propagates
// out of processTransaction -> processBlock -> applyBlock and rejects the whole
// block. go must therefore return an error from Execute so applyTransaction
// reverts and rejects the block identically (instead of silently proceeding
// with a negative limit -> chain split).
//
// A negative origin_energy_limit can be persisted only by a CreateSmartContract
// executed BEFORE the energy-limit hard fork (block 4_727_890): the
// `originEnergyLimit > 0` create-time validation is gated behind
// energyLimitHardForkActive in both impls, and both store the raw proto value.
// Such a contract, triggered post-fork with caller != origin, reaches the
// FixRatio energy path tested here.

// newNegativeOriginLimitCtx builds a post-energy-limit-fork TriggerSmartContract
// context where caller != origin, so triggerEnergyLimit reaches the FixRatio
// path, and the stored contract carries the given percent / origin_energy_limit.
func newNegativeOriginLimitCtx(t *testing.T, percent, originLimit int64) *Context {
	t.Helper()
	caller := tcommon.Address{0x41, 0x10}
	origin := tcommon.Address{0x41, 0x20}
	contractAddr := tcommon.Address{0x41, 0x30}

	tsc := &contractpb.TriggerSmartContract{
		OwnerAddress:    caller[:],
		ContractAddress: contractAddr[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_TriggerSmartContract, tsc, 1_000_000_000)
	enableVM(ctx)
	ctx.DynProps.SetLatestBlockHeaderNumber(blockNumForEnergyLimit)
	ctx.DynProps.SetAllowTvmFreeze(true)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	ctx.DynProps.SetTotalEnergyWeight(1_000_000_000_000)
	ctx.DynProps.Set("total_energy_current_limit", 1_000_000_000_000)

	ctx.State.CreateAccount(caller, corepb.AccountType_Normal)
	ctx.State.AddBalance(caller, 100_000_000)
	ctx.State.AddFreezeV2(caller, corepb.ResourceCode_ENERGY, 3_253_937_000_000)

	ctx.State.CreateAccount(origin, corepb.AccountType_Normal)
	ctx.State.AddFreezeV2(origin, corepb.ResourceCode_ENERGY, 20_000_000_000)

	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:              origin[:],
		ContractAddress:            contractAddr[:],
		ConsumeUserResourcePercent: percent,
		OriginEnergyLimit:          originLimit,
	})
	return ctx
}

// percent == 100: the originPercent <= 0 early-return branch. java still runs the
// origin_energy_limit < 0 guard here (it precedes the percent branches), so go
// must reject too rather than taking the early return.
func TestVMActuatorExecute_NegativeOriginLimit_Percent100_Rejected(t *testing.T) {
	ctx := newNegativeOriginLimitCtx(t, 100, -1)
	act := &VMActuator{}
	if _, err := act.Execute(ctx); err == nil {
		t.Fatal("expected Execute to reject contract with origin_energy_limit < 0 (percent=100), got nil error")
	}
}

// percent == 0: the userPercent <= 0 branch (min(originEnergyLeft, originLimit)).
func TestVMActuatorExecute_NegativeOriginLimit_Percent0_Rejected(t *testing.T) {
	ctx := newNegativeOriginLimitCtx(t, 0, -1)
	act := &VMActuator{}
	if _, err := act.Execute(ctx); err == nil {
		t.Fatal("expected Execute to reject contract with origin_energy_limit < 0 (percent=0), got nil error")
	}
}

// 0 < percent < 100: the proportional branch.
func TestVMActuatorExecute_NegativeOriginLimit_Percent50_Rejected(t *testing.T) {
	ctx := newNegativeOriginLimitCtx(t, 50, -1)
	act := &VMActuator{}
	if _, err := act.Execute(ctx); err == nil {
		t.Fatal("expected Execute to reject contract with origin_energy_limit < 0 (percent=50), got nil error")
	}
}

// Guard against over-rejection: origin_energy_limit == 0 is the proto default,
// which contractOriginEnergyLimit / java getOriginEnergyLimit map to
// creatorDefaultEnergyLimit (positive). The guard is `< 0`, NOT `<= 0` (the
// create-time check is `<= 0`, but the trigger-time check must allow 0), so a
// zero limit must NOT be rejected at trigger time.
func TestVMActuatorExecute_ZeroOriginLimit_NotRejected(t *testing.T) {
	ctx := newNegativeOriginLimitCtx(t, 100, 0)
	act := &VMActuator{}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("origin_energy_limit == 0 must not be rejected (maps to default), got %v", err)
	}
}
