package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestCancelAllUnfreezeV2Validate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.CancelAllUnfreezeV2Contract{
		OwnerAddress: owner[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_CancelAllUnfreezeV2Contract, c, 0)
	ctx.DynProps.SetAllowCancelAllUnfreezeV2(true)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	act := &CancelAllUnfreezeV2Actuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for no pending unfreezes")
	}

	ctx.State.AddUnfreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 1000000, 999999)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestCancelAllUnfreezeV2Execute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.CancelAllUnfreezeV2Contract{
		OwnerAddress: owner[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_CancelAllUnfreezeV2Contract, c, 0)
	ctx.DynProps.SetAllowCancelAllUnfreezeV2(true)
	ctx.DynProps.SetUnfreezeDelayDays(14)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 0) // ensure entry exists
	ctx.State.AddUnfreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 1000000, 999999)
	ctx.State.AddUnfreezeV2(owner, corepb.ResourceCode_ENERGY, 500000, 999999)

	act := &CancelAllUnfreezeV2Actuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}

	if ctx.State.UnfreezeV2Count(owner) != 0 {
		t.Fatal("unfreeze queue not cleared")
	}
	if ctx.State.GetFrozenV2Amount(owner, corepb.ResourceCode_BANDWIDTH) != 1000000 {
		t.Fatalf("bandwidth not re-frozen: %d", ctx.State.GetFrozenV2Amount(owner, corepb.ResourceCode_BANDWIDTH))
	}
	if ctx.State.GetFrozenV2Amount(owner, corepb.ResourceCode_ENERGY) != 500000 {
		t.Fatalf("energy not re-frozen: %d", ctx.State.GetFrozenV2Amount(owner, corepb.ResourceCode_ENERGY))
	}
}
