package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func makeContractState(ctx *Context, owner, contractAddr tcommon.Address, consumePct int64) {
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:              owner[:],
		ConsumeUserResourcePercent: consumePct,
	})
}

func TestUpdateSettingValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}
	c := &contractpb.UpdateSettingContract{
		OwnerAddress:               owner[:],
		ContractAddress:            contractAddr[:],
		ConsumeUserResourcePercent: 75,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateSettingContract, c, 0)
	act := &UpdateSettingActuator{}

	// Owner doesn't exist
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	// Contract doesn't exist
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent contract")
	}

	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner[:],
	})

	// Valid
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestUpdateSettingNonOwner(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	other := tcommon.Address{0x41, 0x03}
	contractAddr := tcommon.Address{0x41, 0x02}
	c := &contractpb.UpdateSettingContract{
		OwnerAddress:               other[:],
		ContractAddress:            contractAddr[:],
		ConsumeUserResourcePercent: 50,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateSettingContract, c, 0)
	ctx.State.CreateAccount(other, corepb.AccountType_Normal)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner[:],
	})

	act := &UpdateSettingActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: sender is not contract origin")
	}
}

func TestUpdateSettingOutOfRange(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}
	c := &contractpb.UpdateSettingContract{
		OwnerAddress:               owner[:],
		ContractAddress:            contractAddr[:],
		ConsumeUserResourcePercent: 101,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateSettingContract, c, 0)
	makeContractState(ctx, owner, contractAddr, 30)

	act := &UpdateSettingActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: consume_percent > 100")
	}
}

func TestUpdateSettingExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}
	c := &contractpb.UpdateSettingContract{
		OwnerAddress:               owner[:],
		ContractAddress:            contractAddr[:],
		ConsumeUserResourcePercent: 80,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateSettingContract, c, 0)
	makeContractState(ctx, owner, contractAddr, 20)

	act := &UpdateSettingActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}
	got := ctx.State.GetContract(contractAddr)
	if got == nil || got.ConsumeUserResourcePercent != 80 {
		t.Fatalf("consume percent not updated: got %v", got)
	}
}
