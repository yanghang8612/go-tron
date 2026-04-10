package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestClearABIValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x21}
	contractAddr := tcommon.Address{0x41, 0x22}
	c := &contractpb.ClearABIContract{
		OwnerAddress:    owner[:],
		ContractAddress: contractAddr[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ClearABIContract, c, 0)
	act := &ClearABIActuator{}

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
		Abi:           &contractpb.SmartContract_ABI{},
	})

	// Valid
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestClearABINonOwner(t *testing.T) {
	owner := tcommon.Address{0x41, 0x21}
	other := tcommon.Address{0x41, 0x23}
	contractAddr := tcommon.Address{0x41, 0x22}
	c := &contractpb.ClearABIContract{
		OwnerAddress:    other[:],
		ContractAddress: contractAddr[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ClearABIContract, c, 0)
	ctx.State.CreateAccount(other, corepb.AccountType_Normal)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner[:],
	})

	act := &ClearABIActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: sender is not contract origin")
	}
}

func TestClearABIExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x21}
	contractAddr := tcommon.Address{0x41, 0x22}
	c := &contractpb.ClearABIContract{
		OwnerAddress:    owner[:],
		ContractAddress: contractAddr[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ClearABIContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner[:],
		Abi:           &contractpb.SmartContract_ABI{},
	})

	act := &ClearABIActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}
	got := ctx.State.GetContract(contractAddr)
	if got == nil {
		t.Fatal("contract deleted unexpectedly")
	}
	if got.Abi != nil {
		t.Fatal("ABI not cleared")
	}
}
