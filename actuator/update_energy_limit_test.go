package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestUpdateEnergyLimitValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x11}
	contractAddr := tcommon.Address{0x41, 0x12}
	c := &contractpb.UpdateEnergyLimitContract{
		OwnerAddress:      owner[:],
		ContractAddress:   contractAddr[:],
		OriginEnergyLimit: 5_000_000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateEnergyLimitContract, c, 0)
	act := &UpdateEnergyLimitActuator{}

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
		OriginAddress:     owner[:],
		OriginEnergyLimit: 1_000_000,
	})

	// Valid
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestUpdateEnergyLimitNonOwner(t *testing.T) {
	owner := tcommon.Address{0x41, 0x11}
	other := tcommon.Address{0x41, 0x13}
	contractAddr := tcommon.Address{0x41, 0x12}
	c := &contractpb.UpdateEnergyLimitContract{
		OwnerAddress:      other[:],
		ContractAddress:   contractAddr[:],
		OriginEnergyLimit: 5_000_000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateEnergyLimitContract, c, 0)
	ctx.State.CreateAccount(other, corepb.AccountType_Normal)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner[:],
	})

	act := &UpdateEnergyLimitActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: sender is not contract origin")
	}
}

func TestUpdateEnergyLimitZeroRejected(t *testing.T) {
	owner := tcommon.Address{0x41, 0x11}
	contractAddr := tcommon.Address{0x41, 0x12}
	c := &contractpb.UpdateEnergyLimitContract{
		OwnerAddress:      owner[:],
		ContractAddress:   contractAddr[:],
		OriginEnergyLimit: 0,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateEnergyLimitContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner[:],
	})

	act := &UpdateEnergyLimitActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: origin_energy_limit must be > 0")
	}
}

func TestUpdateEnergyLimitExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x11}
	contractAddr := tcommon.Address{0x41, 0x12}
	c := &contractpb.UpdateEnergyLimitContract{
		OwnerAddress:      owner[:],
		ContractAddress:   contractAddr[:],
		OriginEnergyLimit: 8_000_000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateEnergyLimitContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:     owner[:],
		OriginEnergyLimit: 1_000_000,
	})

	act := &UpdateEnergyLimitActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}
	got := ctx.State.GetContract(contractAddr)
	if got == nil || got.OriginEnergyLimit != 8_000_000 {
		t.Fatalf("origin_energy_limit not updated: got %v", got)
	}
}
