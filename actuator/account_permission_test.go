package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestAccountPermissionValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.AccountPermissionUpdateContract{
		OwnerAddress: owner[:],
		Owner: &corepb.Permission{
			Threshold: 1,
			Keys: []*corepb.Key{
				{Address: owner[:], Weight: 1},
			},
		},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountPermissionUpdateContract, c, 0)
	ctx.DynProps.SetAllowMultiSign(true)
	act := &AccountPermissionUpdateActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestAccountPermissionNoOwner(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.AccountPermissionUpdateContract{
		OwnerAddress: owner[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountPermissionUpdateContract, c, 0)
	ctx.DynProps.SetAllowMultiSign(true)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	act := &AccountPermissionUpdateActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing owner permission")
	}
}

func TestAccountPermissionThresholdExceedsWeight(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.AccountPermissionUpdateContract{
		OwnerAddress: owner[:],
		Owner: &corepb.Permission{
			Threshold: 10,
			Keys: []*corepb.Key{
				{Address: owner[:], Weight: 1},
			},
		},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountPermissionUpdateContract, c, 0)
	ctx.DynProps.SetAllowMultiSign(true)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	act := &AccountPermissionUpdateActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for threshold > weight")
	}
}

func TestAccountPermissionExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	key2 := tcommon.Address{0x41, 0x02}
	c := &contractpb.AccountPermissionUpdateContract{
		OwnerAddress: owner[:],
		Owner: &corepb.Permission{
			Type:      corepb.Permission_Owner,
			Threshold: 2,
			Keys: []*corepb.Key{
				{Address: owner[:], Weight: 1},
				{Address: key2[:], Weight: 1},
			},
		},
		Actives: []*corepb.Permission{
			{
				Type:      corepb.Permission_Active,
				Id:        2,
				Threshold: 1,
				Keys: []*corepb.Key{
					{Address: owner[:], Weight: 1},
				},
			},
		},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountPermissionUpdateContract, c, 0)
	ctx.DynProps.SetAllowMultiSign(true)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	act := &AccountPermissionUpdateActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}
	acc := ctx.State.GetAccount(owner)
	if acc.OwnerPermission() == nil {
		t.Fatal("owner permission not set")
	}
	if acc.OwnerPermission().Threshold != 2 {
		t.Fatalf("expected threshold 2, got %d", acc.OwnerPermission().Threshold)
	}
}
