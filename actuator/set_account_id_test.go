package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestSetAccountIdValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.SetAccountIdContract{
		OwnerAddress: owner[:],
		AccountId:    []byte("myid"),
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_SetAccountIdContract, c, 0)
	act := &SetAccountIdActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}

	ctx.State.SetAccountId(owner, "existing")
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for already-set id")
	}
}

func TestSetAccountIdExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.SetAccountIdContract{
		OwnerAddress: owner[:],
		AccountId:    []byte("user123"),
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_SetAccountIdContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	act := &SetAccountIdActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}
	if ctx.State.GetAccountId(owner) != "user123" {
		t.Fatal("id not set")
	}
}
