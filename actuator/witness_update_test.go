package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestWitnessUpdateValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.WitnessUpdateContract{
		OwnerAddress: owner[:],
		UpdateUrl:    []byte("http://new-url.com"),
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_WitnessUpdateContract, c, 0)
	act := &WitnessUpdateActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-witness")
	}

	ctx.State.PutWitness(owner, "http://old-url.com")
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestWitnessUpdateExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.WitnessUpdateContract{
		OwnerAddress: owner[:],
		UpdateUrl:    []byte("http://updated.com"),
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_WitnessUpdateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.PutWitness(owner, "http://old.com")

	act := &WitnessUpdateActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}
	w := ctx.State.GetWitness(owner)
	if w.URL() != "http://updated.com" {
		t.Fatalf("URL not updated: %s", w.URL())
	}
}

func TestWitnessUpdateEmptyURL(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.WitnessUpdateContract{
		OwnerAddress: owner[:],
		UpdateUrl:    []byte{},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_WitnessUpdateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.PutWitness(owner, "http://old.com")

	act := &WitnessUpdateActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for empty URL")
	}
}
