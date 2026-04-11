package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestUpdateBrokerageValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.UpdateBrokerageContract{
		OwnerAddress: owner[:],
		Brokerage:    30,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateBrokerageContract, c, 0)
	ctx.DynProps.SetAllowChangeDelegation(true)
	act := &UpdateBrokerageActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-witness")
	}

	ctx.State.PutWitness(owner, "http://w.com")
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestUpdateBrokerageOutOfRange(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.UpdateBrokerageContract{
		OwnerAddress: owner[:],
		Brokerage:    101,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateBrokerageContract, c, 0)
	ctx.DynProps.SetAllowChangeDelegation(true)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.PutWitness(owner, "http://w.com")

	act := &UpdateBrokerageActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for brokerage > 100")
	}
}

func TestUpdateBrokerageExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.UpdateBrokerageContract{
		OwnerAddress: owner[:],
		Brokerage:    50,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateBrokerageContract, c, 0)
	ctx.DynProps.SetAllowChangeDelegation(true)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.PutWitness(owner, "http://w.com")

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	act := &UpdateBrokerageActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}
	if got := rawdb.ReadWitnessBrokerage(db, owner); got != 50 {
		t.Fatalf("expected brokerage 50, got %d", got)
	}
}
