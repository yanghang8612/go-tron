package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestDelegateResourceValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.DelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         1000000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_DelegateResourceContract, c, 0)
	ctx.DynProps.SetAllowDelegateResource(true)
	act := &DelegateResourceActuator{}

	// Accounts don't exist
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.CreateAccount(receiver, corepb.AccountType_Normal)
	ctx.State.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 5000000)

	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestDelegateResourceSelfDelegation(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.DelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: owner[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         1000000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_DelegateResourceContract, c, 0)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	act := &DelegateResourceActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for self-delegation")
	}
}

func TestDelegateResourceExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.DelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         1000000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_DelegateResourceContract, c, 0)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.CreateAccount(receiver, corepb.AccountType_Normal)
	ctx.State.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 5000000)

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db

	act := &DelegateResourceActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}

	// Owner's frozen reduced
	if ctx.State.GetFrozenV2Amount(owner, corepb.ResourceCode_BANDWIDTH) != 4000000 {
		t.Fatalf("owner frozen not reduced")
	}

	// Delegation record
	dr := rawdb.ReadDelegatedResource(db, owner, receiver)
	if dr == nil || dr.FrozenBalanceForBandwidth != 1000000 {
		t.Fatalf("delegation record wrong: %+v", dr)
	}
}
