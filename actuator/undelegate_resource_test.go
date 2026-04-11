package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestUnDelegateResourceValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.UnDelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         500000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UnDelegateResourceContract, c, 0)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	dr := &rawdb.DelegatedResource{
		From: owner, To: receiver,
		FrozenBalanceForBandwidth: 1000000,
	}
	rawdb.WriteDelegatedResource(db, owner, receiver, dr)

	act := &UnDelegateResourceActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestUnDelegateResourceLocked(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.UnDelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         500000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UnDelegateResourceContract, c, 0)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.BlockTime = 1000
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	dr := &rawdb.DelegatedResource{
		From: owner, To: receiver,
		FrozenBalanceForBandwidth: 1000000,
		ExpireTimeForBandwidth:    999999,
	}
	rawdb.WriteDelegatedResource(db, owner, receiver, dr)

	act := &UnDelegateResourceActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for locked delegation")
	}
}

func TestUnDelegateResourceExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.UnDelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         1000000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UnDelegateResourceContract, c, 0)
	ctx.DynProps.SetAllowDelegateResource(true)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.CreateAccount(receiver, corepb.AccountType_Normal)

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	dr := &rawdb.DelegatedResource{
		From: owner, To: receiver,
		FrozenBalanceForBandwidth: 1000000,
	}
	rawdb.WriteDelegatedResource(db, owner, receiver, dr)
	rawdb.WriteDelegationIndex(db, owner, []tcommon.Address{receiver})

	act := &UnDelegateResourceActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}

	// Delegation fully removed
	if rawdb.ReadDelegatedResource(db, owner, receiver) != nil {
		t.Fatal("delegation should be removed")
	}
	// Owner's frozen restored
	if ctx.State.GetFrozenV2Amount(owner, corepb.ResourceCode_BANDWIDTH) != 1000000 {
		t.Fatal("frozen balance not restored")
	}
}
