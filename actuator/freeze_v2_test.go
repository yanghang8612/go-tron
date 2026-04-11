package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeFreezeV2Tx(ownerByte byte, amount int64, resource corepb.ResourceCode) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	fc := &contractpb.FreezeBalanceV2Contract{
		OwnerAddress:  owner.Bytes(),
		FrozenBalance: amount,
		Resource:      resource,
	}
	any, _ := anypb.New(fc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{Type: corepb.Transaction_Contract_FreezeBalanceV2Contract, Parameter: any},
			},
		},
	})
}

func TestFreezeV2Validate(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)

	// Missing account
	tx := makeFreezeV2Tx(1, 100, corepb.ResourceCode_BANDWIDTH)
	act := &FreezeBalanceV2Actuator{}
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowStakingV2(true)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing account")
	}

	// Create account with balance
	seedAccount(statedb, owner, 1000)

	// Zero amount
	tx = makeFreezeV2Tx(1, 0, corepb.ResourceCode_BANDWIDTH)
	ctx = setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowStakingV2(true)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for zero amount")
	}

	// Insufficient balance
	tx = makeFreezeV2Tx(1, 5000, corepb.ResourceCode_BANDWIDTH)
	ctx = setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowStakingV2(true)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for insufficient balance")
	}

	// Success
	tx = makeFreezeV2Tx(1, 500, corepb.ResourceCode_BANDWIDTH)
	ctx = setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowStakingV2(true)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFreezeV2Execute(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 1000)

	tx := makeFreezeV2Tx(1, 500, corepb.ResourceCode_BANDWIDTH)
	act := &FreezeBalanceV2Actuator{}
	ctx := setupContext(t, statedb, tx)
	ctx.DynProps.SetAllowStakingV2(true)

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}

	if statedb.GetBalance(owner) != 500 {
		t.Fatalf("balance: want 500, got %d", statedb.GetBalance(owner))
	}
	if got := statedb.GetFrozenV2Amount(owner, corepb.ResourceCode_BANDWIDTH); got != 500 {
		t.Fatalf("frozen BW: want 500, got %d", got)
	}
}
