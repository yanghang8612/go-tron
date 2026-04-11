package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeWithdrawExpireUnfreezeTx(ownerByte byte) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	wc := &contractpb.WithdrawExpireUnfreezeContract{OwnerAddress: owner.Bytes()}
	any, _ := anypb.New(wc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{Type: corepb.Transaction_Contract_WithdrawExpireUnfreezeContract, Parameter: any},
			},
		},
	})
}

func TestWithdrawExpireUnfreezeValidate(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(50)

	// Missing account
	tx := makeWithdrawExpireUnfreezeTx(50)
	act := &WithdrawExpireUnfreezeActuator{}
	dp := state.NewDynamicProperties()
	dp.SetAllowStakingV2(true)
	ctx := &Context{State: statedb, DynProps: dp, Tx: tx, BlockTime: 1000000}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing account")
	}

	// Create account with no unfrozen entries
	seedAccount(statedb, owner, 0)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for no unfrozen entries")
	}

	// Add unfreeze entry not yet expired
	statedb.AddUnfreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 100, 2000000) // not expired
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for no expired entries")
	}

	// Add expired entry
	statedb.AddUnfreezeV2(owner, corepb.ResourceCode_ENERGY, 200, 500000) // expired
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWithdrawExpireUnfreezeExecute(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(50)

	seedAccount(statedb, owner, 1000)
	statedb.AddUnfreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 100, 500000)  // expired
	statedb.AddUnfreezeV2(owner, corepb.ResourceCode_ENERGY, 200, 800000)     // expired
	statedb.AddUnfreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 300, 2000000) // not expired

	tx := makeWithdrawExpireUnfreezeTx(50)
	act := &WithdrawExpireUnfreezeActuator{}
	ctx := &Context{State: statedb, Tx: tx, BlockTime: 1000000}

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}

	if statedb.GetBalance(owner) != 1300 {
		t.Fatalf("balance: want 1300, got %d", statedb.GetBalance(owner))
	}
	acc := statedb.GetAccount(owner)
	if len(acc.UnfrozenV2()) != 1 {
		t.Fatalf("remaining unfrozen: want 1, got %d", len(acc.UnfrozenV2()))
	}
}
