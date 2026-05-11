package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeWithdrawBalanceTx(ownerByte byte) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	wc := &contractpb.WithdrawBalanceContract{OwnerAddress: owner.Bytes()}
	any, _ := anypb.New(wc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{Type: corepb.Transaction_Contract_WithdrawBalanceContract, Parameter: any},
			},
		},
	})
}

func TestWithdrawBalanceValidate(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(40)

	// Missing account
	tx := makeWithdrawBalanceTx(40)
	act := &WithdrawBalanceActuator{}
	ctx := &Context{State: statedb, Tx: tx, BlockTime: 200000, PrevBlockTime: 200000}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing account")
	}

	// Create account with no allowance
	seedAccount(statedb, owner, 0)
	statedb.SetIsWitness(owner, true)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for zero allowance")
	}

	// Set allowance and recent withdraw time
	statedb.SetAllowance(owner, 16000000)
	statedb.SetLatestWithdrawTime(owner, 100000)

	// Too recent withdraw
	ctx = &Context{State: statedb, Tx: tx, BlockTime: 100000 + 86400000/2, PrevBlockTime: 100000 + 86400000/2}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for too recent withdraw")
	}

	// Success after cooldown
	ctx = &Context{State: statedb, Tx: tx, BlockTime: 100000 + 86400000 + 1, PrevBlockTime: 100000 + 86400000 + 1}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWithdrawBalanceExecute(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(40)

	seedAccount(statedb, owner, 500)
	statedb.SetIsWitness(owner, true)
	statedb.SetAllowance(owner, 16000000)
	statedb.SetLatestWithdrawTime(owner, 0)

	tx := makeWithdrawBalanceTx(40)
	act := &WithdrawBalanceActuator{}
	blockTime := int64(86400000 + 1)
	ctx := &Context{State: statedb, Tx: tx, BlockTime: blockTime, PrevBlockTime: blockTime}

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}

	if statedb.GetBalance(owner) != 500+16000000 {
		t.Fatalf("balance: want %d, got %d", 500+16000000, statedb.GetBalance(owner))
	}
	if statedb.GetAllowance(owner) != 0 {
		t.Fatalf("allowance: want 0, got %d", statedb.GetAllowance(owner))
	}
	if statedb.GetLatestWithdrawTime(owner) != blockTime {
		t.Fatalf("latest_withdraw_time: want %d, got %d", blockTime, statedb.GetLatestWithdrawTime(owner))
	}
}
