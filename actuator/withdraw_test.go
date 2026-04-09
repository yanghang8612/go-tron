package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeWithdrawBalanceTx(owner common.Address) *types.Transaction {
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
	db := rawdb.NewMemoryDatabase()
	owner := makeTestAddr(40)

	tx := makeWithdrawBalanceTx(owner)
	act := &WithdrawBalanceActuator{}
	ctx := &Context{DB: db, Tx: tx, BlockTime: 200000}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing account")
	}

	acc := types.NewAccount(owner, corepb.AccountType_Normal)
	acc.SetIsWitness(true)
	rawdb.WriteAccount(db, owner, acc)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for zero allowance")
	}

	acc.SetAllowance(16000000)
	acc.SetLatestWithdrawTime(100000)
	rawdb.WriteAccount(db, owner, acc)
	ctx = &Context{DB: db, Tx: tx, BlockTime: 100000 + 86400000/2}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for too recent withdraw")
	}

	ctx = &Context{DB: db, Tx: tx, BlockTime: 100000 + 86400000 + 1}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWithdrawBalanceExecute(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := makeTestAddr(40)

	acc := types.NewAccount(owner, corepb.AccountType_Normal)
	acc.SetIsWitness(true)
	acc.SetBalance(500)
	acc.SetAllowance(16000000)
	acc.SetLatestWithdrawTime(0)
	rawdb.WriteAccount(db, owner, acc)

	tx := makeWithdrawBalanceTx(owner)
	act := &WithdrawBalanceActuator{}
	blockTime := int64(86400000 + 1)
	ctx := &Context{DB: db, Tx: tx, BlockTime: blockTime}

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}

	updated := rawdb.ReadAccount(db, owner)
	if updated.Balance() != 500+16000000 {
		t.Fatalf("balance: want %d, got %d", 500+16000000, updated.Balance())
	}
	if updated.Allowance() != 0 {
		t.Fatalf("allowance: want 0, got %d", updated.Allowance())
	}
	if updated.LatestWithdrawTime() != blockTime {
		t.Fatalf("latest_withdraw_time: want %d, got %d", blockTime, updated.LatestWithdrawTime())
	}
}
