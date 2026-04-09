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

func makeWithdrawExpireUnfreezeTx(owner common.Address) *types.Transaction {
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
	db := rawdb.NewMemoryDatabase()
	owner := makeTestAddr(50)

	tx := makeWithdrawExpireUnfreezeTx(owner)
	act := &WithdrawExpireUnfreezeActuator{}
	ctx := &Context{DB: db, Tx: tx, BlockTime: 1000000}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing account")
	}

	acc := types.NewAccount(owner, corepb.AccountType_Normal)
	rawdb.WriteAccount(db, owner, acc)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for no unfrozen entries")
	}

	acc.AddUnfreezeV2(corepb.ResourceCode_BANDWIDTH, 100, 2000000) // not expired
	rawdb.WriteAccount(db, owner, acc)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for no expired entries")
	}

	acc.AddUnfreezeV2(corepb.ResourceCode_ENERGY, 200, 500000) // expired
	rawdb.WriteAccount(db, owner, acc)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWithdrawExpireUnfreezeExecute(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := makeTestAddr(50)

	acc := types.NewAccount(owner, corepb.AccountType_Normal)
	acc.SetBalance(1000)
	acc.AddUnfreezeV2(corepb.ResourceCode_BANDWIDTH, 100, 500000)  // expired
	acc.AddUnfreezeV2(corepb.ResourceCode_ENERGY, 200, 800000)     // expired
	acc.AddUnfreezeV2(corepb.ResourceCode_BANDWIDTH, 300, 2000000) // not expired
	rawdb.WriteAccount(db, owner, acc)

	tx := makeWithdrawExpireUnfreezeTx(owner)
	act := &WithdrawExpireUnfreezeActuator{}
	ctx := &Context{DB: db, Tx: tx, BlockTime: 1000000}

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}

	updated := rawdb.ReadAccount(db, owner)
	if updated.Balance() != 1300 {
		t.Fatalf("balance: want 1300, got %d", updated.Balance())
	}
	if len(updated.UnfrozenV2()) != 1 {
		t.Fatalf("remaining unfrozen: want 1, got %d", len(updated.UnfrozenV2()))
	}
}
