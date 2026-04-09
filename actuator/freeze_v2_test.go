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

func makeTestAddr(b byte) common.Address {
	var addr common.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

func makeFreezeV2Tx(owner common.Address, amount int64, resource corepb.ResourceCode) *types.Transaction {
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
	db := rawdb.NewMemoryDatabase()
	owner := makeTestAddr(1)

	tx := makeFreezeV2Tx(owner, 100, corepb.ResourceCode_BANDWIDTH)
	act := &FreezeBalanceV2Actuator{}
	ctx := &Context{DB: db, Tx: tx}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing account")
	}

	acc := types.NewAccount(owner, corepb.AccountType_Normal)
	acc.SetBalance(1000)
	rawdb.WriteAccount(db, owner, acc)

	tx = makeFreezeV2Tx(owner, 0, corepb.ResourceCode_BANDWIDTH)
	ctx = &Context{DB: db, Tx: tx}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for zero amount")
	}

	tx = makeFreezeV2Tx(owner, 5000, corepb.ResourceCode_BANDWIDTH)
	ctx = &Context{DB: db, Tx: tx}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for insufficient balance")
	}

	tx = makeFreezeV2Tx(owner, 500, corepb.ResourceCode_BANDWIDTH)
	ctx = &Context{DB: db, Tx: tx}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFreezeV2Execute(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := makeTestAddr(1)

	acc := types.NewAccount(owner, corepb.AccountType_Normal)
	acc.SetBalance(1000)
	rawdb.WriteAccount(db, owner, acc)

	tx := makeFreezeV2Tx(owner, 500, corepb.ResourceCode_BANDWIDTH)
	act := &FreezeBalanceV2Actuator{}
	ctx := &Context{DB: db, Tx: tx}

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}

	updated := rawdb.ReadAccount(db, owner)
	if updated.Balance() != 500 {
		t.Fatalf("balance: want 500, got %d", updated.Balance())
	}
	if got := updated.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH); got != 500 {
		t.Fatalf("frozen BW: want 500, got %d", got)
	}
}
