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

func makeUnfreezeV2Tx(owner common.Address, amount int64, resource corepb.ResourceCode) *types.Transaction {
	uc := &contractpb.UnfreezeBalanceV2Contract{
		OwnerAddress:    owner.Bytes(),
		UnfreezeBalance: amount,
		Resource:        resource,
	}
	any, _ := anypb.New(uc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{Type: corepb.Transaction_Contract_UnfreezeBalanceV2Contract, Parameter: any},
			},
		},
	})
}

func TestUnfreezeV2Validate(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := makeTestAddr(3)

	tx := makeUnfreezeV2Tx(owner, 100, corepb.ResourceCode_BANDWIDTH)
	act := &UnfreezeBalanceV2Actuator{}
	ctx := &Context{DB: db, Tx: tx}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing account")
	}

	acc := types.NewAccount(owner, corepb.AccountType_Normal)
	acc.SetBalance(1000)
	acc.AddFreezeV2(corepb.ResourceCode_BANDWIDTH, 500)
	rawdb.WriteAccount(db, owner, acc)

	tx = makeUnfreezeV2Tx(owner, 1000, corepb.ResourceCode_BANDWIDTH)
	ctx = &Context{DB: db, Tx: tx}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for insufficient frozen")
	}

	tx = makeUnfreezeV2Tx(owner, 200, corepb.ResourceCode_BANDWIDTH)
	ctx = &Context{DB: db, Tx: tx}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUnfreezeV2Execute(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := makeTestAddr(3)

	acc := types.NewAccount(owner, corepb.AccountType_Normal)
	acc.SetBalance(1000)
	acc.AddFreezeV2(corepb.ResourceCode_BANDWIDTH, 500)
	rawdb.WriteAccount(db, owner, acc)

	blockTime := int64(100000)
	tx := makeUnfreezeV2Tx(owner, 200, corepb.ResourceCode_BANDWIDTH)
	act := &UnfreezeBalanceV2Actuator{}
	ctx := &Context{DB: db, Tx: tx, BlockTime: blockTime}

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}

	updated := rawdb.ReadAccount(db, owner)
	if got := updated.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH); got != 300 {
		t.Fatalf("frozen BW: want 300, got %d", got)
	}
	unfrozen := updated.UnfrozenV2()
	if len(unfrozen) != 1 {
		t.Fatalf("unfrozen count: want 1, got %d", len(unfrozen))
	}
	if unfrozen[0].UnfreezeAmount != 200 {
		t.Fatalf("unfreeze amount: want 200, got %d", unfrozen[0].UnfreezeAmount)
	}
	expectedExpire := blockTime + 14*86400000
	if unfrozen[0].UnfreezeExpireTime != expectedExpire {
		t.Fatalf("expire time: want %d, got %d", expectedExpire, unfrozen[0].UnfreezeExpireTime)
	}
	if updated.Balance() != 1000 {
		t.Fatalf("balance: want 1000, got %d", updated.Balance())
	}
}
