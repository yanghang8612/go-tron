package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeUnfreezeV2Tx(ownerByte byte, amount int64, resource corepb.ResourceCode) *types.Transaction {
	owner := makeTestAddr(ownerByte)
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
	statedb := setupStateDB(t)
	owner := makeTestAddr(3)

	// Missing account
	tx := makeUnfreezeV2Tx(3, 100, corepb.ResourceCode_BANDWIDTH)
	act := &UnfreezeBalanceV2Actuator{}
	ctx := setupContext(t, statedb, tx)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing account")
	}

	// Create account and freeze some balance
	seedAccount(statedb, owner, 1000)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 500)

	// Insufficient frozen
	tx = makeUnfreezeV2Tx(3, 1000, corepb.ResourceCode_BANDWIDTH)
	ctx = setupContext(t, statedb, tx)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for insufficient frozen")
	}

	// Success
	tx = makeUnfreezeV2Tx(3, 200, corepb.ResourceCode_BANDWIDTH)
	ctx = setupContext(t, statedb, tx)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUnfreezeV2Execute(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(3)
	seedAccount(statedb, owner, 1000)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 500)

	blockTime := int64(100000)
	tx := makeUnfreezeV2Tx(3, 200, corepb.ResourceCode_BANDWIDTH)
	act := &UnfreezeBalanceV2Actuator{}
	ctx := &Context{
		State:       statedb,
		DynProps:    nil,
		Tx:          tx,
		BlockTime:   blockTime,
		BlockNumber: 1,
	}

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}

	if got := statedb.GetFrozenV2Amount(owner, corepb.ResourceCode_BANDWIDTH); got != 300 {
		t.Fatalf("frozen BW: want 300, got %d", got)
	}
	acc := statedb.GetAccount(owner)
	unfrozen := acc.UnfrozenV2()
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
	if statedb.GetBalance(owner) != 1000 {
		t.Fatalf("balance: want 1000, got %d", statedb.GetBalance(owner))
	}
}
