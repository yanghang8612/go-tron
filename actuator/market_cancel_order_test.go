package actuator

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	trawdb "github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeMarketCancelTx(ownerByte byte, orderID []byte) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	c := &contractpb.MarketCancelOrderContract{
		OwnerAddress: owner.Bytes(),
		OrderId:      orderID,
	}
	anyParam, _ := anypb.New(c)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_MarketCancelOrderContract,
					Parameter: anyParam,
				},
			},
		},
	}
	return types.NewTransactionFromPB(pb)
}

// TestMarketCancelOrderValidate_NotOwner creates an order owned by addr 1 and tries to cancel as addr 2.
func TestMarketCancelOrderValidate_NotOwner(t *testing.T) {
	const tokenID = int64(1_000_001)
	statedb := setupStateDB(t)
	owner1 := makeTestAddr(1)
	owner2 := makeTestAddr(2)
	statedb.CreateAccount(owner1, corepb.AccountType_Normal)
	statedb.CreateAccount(owner2, corepb.AccountType_Normal)
	statedb.SetTRC10Balance(owner1, tokenID, 500)

	db := ethrawdb.NewMemoryDatabase()

	// Place an order owned by addr 1
	txSell := makeMarketSellTx(1, []byte("1000001"), 100, []byte("_"), 50)
	ctxSell := setupContext(t, statedb, txSell)
	ctxSell.DB = db
	ctxSell.DynProps.SetAllowSameTokenName(true)
	act := &MarketSellAssetActuator{}
	if _, err := act.Execute(ctxSell); err != nil {
		t.Fatalf("sell execute failed: %v", err)
	}

	// Get the order ID from the book
	pk := trawdb.PriceKey(100, 50)
	ob := trawdb.ReadMarketOrderBook(db, []byte("1000001"), []byte("_"), pk)
	if ob == nil || len(ob.Head) == 0 {
		t.Fatal("expected order in book")
	}
	orderID := bytes.Clone(ob.Head)

	// Try cancel as addr 2 — should fail
	txCancel := makeMarketCancelTx(2, orderID)
	ctxCancel := setupContext(t, statedb, txCancel)
	ctxCancel.DB = db

	cancelAct := &MarketCancelOrderActuator{}
	if err := cancelAct.Validate(ctxCancel); err == nil {
		t.Fatal("expected error: owner address does not match")
	}
}

// TestMarketCancelOrderValidate_AlreadyInactive tries to cancel an order with State=INACTIVE.
func TestMarketCancelOrderValidate_AlreadyInactive(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()

	// Write a fake order with INACTIVE state
	orderID := []byte("fake-order-id-0001")
	owner := makeTestAddr(1)
	order := &corepb.MarketOrder{
		OrderId:                 orderID,
		OwnerAddress:            owner.Bytes(),
		SellTokenId:             []byte("1000001"),
		BuyTokenId:              []byte("_"),
		SellTokenQuantity:       100,
		BuyTokenQuantity:        50,
		SellTokenQuantityRemain: 0,
		State:                   corepb.MarketOrder_INACTIVE,
	}
	if err := trawdb.WriteMarketOrder(db, orderID, order); err != nil {
		t.Fatalf("failed to write order: %v", err)
	}

	statedb := setupStateDB(t)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)

	txCancel := makeMarketCancelTx(1, orderID)
	ctxCancel := setupContext(t, statedb, txCancel)
	ctxCancel.DB = db

	cancelAct := &MarketCancelOrderActuator{}
	if err := cancelAct.Validate(ctxCancel); err == nil {
		t.Fatal("expected error: order is not ACTIVE")
	}
}

// TestMarketCancelOrderExecute_ReturnsTokens places an order, cancels it, and verifies tokens returned.
// Owner sells 100 TRC10(1000001) for 50 TRX. Balance goes from 500 → 400 after sell, then back to 500 after cancel.
func TestMarketCancelOrderExecute_ReturnsTokens(t *testing.T) {
	const tokenID = int64(1_000_001)
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.SetTRC10Balance(owner, tokenID, 500)

	db := ethrawdb.NewMemoryDatabase()

	// Place order: sell 100 TRC10 for 50 TRX
	txSell := makeMarketSellTx(1, []byte("1000001"), 100, []byte("_"), 50)
	ctxSell := setupContext(t, statedb, txSell)
	ctxSell.DB = db
	ctxSell.DynProps.SetAllowSameTokenName(true)
	act := &MarketSellAssetActuator{}
	if _, err := act.Execute(ctxSell); err != nil {
		t.Fatalf("sell execute failed: %v", err)
	}

	// Balance should be 400 after escrow
	if got := statedb.GetTRC10Balance(owner, tokenID); got != 400 {
		t.Fatalf("after sell: want TRC10 balance 400, got %d", got)
	}

	// Get the order ID
	pk := trawdb.PriceKey(100, 50)
	ob := trawdb.ReadMarketOrderBook(db, []byte("1000001"), []byte("_"), pk)
	if ob == nil || len(ob.Head) == 0 {
		t.Fatal("expected order in book")
	}
	orderID := bytes.Clone(ob.Head)

	// Cancel the order
	txCancel := makeMarketCancelTx(1, orderID)
	ctxCancel := setupContext(t, statedb, txCancel)
	ctxCancel.DB = db
	ctxCancel.DynProps.SetAllowSameTokenName(true)

	cancelAct := &MarketCancelOrderActuator{}
	result, err := cancelAct.Execute(ctxCancel)
	if err != nil {
		t.Fatalf("cancel execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}

	// Balance should be back to 500 after cancel
	if got := statedb.GetTRC10Balance(owner, tokenID); got != 500 {
		t.Fatalf("after cancel: want TRC10 balance 500, got %d", got)
	}

	// Order state should be CANCELED
	order := trawdb.ReadMarketOrder(db, orderID)
	if order == nil {
		t.Fatal("order should still exist in DB")
	}
	if order.State != corepb.MarketOrder_CANCELED {
		t.Fatalf("order state: want CANCELED, got %v", order.State)
	}
	if order.SellTokenQuantityRemain != 0 {
		t.Fatalf("SellTokenQuantityRemain: want 0, got %d", order.SellTokenQuantityRemain)
	}
	if order.SellTokenQuantityReturn != 100 {
		t.Fatalf("SellTokenQuantityReturn: want 100, got %d", order.SellTokenQuantityReturn)
	}
	mao := trawdb.ReadMarketAccountOrder(db, owner[:])
	if mao.Count != 0 || len(mao.Orders) != 0 || mao.TotalCount != 1 {
		t.Fatalf("market account order should retain only total_count after cancel, got %+v", mao)
	}
}

// TestMarketCancelOrderExecute_RemovesFromBook places an order, cancels it,
// and verifies the order book is empty and price list is empty.
func TestMarketCancelOrderExecute_RemovesFromBook(t *testing.T) {
	const tokenID = int64(1_000_001)
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.SetTRC10Balance(owner, tokenID, 500)

	db := ethrawdb.NewMemoryDatabase()

	// Place order: sell 100 TRC10 for 50 TRX
	txSell := makeMarketSellTx(1, []byte("1000001"), 100, []byte("_"), 50)
	ctxSell := setupContext(t, statedb, txSell)
	ctxSell.DB = db
	ctxSell.DynProps.SetAllowSameTokenName(true)
	act := &MarketSellAssetActuator{}
	if _, err := act.Execute(ctxSell); err != nil {
		t.Fatalf("sell execute failed: %v", err)
	}

	// Get the order ID
	pk := trawdb.PriceKey(100, 50)
	ob := trawdb.ReadMarketOrderBook(db, []byte("1000001"), []byte("_"), pk)
	if ob == nil || len(ob.Head) == 0 {
		t.Fatal("expected order in book")
	}
	orderID := bytes.Clone(ob.Head)

	// Cancel the order
	txCancel := makeMarketCancelTx(1, orderID)
	ctxCancel := setupContext(t, statedb, txCancel)
	ctxCancel.DB = db
	ctxCancel.DynProps.SetAllowSameTokenName(true)

	cancelAct := &MarketCancelOrderActuator{}
	if _, err := cancelAct.Execute(ctxCancel); err != nil {
		t.Fatalf("cancel execute failed: %v", err)
	}

	// Order book for that price should be nil (deleted)
	obAfter := trawdb.ReadMarketOrderBook(db, []byte("1000001"), []byte("_"), pk)
	if obAfter != nil && len(obAfter.Head) > 0 {
		t.Fatal("order book should be empty after cancel")
	}

	// Price list should be empty
	pl := trawdb.ReadMarketPriceList(db, []byte("1000001"), []byte("_"))
	if pl != nil && len(pl.Prices) > 0 {
		t.Fatalf("price list should be empty after cancel, got %d prices", len(pl.Prices))
	}
	if got := trawdb.ReadMarketPairPriceCount(db, []byte("1000001"), []byte("_")); got != 0 {
		t.Fatalf("pair price count should be removed after cancel, got %d", got)
	}
}
