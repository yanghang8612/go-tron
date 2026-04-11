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

func makeMarketSellTx(ownerByte byte, sellTokenID []byte, sellQty int64, buyTokenID []byte, buyQty int64) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	c := &contractpb.MarketSellAssetContract{
		OwnerAddress:      owner.Bytes(),
		SellTokenId:       sellTokenID,
		SellTokenQuantity: sellQty,
		BuyTokenId:        buyTokenID,
		BuyTokenQuantity:  buyQty,
	}
	anyParam, _ := anypb.New(c)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_MarketSellAssetContract,
					Parameter: anyParam,
				},
			},
		},
	}
	return types.NewTransactionFromPB(pb)
}

// TestMarketSellAssetValidate_Success tests that a TRC10 seller with sufficient balance passes.
func TestMarketSellAssetValidate_Success(t *testing.T) {
	const tokenID = int64(1_000_001)
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.SetTRC10Balance(owner, tokenID, 500)

	tx := makeMarketSellTx(1, []byte("1000001"), 100, []byte("_"), 200)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = ethrawdb.NewMemoryDatabase()

	ctx.DynProps.SetAllowMarketTransaction(true)
	act := &MarketSellAssetActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

// TestMarketSellAssetValidate_InsufficientBalance tests that insufficient balance fails validation.
func TestMarketSellAssetValidate_InsufficientBalance(t *testing.T) {
	const tokenID = int64(1_000_001)
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.SetTRC10Balance(owner, tokenID, 50) // only 50, wants to sell 100

	tx := makeMarketSellTx(1, []byte("1000001"), 100, []byte("_"), 200)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = ethrawdb.NewMemoryDatabase()
	ctx.DynProps.SetAllowMarketTransaction(true)

	act := &MarketSellAssetActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for insufficient balance")
	}
}

// TestMarketSellAssetValidate_SameToken tests that selling and buying the same token fails.
func TestMarketSellAssetValidate_SameToken(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 1_000_000)

	tx := makeMarketSellTx(1, []byte("_"), 100, []byte("_"), 200)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = ethrawdb.NewMemoryDatabase()
	ctx.DynProps.SetAllowMarketTransaction(true)

	act := &MarketSellAssetActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for same sell and buy token")
	}
}

// TestMarketSellAssetExecute_NoMatch tests that an order with no matching counterpart goes into the book.
func TestMarketSellAssetExecute_NoMatch(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 1_000_000)

	db := ethrawdb.NewMemoryDatabase()
	// Sell 100 TRX for 200 TRC10(1000001)
	tx := makeMarketSellTx(1, []byte("_"), 100, []byte("1000001"), 200)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db

	act := &MarketSellAssetActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}

	// Owner should have 100 TRX deducted (escrowed)
	if got := statedb.GetBalance(owner); got != 999_900 {
		t.Fatalf("owner TRX balance: want 999900, got %d", got)
	}

	// Price list should have one entry for TRX -> TRC10(1000001)
	pl := trawdb.ReadMarketPriceList(db, []byte("_"), []byte("1000001"))
	if pl == nil || len(pl.Prices) == 0 {
		t.Fatal("expected price list entry after placing order")
	}

	// Order book should have an order at that price
	pk := trawdb.PriceKey(100, 200)
	ob := trawdb.ReadMarketOrderBook(db, []byte("_"), []byte("1000001"), pk)
	if ob == nil || len(ob.Head) == 0 {
		t.Fatal("expected order book entry after placing order")
	}

	// The order should be ACTIVE
	order := trawdb.ReadMarketOrder(db, ob.Head)
	if order == nil {
		t.Fatal("expected order to exist in DB")
	}
	if order.State != corepb.MarketOrder_ACTIVE {
		t.Fatalf("order state: want ACTIVE, got %v", order.State)
	}
	if order.SellTokenQuantityRemain != 100 {
		t.Fatalf("remaining: want 100, got %d", order.SellTokenQuantityRemain)
	}
}

// TestMarketSellAssetExecute_FullMatch tests a full match between two opposite orders.
// Seller A: sells 100 TRX for 200 TRC10(1000001) — goes into book
// Seller B: sells 200 TRC10 for 100 TRX — should fully match A
// After: A gets 200 TRC10, B gets 100 TRX, both orders INACTIVE
func TestMarketSellAssetExecute_FullMatch(t *testing.T) {
	const tokenID = int64(1_000_001)
	statedb := setupStateDB(t)

	addrA := makeTestAddr(1)
	addrB := makeTestAddr(2)
	statedb.CreateAccount(addrA, corepb.AccountType_Normal)
	statedb.CreateAccount(addrB, corepb.AccountType_Normal)
	statedb.AddBalance(addrA, 1_000_000)          // A has TRX
	statedb.SetTRC10Balance(addrB, tokenID, 1_000) // B has TRC10

	db := ethrawdb.NewMemoryDatabase()
	act := &MarketSellAssetActuator{}

	// Seller A: sell 100 TRX for 200 TRC10 — no match yet, goes into book
	txA := makeMarketSellTx(1, []byte("_"), 100, []byte("1000001"), 200)
	ctxA := setupContext(t, statedb, txA)
	ctxA.DB = db
	if _, err := act.Execute(ctxA); err != nil {
		t.Fatalf("execute A failed: %v", err)
	}

	// Verify A's order is in the book
	pkA := trawdb.PriceKey(100, 200)
	obA := trawdb.ReadMarketOrderBook(db, []byte("_"), []byte("1000001"), pkA)
	if obA == nil || len(obA.Head) == 0 {
		t.Fatal("A's order should be in the book")
	}
	orderAID := bytes.Clone(obA.Head)

	// Seller B: sell 200 TRC10 for 100 TRX — should fully match A
	txB := makeMarketSellTx(2, []byte("1000001"), 200, []byte("_"), 100)
	ctxB := setupContext(t, statedb, txB)
	ctxB.DB = db
	if _, err := act.Execute(ctxB); err != nil {
		t.Fatalf("execute B failed: %v", err)
	}

	// A should have received 200 TRC10
	if got := statedb.GetTRC10Balance(addrA, tokenID); got != 200 {
		t.Fatalf("A TRC10 balance: want 200, got %d", got)
	}

	// B should have received 100 TRX (was deducted 200 TRC10 already)
	if got := statedb.GetBalance(addrB); got != 100 {
		t.Fatalf("B TRX balance: want 100, got %d", got)
	}

	// A's TRX should be 999_900 (deducted 100 TRX)
	if got := statedb.GetBalance(addrA); got != 999_900 {
		t.Fatalf("A TRX balance: want 999900, got %d", got)
	}

	// A's order should be INACTIVE
	orderA := trawdb.ReadMarketOrder(db, orderAID)
	if orderA == nil {
		t.Fatal("A's order should still exist in DB")
	}
	if orderA.State != corepb.MarketOrder_INACTIVE {
		t.Fatalf("A's order state: want INACTIVE, got %v", orderA.State)
	}

	// Order book for TRX->TRC10 should be empty (A consumed)
	obAfter := trawdb.ReadMarketOrderBook(db, []byte("_"), []byte("1000001"), pkA)
	if obAfter != nil && len(obAfter.Head) > 0 {
		t.Fatal("order book for TRX->TRC10 should be empty after full match")
	}
}

// TestMarketSellAssetExecute_PartialMatch tests a partial match.
// Seller A: sells 100 TRX for 200 TRC10 — goes into book
// Seller B: sells 400 TRC10 for 200 TRX — partially matches (gets 100 TRX, gives 200 TRC10)
// B has 200 TRC10 remaining ACTIVE in book
func TestMarketSellAssetExecute_PartialMatch(t *testing.T) {
	const tokenID = int64(1_000_001)
	statedb := setupStateDB(t)

	addrA := makeTestAddr(1)
	addrB := makeTestAddr(2)
	statedb.CreateAccount(addrA, corepb.AccountType_Normal)
	statedb.CreateAccount(addrB, corepb.AccountType_Normal)
	statedb.AddBalance(addrA, 1_000_000)
	statedb.SetTRC10Balance(addrB, tokenID, 2_000)

	db := ethrawdb.NewMemoryDatabase()
	act := &MarketSellAssetActuator{}

	// Seller A: sell 100 TRX for 200 TRC10 — goes into book
	txA := makeMarketSellTx(1, []byte("_"), 100, []byte("1000001"), 200)
	ctxA := setupContext(t, statedb, txA)
	ctxA.DB = db
	if _, err := act.Execute(ctxA); err != nil {
		t.Fatalf("execute A failed: %v", err)
	}

	// Seller B: sell 400 TRC10 for 200 TRX — partially matches A
	// A's price: 100 TRX / 200 TRC10 = 1 TRX per 2 TRC10
	// B wants: 200 TRX for 400 TRC10 = same ratio
	// A has 100 TRX, so A consumes 100 TRX worth from B = 200 TRC10
	// B's remaining = 400 - 200 = 200 TRC10 stays in book
	txB := makeMarketSellTx(2, []byte("1000001"), 400, []byte("_"), 200)
	ctxB := setupContext(t, statedb, txB)
	ctxB.DB = db
	if _, err := act.Execute(ctxB); err != nil {
		t.Fatalf("execute B failed: %v", err)
	}

	// A should have received 200 TRC10
	if got := statedb.GetTRC10Balance(addrA, tokenID); got != 200 {
		t.Fatalf("A TRC10 balance: want 200, got %d", got)
	}

	// B should have received 100 TRX
	if got := statedb.GetBalance(addrB); got != 100 {
		t.Fatalf("B TRX balance: want 100, got %d", got)
	}

	// B's order should be ACTIVE with 200 TRC10 remaining
	// Find B's order in the book for TRC10->TRX
	pkB := trawdb.PriceKey(400, 200)
	obB := trawdb.ReadMarketOrderBook(db, []byte("1000001"), []byte("_"), pkB)
	if obB == nil || len(obB.Head) == 0 {
		t.Fatal("B's remaining order should be in the TRC10->TRX book")
	}
	orderB := trawdb.ReadMarketOrder(db, obB.Head)
	if orderB == nil {
		t.Fatal("B's order should exist in DB")
	}
	if orderB.State != corepb.MarketOrder_ACTIVE {
		t.Fatalf("B's order state: want ACTIVE, got %v", orderB.State)
	}
	if orderB.SellTokenQuantityRemain != 200 {
		t.Fatalf("B remaining: want 200, got %d", orderB.SellTokenQuantityRemain)
	}
}
