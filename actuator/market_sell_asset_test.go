package actuator

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	trawdb "github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
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

func writeMarketAssetIssue(t *testing.T, statedb *state.StateDB, tokenID int64) {
	t.Helper()
	if err := statedb.WriteAssetIssue(tokenID, &contractpb.AssetIssueContract{
		Id: "1000001",
	}); err != nil {
		t.Fatal(err)
	}
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
	writeMarketAssetIssue(t, ctx.State, tokenID)

	ctx.DynProps.SetAllowMarketTransaction(true)
	ctx.DynProps.SetAllowSameTokenName(true)
	act := &MarketSellAssetActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

func TestMarketSellAssetValidate_TRXSellRequiresFeeAndSellQuantity(t *testing.T) {
	const tokenID = int64(1_000_001)
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 100)

	tx := makeMarketSellTx(1, []byte("_"), 100, []byte("1000001"), 200)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = ethrawdb.NewMemoryDatabase()
	writeMarketAssetIssue(t, ctx.State, tokenID)
	ctx.DynProps.SetAllowMarketTransaction(true)
	ctx.DynProps.SetAllowSameTokenName(true)
	ctx.DynProps.SetMarketSellFee(1)

	act := &MarketSellAssetActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error when TRX balance covers sell quantity or fee separately but not their sum")
	}
}

func TestMarketSellAssetValidate_BuyTokenMustExist(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 1_000_000)

	tx := makeMarketSellTx(1, []byte("_"), 100, []byte("1000001"), 200)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = ethrawdb.NewMemoryDatabase()
	ctx.DynProps.SetAllowMarketTransaction(true)
	ctx.DynProps.SetAllowSameTokenName(true)

	act := &MarketSellAssetActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected missing buy token to fail validation")
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
	ctx.DynProps.SetAllowSameTokenName(true)

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
	ctx.DynProps.SetAllowSameTokenName(true)

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
	ctx.DynProps.SetAllowSameTokenName(true)

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
	pl := statedb.ReadMarketPriceList([]byte("_"), []byte("1000001"))
	if pl == nil || len(pl.Prices) == 0 {
		t.Fatal("expected price list entry after placing order")
	}

	// Order book should have an order at that price
	pk := trawdb.PriceKey(100, 200)
	ob := statedb.ReadMarketOrderBook([]byte("_"), []byte("1000001"), pk)
	if ob == nil || len(ob.Head) == 0 {
		t.Fatal("expected order book entry after placing order")
	}

	// The order should be ACTIVE
	order := statedb.ReadMarketOrder(ob.Head)
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
	statedb.AddBalance(addrA, 1_000_000)           // A has TRX
	statedb.SetTRC10Balance(addrB, tokenID, 1_000) // B has TRC10

	db := ethrawdb.NewMemoryDatabase()
	act := &MarketSellAssetActuator{}

	// Seller A: sell 100 TRX for 200 TRC10 — no match yet, goes into book
	txA := makeMarketSellTx(1, []byte("_"), 100, []byte("1000001"), 200)
	ctxA := setupContext(t, statedb, txA)
	ctxA.DB = db
	ctxA.DynProps.SetAllowSameTokenName(true)
	if _, err := act.Execute(ctxA); err != nil {
		t.Fatalf("execute A failed: %v", err)
	}

	// Verify A's order is in the book
	pkA := trawdb.PriceKey(100, 200)
	obA := statedb.ReadMarketOrderBook([]byte("_"), []byte("1000001"), pkA)
	if obA == nil || len(obA.Head) == 0 {
		t.Fatal("A's order should be in the book")
	}
	orderAID := bytes.Clone(obA.Head)

	// Seller B: sell 200 TRC10 for 100 TRX — should fully match A
	txB := makeMarketSellTx(2, []byte("1000001"), 200, []byte("_"), 100)
	ctxB := setupContext(t, statedb, txB)
	ctxB.DB = db
	ctxB.DynProps.SetAllowSameTokenName(true)
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
	orderA := statedb.ReadMarketOrder(orderAID)
	if orderA == nil {
		t.Fatal("A's order should still exist in DB")
	}
	if orderA.State != corepb.MarketOrder_INACTIVE {
		t.Fatalf("A's order state: want INACTIVE, got %v", orderA.State)
	}
	maoA := statedb.ReadMarketAccountOrder(addrA[:])
	if maoA.Count != 0 || len(maoA.Orders) != 0 || maoA.TotalCount != 1 {
		t.Fatalf("A market account order should retain only total_count after full fill, got %+v", maoA)
	}
	maoB := statedb.ReadMarketAccountOrder(addrB[:])
	if maoB.Count != 0 || len(maoB.Orders) != 0 || maoB.TotalCount != 1 {
		t.Fatalf("B market account order should retain only total_count after full fill, got %+v", maoB)
	}

	// Order book for TRX->TRC10 should be empty (A consumed)
	obAfter := statedb.ReadMarketOrderBook([]byte("_"), []byte("1000001"), pkA)
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
	ctxA.DynProps.SetAllowSameTokenName(true)
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
	ctxB.DynProps.SetAllowSameTokenName(true)
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
	obB := statedb.ReadMarketOrderBook([]byte("1000001"), []byte("_"), pkB)
	if obB == nil || len(obB.Head) == 0 {
		t.Fatal("B's remaining order should be in the TRC10->TRX book")
	}
	orderB := statedb.ReadMarketOrder(obB.Head)
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

func TestMarketSellAssetExecute_ReturnsTakerRemainWhenBuyRoundsToZero(t *testing.T) {
	const tokenID = int64(1_000_001)
	statedb := setupStateDB(t)

	maker1 := makeTestAddr(1)
	maker2 := makeTestAddr(2)
	taker := makeTestAddr(3)
	statedb.CreateAccount(maker1, corepb.AccountType_Normal)
	statedb.CreateAccount(maker2, corepb.AccountType_Normal)
	statedb.CreateAccount(taker, corepb.AccountType_Normal)
	statedb.SetTRC10Balance(maker1, tokenID, 100)
	statedb.SetTRC10Balance(maker2, tokenID, 100)
	statedb.AddBalance(taker, 7)

	db := ethrawdb.NewMemoryDatabase()
	act := &MarketSellAssetActuator{}
	for _, owner := range []byte{1, 2} {
		tx := makeMarketSellTx(owner, []byte("1000001"), 5, []byte("_"), 6)
		ctx := setupContext(t, statedb, tx)
		ctx.DB = db
		ctx.DynProps.SetAllowSameTokenName(true)
		if _, err := act.Execute(ctx); err != nil {
			t.Fatalf("maker %d execute failed: %v", owner, err)
		}
	}

	tx := makeMarketSellTx(3, []byte("_"), 7, []byte("1000001"), 5)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db
	ctx.DynProps.SetAllowSameTokenName(true)
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("taker execute failed: %v", err)
	}

	if len(result.OrderDetails) != 1 {
		t.Fatalf("order details: want 1 Java-compatible fill, got %d", len(result.OrderDetails))
	}
	if got := result.OrderDetails[0].FillSellQuantity; got != 6 {
		t.Fatalf("first fill sell quantity: want 6, got %d", got)
	}
	if got := result.OrderDetails[0].FillBuyQuantity; got != 5 {
		t.Fatalf("first fill buy quantity: want 5, got %d", got)
	}
	if got := statedb.GetBalance(maker1); got != 6 {
		t.Fatalf("maker1 TRX balance: want 6, got %d", got)
	}
	if got := statedb.GetBalance(maker2); got != 0 {
		t.Fatalf("maker2 TRX balance must not receive a zero-buy fill: want 0, got %d", got)
	}
	if got := statedb.GetBalance(taker); got != 1 {
		t.Fatalf("taker TRX balance should get rounded remainder back: want 1, got %d", got)
	}
	if got := statedb.GetTRC10Balance(taker, tokenID); got != 5 {
		t.Fatalf("taker TRC10 balance: want 5, got %d", got)
	}

	takerOrder := statedb.ReadMarketOrder(result.OrderID)
	if takerOrder == nil {
		t.Fatal("taker order should be written")
	}
	if takerOrder.State != corepb.MarketOrder_INACTIVE {
		t.Fatalf("taker order state: want INACTIVE, got %v", takerOrder.State)
	}
	if takerOrder.SellTokenQuantityRemain != 0 || takerOrder.SellTokenQuantityReturn != 1 {
		t.Fatalf("taker rounded return: want remain=0 return=1, got remain=%d return=%d", takerOrder.SellTokenQuantityRemain, takerOrder.SellTokenQuantityReturn)
	}

	ob := statedb.ReadMarketOrderBook([]byte("1000001"), []byte("_"), trawdb.PriceKey(5, 6))
	if ob == nil || len(ob.Head) == 0 {
		t.Fatal("second maker order should remain active at the same price")
	}
	remainingMaker := statedb.ReadMarketOrder(ob.Head)
	if remainingMaker == nil {
		t.Fatal("remaining maker order should exist")
	}
	if !bytes.Equal(remainingMaker.OwnerAddress, maker2[:]) || remainingMaker.State != corepb.MarketOrder_ACTIVE || remainingMaker.SellTokenQuantityRemain != 5 {
		t.Fatalf("remaining maker order mismatch: owner=%x state=%v remain=%d", remainingMaker.OwnerAddress, remainingMaker.State, remainingMaker.SellTokenQuantityRemain)
	}
}

func TestMarketSellAssetExecute_ReturnsMakerRemainWhenMakerReceiveRoundsToZero(t *testing.T) {
	const tokenID = int64(1_000_001)
	statedb := setupStateDB(t)

	maker := makeTestAddr(1)
	taker := makeTestAddr(2)
	statedb.CreateAccount(maker, corepb.AccountType_Normal)
	statedb.CreateAccount(taker, corepb.AccountType_Normal)
	statedb.SetTRC10Balance(maker, tokenID, 100)
	statedb.AddBalance(taker, 2)

	db := ethrawdb.NewMemoryDatabase()
	act := &MarketSellAssetActuator{}

	txMaker := makeMarketSellTx(1, []byte("1000001"), 10, []byte("_"), 6)
	ctxMaker := setupContext(t, statedb, txMaker)
	ctxMaker.DB = db
	ctxMaker.DynProps.SetAllowSameTokenName(true)
	makerResult, err := act.Execute(ctxMaker)
	if err != nil {
		t.Fatalf("maker execute failed: %v", err)
	}
	makerOrder := statedb.ReadMarketOrder(makerResult.OrderID)
	if makerOrder == nil {
		t.Fatal("maker order should exist")
	}
	makerOrder.SellTokenQuantityRemain = 1
	if err := statedb.WriteMarketOrder(makerResult.OrderID, makerOrder); err != nil {
		t.Fatalf("write adjusted maker order: %v", err)
	}

	txTaker := makeMarketSellTx(2, []byte("_"), 2, []byte("1000001"), 1)
	ctxTaker := setupContext(t, statedb, txTaker)
	ctxTaker.DB = db
	ctxTaker.DynProps.SetAllowSameTokenName(true)
	takerResult, err := act.Execute(ctxTaker)
	if err != nil {
		t.Fatalf("taker execute failed: %v", err)
	}

	if len(takerResult.OrderDetails) != 0 {
		t.Fatalf("zero maker receive must not create order detail, got %d", len(takerResult.OrderDetails))
	}
	if got := statedb.GetTRC10Balance(maker, tokenID); got != 91 {
		t.Fatalf("maker TRC10 balance should get 1 remaining token back: want 91, got %d", got)
	}
	if got := statedb.GetBalance(maker); got != 0 {
		t.Fatalf("maker TRX balance must not receive a rounded-zero fill: want 0, got %d", got)
	}
	if got := statedb.GetTRC10Balance(taker, tokenID); got != 0 {
		t.Fatalf("taker TRC10 balance must not receive maker returned remain: want 0, got %d", got)
	}

	makerOrder = statedb.ReadMarketOrder(makerResult.OrderID)
	if makerOrder.State != corepb.MarketOrder_INACTIVE {
		t.Fatalf("maker order state: want INACTIVE, got %v", makerOrder.State)
	}
	if makerOrder.SellTokenQuantityRemain != 0 || makerOrder.SellTokenQuantityReturn != 1 {
		t.Fatalf("maker rounded return: want remain=0 return=1, got remain=%d return=%d", makerOrder.SellTokenQuantityRemain, makerOrder.SellTokenQuantityReturn)
	}
	mao := statedb.ReadMarketAccountOrder(maker[:])
	if mao.Count != 0 || len(mao.Orders) != 0 {
		t.Fatalf("maker account order should remove inactive rounded order, got %+v", mao)
	}
	if ob := statedb.ReadMarketOrderBook([]byte("1000001"), []byte("_"), trawdb.PriceKey(10, 6)); ob != nil && len(ob.Head) > 0 {
		t.Fatal("maker price level should be removed after rounded-zero inactive order")
	}

	takerOrder := statedb.ReadMarketOrder(takerResult.OrderID)
	if takerOrder == nil {
		t.Fatal("taker remainder order should be written")
	}
	if takerOrder.State != corepb.MarketOrder_ACTIVE || takerOrder.SellTokenQuantityRemain != 2 {
		t.Fatalf("taker order should remain active with full sell quantity, state=%v remain=%d", takerOrder.State, takerOrder.SellTokenQuantityRemain)
	}
}

func TestMarketSellAssetExecute_TooManyMatchesFails(t *testing.T) {
	const tokenID = int64(1_000_001)
	statedb := setupStateDB(t)
	maker := makeTestAddr(1)
	taker := makeTestAddr(2)
	statedb.CreateAccount(maker, corepb.AccountType_Normal)
	statedb.CreateAccount(taker, corepb.AccountType_Normal)
	statedb.AddBalance(maker, 100)
	statedb.SetTRC10Balance(taker, tokenID, 100)

	db := ethrawdb.NewMemoryDatabase()
	act := &MarketSellAssetActuator{}
	for i := 0; i <= maxMarketMatchNum; i++ {
		tx := makeMarketSellTx(1, []byte("_"), 1, []byte("1000001"), 1)
		ctx := setupContext(t, statedb, tx)
		ctx.DB = db
		ctx.DynProps.SetAllowSameTokenName(true)
		if _, err := act.Execute(ctx); err != nil {
			t.Fatalf("maker order %d failed: %v", i, err)
		}
	}

	tx := makeMarketSellTx(2, []byte("1000001"), int64(maxMarketMatchNum+1), []byte("_"), int64(maxMarketMatchNum+1))
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db
	ctx.DynProps.SetAllowSameTokenName(true)
	if _, err := act.Execute(ctx); err == nil {
		t.Fatal("expected too many matches error")
	}
}
