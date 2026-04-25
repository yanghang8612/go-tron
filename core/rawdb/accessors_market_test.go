package rawdb

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestWriteReadMarketOrder(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	orderID := []byte("order-id-0001")
	order := &corepb.MarketOrder{
		OrderId:                 orderID,
		OwnerAddress:            []byte{0x41, 0x01},
		CreateTime:              1_700_000_000_000,
		SellTokenId:             []byte("_"),
		SellTokenQuantity:       1000,
		BuyTokenId:              []byte("1000001"),
		BuyTokenQuantity:        500,
		SellTokenQuantityRemain: 1000,
		SellTokenQuantityReturn: 0,
		State:                   corepb.MarketOrder_ACTIVE,
	}
	if err := WriteMarketOrder(db, orderID, order); err != nil {
		t.Fatal(err)
	}
	got := ReadMarketOrder(db, orderID)
	if got == nil {
		t.Fatal("expected market order to be found")
	}
	if string(got.OrderId) != string(orderID) {
		t.Fatalf("OrderId: want %s, got %s", orderID, got.OrderId)
	}
	if got.SellTokenQuantity != 1000 {
		t.Fatalf("SellTokenQuantity: want 1000, got %d", got.SellTokenQuantity)
	}
	if got.BuyTokenQuantity != 500 {
		t.Fatalf("BuyTokenQuantity: want 500, got %d", got.BuyTokenQuantity)
	}
	if got.State != corepb.MarketOrder_ACTIVE {
		t.Fatalf("State: want ACTIVE, got %v", got.State)
	}
}

func TestReadMarketOrder_NotFound(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	if got := ReadMarketOrder(db, []byte("no-such-order")); got != nil {
		t.Fatal("expected nil for unknown order")
	}
}

func TestWriteReadMarketAccountOrder(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := []byte{0x41, 0x02, 0x03}
	mao := &corepb.MarketAccountOrder{
		OwnerAddress: owner,
		Orders:       [][]byte{[]byte("order-a"), []byte("order-b")},
		Count:        2,
		TotalCount:   5,
	}
	if err := WriteMarketAccountOrder(db, owner, mao); err != nil {
		t.Fatal(err)
	}
	got := ReadMarketAccountOrder(db, owner)
	if got == nil {
		t.Fatal("expected market account order to be found")
	}
	if len(got.Orders) != 2 {
		t.Fatalf("Orders len: want 2, got %d", len(got.Orders))
	}
	if string(got.Orders[0]) != "order-a" {
		t.Fatalf("Orders[0]: want order-a, got %s", got.Orders[0])
	}
	if got.Count != 2 {
		t.Fatalf("Count: want 2, got %d", got.Count)
	}
	if got.TotalCount != 5 {
		t.Fatalf("TotalCount: want 5, got %d", got.TotalCount)
	}
}

func TestReadMarketAccountOrder_NotFound(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := []byte{0x41, 0x99}
	got := ReadMarketAccountOrder(db, owner)
	// should return empty struct with owner address, not nil
	if got == nil {
		t.Fatal("expected non-nil default MarketAccountOrder")
	}
	if string(got.OwnerAddress) != string(owner) {
		t.Fatalf("OwnerAddress: want %v, got %v", owner, got.OwnerAddress)
	}
	if len(got.Orders) != 0 {
		t.Fatalf("Orders: want empty, got %v", got.Orders)
	}
}

func TestWriteReadMarketOrderBook(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	sell := []byte("_")
	buy := []byte("1000001")
	pk := PriceKey(200, 100)
	list := &corepb.MarketOrderIdList{
		Head: []byte("order-head"),
		Tail: []byte("order-tail"),
	}
	if err := WriteMarketOrderBook(db, sell, buy, pk, list); err != nil {
		t.Fatal(err)
	}
	got := ReadMarketOrderBook(db, sell, buy, pk)
	if got == nil {
		t.Fatal("expected market order book to be found")
	}
	if string(got.Head) != "order-head" {
		t.Fatalf("Head: want order-head, got %s", got.Head)
	}
	if string(got.Tail) != "order-tail" {
		t.Fatalf("Tail: want order-tail, got %s", got.Tail)
	}
	// delete and verify gone
	if err := DeleteMarketOrderBook(db, sell, buy, pk); err != nil {
		t.Fatal(err)
	}
	if after := ReadMarketOrderBook(db, sell, buy, pk); after != nil {
		t.Fatal("expected nil after delete")
	}
}

func TestWriteReadMarketPriceList(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	sell := []byte("_")
	buy := []byte("1000001")
	pl := &corepb.MarketPriceList{
		SellTokenId: sell,
		BuyTokenId:  buy,
		Prices: []*corepb.MarketPrice{
			{SellTokenQuantity: 2, BuyTokenQuantity: 1},
			{SellTokenQuantity: 3, BuyTokenQuantity: 2},
		},
	}
	if err := WriteMarketPriceList(db, sell, buy, pl); err != nil {
		t.Fatal(err)
	}
	got := ReadMarketPriceList(db, sell, buy)
	if got == nil {
		t.Fatal("expected market price list to be found")
	}
	if len(got.Prices) != 2 {
		t.Fatalf("Prices len: want 2, got %d", len(got.Prices))
	}
	if got.Prices[0].SellTokenQuantity != 2 || got.Prices[0].BuyTokenQuantity != 1 {
		t.Fatalf("Prices[0]: want {2,1}, got {%d,%d}", got.Prices[0].SellTokenQuantity, got.Prices[0].BuyTokenQuantity)
	}
}

func TestReadMarketPriceList_NotFound(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	sell := []byte("_")
	buy := []byte("9999999")
	got := ReadMarketPriceList(db, sell, buy)
	// returns default with token IDs set
	if got == nil {
		t.Fatal("expected non-nil default MarketPriceList")
	}
	if string(got.SellTokenId) != string(sell) {
		t.Fatalf("SellTokenId: want %s, got %s", sell, got.SellTokenId)
	}
	if len(got.Prices) != 0 {
		t.Fatalf("Prices: want empty, got %v", got.Prices)
	}
}

func TestMarketPairPriceCount_RoundTrip(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	sell := []byte("_")
	buy := []byte("1000001")

	if got := ReadMarketPairPriceCount(db, sell, buy); got != 0 {
		t.Fatalf("expected 0 for absent pair, got %d", got)
	}

	WriteMarketPairPriceCount(db, sell, buy, 5)
	if got := ReadMarketPairPriceCount(db, sell, buy); got != 5 {
		t.Fatalf("expected 5, got %d", got)
	}
}

func TestMarketPairPriceCount_Incr(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	sell := []byte("_")
	buy := []byte("1000002")

	IncrMarketPairPriceCount(db, sell, buy, 1)
	if got := ReadMarketPairPriceCount(db, sell, buy); got != 1 {
		t.Fatalf("expected 1 after first incr, got %d", got)
	}
	IncrMarketPairPriceCount(db, sell, buy, 1)
	if got := ReadMarketPairPriceCount(db, sell, buy); got != 2 {
		t.Fatalf("expected 2 after second incr, got %d", got)
	}
	IncrMarketPairPriceCount(db, sell, buy, -1)
	if got := ReadMarketPairPriceCount(db, sell, buy); got != 1 {
		t.Fatalf("expected 1 after decr, got %d", got)
	}
}

func TestMarketPairPriceCount_PairSeparation(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	WriteMarketPairPriceCount(db, []byte("_"), []byte("1000001"), 10)
	WriteMarketPairPriceCount(db, []byte("_"), []byte("1000002"), 20)

	if got := ReadMarketPairPriceCount(db, []byte("_"), []byte("1000001")); got != 10 {
		t.Fatalf("pair1: expected 10, got %d", got)
	}
	if got := ReadMarketPairPriceCount(db, []byte("_"), []byte("1000002")); got != 20 {
		t.Fatalf("pair2: expected 20, got %d", got)
	}
}

func TestPriceKey_Normalization(t *testing.T) {
	// 200/100 should normalize to 2/1
	pk200_100 := PriceKey(200, 100)
	pk2_1 := PriceKey(2, 1)
	if pk200_100 != pk2_1 {
		t.Fatalf("PriceKey(200,100) != PriceKey(2,1): %v vs %v", pk200_100, pk2_1)
	}

	// 6/4 should normalize to 3/2
	pk6_4 := PriceKey(6, 4)
	pk3_2 := PriceKey(3, 2)
	if pk6_4 != pk3_2 {
		t.Fatalf("PriceKey(6,4) != PriceKey(3,2): %v vs %v", pk6_4, pk3_2)
	}

	// 2/1 should not equal 3/2
	if pk2_1 == pk3_2 {
		t.Fatalf("PriceKey(2,1) should not equal PriceKey(3,2)")
	}
}
