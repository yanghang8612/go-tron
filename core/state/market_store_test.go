package state

import (
	"bytes"
	"strings"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// marketOrder builds a minimal ACTIVE order for the rooting tests.
func marketOrder(id byte, owner byte, sell []byte, sellQty int64, buy []byte, buyQty int64) *corepb.MarketOrder {
	o := make([]byte, 21)
	o[0] = 0x41
	o[20] = owner
	return &corepb.MarketOrder{
		OrderId:                 []byte{id},
		OwnerAddress:            o,
		SellTokenId:             sell,
		SellTokenQuantity:       sellQty,
		BuyTokenId:              buy,
		BuyTokenQuantity:        buyQty,
		SellTokenQuantityRemain: sellQty,
		State:                   corepb.MarketOrder_ACTIVE,
	}
}

// Absent reads honor the prior rawdb readers' contract: order/order-book read
// back nil, while account-order and price-list read back a zero-but-non-nil
// struct (callers mutate them in place).
func TestMarketStoreAbsentReads(t *testing.T) {
	sdb := newTestStateDB(t)

	if got := sdb.ReadMarketOrder([]byte("nope")); got != nil {
		t.Fatalf("absent order should be nil, got %+v", got)
	}
	pk := rawdb.PriceKey(2, 1)
	if got := sdb.ReadMarketOrderBook([]byte("_"), []byte("1000001"), pk); got != nil {
		t.Fatalf("absent order book should be nil, got %+v", got)
	}
	if got := sdb.ReadMarketPairPriceCount([]byte("_"), []byte("1000001")); got != 0 {
		t.Fatalf("absent price count should be 0, got %d", got)
	}

	owner := []byte{0x41, 0x09}
	mao := sdb.ReadMarketAccountOrder(owner)
	if mao == nil || !bytes.Equal(mao.OwnerAddress, owner) || len(mao.Orders) != 0 {
		t.Fatalf("absent account order should be zero-but-non-nil with owner set, got %+v", mao)
	}
	pl := sdb.ReadMarketPriceList([]byte("_"), []byte("1000001"))
	if pl == nil || !bytes.Equal(pl.SellTokenId, []byte("_")) || !bytes.Equal(pl.BuyTokenId, []byte("1000001")) || len(pl.Prices) != 0 {
		t.Fatalf("absent price list should be zero-but-non-nil with token ids set, got %+v", pl)
	}
}

// The five sub-stores share one domain but address disjoint key-spaces: a
// price-level key for (sell,buy,pk) does not collide with the pair-level
// count/price-list key for (sell,buy), nor an order keyed by id with an account
// keyed by the same bytes. Distinct one-byte tags guarantee separation.
func TestMarketStoreSubStoreSeparation(t *testing.T) {
	sdb := newTestStateDB(t)
	sell, buy := []byte("_"), []byte("1000001")
	pk := rawdb.PriceKey(100, 200)

	ord := marketOrder(1, 1, sell, 100, buy, 200)
	if err := sdb.WriteMarketOrder(ord.OrderId, ord); err != nil {
		t.Fatal(err)
	}
	// An account keyed by the SAME bytes as the order id must not alias it.
	mao := &corepb.MarketAccountOrder{OwnerAddress: ord.OrderId, Orders: [][]byte{ord.OrderId}, Count: 1, TotalCount: 1}
	if err := sdb.WriteMarketAccountOrder(ord.OrderId, mao); err != nil {
		t.Fatal(err)
	}
	ob := &corepb.MarketOrderIdList{Head: ord.OrderId, Tail: ord.OrderId}
	if err := sdb.WriteMarketOrderBook(sell, buy, pk, ob); err != nil {
		t.Fatal(err)
	}
	if err := sdb.WriteMarketPairPriceCount(sell, buy, 1); err != nil {
		t.Fatal(err)
	}
	pl := &corepb.MarketPriceList{SellTokenId: sell, BuyTokenId: buy, Prices: []*corepb.MarketPrice{{SellTokenQuantity: 100, BuyTokenQuantity: 200}}}
	if err := sdb.WriteMarketPriceList(sell, buy, pl); err != nil {
		t.Fatal(err)
	}

	if got := sdb.ReadMarketOrder(ord.OrderId); got == nil || got.SellTokenQuantity != 100 {
		t.Fatalf("order readback: %+v", got)
	}
	if got := sdb.ReadMarketAccountOrder(ord.OrderId); got.Count != 1 || len(got.Orders) != 1 {
		t.Fatalf("account order readback: %+v", got)
	}
	if got := sdb.ReadMarketOrderBook(sell, buy, pk); got == nil || !bytes.Equal(got.Head, ord.OrderId) {
		t.Fatalf("order book readback: %+v", got)
	}
	if got := sdb.ReadMarketPairPriceCount(sell, buy); got != 1 {
		t.Fatalf("price count readback: %d", got)
	}
	if got := sdb.ReadMarketPriceList(sell, buy); len(got.Prices) != 1 {
		t.Fatalf("price list readback: %+v", got)
	}
}

// IncrMarketPairPriceCount adds deltas and DeleteMarketPairPriceCount clears the
// pair, mirroring java-tron's addNewPriceKey / delete on the last level.
func TestMarketStorePairPriceCount(t *testing.T) {
	sdb := newTestStateDB(t)
	sell, buy := []byte("_"), []byte("1000002")

	if err := sdb.IncrMarketPairPriceCount(sell, buy, 1); err != nil {
		t.Fatal(err)
	}
	if err := sdb.IncrMarketPairPriceCount(sell, buy, 1); err != nil {
		t.Fatal(err)
	}
	if got := sdb.ReadMarketPairPriceCount(sell, buy); got != 2 {
		t.Fatalf("after two incrs: want 2, got %d", got)
	}
	if err := sdb.IncrMarketPairPriceCount(sell, buy, -1); err != nil {
		t.Fatal(err)
	}
	if got := sdb.ReadMarketPairPriceCount(sell, buy); got != 1 {
		t.Fatalf("after decr: want 1, got %d", got)
	}
	if err := sdb.DeleteMarketPairPriceCount(sell, buy); err != nil {
		t.Fatal(err)
	}
	if got := sdb.ReadMarketPairPriceCount(sell, buy); got != 0 {
		t.Fatalf("after delete: want 0, got %d", got)
	}
}

func TestMarketStorePairPriceCountStrictSurfacesMalformedLength(t *testing.T) {
	sdb := newTestStateDB(t)
	sell, buy := []byte("_"), []byte("1000003")
	malformed := []byte{0x01, 0x02, 0x03}

	if err := sdb.SystemKVPut(kvdomains.SystemMarket, marketPairToPriceKVKey(sell, buy), malformed); err != nil {
		t.Fatalf("write malformed price count: %v", err)
	}
	if got := sdb.ReadMarketPairPriceCount(sell, buy); got != 0 {
		t.Fatalf("compat price count = %d, want 0 for malformed row", got)
	}
	got, ok, err := sdb.ReadMarketPairPriceCountStrict(sell, buy)
	if err == nil || !ok || got != 0 || !strings.Contains(err.Error(), "length 3, want 8") {
		t.Fatalf("strict price count = %d ok=%v err=%v, want malformed length error", got, ok, err)
	}
	if err := sdb.IncrMarketPairPriceCount(sell, buy, 1); err == nil || !strings.Contains(err.Error(), "length 3, want 8") {
		t.Fatalf("IncrMarketPairPriceCount malformed err = %v, want length error", err)
	}
	if err := sdb.DecrementMarketPairPriceCount(sell, buy); err == nil || !strings.Contains(err.Error(), "length 3, want 8") {
		t.Fatalf("DecrementMarketPairPriceCount malformed err = %v, want length error", err)
	}
	raw, exists, err := sdb.SystemKVGet(kvdomains.SystemMarket, marketPairToPriceKVKey(sell, buy))
	if err != nil || !exists || !bytes.Equal(raw, malformed) {
		t.Fatalf("malformed price count after failed updates = %x exists=%v err=%v, want preserved %x", raw, exists, err, malformed)
	}
}

// TestMarketStoreAnchorAndRewind is the state-layer gate for market rooting:
// committing market writes moves the state root (anchor), and reopening an old
// root recovers the old order book across all five sub-stores (rewind). Mirrors
// applyBlock's per-block parent-root open with a fresh StateDB per commit.
func TestMarketStoreAnchorAndRewind(t *testing.T) {
	sdb := newTestStateDB(t)
	sell, buy := []byte("_"), []byte("1000001")
	pk := rawdb.PriceKey(100, 200)

	// R1: one standing order on the book at price 100:200, with its account
	// list, order-book head/tail, distinct-price count, and price list.
	ord := marketOrder(1, 1, sell, 100, buy, 200)
	if err := sdb.WriteMarketOrder(ord.OrderId, ord); err != nil {
		t.Fatal(err)
	}
	mao := &corepb.MarketAccountOrder{OwnerAddress: ord.OwnerAddress, Orders: [][]byte{ord.OrderId}, Count: 1, TotalCount: 1}
	if err := sdb.WriteMarketAccountOrder(ord.OwnerAddress, mao); err != nil {
		t.Fatal(err)
	}
	if err := sdb.WriteMarketOrderBook(sell, buy, pk, &corepb.MarketOrderIdList{Head: ord.OrderId, Tail: ord.OrderId}); err != nil {
		t.Fatal(err)
	}
	if err := sdb.IncrMarketPairPriceCount(sell, buy, 1); err != nil {
		t.Fatal(err)
	}
	plR1 := &corepb.MarketPriceList{SellTokenId: sell, BuyTokenId: buy, Prices: []*corepb.MarketPrice{{SellTokenQuantity: 100, BuyTokenQuantity: 200}}}
	if err := sdb.WriteMarketPriceList(sell, buy, plR1); err != nil {
		t.Fatal(err)
	}
	r1, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit R1: %v", err)
	}

	// R2 on a fresh StateDB: the order is fully canceled — order CANCELED, account
	// list empties, order book + price level + count removed.
	sdb2, err := New(r1, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	ord2 := marketOrder(1, 1, sell, 100, buy, 200)
	ord2.State = corepb.MarketOrder_CANCELED
	ord2.SellTokenQuantityRemain = 0
	ord2.SellTokenQuantityReturn = 100
	if err := sdb2.WriteMarketOrder(ord2.OrderId, ord2); err != nil {
		t.Fatal(err)
	}
	if err := sdb2.WriteMarketAccountOrder(ord.OwnerAddress, &corepb.MarketAccountOrder{OwnerAddress: ord.OwnerAddress, Count: 0, TotalCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := sdb2.DeleteMarketOrderBook(sell, buy, pk); err != nil {
		t.Fatal(err)
	}
	if err := sdb2.DeleteMarketPairPriceCount(sell, buy); err != nil {
		t.Fatal(err)
	}
	if err := sdb2.WriteMarketPriceList(sell, buy, &corepb.MarketPriceList{SellTokenId: sell, BuyTokenId: buy}); err != nil {
		t.Fatal(err)
	}
	r2, err := sdb2.Commit()
	if err != nil {
		t.Fatalf("commit R2: %v", err)
	}

	if r1 == r2 {
		t.Fatal("anchor: market change did not move the state root")
	}

	// Flat latest is authoritative: opening R1 reads the current canceled/empty view.
	atR1, err := New(r1, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := atR1.ReadMarketOrder(ord.OrderId); got == nil || got.State != corepb.MarketOrder_CANCELED {
		t.Fatalf("R1-open order should be CANCELED: %+v", got)
	}
	if got := atR1.ReadMarketAccountOrder(ord.OwnerAddress); got.Count != 0 || len(got.Orders) != 0 {
		t.Fatalf("R1-open account order should be empty: %+v", got)
	}
	if got := atR1.ReadMarketOrderBook(sell, buy, pk); got != nil {
		t.Fatalf("R1-open order book should be gone: %+v", got)
	}
	if got := atR1.ReadMarketPairPriceCount(sell, buy); got != 0 {
		t.Fatalf("R1-open price count should be 0, got %d", got)
	}
	if got := atR1.ReadMarketPriceList(sell, buy); len(got.Prices) != 0 {
		t.Fatalf("R1-open price list should be empty: %+v", got)
	}

	// R2 keeps its own canceled/empty view.
	atR2, err := New(r2, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := atR2.ReadMarketOrder(ord.OrderId); got == nil || got.State != corepb.MarketOrder_CANCELED {
		t.Fatalf("R2 order should be CANCELED: %+v", got)
	}
	if got := atR2.ReadMarketAccountOrder(ord.OwnerAddress); got.Count != 0 || len(got.Orders) != 0 {
		t.Fatalf("R2 account order should be empty: %+v", got)
	}
	if got := atR2.ReadMarketOrderBook(sell, buy, pk); got != nil {
		t.Fatalf("R2 order book should be gone: %+v", got)
	}
	if got := atR2.ReadMarketPairPriceCount(sell, buy); got != 0 {
		t.Fatalf("R2 price count should be 0, got %d", got)
	}
	if got := atR2.ReadMarketPriceList(sell, buy); len(got.Prices) != 0 {
		t.Fatalf("R2 price list should be empty: %+v", got)
	}
}

func TestMarketHistoryAtSurfacesCorruptProtobuf(t *testing.T) {
	f := newHistoryFixture(t)
	orderID := []byte("order-corrupt")
	owner := []byte{0x41, 0x6d}
	sellTokenID := []byte("sell")
	buyTokenID := []byte("buy")
	pk := rawdb.PriceKey(2, 3)
	corruptProto := []byte{0x80}

	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		if err := s.SystemKVPut(kvdomains.SystemMarket, marketOrderKVKey(orderID), corruptProto); err != nil {
			t.Fatalf("write corrupt market order: %v", err)
		}
		if err := s.SystemKVPut(kvdomains.SystemMarket, marketAccountOrderKVKey(owner), corruptProto); err != nil {
			t.Fatalf("write corrupt market account order: %v", err)
		}
		if err := s.SystemKVPut(kvdomains.SystemMarket, marketPriceListKVKey(sellTokenID, buyTokenID), corruptProto); err != nil {
			t.Fatalf("write corrupt market price list: %v", err)
		}
		if err := s.SystemKVPut(kvdomains.SystemMarket, marketOrderBookKVKey(sellTokenID, buyTokenID, pk), corruptProto); err != nil {
			t.Fatalf("write corrupt market order book: %v", err)
		}
		if err := s.SystemKVPut(kvdomains.SystemMarket, marketPairToPriceKVKey(sellTokenID, buyTokenID), []byte{0x01, 0x02, 0x03}); err != nil {
			t.Fatalf("write corrupt market pair price count: %v", err)
		}
	})
	f.applyBlock(tcommon.Hash{0x02}, func(*StateDB) {})

	order, err := f.reader().MarketOrderAt(orderID, 1)
	if err == nil {
		t.Fatal("MarketOrderAt corrupt protobuf error = nil")
	}
	if order != nil {
		t.Fatalf("MarketOrderAt corrupt protobuf order = %+v, want nil", order)
	}
	if !strings.Contains(err.Error(), "decode market order at block 1") {
		t.Fatalf("MarketOrderAt corrupt protobuf error = %v, want decode market order context", err)
	}

	accountOrder, err := f.reader().MarketAccountOrderAt(owner, 1)
	if err == nil {
		t.Fatal("MarketAccountOrderAt corrupt protobuf error = nil")
	}
	if accountOrder != nil {
		t.Fatalf("MarketAccountOrderAt corrupt protobuf account order = %+v, want nil", accountOrder)
	}
	if !strings.Contains(err.Error(), "decode market account order at block 1") {
		t.Fatalf("MarketAccountOrderAt corrupt protobuf error = %v, want decode market account order context", err)
	}

	priceList, err := f.reader().MarketPriceListAt(sellTokenID, buyTokenID, 1)
	if err == nil {
		t.Fatal("MarketPriceListAt corrupt protobuf error = nil")
	}
	if priceList != nil {
		t.Fatalf("MarketPriceListAt corrupt protobuf price list = %+v, want nil", priceList)
	}
	if !strings.Contains(err.Error(), "decode market price list at block 1") {
		t.Fatalf("MarketPriceListAt corrupt protobuf error = %v, want decode market price list context", err)
	}

	orderBook, err := f.reader().MarketOrderBookAt(sellTokenID, buyTokenID, pk, 1)
	if err == nil {
		t.Fatal("MarketOrderBookAt corrupt protobuf error = nil")
	}
	if orderBook != nil {
		t.Fatalf("MarketOrderBookAt corrupt protobuf order book = %+v, want nil", orderBook)
	}
	if !strings.Contains(err.Error(), "decode market order book at block 1") {
		t.Fatalf("MarketOrderBookAt corrupt protobuf error = %v, want decode market order book context", err)
	}

	count, ok, err := f.reader().MarketPairPriceCountAt(sellTokenID, buyTokenID, 1)
	if err == nil {
		t.Fatal("MarketPairPriceCountAt corrupt length error = nil")
	}
	if count != 0 || !ok {
		t.Fatalf("MarketPairPriceCountAt corrupt length = %d ok=%v, want zero true", count, ok)
	}
	if !strings.Contains(err.Error(), "decode market pair price count at block 1") ||
		!strings.Contains(err.Error(), "length 3, want 8") {
		t.Fatalf("MarketPairPriceCountAt corrupt length error = %v, want decode length context", err)
	}
}
