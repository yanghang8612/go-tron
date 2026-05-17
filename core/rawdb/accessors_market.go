package rawdb

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/ethdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

type marketPairPriceStore interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
}

// WriteMarketOrder stores a MarketOrder keyed by orderID.
func WriteMarketOrder(db ethdb.KeyValueWriter, orderID []byte, order *corepb.MarketOrder) error {
	data, err := proto.Marshal(order)
	if err != nil {
		return err
	}
	return db.Put(marketOrderKey(orderID), data)
}

// ReadMarketOrder returns the MarketOrder for orderID, or nil if not found.
func ReadMarketOrder(db ethdb.KeyValueReader, orderID []byte) *corepb.MarketOrder {
	data, err := db.Get(marketOrderKey(orderID))
	if err != nil || len(data) == 0 {
		return nil
	}
	var o corepb.MarketOrder
	if err := proto.Unmarshal(data, &o); err != nil {
		return nil
	}
	return &o
}

// WriteMarketAccountOrder stores a MarketAccountOrder keyed by owner address.
func WriteMarketAccountOrder(db ethdb.KeyValueWriter, ownerAddr []byte, mao *corepb.MarketAccountOrder) error {
	data, err := proto.Marshal(mao)
	if err != nil {
		return err
	}
	return db.Put(marketAccountOrderKey(ownerAddr), data)
}

// ReadMarketAccountOrder returns the MarketAccountOrder for ownerAddr.
// Returns a zero-value struct with OwnerAddress set if not found.
func ReadMarketAccountOrder(db ethdb.KeyValueReader, ownerAddr []byte) *corepb.MarketAccountOrder {
	data, err := db.Get(marketAccountOrderKey(ownerAddr))
	if err != nil || len(data) == 0 {
		return &corepb.MarketAccountOrder{OwnerAddress: ownerAddr}
	}
	var mao corepb.MarketAccountOrder
	if err := proto.Unmarshal(data, &mao); err != nil {
		return &corepb.MarketAccountOrder{OwnerAddress: ownerAddr}
	}
	return &mao
}

// WriteMarketOrderBook stores a MarketOrderIdList for a given (sellTokenID, buyTokenID, price) triple.
func WriteMarketOrderBook(db ethdb.KeyValueWriter, sellTokenID, buyTokenID []byte, pk [16]byte, list *corepb.MarketOrderIdList) error {
	data, err := proto.Marshal(list)
	if err != nil {
		return err
	}
	return db.Put(marketOrderBookKey(sellTokenID, buyTokenID, pk), data)
}

// ReadMarketOrderBook returns the MarketOrderIdList for the given token pair and price key, or nil if not found.
func ReadMarketOrderBook(db ethdb.KeyValueReader, sellTokenID, buyTokenID []byte, pk [16]byte) *corepb.MarketOrderIdList {
	data, err := db.Get(marketOrderBookKey(sellTokenID, buyTokenID, pk))
	if err != nil || len(data) == 0 {
		return nil
	}
	var list corepb.MarketOrderIdList
	if err := proto.Unmarshal(data, &list); err != nil {
		return nil
	}
	return &list
}

// DeleteMarketOrderBook removes the MarketOrderIdList for the given token pair and price key.
func DeleteMarketOrderBook(db ethdb.KeyValueWriter, sellTokenID, buyTokenID []byte, pk [16]byte) error {
	return db.Delete(marketOrderBookKey(sellTokenID, buyTokenID, pk))
}

// WriteMarketPriceList stores a MarketPriceList for a (sellTokenID, buyTokenID) pair.
func WriteMarketPriceList(db ethdb.KeyValueWriter, sellTokenID, buyTokenID []byte, pl *corepb.MarketPriceList) error {
	data, err := proto.Marshal(pl)
	if err != nil {
		return err
	}
	return db.Put(marketPriceListKey(sellTokenID, buyTokenID), data)
}

// ReadMarketPairPriceCount returns the number of distinct prices for a token
// pair. Zero means the pair has no standing orders. Mirrors java-tron
// MarketPairToPriceStore.getPriceNum.
func ReadMarketPairPriceCount(db ethdb.KeyValueReader, sellTokenID, buyTokenID []byte) int64 {
	data, err := db.Get(marketPairToPriceKey(sellTokenID, buyTokenID))
	if err != nil || len(data) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(data))
}

// WriteMarketPairPriceCount stores the price count for a token pair. Mirrors
// java-tron MarketPairToPriceStore.setPriceNum.
func WriteMarketPairPriceCount(db ethdb.KeyValueWriter, sellTokenID, buyTokenID []byte, count int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(count))
	_ = db.Put(marketPairToPriceKey(sellTokenID, buyTokenID), buf[:])
}

// DeleteMarketPairPriceCount removes the distinct-price counter for a token
// pair, matching java-tron's MarketPairToPriceStore.delete when the last price
// level is consumed.
func DeleteMarketPairPriceCount(db ethdb.KeyValueWriter, sellTokenID, buyTokenID []byte) error {
	return db.Delete(marketPairToPriceKey(sellTokenID, buyTokenID))
}

// IncrMarketPairPriceCount atomically adds delta to the price count for a
// pair. Mirrors java-tron MarketPairToPriceStore.addNewPriceKey (increment)
// and the symmetric decrement on order cancellation.
func IncrMarketPairPriceCount(db marketPairPriceStore, sellTokenID, buyTokenID []byte, delta int64) {
	cur := ReadMarketPairPriceCount(db, sellTokenID, buyTokenID)
	WriteMarketPairPriceCount(db, sellTokenID, buyTokenID, cur+delta)
}

// ReadMarketPriceList returns the MarketPriceList for a token pair.
// Returns a zero-value struct with token IDs set if not found.
func ReadMarketPriceList(db ethdb.KeyValueReader, sellTokenID, buyTokenID []byte) *corepb.MarketPriceList {
	data, err := db.Get(marketPriceListKey(sellTokenID, buyTokenID))
	if err != nil || len(data) == 0 {
		return &corepb.MarketPriceList{SellTokenId: sellTokenID, BuyTokenId: buyTokenID}
	}
	var pl corepb.MarketPriceList
	if err := proto.Unmarshal(data, &pl); err != nil {
		return &corepb.MarketPriceList{SellTokenId: sellTokenID, BuyTokenId: buyTokenID}
	}
	return &pl
}
