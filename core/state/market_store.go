package state

import (
	"encoding/binary"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// Market (DEX) order state is rooted into the reserved system account's
// SystemMarket KV so the whole order book rewinds with the full state root,
// replacing the five flat rawdb buckets that previously held it. java-tron
// keeps these in five dedicated stores; all five are rooted here for full
// parity (the locked per-cycle full-parity decision), with no derived/rooted
// split inside the domain:
//
//   - MarketOrderStore (mo-): the MarketOrder proto keyed by order id.
//   - MarketAccountStore (mao-): the per-owner MarketAccountOrder (active order
//     ids + counts) keyed by 21-byte owner address.
//   - MarketPairPriceToOrderStore (mop-): the MarketOrderIdList (head/tail of
//     the price-level linked list) keyed by (sellToken, buyToken, priceKey).
//   - MarketPairToPriceStore (mptop-): the int64 distinct-price count keyed by
//     (sellToken, buyToken).
//   - MarketPriceList (mpl-): go-tron's materialized MarketPriceList per pair,
//     keyed by (sellToken, buyToken). (java-tron recomputes this; that
//     pre-existing divergence is documented at the deleted rawdb accessors and
//     is orthogonal to this storage move.)
//
// All five share the one SystemMarket domain, disambiguated by a single-byte
// sub-store tag prefixed to the logical key. The remainder of each key is the
// composite the flat bucket used, preserved byte-for-byte so a rooted record is
// addressed identically to its old flat key (the '|' separators between token
// ids and the 16-byte price key are kept verbatim):
//
//	mo:    tag || orderID
//	mao:   tag || ownerAddr
//	mop:   tag || sellTokenID || '|' || buyTokenID || '|' || priceKey[16]
//	mptop: tag || sellTokenID || '|' || buyTokenID
//	mpl:   tag || sellTokenID || '|' || buyTokenID
//
// Values reuse the existing proto wire format (proto.Marshal) and the 8-byte
// big-endian count verbatim — no new on-disk encoding lineage is introduced.
//
// There is no enumeration over the domain: the three live RPCs (GetMarketOrderByID,
// GetMarketOrdersByAccount, GetMarketPriceByPair) all address known keys, so the
// Keccak256-hashed KV keys (which preclude a prefix scan) are never walked. No
// MarketPairList / MarketOrderListByPair RPC exists in go-tron.
const (
	marketOrderTag        byte = 0x01
	marketAccountOrderTag byte = 0x02
	marketOrderBookTag    byte = 0x03
	marketPairToPriceTag  byte = 0x04
	marketPriceListTag    byte = 0x05
)

// marketTagKey builds tag || body.
func marketTagKey(tag byte, body []byte) []byte {
	out := make([]byte, 1+len(body))
	out[0] = tag
	copy(out[1:], body)
	return out
}

// marketPairKey builds sellTokenID || '|' || buyTokenID, the shared pair body of
// the mop/mptop/mpl composites (matching the flat rawdb key layout).
func marketPairKey(sellTokenID, buyTokenID []byte) []byte {
	body := make([]byte, 0, len(sellTokenID)+1+len(buyTokenID))
	body = append(body, sellTokenID...)
	body = append(body, '|')
	body = append(body, buyTokenID...)
	return body
}

func marketOrderKVKey(orderID []byte) []byte {
	return marketTagKey(marketOrderTag, orderID)
}

func marketAccountOrderKVKey(ownerAddr []byte) []byte {
	return marketTagKey(marketAccountOrderTag, ownerAddr)
}

func marketOrderBookKVKey(sellTokenID, buyTokenID []byte, pk [16]byte) []byte {
	body := marketPairKey(sellTokenID, buyTokenID)
	body = append(body, '|')
	body = append(body, pk[:]...)
	return marketTagKey(marketOrderBookTag, body)
}

func marketPairToPriceKVKey(sellTokenID, buyTokenID []byte) []byte {
	return marketTagKey(marketPairToPriceTag, marketPairKey(sellTokenID, buyTokenID))
}

func marketPriceListKVKey(sellTokenID, buyTokenID []byte) []byte {
	return marketTagKey(marketPriceListTag, marketPairKey(sellTokenID, buyTokenID))
}

// ReadMarketOrder returns the rooted MarketOrder for orderID, or nil if absent.
// A KV/unmarshal error is swallowed to nil, matching the prior rawdb reader (its
// callers treat nil as "no order").
func (s *StateDB) ReadMarketOrder(orderID []byte) *corepb.MarketOrder {
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.SystemMarket, marketOrderKVKey(orderID))
	if err != nil || !ok || len(raw) == 0 {
		return nil
	}
	o := &corepb.MarketOrder{}
	if err := proto.Unmarshal(raw, o); err != nil {
		return nil
	}
	return o
}

// WriteMarketOrder stages a MarketOrder keyed by orderID. The error is non-nil
// only for a proto marshal failure or an unregistered domain (a programmer
// error), since SystemMarket is registered at init.
func (s *StateDB) WriteMarketOrder(orderID []byte, order *corepb.MarketOrder) error {
	data, err := proto.Marshal(order)
	if err != nil {
		return err
	}
	return s.SystemKVPut(kvdomains.SystemMarket, marketOrderKVKey(orderID), data)
}

// ReadMarketAccountOrder returns the rooted MarketAccountOrder for ownerAddr. As
// with the prior rawdb reader it never returns nil: an absent or malformed entry
// yields a zero-value struct with OwnerAddress set, because callers mutate the
// result in place (e.g. mao.TotalCount++).
func (s *StateDB) ReadMarketAccountOrder(ownerAddr []byte) *corepb.MarketAccountOrder {
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.SystemMarket, marketAccountOrderKVKey(ownerAddr))
	if err != nil || !ok || len(raw) == 0 {
		return &corepb.MarketAccountOrder{OwnerAddress: ownerAddr}
	}
	mao := &corepb.MarketAccountOrder{}
	if err := proto.Unmarshal(raw, mao); err != nil {
		return &corepb.MarketAccountOrder{OwnerAddress: ownerAddr}
	}
	return mao
}

// WriteMarketAccountOrder stages a MarketAccountOrder keyed by owner address.
func (s *StateDB) WriteMarketAccountOrder(ownerAddr []byte, mao *corepb.MarketAccountOrder) error {
	data, err := proto.Marshal(mao)
	if err != nil {
		return err
	}
	return s.SystemKVPut(kvdomains.SystemMarket, marketAccountOrderKVKey(ownerAddr), data)
}

// ReadMarketOrderBook returns the rooted MarketOrderIdList for the (sellToken,
// buyToken, priceKey) triple, or nil if absent (callers nil-check).
func (s *StateDB) ReadMarketOrderBook(sellTokenID, buyTokenID []byte, pk [16]byte) *corepb.MarketOrderIdList {
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.SystemMarket, marketOrderBookKVKey(sellTokenID, buyTokenID, pk))
	if err != nil || !ok || len(raw) == 0 {
		return nil
	}
	list := &corepb.MarketOrderIdList{}
	if err := proto.Unmarshal(raw, list); err != nil {
		return nil
	}
	return list
}

// WriteMarketOrderBook stages a MarketOrderIdList for a price level.
func (s *StateDB) WriteMarketOrderBook(sellTokenID, buyTokenID []byte, pk [16]byte, list *corepb.MarketOrderIdList) error {
	data, err := proto.Marshal(list)
	if err != nil {
		return err
	}
	return s.SystemKVPut(kvdomains.SystemMarket, marketOrderBookKVKey(sellTokenID, buyTokenID, pk), data)
}

// DeleteMarketOrderBook removes the price-level linked list for the triple.
func (s *StateDB) DeleteMarketOrderBook(sellTokenID, buyTokenID []byte, pk [16]byte) error {
	return s.SystemKVDelete(kvdomains.SystemMarket, marketOrderBookKVKey(sellTokenID, buyTokenID, pk))
}

// ReadMarketPairPriceCount returns the distinct-price count for a pair (zero if
// absent or malformed), mirroring java-tron MarketPairToPriceStore.getPriceNum.
func (s *StateDB) ReadMarketPairPriceCount(sellTokenID, buyTokenID []byte) int64 {
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.SystemMarket, marketPairToPriceKVKey(sellTokenID, buyTokenID))
	if err != nil || !ok || len(raw) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(raw))
}

// WriteMarketPairPriceCount stores the distinct-price count for a pair, mirroring
// java-tron MarketPairToPriceStore.setPriceNum.
func (s *StateDB) WriteMarketPairPriceCount(sellTokenID, buyTokenID []byte, count int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(count))
	return s.SystemKVPut(kvdomains.SystemMarket, marketPairToPriceKVKey(sellTokenID, buyTokenID), buf[:])
}

// DeleteMarketPairPriceCount removes the distinct-price counter for a pair,
// matching java-tron MarketPairToPriceStore.delete on the last price level.
func (s *StateDB) DeleteMarketPairPriceCount(sellTokenID, buyTokenID []byte) error {
	return s.SystemKVDelete(kvdomains.SystemMarket, marketPairToPriceKVKey(sellTokenID, buyTokenID))
}

// IncrMarketPairPriceCount adds delta to a pair's distinct-price count, mirroring
// java-tron MarketPairToPriceStore.addNewPriceKey (and the symmetric decrement on
// cancellation).
func (s *StateDB) IncrMarketPairPriceCount(sellTokenID, buyTokenID []byte, delta int64) error {
	cur := s.ReadMarketPairPriceCount(sellTokenID, buyTokenID)
	return s.WriteMarketPairPriceCount(sellTokenID, buyTokenID, cur+delta)
}

// ReadMarketPriceList returns the rooted MarketPriceList for a pair. As with the
// prior rawdb reader it never returns nil: an absent or malformed entry yields a
// zero-value struct with the token ids set, because callers append to it in place.
func (s *StateDB) ReadMarketPriceList(sellTokenID, buyTokenID []byte) *corepb.MarketPriceList {
	raw, ok, err := s.systemKVGetForDecoding(kvdomains.SystemMarket, marketPriceListKVKey(sellTokenID, buyTokenID))
	if err != nil || !ok || len(raw) == 0 {
		return &corepb.MarketPriceList{SellTokenId: sellTokenID, BuyTokenId: buyTokenID}
	}
	pl := &corepb.MarketPriceList{}
	if err := proto.Unmarshal(raw, pl); err != nil {
		return &corepb.MarketPriceList{SellTokenId: sellTokenID, BuyTokenId: buyTokenID}
	}
	return pl
}

// WriteMarketPriceList stages the materialized MarketPriceList for a pair.
func (s *StateDB) WriteMarketPriceList(sellTokenID, buyTokenID []byte, pl *corepb.MarketPriceList) error {
	data, err := proto.Marshal(pl)
	if err != nil {
		return err
	}
	return s.SystemKVPut(kvdomains.SystemMarket, marketPriceListKVKey(sellTokenID, buyTokenID), data)
}
