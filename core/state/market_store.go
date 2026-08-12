package state

import (
	"bytes"
	"encoding/binary"
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// Market (DEX) order state is rooted into the reserved system account's
// SystemMarket KV so the whole order book rewinds with the full state root,
// replacing the five flat rawdb buckets that previously held it. java-tron
// keeps these in five dedicated stores; those flat stores plus go-tron's
// materialized pair helpers are rooted here for full
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
//   - MarketPairIndex (mpi): go-tron's materialized MarketOrderPairList of
//     pairs with a non-empty MarketPriceList, used for GetMarketPairList without
//     scanning the hashed SystemMarket keys.
//
// All sub-stores share the one SystemMarket domain, disambiguated by a single-byte
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
//	mpi:   tag
//
// Message-shaped values use versioned, hand-written deterministic encodings.
// Protobuf wire values are rejected. Pair price counts remain fixed 8-byte
// big-endian values.
//
// Point and pair-order-list RPCs address known keys, so the Keccak256-hashed KV
// keys (which preclude a prefix scan) are never walked. MarketOrderListByPair is
// reconstructed from the materialized pair price list plus price-level
// order-book rows. MarketPairList reads the explicit mpi row maintained alongside
// the pair price list.
const (
	marketOrderTag        byte = 0x01
	marketAccountOrderTag byte = 0x02
	marketOrderBookTag    byte = 0x03
	marketPairToPriceTag  byte = 0x04
	marketPriceListTag    byte = 0x05
	marketPairIndexTag    byte = 0x06
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

func marketPairIndexKVKey() []byte {
	return []byte{marketPairIndexTag}
}

// ReadMarketOrder returns the rooted MarketOrder for orderID, or nil if absent.
// Corruption retains the compatibility return shape but poisons the StateDB so
// transaction and commit boundaries cannot treat it as a semantic miss.
func (s *StateDB) ReadMarketOrder(orderID []byte) *corepb.MarketOrder {
	o, ok, err := s.ReadMarketOrderStrict(orderID)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read market order %x", orderID), err)
		return nil
	}
	if !ok {
		return nil
	}
	return o
}

// ReadMarketOrderStrict returns the rooted MarketOrder for orderID and
// distinguishes missing rows from unreadable or malformed rooted data.
func (s *StateDB) ReadMarketOrderStrict(orderID []byte) (*corepb.MarketOrder, bool, error) {
	raw, ok, err := s.SystemKVGet(kvdomains.SystemMarket, marketOrderKVKey(orderID))
	if err != nil || !ok {
		return nil, ok, err
	}
	if len(raw) == 0 {
		return nil, true, fmt.Errorf("decode market order: empty value")
	}
	o, err := decodeMarketOrder(raw)
	if err != nil {
		return nil, true, fmt.Errorf("decode market order: %w", err)
	}
	return o, true, nil
}

// MarketOrderAt reconstructs a rooted MarketOrder at the end of blockNum.
func (r *PersistentHistoryReader) MarketOrderAt(orderID []byte, blockNum uint64) (*corepb.MarketOrder, error) {
	raw, ok, err := r.AccountKVAt(tcommon.SystemAccountAddress, kvdomains.SystemMarket, marketOrderKVKey(orderID), blockNum)
	if err != nil || !ok {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("decode market order at block %d: empty value", blockNum)
	}
	o, err := decodeMarketOrder(raw)
	if err != nil {
		return nil, fmt.Errorf("decode market order at block %d: %w", blockNum, err)
	}
	return o, nil
}

// WriteMarketOrder stages a MarketOrder keyed by orderID. The error is non-nil
// only for a codec failure or an unregistered domain (a programmer
// error), since SystemMarket is registered at init.
func (s *StateDB) WriteMarketOrder(orderID []byte, order *corepb.MarketOrder) error {
	data, err := encodeMarketOrder(order)
	if err != nil {
		return err
	}
	return s.SystemKVPut(kvdomains.SystemMarket, marketOrderKVKey(orderID), data)
}

// ReadMarketAccountOrder returns the rooted MarketAccountOrder for ownerAddr. As
// with the prior rawdb reader it never returns nil: an absent entry yields a
// zero-value struct with OwnerAddress set, because callers mutate the result in
// place (e.g. mao.TotalCount++). Corruption also returns that compatibility
// value, but records a sticky state error before doing so.
func (s *StateDB) ReadMarketAccountOrder(ownerAddr []byte) *corepb.MarketAccountOrder {
	mao, ok, err := s.ReadMarketAccountOrderStrict(ownerAddr)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read market account order %x", ownerAddr), err)
		return &corepb.MarketAccountOrder{OwnerAddress: ownerAddr}
	}
	if !ok {
		return &corepb.MarketAccountOrder{OwnerAddress: ownerAddr}
	}
	return mao
}

// ReadMarketAccountOrderStrict returns the rooted MarketAccountOrder for owner
// and distinguishes an absent row from unreadable or malformed rooted data. The
// absent case mirrors ReadMarketAccountOrder's zero-but-non-nil result.
func (s *StateDB) ReadMarketAccountOrderStrict(ownerAddr []byte) (*corepb.MarketAccountOrder, bool, error) {
	raw, ok, err := s.SystemKVGet(kvdomains.SystemMarket, marketAccountOrderKVKey(ownerAddr))
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return &corepb.MarketAccountOrder{OwnerAddress: ownerAddr}, false, nil
	}
	if len(raw) == 0 {
		return nil, true, fmt.Errorf("decode market account order: empty value")
	}
	mao, err := decodeMarketAccountOrder(raw)
	if err != nil {
		return nil, true, fmt.Errorf("decode market account order: %w", err)
	}
	return mao, true, nil
}

// MarketAccountOrderAt reconstructs a rooted MarketAccountOrder at the end of
// blockNum. It mirrors ReadMarketAccountOrder's non-nil absent result, but
// surfaces malformed archive payloads as data errors.
func (r *PersistentHistoryReader) MarketAccountOrderAt(ownerAddr []byte, blockNum uint64) (*corepb.MarketAccountOrder, error) {
	raw, ok, err := r.AccountKVAt(tcommon.SystemAccountAddress, kvdomains.SystemMarket, marketAccountOrderKVKey(ownerAddr), blockNum)
	if err != nil {
		return nil, err
	}
	if !ok {
		return &corepb.MarketAccountOrder{OwnerAddress: ownerAddr}, nil
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("decode market account order at block %d: empty value", blockNum)
	}
	mao, err := decodeMarketAccountOrder(raw)
	if err != nil {
		return nil, fmt.Errorf("decode market account order at block %d: %w", blockNum, err)
	}
	return mao, nil
}

// WriteMarketAccountOrder stages a MarketAccountOrder keyed by owner address.
func (s *StateDB) WriteMarketAccountOrder(ownerAddr []byte, mao *corepb.MarketAccountOrder) error {
	data, err := encodeMarketAccountOrder(mao)
	if err != nil {
		return err
	}
	return s.SystemKVPut(kvdomains.SystemMarket, marketAccountOrderKVKey(ownerAddr), data)
}

// ReadMarketOrderBook returns the rooted MarketOrderIdList for the (sellToken,
// buyToken, priceKey) triple, or nil if absent (callers nil-check).
func (s *StateDB) ReadMarketOrderBook(sellTokenID, buyTokenID []byte, pk [16]byte) *corepb.MarketOrderIdList {
	list, ok, err := s.ReadMarketOrderBookStrict(sellTokenID, buyTokenID, pk)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read market order book %q/%q/%x", sellTokenID, buyTokenID, pk), err)
		return nil
	}
	if !ok {
		return nil
	}
	return list
}

// ReadMarketOrderBookStrict returns the rooted price-level order id list and
// distinguishes missing rows from unreadable or malformed rooted data.
func (s *StateDB) ReadMarketOrderBookStrict(sellTokenID, buyTokenID []byte, pk [16]byte) (*corepb.MarketOrderIdList, bool, error) {
	raw, ok, err := s.SystemKVGet(kvdomains.SystemMarket, marketOrderBookKVKey(sellTokenID, buyTokenID, pk))
	if err != nil || !ok {
		return nil, ok, err
	}
	if len(raw) == 0 {
		return nil, true, fmt.Errorf("decode market order book: empty value")
	}
	list, err := decodeMarketOrderBook(raw)
	if err != nil {
		return nil, true, fmt.Errorf("decode market order book: %w", err)
	}
	return list, true, nil
}

// MarketOrderBookAt reconstructs a rooted price-level order id list at the end
// of blockNum, surfacing malformed archive payloads as data errors.
func (r *PersistentHistoryReader) MarketOrderBookAt(sellTokenID, buyTokenID []byte, pk [16]byte, blockNum uint64) (*corepb.MarketOrderIdList, error) {
	raw, ok, err := r.AccountKVAt(tcommon.SystemAccountAddress, kvdomains.SystemMarket, marketOrderBookKVKey(sellTokenID, buyTokenID, pk), blockNum)
	if err != nil || !ok {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("decode market order book at block %d: empty value", blockNum)
	}
	list, err := decodeMarketOrderBook(raw)
	if err != nil {
		return nil, fmt.Errorf("decode market order book at block %d: %w", blockNum, err)
	}
	return list, nil
}

// WriteMarketOrderBook stages a MarketOrderIdList for a price level.
func (s *StateDB) WriteMarketOrderBook(sellTokenID, buyTokenID []byte, pk [16]byte, list *corepb.MarketOrderIdList) error {
	data, err := encodeMarketOrderBook(list)
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
// absent). Malformed rows retain the zero compatibility result while poisoning
// the StateDB.
func (s *StateDB) ReadMarketPairPriceCount(sellTokenID, buyTokenID []byte) int64 {
	count, ok, err := s.ReadMarketPairPriceCountStrict(sellTokenID, buyTokenID)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read market pair price count %q/%q", sellTokenID, buyTokenID), err)
		return 0
	}
	if !ok {
		return 0
	}
	return count
}

// ReadMarketPairPriceCountStrict returns the distinct-price count for a pair
// and distinguishes a missing row from unreadable or malformed rooted data.
func (s *StateDB) ReadMarketPairPriceCountStrict(sellTokenID, buyTokenID []byte) (int64, bool, error) {
	raw, ok, err := s.SystemKVGet(kvdomains.SystemMarket, marketPairToPriceKVKey(sellTokenID, buyTokenID))
	if err != nil || !ok {
		return 0, false, err
	}
	if len(raw) != 8 {
		return 0, true, fmt.Errorf("decode market pair price count: length %d, want 8", len(raw))
	}
	return int64(binary.BigEndian.Uint64(raw)), true, nil
}

// MarketPairPriceCountAt reconstructs the distinct-price count for a pair at
// the end of blockNum, surfacing malformed archive payloads as data errors.
func (r *PersistentHistoryReader) MarketPairPriceCountAt(sellTokenID, buyTokenID []byte, blockNum uint64) (int64, bool, error) {
	raw, ok, err := r.AccountKVAt(tcommon.SystemAccountAddress, kvdomains.SystemMarket, marketPairToPriceKVKey(sellTokenID, buyTokenID), blockNum)
	if err != nil || !ok {
		return 0, false, err
	}
	if len(raw) != 8 {
		return 0, true, fmt.Errorf("decode market pair price count at block %d: length %d, want 8", blockNum, len(raw))
	}
	return int64(binary.BigEndian.Uint64(raw)), true, nil
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
	cur, _, err := s.ReadMarketPairPriceCountStrict(sellTokenID, buyTokenID)
	if err != nil {
		return err
	}
	return s.WriteMarketPairPriceCount(sellTokenID, buyTokenID, cur+delta)
}

// DecrementMarketPairPriceCount decrements the distinct-price count and deletes
// the row when the last price level disappears.
func (s *StateDB) DecrementMarketPairPriceCount(sellTokenID, buyTokenID []byte) error {
	count, _, err := s.ReadMarketPairPriceCountStrict(sellTokenID, buyTokenID)
	if err != nil {
		return err
	}
	if count <= 1 {
		return s.DeleteMarketPairPriceCount(sellTokenID, buyTokenID)
	}
	return s.WriteMarketPairPriceCount(sellTokenID, buyTokenID, count-1)
}

// ReadMarketPriceList returns the rooted MarketPriceList for a pair. As with the
// prior rawdb reader it never returns nil: an absent entry yields a zero-value
// struct with the token ids set, because callers append to it in place.
// Corruption retains that shape while poisoning the StateDB.
func (s *StateDB) ReadMarketPriceList(sellTokenID, buyTokenID []byte) *corepb.MarketPriceList {
	pl, ok, err := s.ReadMarketPriceListStrict(sellTokenID, buyTokenID)
	if err != nil {
		s.recordStateError(fmt.Sprintf("read market price list %q/%q", sellTokenID, buyTokenID), err)
		return &corepb.MarketPriceList{SellTokenId: sellTokenID, BuyTokenId: buyTokenID}
	}
	if !ok {
		return &corepb.MarketPriceList{SellTokenId: sellTokenID, BuyTokenId: buyTokenID}
	}
	return pl
}

// ReadMarketPriceListStrict returns the rooted MarketPriceList for a pair and
// distinguishes an absent row from unreadable or malformed rooted data. The
// absent case mirrors ReadMarketPriceList's zero-but-non-nil result.
func (s *StateDB) ReadMarketPriceListStrict(sellTokenID, buyTokenID []byte) (*corepb.MarketPriceList, bool, error) {
	raw, ok, err := s.SystemKVGet(kvdomains.SystemMarket, marketPriceListKVKey(sellTokenID, buyTokenID))
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return &corepb.MarketPriceList{SellTokenId: sellTokenID, BuyTokenId: buyTokenID}, false, nil
	}
	if len(raw) == 0 {
		return nil, true, fmt.Errorf("decode market price list: empty value")
	}
	pl, err := decodeMarketPriceList(raw)
	if err != nil {
		return nil, true, fmt.Errorf("decode market price list: %w", err)
	}
	return pl, true, nil
}

// MarketPriceListAt reconstructs a rooted MarketPriceList at the end of
// blockNum. It mirrors ReadMarketPriceList's non-nil absent result, but
// surfaces malformed archive payloads as data errors.
func (r *PersistentHistoryReader) MarketPriceListAt(sellTokenID, buyTokenID []byte, blockNum uint64) (*corepb.MarketPriceList, error) {
	raw, ok, err := r.AccountKVAt(tcommon.SystemAccountAddress, kvdomains.SystemMarket, marketPriceListKVKey(sellTokenID, buyTokenID), blockNum)
	if err != nil {
		return nil, err
	}
	if !ok {
		return &corepb.MarketPriceList{SellTokenId: sellTokenID, BuyTokenId: buyTokenID}, nil
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("decode market price list at block %d: empty value", blockNum)
	}
	pl, err := decodeMarketPriceList(raw)
	if err != nil {
		return nil, fmt.Errorf("decode market price list at block %d: %w", blockNum, err)
	}
	return pl, nil
}

// WriteMarketPriceList stages the materialized MarketPriceList for a pair.
func (s *StateDB) WriteMarketPriceList(sellTokenID, buyTokenID []byte, pl *corepb.MarketPriceList) error {
	if pl == nil {
		pl = &corepb.MarketPriceList{SellTokenId: sellTokenID, BuyTokenId: buyTokenID}
	}
	index, indexDirty, err := s.updatedMarketPairIndex(sellTokenID, buyTokenID, len(pl.GetPrices()) > 0)
	if err != nil {
		return err
	}
	data, err := encodeMarketPriceList(pl)
	if err != nil {
		return err
	}
	if err := s.SystemKVPut(kvdomains.SystemMarket, marketPriceListKVKey(sellTokenID, buyTokenID), data); err != nil {
		return err
	}
	if !indexDirty {
		return nil
	}
	return s.writeMarketPairListIndex(index)
}

// ReadMarketPairList returns the rooted materialized list of market pairs.
// Absent rows are treated as an empty list. Malformed rows retain that return
// shape while poisoning the StateDB.
func (s *StateDB) ReadMarketPairList() *corepb.MarketOrderPairList {
	list, _, err := s.ReadMarketPairListStrict()
	if err != nil {
		s.recordStateError("read market pair index", err)
		return &corepb.MarketOrderPairList{}
	}
	return list
}

// ReadMarketPairListStrict returns the rooted materialized list of market pairs
// and distinguishes an absent row from unreadable or malformed rooted data.
func (s *StateDB) ReadMarketPairListStrict() (*corepb.MarketOrderPairList, bool, error) {
	raw, ok, err := s.SystemKVGet(kvdomains.SystemMarket, marketPairIndexKVKey())
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return &corepb.MarketOrderPairList{}, false, nil
	}
	if len(raw) == 0 {
		return nil, true, fmt.Errorf("decode market pair index: empty value")
	}
	list, err := decodeMarketPairList(raw)
	if err != nil {
		return nil, true, fmt.Errorf("decode market pair index: %w", err)
	}
	return list, true, nil
}

// MarketPairListAt reconstructs the materialized pair index at the end of
// blockNum, surfacing malformed archive payloads as data errors.
func (r *PersistentHistoryReader) MarketPairListAt(blockNum uint64) (*corepb.MarketOrderPairList, error) {
	raw, ok, err := r.AccountKVAt(tcommon.SystemAccountAddress, kvdomains.SystemMarket, marketPairIndexKVKey(), blockNum)
	if err != nil {
		return nil, err
	}
	if !ok {
		return &corepb.MarketOrderPairList{}, nil
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("decode market pair list at block %d: empty value", blockNum)
	}
	list, err := decodeMarketPairList(raw)
	if err != nil {
		return nil, fmt.Errorf("decode market pair list at block %d: %w", blockNum, err)
	}
	return list, nil
}

func (s *StateDB) writeMarketPairListIndex(list *corepb.MarketOrderPairList) error {
	if len(list.GetOrderPair()) == 0 {
		return s.SystemKVDelete(kvdomains.SystemMarket, marketPairIndexKVKey())
	}
	data, err := encodeMarketPairList(list)
	if err != nil {
		return err
	}
	return s.SystemKVPut(kvdomains.SystemMarket, marketPairIndexKVKey(), data)
}

func (s *StateDB) updatedMarketPairIndex(sellTokenID, buyTokenID []byte, active bool) (*corepb.MarketOrderPairList, bool, error) {
	list, _, err := s.ReadMarketPairListStrict()
	if err != nil {
		return nil, false, err
	}
	pairs := list.GetOrderPair()
	found := -1
	for i, pair := range pairs {
		if bytes.Equal(pair.GetSellTokenId(), sellTokenID) && bytes.Equal(pair.GetBuyTokenId(), buyTokenID) {
			found = i
			break
		}
	}
	switch {
	case active && found >= 0:
		return list, false, nil
	case active:
		list.OrderPair = append(pairs, &corepb.MarketOrderPair{
			SellTokenId: append([]byte(nil), sellTokenID...),
			BuyTokenId:  append([]byte(nil), buyTokenID...),
		})
	case found >= 0:
		list.OrderPair = append(pairs[:found], pairs[found+1:]...)
	default:
		return list, false, nil
	}
	return list, true, nil
}
