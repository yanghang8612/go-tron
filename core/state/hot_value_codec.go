package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"unicode/utf8"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Hot rooted values use hand-written, allocation-conscious codecs. The zero
// prefix is an invalid protobuf field tag, so accidental protobuf state fails
// closed. Each message kind owns a magic byte and an explicit version.
var hotValuePrefix = [...]byte{0x00, 'G', 'T'}

const (
	hotValueVersion byte = 1
	hotAsset        byte = 'A'
	hotExchange     byte = 'E'
	hotMarketOrder  byte = 'O'
	hotMarketAcct   byte = 'C'
	hotMarketBook   byte = 'B'
	hotMarketPrices byte = 'P'
	hotMarketPairs  byte = 'R'
)

type hotFieldSchema struct {
	number protoreflect.FieldNumber
	kind   protoreflect.Kind
	list   bool
}

func hotSchemaMatches(message proto.Message, expected ...hotFieldSchema) bool {
	fields := message.ProtoReflect().Descriptor().Fields()
	if fields.Len() != len(expected) {
		return false
	}
	for _, want := range expected {
		field := fields.ByNumber(want.number)
		if field == nil || field.Kind() != want.kind || field.IsList() != want.list || field.IsMap() {
			return false
		}
	}
	return true
}

var (
	hotAssetSchemaOK = hotSchemaMatches(new(contractpb.AssetIssueContract),
		hotFieldSchema{41, protoreflect.StringKind, false},
		hotFieldSchema{1, protoreflect.BytesKind, false}, hotFieldSchema{2, protoreflect.BytesKind, false}, hotFieldSchema{3, protoreflect.BytesKind, false},
		hotFieldSchema{4, protoreflect.Int64Kind, false}, hotFieldSchema{5, protoreflect.MessageKind, true},
		hotFieldSchema{6, protoreflect.Int32Kind, false}, hotFieldSchema{7, protoreflect.Int32Kind, false}, hotFieldSchema{8, protoreflect.Int32Kind, false},
		hotFieldSchema{9, protoreflect.Int64Kind, false}, hotFieldSchema{10, protoreflect.Int64Kind, false}, hotFieldSchema{11, protoreflect.Int64Kind, false},
		hotFieldSchema{16, protoreflect.Int32Kind, false}, hotFieldSchema{20, protoreflect.BytesKind, false}, hotFieldSchema{21, protoreflect.BytesKind, false},
		hotFieldSchema{22, protoreflect.Int64Kind, false}, hotFieldSchema{23, protoreflect.Int64Kind, false}, hotFieldSchema{24, protoreflect.Int64Kind, false}, hotFieldSchema{25, protoreflect.Int64Kind, false},
	) && hotSchemaMatches(new(contractpb.AssetIssueContract_FrozenSupply),
		hotFieldSchema{1, protoreflect.Int64Kind, false}, hotFieldSchema{2, protoreflect.Int64Kind, false},
	)
	hotExchangeSchemaOK = hotSchemaMatches(new(corepb.Exchange),
		hotFieldSchema{1, protoreflect.Int64Kind, false}, hotFieldSchema{2, protoreflect.BytesKind, false}, hotFieldSchema{3, protoreflect.Int64Kind, false},
		hotFieldSchema{6, protoreflect.BytesKind, false}, hotFieldSchema{7, protoreflect.Int64Kind, false}, hotFieldSchema{8, protoreflect.BytesKind, false}, hotFieldSchema{9, protoreflect.Int64Kind, false},
	)
	hotMarketOrderSchemaOK = hotSchemaMatches(new(corepb.MarketOrder),
		hotFieldSchema{1, protoreflect.BytesKind, false}, hotFieldSchema{2, protoreflect.BytesKind, false}, hotFieldSchema{3, protoreflect.Int64Kind, false},
		hotFieldSchema{4, protoreflect.BytesKind, false}, hotFieldSchema{5, protoreflect.Int64Kind, false}, hotFieldSchema{6, protoreflect.BytesKind, false}, hotFieldSchema{7, protoreflect.Int64Kind, false},
		hotFieldSchema{9, protoreflect.Int64Kind, false}, hotFieldSchema{10, protoreflect.Int64Kind, false}, hotFieldSchema{11, protoreflect.EnumKind, false},
		hotFieldSchema{12, protoreflect.BytesKind, false}, hotFieldSchema{13, protoreflect.BytesKind, false},
	)
	hotMarketAccountSchemaOK = hotSchemaMatches(new(corepb.MarketAccountOrder),
		hotFieldSchema{1, protoreflect.BytesKind, false}, hotFieldSchema{2, protoreflect.BytesKind, true},
		hotFieldSchema{3, protoreflect.Int64Kind, false}, hotFieldSchema{4, protoreflect.Int64Kind, false},
	)
	hotMarketBookSchemaOK = hotSchemaMatches(new(corepb.MarketOrderIdList),
		hotFieldSchema{1, protoreflect.BytesKind, false}, hotFieldSchema{2, protoreflect.BytesKind, false},
	)
	hotMarketPricesSchemaOK = hotSchemaMatches(new(corepb.MarketPriceList),
		hotFieldSchema{1, protoreflect.BytesKind, false}, hotFieldSchema{2, protoreflect.BytesKind, false}, hotFieldSchema{3, protoreflect.MessageKind, true},
	) && hotSchemaMatches(new(corepb.MarketPrice),
		hotFieldSchema{1, protoreflect.Int64Kind, false}, hotFieldSchema{2, protoreflect.Int64Kind, false},
	)
	hotMarketPairsSchemaOK = hotSchemaMatches(new(corepb.MarketOrderPairList),
		hotFieldSchema{1, protoreflect.MessageKind, true},
	) && hotSchemaMatches(new(corepb.MarketOrderPair),
		hotFieldSchema{1, protoreflect.BytesKind, false}, hotFieldSchema{2, protoreflect.BytesKind, false},
	)
)

func requireHotSchema(ok bool, name string) error {
	if !ok {
		return fmt.Errorf("%s protobuf schema changed; update hot rooted-state codec and version", name)
	}
	return nil
}

func hotSchemaStatus(kind byte) (bool, string) {
	switch kind {
	case hotAsset:
		return hotAssetSchemaOK, "AssetIssueContract"
	case hotExchange:
		return hotExchangeSchemaOK, "Exchange"
	case hotMarketOrder:
		return hotMarketOrderSchemaOK, "MarketOrder"
	case hotMarketAcct:
		return hotMarketAccountSchemaOK, "MarketAccountOrder"
	case hotMarketBook:
		return hotMarketBookSchemaOK, "MarketOrderIdList"
	case hotMarketPrices:
		return hotMarketPricesSchemaOK, "MarketPriceList"
	case hotMarketPairs:
		return hotMarketPairsSchemaOK, "MarketOrderPairList"
	default:
		return false, fmt.Sprintf("unknown hot value kind %#x", kind)
	}
}

func requireHotKindSchema(kind byte) error {
	ok, name := hotSchemaStatus(kind)
	return requireHotSchema(ok, name)
}

func appendHotHeader(dst []byte, kind byte) []byte {
	dst = append(dst, hotValuePrefix[:]...)
	return append(dst, kind, hotValueVersion)
}

func isHotValue(raw []byte, kind byte) bool {
	return len(raw) >= 5 && bytes.Equal(raw[:3], hotValuePrefix[:]) && raw[3] == kind
}

func hotPayload(raw []byte, kind byte) ([]byte, error) {
	if ok, name := hotSchemaStatus(kind); !ok {
		return nil, requireHotSchema(false, name)
	}
	if !isHotValue(raw, kind) {
		return nil, errors.New("non-native hot rooted-state value")
	}
	if raw[4] != hotValueVersion {
		return nil, fmt.Errorf("unsupported hot value version %d", raw[4])
	}
	return raw[5:], nil
}

type hotEncoder []byte

func (e hotEncoder) i32(v int32) hotEncoder { return binary.BigEndian.AppendUint32(e, uint32(v)) }
func (e hotEncoder) i64(v int64) hotEncoder { return binary.BigEndian.AppendUint64(e, uint64(v)) }
func (e hotEncoder) count(v int) hotEncoder { return binary.AppendUvarint(e, uint64(v)) }
func (e hotEncoder) bytes(v []byte) hotEncoder {
	e = e.count(len(v))
	return append(e, v...)
}
func (e hotEncoder) unknown(m proto.Message) hotEncoder {
	return e.bytes(m.ProtoReflect().GetUnknown())
}

type hotDecoder struct {
	raw []byte
	pos int
}

func (d *hotDecoder) remaining() int { return len(d.raw) - d.pos }
func (d *hotDecoder) finish() error {
	if d.remaining() != 0 {
		return fmt.Errorf("%d trailing bytes", d.remaining())
	}
	return nil
}
func (d *hotDecoder) i32() (int32, error) {
	if d.remaining() < 4 {
		return 0, errors.New("truncated int32")
	}
	v := int32(binary.BigEndian.Uint32(d.raw[d.pos:]))
	d.pos += 4
	return v, nil
}
func (d *hotDecoder) i64() (int64, error) {
	if d.remaining() < 8 {
		return 0, errors.New("truncated int64")
	}
	v := int64(binary.BigEndian.Uint64(d.raw[d.pos:]))
	d.pos += 8
	return v, nil
}
func (d *hotDecoder) count() (int, error) {
	if d.remaining() == 0 {
		return 0, errors.New("truncated length")
	}
	v, n := binary.Uvarint(d.raw[d.pos:])
	if n <= 0 || v > uint64(^uint(0)>>1) {
		return 0, errors.New("invalid length")
	}
	var canonical [binary.MaxVarintLen64]byte
	if n != binary.PutUvarint(canonical[:], v) {
		return 0, errors.New("non-canonical length")
	}
	d.pos += n
	return int(v), nil
}
func (d *hotDecoder) unknown(m proto.Message) error {
	v, err := d.bytes()
	if err != nil {
		return err
	}
	m.ProtoReflect().SetUnknown(v)
	return nil
}
func (d *hotDecoder) bytesView() ([]byte, error) {
	n, err := d.count()
	if err != nil {
		return nil, err
	}
	if n < 0 || n > d.remaining() {
		return nil, errors.New("truncated bytes")
	}
	v := d.raw[d.pos : d.pos+n]
	d.pos += n
	return v, nil
}
func (d *hotDecoder) bytes() ([]byte, error) {
	v, err := d.bytesView()
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), v...), nil
}

func encodeAssetIssue(c *contractpb.AssetIssueContract) ([]byte, error) {
	if c == nil {
		return nil, errors.New("nil asset issue")
	}
	if err := requireHotKindSchema(hotAsset); err != nil {
		return nil, err
	}
	if !utf8.ValidString(c.Id) {
		return nil, errors.New("invalid UTF-8 asset id")
	}
	e := hotEncoder(appendHotHeader(make([]byte, 0, 192), hotAsset))
	e = e.bytes([]byte(c.Id)).bytes(c.OwnerAddress).bytes(c.Name).bytes(c.Abbr).i64(c.TotalSupply)
	e = e.count(len(c.FrozenSupply))
	for _, frozen := range c.FrozenSupply {
		if frozen == nil {
			return nil, errors.New("nil asset frozen supply")
		}
		e = e.i64(frozen.FrozenAmount).i64(frozen.FrozenDays).unknown(frozen)
	}
	e = e.i32(c.TrxNum).i32(c.Precision).i32(c.Num)
	e = e.i64(c.StartTime).i64(c.EndTime).i64(c.Order).i32(c.VoteScore)
	e = e.bytes(c.Description).bytes(c.Url)
	// Public usage/time live in their own fixed-width row. Zero placeholders
	// keep this v1 layout stable while making accidental old-reader merging
	// deterministic.
	e = e.i64(c.FreeAssetNetLimit).i64(c.PublicFreeAssetNetLimit).i64(0).i64(0).unknown(c)
	return e, nil
}

func decodeAssetIssueNative(raw []byte) (*contractpb.AssetIssueContract, error) {
	payload, err := hotPayload(raw, hotAsset)
	if err != nil {
		return nil, err
	}
	d := hotDecoder{raw: payload}
	id, err := d.bytes()
	if err != nil {
		return nil, err
	}
	if !utf8.Valid(id) {
		return nil, errors.New("invalid UTF-8 asset id")
	}
	owner, err := d.bytes()
	if err != nil {
		return nil, err
	}
	name, err := d.bytes()
	if err != nil {
		return nil, err
	}
	abbr, err := d.bytes()
	if err != nil {
		return nil, err
	}
	total, err := d.i64()
	if err != nil {
		return nil, err
	}
	frozenCount, err := d.count()
	if err != nil {
		return nil, err
	}
	c := &contractpb.AssetIssueContract{Id: string(id), OwnerAddress: owner, Name: name, Abbr: abbr, TotalSupply: total}
	if frozenCount > d.remaining()/17 {
		return nil, errors.New("asset frozen supply count exceeds payload")
	}
	c.FrozenSupply = make([]*contractpb.AssetIssueContract_FrozenSupply, 0, frozenCount)
	for i := 0; i < frozenCount; i++ {
		amount, err := d.i64()
		if err != nil {
			return nil, err
		}
		days, err := d.i64()
		if err != nil {
			return nil, err
		}
		frozen := &contractpb.AssetIssueContract_FrozenSupply{FrozenAmount: amount, FrozenDays: days}
		if err := d.unknown(frozen); err != nil {
			return nil, err
		}
		c.FrozenSupply = append(c.FrozenSupply, frozen)
	}
	if c.TrxNum, err = d.i32(); err != nil {
		return nil, err
	}
	if c.Precision, err = d.i32(); err != nil {
		return nil, err
	}
	if c.Num, err = d.i32(); err != nil {
		return nil, err
	}
	if c.StartTime, err = d.i64(); err != nil {
		return nil, err
	}
	if c.EndTime, err = d.i64(); err != nil {
		return nil, err
	}
	if c.Order, err = d.i64(); err != nil {
		return nil, err
	}
	if c.VoteScore, err = d.i32(); err != nil {
		return nil, err
	}
	if c.Description, err = d.bytes(); err != nil {
		return nil, err
	}
	if c.Url, err = d.bytes(); err != nil {
		return nil, err
	}
	if c.FreeAssetNetLimit, err = d.i64(); err != nil {
		return nil, err
	}
	if c.PublicFreeAssetNetLimit, err = d.i64(); err != nil {
		return nil, err
	}
	if c.PublicFreeAssetNetUsage, err = d.i64(); err != nil {
		return nil, err
	}
	if c.PublicFreeAssetNetUsage != 0 {
		return nil, errors.New("non-canonical asset metadata public usage placeholder")
	}
	if c.PublicLatestFreeNetTime, err = d.i64(); err != nil {
		return nil, err
	}
	if c.PublicLatestFreeNetTime != 0 {
		return nil, errors.New("non-canonical asset metadata public time placeholder")
	}
	if err := d.unknown(c); err != nil {
		return nil, err
	}
	return c, d.finish()
}

// validateAssetIssueNative validates the complete hot AssetIssue layout without
// allocating the message and byte fields. TransferAsset only needs an existence
// predicate, but existence must not turn malformed metadata into valid state.
func validateAssetIssueNative(raw []byte) error {
	_, err := validateAssetIssueNativeID(raw)
	return err
}

// validateAssetIssueNativeID performs the same full structural scan as
// validateAssetIssueNative and returns a borrowed view of the first ID field.
// The view is valid only while raw remains alive and unchanged.
func validateAssetIssueNativeID(raw []byte) ([]byte, error) {
	payload, err := hotPayload(raw, hotAsset)
	if err != nil {
		return nil, err
	}
	d := hotDecoder{raw: payload}
	id, err := d.bytesView()
	if err != nil {
		return nil, err
	}
	if !utf8.Valid(id) {
		return nil, errors.New("invalid UTF-8 asset id")
	}
	for range 3 { // owner, name, abbreviation
		if _, err := d.bytesView(); err != nil {
			return nil, err
		}
	}
	if _, err := d.i64(); err != nil { // total supply
		return nil, err
	}
	frozenCount, err := d.count()
	if err != nil {
		return nil, err
	}
	if frozenCount > d.remaining()/17 {
		return nil, errors.New("asset frozen supply count exceeds payload")
	}
	for range frozenCount {
		if _, err := d.i64(); err != nil {
			return nil, err
		}
		if _, err := d.i64(); err != nil {
			return nil, err
		}
		if _, err := d.bytesView(); err != nil { // nested unknown fields
			return nil, err
		}
	}
	for range 3 { // trx_num, precision, num
		if _, err := d.i32(); err != nil {
			return nil, err
		}
	}
	for range 3 { // start_time, end_time, order
		if _, err := d.i64(); err != nil {
			return nil, err
		}
	}
	if _, err := d.i32(); err != nil { // vote_score
		return nil, err
	}
	for range 2 { // description, URL
		if _, err := d.bytesView(); err != nil {
			return nil, err
		}
	}
	for range 2 { // free and public net limits
		if _, err := d.i64(); err != nil {
			return nil, err
		}
	}
	usage, err := d.i64()
	if err != nil {
		return nil, err
	}
	latest, err := d.i64()
	if err != nil {
		return nil, err
	}
	if usage != 0 || latest != 0 {
		return nil, errors.New("non-canonical asset metadata bandwidth placeholders")
	}
	if _, err := d.bytesView(); err != nil { // top-level unknown fields
		return nil, err
	}
	if err := d.finish(); err != nil {
		return nil, err
	}
	return id, nil
}

func encodeExchange(ex *corepb.Exchange) ([]byte, error) {
	if ex == nil {
		return nil, errors.New("nil exchange")
	}
	if err := requireHotKindSchema(hotExchange); err != nil {
		return nil, err
	}
	e := hotEncoder(appendHotHeader(make([]byte, 0, 96), hotExchange))
	e = e.i64(ex.ExchangeId).bytes(ex.CreatorAddress).i64(ex.CreateTime)
	e = e.bytes(ex.FirstTokenId).i64(ex.FirstTokenBalance).bytes(ex.SecondTokenId).i64(ex.SecondTokenBalance).unknown(ex)
	return e, nil
}

func decodeExchange(raw []byte) (*corepb.Exchange, error) {
	payload, err := hotPayload(raw, hotExchange)
	if err != nil {
		return nil, err
	}
	d := hotDecoder{raw: payload}
	ex := &corepb.Exchange{}
	if ex.ExchangeId, err = d.i64(); err != nil {
		return nil, err
	}
	if ex.CreatorAddress, err = d.bytes(); err != nil {
		return nil, err
	}
	if ex.CreateTime, err = d.i64(); err != nil {
		return nil, err
	}
	if ex.FirstTokenId, err = d.bytes(); err != nil {
		return nil, err
	}
	if ex.FirstTokenBalance, err = d.i64(); err != nil {
		return nil, err
	}
	if ex.SecondTokenId, err = d.bytes(); err != nil {
		return nil, err
	}
	if ex.SecondTokenBalance, err = d.i64(); err != nil {
		return nil, err
	}
	if err := d.unknown(ex); err != nil {
		return nil, err
	}
	return ex, d.finish()
}

func encodeMarketOrder(o *corepb.MarketOrder) ([]byte, error) {
	if o == nil {
		return nil, errors.New("nil market order")
	}
	if err := requireHotKindSchema(hotMarketOrder); err != nil {
		return nil, err
	}
	e := hotEncoder(appendHotHeader(make([]byte, 0, 160), hotMarketOrder))
	e = e.bytes(o.OrderId).bytes(o.OwnerAddress).i64(o.CreateTime).bytes(o.SellTokenId).i64(o.SellTokenQuantity)
	e = e.bytes(o.BuyTokenId).i64(o.BuyTokenQuantity).i64(o.SellTokenQuantityRemain).i64(o.SellTokenQuantityReturn)
	e = e.i32(int32(o.State)).bytes(o.Prev).bytes(o.Next).unknown(o)
	return e, nil
}

func decodeMarketOrder(raw []byte) (*corepb.MarketOrder, error) {
	payload, err := hotPayload(raw, hotMarketOrder)
	if err != nil {
		return nil, err
	}
	d := hotDecoder{raw: payload}
	o := &corepb.MarketOrder{}
	if o.OrderId, err = d.bytes(); err != nil {
		return nil, err
	}
	if o.OwnerAddress, err = d.bytes(); err != nil {
		return nil, err
	}
	if o.CreateTime, err = d.i64(); err != nil {
		return nil, err
	}
	if o.SellTokenId, err = d.bytes(); err != nil {
		return nil, err
	}
	if o.SellTokenQuantity, err = d.i64(); err != nil {
		return nil, err
	}
	if o.BuyTokenId, err = d.bytes(); err != nil {
		return nil, err
	}
	if o.BuyTokenQuantity, err = d.i64(); err != nil {
		return nil, err
	}
	if o.SellTokenQuantityRemain, err = d.i64(); err != nil {
		return nil, err
	}
	if o.SellTokenQuantityReturn, err = d.i64(); err != nil {
		return nil, err
	}
	state, err := d.i32()
	if err != nil {
		return nil, err
	}
	o.State = corepb.MarketOrder_State(state)
	if o.Prev, err = d.bytes(); err != nil {
		return nil, err
	}
	if o.Next, err = d.bytes(); err != nil {
		return nil, err
	}
	if err := d.unknown(o); err != nil {
		return nil, err
	}
	return o, d.finish()
}

func encodeMarketAccountOrder(o *corepb.MarketAccountOrder) ([]byte, error) {
	if o == nil {
		return nil, errors.New("nil market account order")
	}
	if err := requireHotKindSchema(hotMarketAcct); err != nil {
		return nil, err
	}
	e := hotEncoder(appendHotHeader(make([]byte, 0, 96), hotMarketAcct)).bytes(o.OwnerAddress).count(len(o.Orders))
	for _, id := range o.Orders {
		e = e.bytes(id)
	}
	return e.i64(o.Count).i64(o.TotalCount).unknown(o), nil
}

func decodeMarketAccountOrder(raw []byte) (*corepb.MarketAccountOrder, error) {
	payload, err := hotPayload(raw, hotMarketAcct)
	if err != nil {
		return nil, err
	}
	d := hotDecoder{raw: payload}
	o := &corepb.MarketAccountOrder{}
	if o.OwnerAddress, err = d.bytes(); err != nil {
		return nil, err
	}
	n, err := d.count()
	if err != nil {
		return nil, err
	}
	if n > d.remaining() {
		return nil, errors.New("market account order count exceeds payload")
	}
	o.Orders = make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		id, err := d.bytes()
		if err != nil {
			return nil, err
		}
		o.Orders = append(o.Orders, id)
	}
	if o.Count, err = d.i64(); err != nil {
		return nil, err
	}
	if o.TotalCount, err = d.i64(); err != nil {
		return nil, err
	}
	if err := d.unknown(o); err != nil {
		return nil, err
	}
	return o, d.finish()
}

func encodeMarketOrderBook(o *corepb.MarketOrderIdList) ([]byte, error) {
	if o == nil {
		return nil, errors.New("nil market order book")
	}
	if err := requireHotKindSchema(hotMarketBook); err != nil {
		return nil, err
	}
	return hotEncoder(appendHotHeader(make([]byte, 0, 80), hotMarketBook)).bytes(o.Head).bytes(o.Tail).unknown(o), nil
}

func decodeMarketOrderBook(raw []byte) (*corepb.MarketOrderIdList, error) {
	payload, err := hotPayload(raw, hotMarketBook)
	if err != nil {
		return nil, err
	}
	d := hotDecoder{raw: payload}
	o := &corepb.MarketOrderIdList{}
	if o.Head, err = d.bytes(); err != nil {
		return nil, err
	}
	if o.Tail, err = d.bytes(); err != nil {
		return nil, err
	}
	if err := d.unknown(o); err != nil {
		return nil, err
	}
	return o, d.finish()
}

func encodeMarketPriceList(pl *corepb.MarketPriceList) ([]byte, error) {
	if pl == nil {
		return nil, errors.New("nil market price list")
	}
	if err := requireHotKindSchema(hotMarketPrices); err != nil {
		return nil, err
	}
	e := hotEncoder(appendHotHeader(make([]byte, 0, 80+16*len(pl.Prices)), hotMarketPrices)).bytes(pl.SellTokenId).bytes(pl.BuyTokenId).count(len(pl.Prices))
	for _, price := range pl.Prices {
		if price == nil {
			return nil, errors.New("nil market price")
		}
		e = e.i64(price.SellTokenQuantity).i64(price.BuyTokenQuantity).unknown(price)
	}
	return e.unknown(pl), nil
}

func decodeMarketPriceList(raw []byte) (*corepb.MarketPriceList, error) {
	payload, err := hotPayload(raw, hotMarketPrices)
	if err != nil {
		return nil, err
	}
	d := hotDecoder{raw: payload}
	pl := &corepb.MarketPriceList{}
	if pl.SellTokenId, err = d.bytes(); err != nil {
		return nil, err
	}
	if pl.BuyTokenId, err = d.bytes(); err != nil {
		return nil, err
	}
	n, err := d.count()
	if err != nil {
		return nil, err
	}
	if n > d.remaining()/17 {
		return nil, errors.New("market price count exceeds payload")
	}
	pl.Prices = make([]*corepb.MarketPrice, 0, n)
	for i := 0; i < n; i++ {
		sell, err := d.i64()
		if err != nil {
			return nil, err
		}
		buy, err := d.i64()
		if err != nil {
			return nil, err
		}
		price := &corepb.MarketPrice{SellTokenQuantity: sell, BuyTokenQuantity: buy}
		if err := d.unknown(price); err != nil {
			return nil, err
		}
		pl.Prices = append(pl.Prices, price)
	}
	if err := d.unknown(pl); err != nil {
		return nil, err
	}
	return pl, d.finish()
}

func encodeMarketPairList(list *corepb.MarketOrderPairList) ([]byte, error) {
	if list == nil {
		return nil, errors.New("nil market pair list")
	}
	if err := requireHotKindSchema(hotMarketPairs); err != nil {
		return nil, err
	}
	e := hotEncoder(appendHotHeader(make([]byte, 0, 64), hotMarketPairs)).count(len(list.OrderPair))
	for _, pair := range list.OrderPair {
		if pair == nil {
			return nil, errors.New("nil market pair")
		}
		e = e.bytes(pair.SellTokenId).bytes(pair.BuyTokenId).unknown(pair)
	}
	return e.unknown(list), nil
}

func decodeMarketPairList(raw []byte) (*corepb.MarketOrderPairList, error) {
	payload, err := hotPayload(raw, hotMarketPairs)
	if err != nil {
		return nil, err
	}
	d := hotDecoder{raw: payload}
	list := &corepb.MarketOrderPairList{}
	n, err := d.count()
	if err != nil {
		return nil, err
	}
	if n > d.remaining() {
		return nil, errors.New("market pair count exceeds payload")
	}
	list.OrderPair = make([]*corepb.MarketOrderPair, 0, n)
	for i := 0; i < n; i++ {
		sell, err := d.bytes()
		if err != nil {
			return nil, err
		}
		buy, err := d.bytes()
		if err != nil {
			return nil, err
		}
		pair := &corepb.MarketOrderPair{SellTokenId: sell, BuyTokenId: buy}
		if err := d.unknown(pair); err != nil {
			return nil, err
		}
		list.OrderPair = append(list.OrderPair, pair)
	}
	if err := d.unknown(list); err != nil {
		return nil, err
	}
	return list, d.finish()
}
