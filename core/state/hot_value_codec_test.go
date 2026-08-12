package state

import (
	"bytes"
	"testing"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func hotUnknown(tag protowire.Number, value uint64) []byte {
	raw := protowire.AppendTag(nil, tag, protowire.VarintType)
	return protowire.AppendVarint(raw, value)
}

func TestHotValueCodecsNativeRoundTrip(t *testing.T) {
	asset := testAssetIssueContract()
	asset.PublicFreeAssetNetUsage = 91
	asset.PublicLatestFreeNetTime = 92
	asset.ProtoReflect().SetUnknown(hotUnknown(99, 1))
	asset.FrozenSupply[0].ProtoReflect().SetUnknown(hotUnknown(98, 2))
	assetRaw, err := encodeAssetIssue(asset)
	if err != nil {
		t.Fatal(err)
	}
	if !isHotValue(assetRaw, hotAsset) {
		t.Fatalf("asset native prefix: %x", assetRaw[:5])
	}
	gotAsset, err := decodeAssetIssueNative(assetRaw)
	if err != nil {
		t.Fatalf("decode native asset: %v", err)
	}
	wantAsset := proto.Clone(asset).(*contractpb.AssetIssueContract)
	wantAsset.PublicFreeAssetNetUsage, wantAsset.PublicLatestFreeNetTime = 0, 0
	if !proto.Equal(gotAsset, wantAsset) {
		t.Fatalf("asset round trip\n got %v\nwant %v", gotAsset, wantAsset)
	}

	ex := &corepb.Exchange{ExchangeId: 7, CreatorAddress: []byte("owner"), CreateTime: 8, FirstTokenId: []byte("A"), FirstTokenBalance: 9, SecondTokenId: []byte("B"), SecondTokenBalance: 10}
	ex.ProtoReflect().SetUnknown(hotUnknown(99, 3))
	assertHotRoundTrip(t, ex, encodeExchange, decodeExchange)

	order := marketOrder(1, 2, []byte("A"), 3, []byte("B"), 4)
	order.CreateTime, order.SellTokenQuantityReturn, order.Prev, order.Next = 5, 6, []byte("p"), []byte("n")
	order.ProtoReflect().SetUnknown(hotUnknown(99, 4))
	assertHotRoundTrip(t, order, encodeMarketOrder, decodeMarketOrder)

	acct := &corepb.MarketAccountOrder{OwnerAddress: []byte("owner"), Orders: [][]byte{[]byte("a"), []byte("b")}, Count: 2, TotalCount: 3}
	acct.ProtoReflect().SetUnknown(hotUnknown(99, 5))
	assertHotRoundTrip(t, acct, encodeMarketAccountOrder, decodeMarketAccountOrder)

	book := &corepb.MarketOrderIdList{Head: []byte("h"), Tail: []byte("t")}
	book.ProtoReflect().SetUnknown(hotUnknown(99, 6))
	assertHotRoundTrip(t, book, encodeMarketOrderBook, decodeMarketOrderBook)

	price := &corepb.MarketPrice{SellTokenQuantity: 11, BuyTokenQuantity: 12}
	price.ProtoReflect().SetUnknown(hotUnknown(98, 7))
	prices := &corepb.MarketPriceList{SellTokenId: []byte("A"), BuyTokenId: []byte("B"), Prices: []*corepb.MarketPrice{price}}
	prices.ProtoReflect().SetUnknown(hotUnknown(99, 8))
	assertHotRoundTrip(t, prices, encodeMarketPriceList, decodeMarketPriceList)

	pair := &corepb.MarketOrderPair{SellTokenId: []byte("A"), BuyTokenId: []byte("B")}
	pair.ProtoReflect().SetUnknown(hotUnknown(98, 9))
	pairs := &corepb.MarketOrderPairList{OrderPair: []*corepb.MarketOrderPair{pair}}
	pairs.ProtoReflect().SetUnknown(hotUnknown(99, 10))
	assertHotRoundTrip(t, pairs, encodeMarketPairList, decodeMarketPairList)
}

func TestHotValueCodecSchemaGuardsMatchCurrentProtocol(t *testing.T) {
	for name, ok := range map[string]bool{
		"asset": hotAssetSchemaOK, "exchange": hotExchangeSchemaOK,
		"market-order": hotMarketOrderSchemaOK, "market-account": hotMarketAccountSchemaOK,
		"market-book": hotMarketBookSchemaOK, "market-prices": hotMarketPricesSchemaOK,
		"market-pairs": hotMarketPairsSchemaOK,
	} {
		if !ok {
			t.Errorf("%s schema guard does not match current protobuf descriptors", name)
		}
	}
}

func TestAccountRowCodecSchemaGuardsMatchExactProtocolLayout(t *testing.T) {
	for name, ok := range map[string]bool{
		"permission":  accountPermissionRowSchemaOK,
		"vote":        accountVoteRowSchemaOK,
		"frozen":      accountFrozenRowSchemaOK,
		"unfrozen-v2": accountUnfrozenV2SchemaOK,
		"resource":    accountResourceRowSchemaOK,
	} {
		if !ok {
			t.Errorf("%s account-row schema guard does not match current protobuf descriptors", name)
		}
	}
	wrongKind := append([]accountFieldSchema(nil), accountVoteSchema...)
	wrongKind[0].kind = protoreflect.Int64Kind
	if accountMessageSchemaMatches(new(corepb.Vote), wrongKind...) {
		t.Fatal("account-row schema guard accepted a field-kind mismatch")
	}
	wrongCardinality := append([]accountFieldSchema(nil), accountVoteSchema...)
	wrongCardinality[0].list = true
	if accountMessageSchemaMatches(new(corepb.Vote), wrongCardinality...) {
		t.Fatal("account-row schema guard accepted a cardinality mismatch")
	}
}

func TestHotValueCodecRejectsNonCanonicalLength(t *testing.T) {
	raw, err := encodeAssetIssue(testAssetIssueContract())
	if err != nil {
		t.Fatal(err)
	}
	if raw[5] >= 0x80 {
		t.Fatalf("test requires one-byte first-field length, got %#x", raw[5])
	}
	corrupt := make([]byte, 0, len(raw)+1)
	corrupt = append(corrupt, raw[:5]...)
	corrupt = append(corrupt, raw[5]|0x80, 0x00)
	corrupt = append(corrupt, raw[6:]...)
	if _, err := decodeAssetIssueNative(corrupt); err == nil {
		t.Fatal("non-canonical hot-value length was accepted")
	}
}

func TestAssetCodecRejectsNonCanonicalMetadataBandwidthPlaceholders(t *testing.T) {
	raw, err := encodeAssetIssue(testAssetIssueContract())
	if err != nil {
		t.Fatal(err)
	}
	// The two split bandwidth placeholders are the final four fixed-width
	// integers before the top-level unknown-field trailer. Locate them through
	// the decoder so the test does not depend on earlier variable field lengths.
	payload, err := hotPayload(raw, hotAsset)
	if err != nil {
		t.Fatal(err)
	}
	d := hotDecoder{raw: payload}
	for range 4 {
		if _, err := d.bytesView(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.i64(); err != nil {
		t.Fatal(err)
	}
	n, err := d.count()
	if err != nil {
		t.Fatal(err)
	}
	for range n {
		for range 2 {
			if _, err := d.i64(); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := d.bytesView(); err != nil {
			t.Fatal(err)
		}
	}
	d.pos += 3*4 + 3*8 + 4
	for range 2 {
		if _, err := d.bytesView(); err != nil {
			t.Fatal(err)
		}
	}
	d.pos += 2 * 8 // free/public limits
	corrupt := bytes.Clone(raw)
	corrupt[len(hotValuePrefix)+2+d.pos] = 1
	if _, err := decodeAssetIssueNative(corrupt); err == nil {
		t.Fatal("non-zero metadata bandwidth placeholder was accepted")
	}
}

func assertHotRoundTrip[T proto.Message](t *testing.T, want T, encode func(T) ([]byte, error), decode func([]byte) (T, error)) {
	t.Helper()
	native, err := encode(want)
	if err != nil {
		t.Fatal(err)
	}
	if len(native) == 0 || native[0] != 0 {
		t.Fatalf("native codec prefix = %x", native)
	}
	got, err := decode(native)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, want) {
		t.Fatalf("native round trip\n got %v\nwant %v", got, want)
	}
	protobufWire, err := proto.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = decode(protobufWire); err == nil {
		t.Fatal("protobuf wire value was accepted as rooted state")
	}
}

func TestAssetStoreSplitsBandwidthAndNativeIDRead(t *testing.T) {
	sdb := newTestStateDB(t)
	asset := testAssetIssueContract()
	asset.PublicFreeAssetNetUsage = 123
	asset.PublicLatestFreeNetTime = 456
	id := int64(1_000_001)
	if err := sdb.WriteAssetIssue(id, asset); err != nil {
		t.Fatal(err)
	}

	metaRaw, ok, err := sdb.SystemKVGet(kvdomains.SystemAsset, assetIDKey(assetV2Tag, id))
	if err != nil || !ok {
		t.Fatalf("metadata read: ok=%v err=%v", ok, err)
	}
	if !isHotValue(metaRaw, hotAsset) {
		t.Fatalf("metadata is not native: %x", metaRaw[:min(5, len(metaRaw))])
	}
	hotRaw, ok, err := sdb.SystemKVGet(kvdomains.SystemAsset, assetIDKey(assetV2BandwidthTag, id))
	if err != nil || !ok {
		t.Fatalf("bandwidth read: ok=%v err=%v", ok, err)
	}
	if len(hotRaw) != 17 {
		t.Fatalf("bandwidth value length = %d", len(hotRaw))
	}

	got := sdb.ReadAssetIssue(id)
	if !proto.Equal(got, asset) {
		t.Fatalf("merged asset\n got %v\nwant %v", got, asset)
	}
	gotID, err := decodeAssetIssueID(metaRaw)
	if err != nil || gotID != id {
		t.Fatalf("native ID = %d err=%v", gotID, err)
	}

	if err := sdb.WriteAssetIssueBandwidth(id, 777, 888); err != nil {
		t.Fatal(err)
	}
	metaAfter, _, _ := sdb.SystemKVGet(kvdomains.SystemAsset, assetIDKey(assetV2Tag, id))
	if !bytes.Equal(metaAfter, metaRaw) {
		t.Fatal("bandwidth-only write rewrote static metadata")
	}
	got = sdb.ReadAssetIssue(id)
	if got.PublicFreeAssetNetUsage != 777 || got.PublicLatestFreeNetTime != 888 {
		t.Fatalf("updated bandwidth: %+v", got)
	}
}

func TestHasAssetIssueRejectsMalformedNativeValue(t *testing.T) {
	sdb := newTestStateDB(t)
	const id = int64(1_000_001)
	if err := sdb.SystemKVPut(kvdomains.SystemAsset, assetIDKey(assetV2Tag, id), []byte{1}); err != nil {
		t.Fatal(err)
	}
	if sdb.HasAssetIssue(id) {
		t.Fatal("malformed asset metadata was accepted as an existing asset")
	}
	if sdb.Error() == nil {
		t.Fatal("malformed asset metadata did not poison the StateDB")
	}
}

func TestAssetIssueIDFastPathValidatesCompleteNativeRow(t *testing.T) {
	sdb := newTestStateDB(t)
	asset := testAssetIssueContract()
	raw, err := encodeAssetIssue(asset)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := hotPayload(raw, hotAsset)
	if err != nil {
		t.Fatal(err)
	}
	d := hotDecoder{raw: payload}
	if _, err := d.bytesView(); err != nil {
		t.Fatal(err)
	}
	// Keep the complete header and ID field while dropping every subsequent
	// field. The former prefix-only implementation accepted this row.
	truncated := append([]byte(nil), raw[:len(raw)-len(d.raw)]...)
	if _, err := decodeAssetIssueID(truncated); err == nil {
		t.Fatal("ID fast path accepted truncated asset metadata")
	}

	name := []byte("truncated-token")
	if err := sdb.SystemKVPut(kvdomains.SystemAsset, assetBytesKey(assetLegacyTag, name), truncated); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := sdb.ReadAssetIssueIDByName(name); err == nil || !ok {
		t.Fatalf("legacy ID read: ok=%v err=%v, want present decode error", ok, err)
	}
	if sdb.Error() == nil {
		t.Fatal("truncated asset metadata did not poison StateDB")
	}
}

func TestAssetScalarReadersRejectTrailingBytes(t *testing.T) {
	sdb := newTestStateDB(t)
	name := []byte("token")
	key := assetBytesKey(assetNameIndexTag, name)
	if err := sdb.SystemKVPut(kvdomains.SystemAsset, key, make([]byte, 9)); err != nil {
		t.Fatal(err)
	}
	if _, ok := sdb.ReadAssetNameIndex(name); ok {
		t.Fatal("non-strict asset index reader accepted trailing bytes")
	}
	if _, ok, err := sdb.ReadAssetNameIndexStrict(name); err == nil || !ok {
		t.Fatalf("strict asset index read: ok=%v err=%v", ok, err)
	}
}
