package state

import (
	"bytes"
	"testing"

	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

var assetIssueBenchmarkSink *contractpb.AssetIssueContract
var assetIssueIDBenchmarkSink int64
var assetBandwidthViewBenchmarkSink assetBandwidthView

func testAssetIssueContract() *contractpb.AssetIssueContract {
	return &contractpb.AssetIssueContract{
		Id:                      "1000001",
		OwnerAddress:            append([]byte{0x41}, bytes.Repeat([]byte{0x11}, 20)...),
		Name:                    []byte("native-token"),
		Abbr:                    []byte("NATIVE"),
		TotalSupply:             9_000_000,
		FrozenSupply:            []*contractpb.AssetIssueContract_FrozenSupply{{FrozenAmount: 1_000, FrozenDays: 3}},
		TrxNum:                  1,
		Precision:               6,
		Num:                     1_000_000,
		StartTime:               1_700_000_000_000,
		EndTime:                 1_800_000_000_000,
		VoteScore:               1,
		Description:             []byte("native asset metadata"),
		Url:                     []byte("https://example.invalid/native"),
		FreeAssetNetLimit:       1_000,
		PublicFreeAssetNetLimit: 2_000,
	}
}

func BenchmarkDecodeAssetIssueNative(b *testing.B) {
	raw, err := encodeAssetIssue(testAssetIssueContract())
	if err != nil {
		b.Fatal(err)
	}
	b.Run("Full", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			assetIssueBenchmarkSink, err = decodeAssetIssue(raw)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("IDOnly", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			assetIssueIDBenchmarkSink, err = decodeAssetIssueID(raw)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("BandwidthView", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			assetBandwidthViewBenchmarkSink, err = decodeAssetBandwidthView(raw)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("ValidateOnly", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err = validateAssetIssueNative(raw); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestDecodeAssetBandwidthViewMatchesFullAsset(t *testing.T) {
	asset := testAssetIssueContract()
	raw, err := encodeAssetIssue(asset)
	if err != nil {
		t.Fatal(err)
	}
	view, err := decodeAssetBandwidthView(raw)
	if err != nil {
		t.Fatal(err)
	}
	if view.tokenID != 1_000_001 || !bytes.Equal(view.owner[:], asset.OwnerAddress) ||
		view.freeAssetNetLimit != asset.FreeAssetNetLimit || view.publicFreeAssetNetLimit != asset.PublicFreeAssetNetLimit {
		t.Fatalf("bandwidth view = %+v, asset = %+v", view, asset)
	}
	if _, err := decodeAssetBandwidthView(raw[:len(raw)-1]); err == nil {
		t.Fatal("truncated asset metadata was accepted")
	}
}
