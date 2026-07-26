package state

import (
	"bytes"
	"testing"
	"unsafe"

	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

var assetIssueTimeBenchmarkSink int64
var assetIssueBenchmarkSink *contractpb.AssetIssueContract

func testAssetIssueContract() *contractpb.AssetIssueContract {
	return &contractpb.AssetIssueContract{
		Id:           "1000001",
		OwnerAddress: append([]byte{0x41}, bytes.Repeat([]byte{0x11}, 20)...),
		Name:         []byte("arena-token"),
		Abbr:         []byte("ARENA"),
		TotalSupply:  9_000_000,
		FrozenSupply: []*contractpb.AssetIssueContract_FrozenSupply{{
			FrozenAmount: 1_000,
			FrozenDays:   3,
		}},
		TrxNum:                  1,
		Precision:               6,
		Num:                     1_000_000,
		StartTime:               1_700_000_000_000,
		EndTime:                 1_800_000_000_000,
		VoteScore:               1,
		Description:             []byte("asset metadata byte fields share one decoding arena"),
		Url:                     []byte("https://example.invalid/arena"),
		FreeAssetNetLimit:       1_000,
		PublicFreeAssetNetLimit: 2_000,
	}
}

func TestDecodeAssetIssueMatchesGeneratedDecoder(t *testing.T) {
	raw, err := proto.Marshal(testAssetIssueContract())
	if err != nil {
		t.Fatal(err)
	}
	// Unknown fields must still be retained by the generated decoder.
	raw = protowire.AppendTag(raw, 99, protowire.BytesType)
	raw = protowire.AppendBytes(raw, []byte("future-field"))

	want := &contractpb.AssetIssueContract{}
	if err := proto.Unmarshal(raw, want); err != nil {
		t.Fatal(err)
	}
	got, err := decodeAssetIssue(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, want) {
		t.Fatalf("decoded asset mismatch\n got: %v\nwant: %v", got, want)
	}

	ownerBefore := append([]byte(nil), got.OwnerAddress...)
	for i := range raw {
		raw[i] ^= 0xff
	}
	if !bytes.Equal(got.OwnerAddress, ownerBefore) {
		t.Fatal("decoded asset aliases the input wire buffer")
	}
}

func TestDecodeAssetIssueByteFieldsShareArena(t *testing.T) {
	raw, err := proto.Marshal(testAssetIssueContract())
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeAssetIssue(raw)
	if err != nil {
		t.Fatal(err)
	}
	fields := [][]byte{got.OwnerAddress, got.Name, got.Abbr, got.Description, got.Url}
	for i := 1; i < len(fields); i++ {
		previousEnd := uintptr(unsafe.Pointer(unsafe.SliceData(fields[i-1]))) + uintptr(cap(fields[i-1]))
		currentStart := uintptr(unsafe.Pointer(unsafe.SliceData(fields[i])))
		if currentStart != previousEnd {
			t.Fatalf("byte fields %d and %d do not occupy adjacent arena spans", i-1, i)
		}
	}
}

func TestDecodeAssetIssueDuplicateByteFieldKeepsLastValue(t *testing.T) {
	raw, err := proto.Marshal(testAssetIssueContract())
	if err != nil {
		t.Fatal(err)
	}
	large := bytes.Repeat([]byte("x"), 257)
	raw = protowire.AppendTag(raw, 20, protowire.BytesType)
	raw = protowire.AppendBytes(raw, large)
	raw = protowire.AppendTag(raw, 20, protowire.BytesType)
	raw = protowire.AppendBytes(raw, []byte("last"))

	got, err := decodeAssetIssue(raw)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Description) != "last" {
		t.Fatalf("duplicate-field last value: got %q", got.Description)
	}
	if cap(got.Description) != len("last") {
		t.Fatalf("description arena span: got %d, want %d", cap(got.Description), len("last"))
	}
}

func TestDecodeAssetIssueLargeByteFieldsUseOwnedFallbackArena(t *testing.T) {
	asset := testAssetIssueContract()
	asset.Description = bytes.Repeat([]byte("d"), assetIssueInlineByteArenaSize)
	asset.Url = bytes.Repeat([]byte("u"), 97)
	raw, err := proto.Marshal(asset)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeAssetIssue(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, asset) {
		t.Fatalf("large decoded asset mismatch\n got: %v\nwant: %v", got, asset)
	}
	for i := range raw {
		raw[i] = 0
	}
	if !bytes.Equal(got.Description, asset.Description) || !bytes.Equal(got.Url, asset.Url) {
		t.Fatal("large decoded fields alias the input wire buffer")
	}
}

func TestDecodeAssetIssueMalformedMatchesGeneratedError(t *testing.T) {
	raw, err := proto.Marshal(testAssetIssueContract())
	if err != nil {
		t.Fatal(err)
	}
	malformed := append(raw, 0x80)

	want := &contractpb.AssetIssueContract{}
	wantErr := proto.Unmarshal(malformed, want)
	_, gotErr := decodeAssetIssue(malformed)
	if wantErr == nil || gotErr == nil {
		t.Fatalf("malformed errors: optimized=%v generated=%v", gotErr, wantErr)
	}
	if gotErr.Error() != wantErr.Error() {
		t.Fatalf("malformed error mismatch: optimized=%q generated=%q", gotErr, wantErr)
	}
}

func FuzzDecodeAssetIssueMatchesGeneratedDecoder(f *testing.F) {
	valid, err := proto.Marshal(testAssetIssueContract())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add([]byte(nil))
	f.Add([]byte{0x80})
	f.Add(protowire.AppendBytes(protowire.AppendTag(nil, 20, protowire.BytesType), []byte("description")))
	f.Fuzz(func(t *testing.T, raw []byte) {
		want := &contractpb.AssetIssueContract{}
		wantErr := proto.Unmarshal(raw, want)
		got, gotErr := decodeAssetIssue(raw)
		if (gotErr != nil) != (wantErr != nil) {
			t.Fatalf("error mismatch: optimized=%v generated=%v raw=%x", gotErr, wantErr, raw)
		}
		if gotErr == nil && !proto.Equal(got, want) {
			t.Fatalf("value mismatch\nraw: %x\n got: %v\nwant: %v", raw, got, want)
		}
	})
}

func BenchmarkStateDBReadAssetIssueTimeDirty(b *testing.B) {
	sdb := newTestStateDB(b)
	if err := sdb.WriteAssetIssueTime(1_000_001, 1_234_567); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		assetIssueTimeBenchmarkSink = sdb.ReadAssetIssueTime(1_000_001)
	}
}

func BenchmarkDecodeAssetIssue(b *testing.B) {
	raw, err := proto.Marshal(testAssetIssueContract())
	if err != nil {
		b.Fatal(err)
	}
	b.Run("Generated", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			asset := &contractpb.AssetIssueContract{}
			if err := proto.Unmarshal(raw, asset); err != nil {
				b.Fatal(err)
			}
			assetIssueBenchmarkSink = asset
		}
	})
	b.Run("ByteArena", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			assetIssueBenchmarkSink, err = decodeAssetIssue(raw)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
