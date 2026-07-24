package types

import (
	"bytes"
	"fmt"
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestUnmarshalAccountDirectMapsEquivalent(t *testing.T) {
	for entries := 1; entries <= 64; entries *= 2 {
		original := accountUnmarshalFixture(entries)
		original.ProtoReflect().SetUnknown(protowire.AppendVarint(
			protowire.AppendTag(nil, 100, protowire.VarintType), uint64(entries),
		))
		data, err := proto.MarshalOptions{Deterministic: true}.Marshal(original)
		if err != nil {
			t.Fatal(err)
		}

		want := new(corepb.Account)
		if err := proto.Unmarshal(data, want); err != nil {
			t.Fatal(err)
		}
		gotAccount, err := UnmarshalAccount(data)
		if err != nil {
			t.Fatalf("entries=%d: %v", entries, err)
		}
		got := gotAccount.Proto()
		if !proto.Equal(got, want) {
			t.Fatalf("entries=%d: direct unmarshal differs\ngot:  %v\nwant: %v", entries, got, want)
		}

		gotWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(got)
		if err != nil {
			t.Fatal(err)
		}
		wantWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(gotWire, wantWire) {
			t.Fatalf("entries=%d: deterministic wire differs", entries)
		}
	}
}

func TestUnmarshalAccountDirectMapsWireSemantics(t *testing.T) {
	data := protowire.AppendTag(nil, 4, protowire.VarintType)
	data = protowire.AppendVarint(data, 99)
	// Interleave map fields and encode duplicate entry key/value fields. The
	// last field in an entry and the last entry for a key must win.
	entry := protowire.AppendTag(nil, 1, protowire.BytesType)
	entry = protowire.AppendString(entry, "ignored")
	entry = protowire.AppendTag(entry, 2, protowire.VarintType)
	entry = protowire.AppendVarint(entry, 1)
	entry = protowire.AppendTag(entry, 9, protowire.VarintType)
	entry = protowire.AppendVarint(entry, 1234)
	entry = protowire.AppendTag(entry, 1, protowire.BytesType)
	entry = protowire.AppendString(entry, "asset")
	entry = protowire.AppendTag(entry, 2, protowire.VarintType)
	negativeSeven := int64(-7)
	entry = protowire.AppendVarint(entry, uint64(negativeSeven))
	data = protowire.AppendTag(data, 6, protowire.BytesType)
	data = protowire.AppendBytes(data, entry)
	data = protowire.AppendTag(data, 3, protowire.BytesType)
	data = protowire.AppendBytes(data, []byte{0x41, 0x01})
	data = appendAccountMapEntryWire(data, 6, "asset", 42)
	data = appendAccountMapEntryWire(data, 59, "asset-v2", -19)

	want := new(corepb.Account)
	if err := proto.Unmarshal(data, want); err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalAccount(data)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got.Proto(), want) {
		t.Fatalf("direct unmarshal differs\ngot:  %v\nwant: %v", got.Proto(), want)
	}
	if got.Proto().Asset["asset"] != 42 || got.Proto().FreeAssetNetUsageV2["asset-v2"] != -19 {
		t.Fatalf("last-write or negative value semantics lost: %v %v", got.Proto().Asset, got.Proto().FreeAssetNetUsageV2)
	}
}

func TestUnmarshalAccountDirectMapsRejectsMalformedWire(t *testing.T) {
	invalidUTF8Entry := protowire.AppendTag(nil, 1, protowire.BytesType)
	invalidUTF8Entry = protowire.AppendBytes(invalidUTF8Entry, []byte{0xff})
	tests := []struct {
		name string
		data []byte
	}{
		{name: "truncated outer entry", data: append(protowire.AppendTag(nil, 6, protowire.BytesType), 5, 1)},
		{name: "invalid UTF-8 key", data: protowire.AppendBytes(protowire.AppendTag(nil, 6, protowire.BytesType), invalidUTF8Entry)},
		{name: "wrong key wire type", data: protowire.AppendBytes(protowire.AppendTag(nil, 6, protowire.BytesType), protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), 1))},
		{name: "wrong outer wire type fallback", data: protowire.AppendVarint(protowire.AppendTag(nil, 6, protowire.VarintType), 1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			genericErr := proto.Unmarshal(tt.data, new(corepb.Account))
			_, directErr := UnmarshalAccount(tt.data)
			if (genericErr != nil) != (directErr != nil) {
				t.Fatalf("error mismatch: generic=%v direct=%v", genericErr, directErr)
			}
		})
	}
}

func TestUnmarshalAccountWithoutMapsUsesGenericPath(t *testing.T) {
	original := &corepb.Account{Address: []byte{0x41, 1, 2}, Balance: 123}
	data, err := proto.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	if pb, err, handled := unmarshalAccountDirectMaps(data); handled || pb != nil || err != nil {
		t.Fatalf("direct result = (%v, %v, %v), want generic fallback", pb, err, handled)
	}
	got, err := UnmarshalAccount(data)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got.Proto(), original) {
		t.Fatalf("fallback differs: got %v want %v", got.Proto(), original)
	}
}

func appendAccountMapEntryWire(dst []byte, number protowire.Number, key string, value int64) []byte {
	entry := protowire.AppendTag(nil, 1, protowire.BytesType)
	entry = protowire.AppendString(entry, key)
	entry = protowire.AppendTag(entry, 2, protowire.VarintType)
	entry = protowire.AppendVarint(entry, uint64(value))
	dst = protowire.AppendTag(dst, number, protowire.BytesType)
	return protowire.AppendBytes(dst, entry)
}

func accountUnmarshalFixture(entries int) *corepb.Account {
	pb := &corepb.Account{
		AccountName:                []byte("benchmark"),
		Type:                       corepb.AccountType_AssetIssue,
		Address:                    bytes.Repeat([]byte{0x41}, 21),
		Balance:                    1_000_000,
		NetUsage:                   123,
		CreateTime:                 456,
		Votes:                      []*corepb.Vote{{VoteAddress: []byte{1, 2, 3}, VoteCount: 7}},
		FrozenV2:                   []*corepb.Account_FreezeV2{{Type: corepb.ResourceCode_ENERGY, Amount: 99}},
		Asset:                      make(map[string]int64, entries),
		AssetV2:                    make(map[string]int64, entries),
		LatestAssetOperationTime:   make(map[string]int64, entries),
		LatestAssetOperationTimeV2: make(map[string]int64, entries),
		FreeAssetNetUsage:          make(map[string]int64, entries),
		FreeAssetNetUsageV2:        make(map[string]int64, entries),
	}
	for i := 0; i < entries; i++ {
		key := fmt.Sprintf("asset-%05d", i)
		pb.Asset[key] = int64(i + 1)
		pb.AssetV2[key] = -int64(i + 2)
		pb.LatestAssetOperationTime[key] = int64(i + 3)
		pb.LatestAssetOperationTimeV2[key] = int64(i + 4)
		pb.FreeAssetNetUsage[key] = int64(i + 5)
		pb.FreeAssetNetUsageV2[key] = int64(i + 6)
	}
	return pb
}

var benchmarkUnmarshalAccountSink *Account

func BenchmarkUnmarshalAccountMaps(b *testing.B) {
	for _, entries := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("entries-%d", entries), func(b *testing.B) {
			payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(accountUnmarshalFixture(entries))
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(payload)))
			b.Run("direct-maps", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					account, err := UnmarshalAccount(payload)
					if err != nil {
						b.Fatal(err)
					}
					benchmarkUnmarshalAccountSink = account
				}
			})
			b.Run("protobuf-generic", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					pb := new(corepb.Account)
					if err := proto.Unmarshal(payload, pb); err != nil {
						b.Fatal(err)
					}
					benchmarkUnmarshalAccountSink = &Account{pb: pb}
				}
			})
		})
	}
}

func BenchmarkUnmarshalAccountNoMaps(b *testing.B) {
	payload, err := proto.Marshal(&corepb.Account{
		AccountName: []byte("plain"),
		Address:     bytes.Repeat([]byte{0x41}, 21),
		Balance:     1_000_000,
		Votes:       []*corepb.Vote{{VoteAddress: []byte{1, 2, 3}, VoteCount: 7}},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Run("account-wrapper", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			account, err := UnmarshalAccount(payload)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkUnmarshalAccountSink = account
		}
	})
	b.Run("protobuf-generic", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			pb := new(corepb.Account)
			if err := proto.Unmarshal(payload, pb); err != nil {
				b.Fatal(err)
			}
			benchmarkUnmarshalAccountSink = &Account{pb: pb}
		}
	})
}
