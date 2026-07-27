package state

import (
	"bytes"
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestUnmarshalExchangeEquivalentAndOwned(t *testing.T) {
	original := &corepb.Exchange{
		ExchangeId:         17,
		CreatorAddress:     bytes.Repeat([]byte{0x41}, 21),
		CreateTime:         -19,
		FirstTokenId:       []byte("1000001"),
		FirstTokenBalance:  -23,
		SecondTokenId:      []byte("_"),
		SecondTokenBalance: 29,
	}
	original.ProtoReflect().SetUnknown(protowire.AppendVarint(
		protowire.AppendTag(nil, 100, protowire.VarintType), 31,
	))
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	want := new(corepb.Exchange)
	if err := proto.Unmarshal(data, want); err != nil {
		t.Fatal(err)
	}
	got, err := unmarshalExchange(data)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, want) {
		t.Fatalf("direct exchange differs\ngot:  %v\nwant: %v", got, want)
	}
	wantCreator := append([]byte(nil), got.CreatorAddress...)
	wantFirst := append([]byte(nil), got.FirstTokenId...)
	wantSecond := append([]byte(nil), got.SecondTokenId...)
	clear(data)
	if !bytes.Equal(got.CreatorAddress, wantCreator) || !bytes.Equal(got.FirstTokenId, wantFirst) || !bytes.Equal(got.SecondTokenId, wantSecond) {
		t.Fatal("decoded exchange byte fields alias the wire input")
	}
}

func TestUnmarshalExchangeDuplicateFieldsLastWins(t *testing.T) {
	data := protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), 1)
	data = protowire.AppendBytes(protowire.AppendTag(data, 6, protowire.BytesType), []byte("old"))
	data = protowire.AppendVarint(protowire.AppendTag(data, 1, protowire.VarintType), 2)
	data = protowire.AppendBytes(protowire.AppendTag(data, 6, protowire.BytesType), []byte("new"))
	want := new(corepb.Exchange)
	if err := proto.Unmarshal(data, want); err != nil {
		t.Fatal(err)
	}
	got, err := unmarshalExchange(data)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, want) {
		t.Fatalf("duplicate semantics differ: got %v want %v", got, want)
	}
}

func FuzzUnmarshalExchangeEquivalent(f *testing.F) {
	for _, pb := range []*corepb.Exchange{
		{},
		{ExchangeId: 1, CreatorAddress: bytes.Repeat([]byte{0x41}, 21), FirstTokenId: []byte("1000001"), SecondTokenId: []byte("_")},
	} {
		data, err := proto.Marshal(pb)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(data)
	}
	f.Add([]byte{0x32, 0x05, 0x01})
	f.Add([]byte{0x08, 0x80})
	f.Add([]byte("\xb1\xb4\x8c\xc7000000000"))
	f.Add([]byte("\xe8\x000"))

	f.Fuzz(func(t *testing.T, data []byte) {
		want := new(corepb.Exchange)
		genericErr := proto.Unmarshal(data, want)
		got, directErr := unmarshalExchange(data)
		if (genericErr != nil) != (directErr != nil) {
			t.Fatalf("error mismatch: generic=%v direct=%v data=%x", genericErr, directErr, data)
		}
		if genericErr == nil && !proto.Equal(got, want) {
			t.Fatalf("decoded exchange mismatch\ngot:  %v unknown=%x\nwant: %v unknown=%x\ndata: %x",
				got, got.ProtoReflect().GetUnknown(), want, want.ProtoReflect().GetUnknown(), data)
		}
	})
}

var exchangeUnmarshalBenchmarkSink *corepb.Exchange

func BenchmarkUnmarshalExchange(b *testing.B) {
	payload, err := proto.Marshal(&corepb.Exchange{
		ExchangeId:         17,
		CreatorAddress:     bytes.Repeat([]byte{0x41}, 21),
		CreateTime:         19,
		FirstTokenId:       []byte("1000001"),
		FirstTokenBalance:  23,
		SecondTokenId:      []byte("_"),
		SecondTokenBalance: 29,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Run("Protobuf", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			ex := new(corepb.Exchange)
			if err := proto.Unmarshal(payload, ex); err != nil {
				b.Fatal(err)
			}
			exchangeUnmarshalBenchmarkSink = ex
		}
	})
	b.Run("DirectCombinedBytes", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			ex, err := unmarshalExchange(payload)
			if err != nil {
				b.Fatal(err)
			}
			exchangeUnmarshalBenchmarkSink = ex
		}
	})
}
