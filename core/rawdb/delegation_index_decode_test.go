package rawdb

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func delegationAddress(seed byte) []byte {
	return append([]byte{0x41}, bytes.Repeat([]byte{seed}, 20)...)
}

func TestDecodeDrAccountIndexLegacyMatchesProto(t *testing.T) {
	want := &corepb.DelegatedResourceAccountIndex{
		Account:      delegationAddress(1),
		FromAccounts: [][]byte{delegationAddress(2), {}, delegationAddress(3)},
		ToAccounts:   [][]byte{delegationAddress(4), delegationAddress(5)},
		Timestamp:    -9,
	}
	raw, err := proto.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	// Exercise final-value semantics on the arena path.
	raw = protowire.AppendBytes(protowire.AppendTag(raw, 1, protowire.BytesType), delegationAddress(6))
	unknownRaw := bytes.Clone(raw)
	// Also cover preservation of both truly unknown fields and known fields
	// carrying an unexpected wire type on the generated fallback.
	unknownRaw = protowire.AppendVarint(protowire.AppendTag(unknownRaw, 2, protowire.VarintType), 7)
	unknownRaw = protowire.AppendBytes(protowire.AppendTag(unknownRaw, 99, protowire.BytesType), []byte("future"))
	// The protobuf runtime canonicalizes this overlong unknown tag to 0x10.
	unknownRaw = append(unknownRaw, 0x90, 0x00, 0x30)

	var generated corepb.DelegatedResourceAccountIndex
	if err := proto.Unmarshal(raw, &generated); err != nil {
		t.Fatal(err)
	}
	got, err := DecodeDrAccountIndexLegacy(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, &generated) {
		t.Fatalf("decoded index mismatch:\n got  %v\n want %v", got, &generated)
	}

	// The decoder must not retain the database buffer.
	before, err := proto.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for i := range raw {
		raw[i] ^= 0xff
	}
	after, err := proto.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("decoded index aliases the input buffer")
	}

	generated.Reset()
	if err := proto.Unmarshal(unknownRaw, &generated); err != nil {
		t.Fatal(err)
	}
	got, err = DecodeDrAccountIndexLegacy(unknownRaw)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, &generated) {
		t.Fatalf("decoded fallback mismatch:\n got  %v\n want %v", got, &generated)
	}
}

func TestDecodeDrAccountIndexLegacyRejectsMalformedWire(t *testing.T) {
	cases := [][]byte{
		{0},
		append(protowire.AppendTag(nil, 2, protowire.BytesType), 5, 1),
		protowire.AppendTag(nil, 1, protowire.EndGroupType),
	}
	for _, raw := range cases {
		if _, err := DecodeDrAccountIndexLegacy(raw); err == nil {
			t.Fatalf("DecodeDrAccountIndexLegacy(%x) succeeded", raw)
		}
	}
}

func FuzzDecodeDrAccountIndexLegacy(f *testing.F) {
	seed, _ := proto.Marshal(&corepb.DelegatedResourceAccountIndex{
		Account:      delegationAddress(1),
		FromAccounts: [][]byte{delegationAddress(2), delegationAddress(3)},
		ToAccounts:   [][]byte{delegationAddress(4)},
		Timestamp:    42,
	})
	f.Add(seed)
	f.Fuzz(func(t *testing.T, raw []byte) {
		var generated corepb.DelegatedResourceAccountIndex
		generatedErr := proto.Unmarshal(raw, &generated)
		got, gotErr := DecodeDrAccountIndexLegacy(raw)
		if (gotErr == nil) != (generatedErr == nil) {
			t.Fatalf("error mismatch: custom=%v generated=%v raw=%x", gotErr, generatedErr, raw)
		}
		if gotErr == nil && !proto.Equal(got, &generated) {
			t.Fatalf("value mismatch: custom=%v unknown=%x generated=%v unknown=%x raw=%x", got, got.ProtoReflect().GetUnknown(), &generated, generated.ProtoReflect().GetUnknown(), raw)
		}
	})
}

var benchmarkDelegationIndex *corepb.DelegatedResourceAccountIndex

func BenchmarkDecodeDrAccountIndexLegacy(b *testing.B) {
	rec := &corepb.DelegatedResourceAccountIndex{Account: delegationAddress(1), Timestamp: 42}
	for i := 0; i < 32; i++ {
		rec.FromAccounts = append(rec.FromAccounts, delegationAddress(byte(i+2)))
		rec.ToAccounts = append(rec.ToAccounts, delegationAddress(byte(i+34)))
	}
	raw, err := proto.Marshal(rec)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.Run("Generated", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var decoded corepb.DelegatedResourceAccountIndex
			if err := proto.Unmarshal(raw, &decoded); err != nil {
				b.Fatal(err)
			}
			benchmarkDelegationIndex = &decoded
		}
	})
	b.Run("Arena", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			decoded, err := DecodeDrAccountIndexLegacy(raw)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkDelegationIndex = decoded
		}
	})
}
