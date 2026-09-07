package statecodec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestDelegationIndexCodecSchema(t *testing.T) {
	fields := new(corepb.DelegatedResourceAccountIndex).ProtoReflect().Descriptor().Fields()
	if fields.Len() != 4 {
		t.Fatal("delegation index schema changed; update the specialized native codec")
	}
	for number := protoreflect.FieldNumber(1); number <= 4; number++ {
		fd := fields.ByNumber(number)
		kind := protoreflect.BytesKind
		if number == 4 {
			kind = protoreflect.Int64Kind
		}
		if fd == nil || fd.Kind() != kind || fd.IsList() != (number == 2 || number == 3) || fd.IsMap() || fd.HasPresence() {
			t.Fatalf("field %d schema changed; update the specialized native codec", number)
		}
	}
}

func TestDelegationIndexCodecDifferential(t *testing.T) {
	cases := []*corepb.DelegatedResourceAccountIndex{
		{},
		{Account: []byte{}},
		{FromAccounts: [][]byte{nil, {}, {1}, {1}, {0, 0xff}}},
		{ToAccounts: [][]byte{nil, {}, {1}, {1}, {0, 0xff}}},
		{Timestamp: math.MinInt64},
		{Timestamp: math.MaxInt64},
		{Account: []byte{0x41}, FromAccounts: [][]byte{{0x41}, nil}, ToAccounts: [][]byte{{0x42}, nil}, Timestamp: -1},
		delegationBenchmarkMessage(32),
		delegationBenchmarkMessage(1_000),
	}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 100; i++ {
		data := make([]byte, rng.Intn(2_000))
		_, _ = rng.Read(data)
		cases = append(cases, delegationMessageFromBytes(data))
	}
	for i, in := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			assertDelegationMarshalEquivalent(t, in)
		})
	}
	// Unknown trailers are deliberately opaque, including invalid protobuf wire.
	for _, unknown := range [][]byte{{0xa0, 0x06, 1}, {0}, {0xff, 0xff, 0xff}, bytes.Repeat([]byte{1}, 128)} {
		in := delegationBenchmarkMessage(32)
		in.ProtoReflect().SetUnknown(unknown)
		assertDelegationMarshalEquivalent(t, in)
	}

	var nilMessage *corepb.DelegatedResourceAccountIndex
	_, wantErr := Marshal(nilMessage)
	_, gotErr := MarshalDelegationIndex(nilMessage)
	if wantErr == nil || gotErr == nil || gotErr.Error() != wantErr.Error() {
		t.Fatalf("nil message errors differ: got %v, want %v", gotErr, wantErr)
	}
}

func assertDelegationMarshalEquivalent(t testing.TB, in *corepb.DelegatedResourceAccountIndex) {
	t.Helper()
	want, err := Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	got, err := MarshalDelegationIndex(in)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("native encodings differ:\ngot  %x\nwant %x", got, want)
	}
	if len(got) != cap(got) {
		t.Fatalf("output is not exactly sized: len=%d cap=%d", len(got), cap(got))
	}
	assertDelegationUnmarshalEquivalent(t, want)
}

func assertDelegationUnmarshalEquivalent(t testing.TB, data []byte) {
	t.Helper()
	var want corepb.DelegatedResourceAccountIndex
	wantErr := Unmarshal(data, &want)
	got, gotErr := UnmarshalDelegationIndex(data)
	if wantErr != nil {
		if gotErr == nil || gotErr.Error() != wantErr.Error() {
			t.Fatalf("decode errors differ for %x: got %v, want %v", data, gotErr, wantErr)
		}
		return
	}
	if gotErr != nil {
		t.Fatalf("specialized decoder rejected generic-valid %x: %v", data, gotErr)
	}
	if !proto.Equal(got, &want) || !bytes.Equal(got.ProtoReflect().GetUnknown(), want.ProtoReflect().GetUnknown()) {
		t.Fatalf("decoded messages differ:\ngot  %v\nwant %v", got, &want)
	}
	for _, list := range [][][]byte{got.FromAccounts, got.ToAccounts} {
		for _, address := range list {
			if len(address) == 0 && address != nil {
				t.Fatal("empty list element must decode to nil as in generic decoder")
			}
		}
	}
	wantEncoded, err := Marshal(&want)
	if err != nil {
		t.Fatal(err)
	}
	gotEncoded, err := MarshalDelegationIndex(got)
	if err != nil || !bytes.Equal(gotEncoded, wantEncoded) {
		t.Fatalf("re-encoded bytes differ: error=%v", err)
	}
}

func TestDelegationIndexCodecRejectsMalformedNative(t *testing.T) {
	valid, err := Marshal(delegationBenchmarkMessage(3))
	if err != nil {
		t.Fatal(err)
	}
	for cut := range valid {
		assertDelegationUnmarshalEquivalent(t, valid[:cut])
	}
	for i := range valid {
		for bit := byte(1); bit != 0; bit <<= 1 {
			mutated := bytes.Clone(valid)
			mutated[i] ^= bit
			assertDelegationUnmarshalEquivalent(t, mutated)
		}
	}
	cases := [][]byte{
		nil,
		{},
		{0x0a, 1, 0x41}, // Protobuf wire format.
		append(bytes.Clone(magic[:]), 0, 0, 0),
		append(bytes.Clone(magic[:]), 5, 0),
		append(bytes.Clone(magic[:]), 0x80, 0, 0),
		append(bytes.Clone(magic[:]), 0, 0x80, 0),
	}
	// Every number/type combination covers absent schema fields, each shape,
	// mismatched scalar kinds, and explicit defaults.
	for number := uint64(0); number <= 5; number++ {
		for typeCode := 0; typeCode <= math.MaxUint8; typeCode++ {
			for _, payload := range [][]byte{nil, {0}, {1, 0}, make([]byte, 8)} {
				cases = append(cases, nativeFieldValue(number, byte(typeCode), payload))
			}
		}
	}
	for _, payload := range [][]byte{
		{1}, {1, 1}, {1, 0, 0}, {2, 0},
		{0x81, 0, 0}, {1, 0x80, 0}, // Overlong count/length varints.
		binary.AppendUvarint(nil, math.MaxUint64),
	} {
		cases = append(cases, nativeFieldValue(2, shapeList|byte(protoreflect.BytesKind), payload))
	}
	for _, number := range []uint64{1<<32 + 1, math.MaxUint64} {
		cases = append(cases, nativeFieldValue(number, byte(protoreflect.BytesKind), []byte{1}))
	}
	// Ordered known fields may appear only once; reject duplicate and reversed
	// fields even when each individual field would decode successfully.
	for _, numbers := range [][]byte{{1, 1}, {3, 2}} {
		data := append(bytes.Clone(magic[:]), 2)
		for _, number := range numbers {
			if number == 1 {
				data = append(data, number, byte(protoreflect.BytesKind), 1, 1)
			} else {
				data = append(data, number, shapeList|byte(protoreflect.BytesKind), 2, 1, 0)
			}
		}
		cases = append(cases, append(data, 0))
	}
	for _, data := range cases {
		assertDelegationUnmarshalEquivalent(t, data)
	}
}

func TestDelegationIndexDecodeOwnership(t *testing.T) {
	in := delegationBenchmarkMessage(3)
	in.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 1})
	data, err := Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := UnmarshalDelegationIndex(data)
	if err != nil {
		t.Fatal(err)
	}
	clear(data)
	if !proto.Equal(in, out) {
		t.Fatal("decoded message aliases input")
	}
	for _, value := range append(append([][]byte{out.Account, out.ProtoReflect().GetUnknown()}, out.FromAccounts...), out.ToAccounts...) {
		if len(value) != cap(value) {
			t.Fatalf("byte slice append could overwrite adjacent arena value: len=%d cap=%d", len(value), cap(value))
		}
	}
	if len(out.FromAccounts) != cap(out.FromAccounts) || len(out.ToAccounts) != cap(out.ToAccounts) {
		t.Fatal("list append could overwrite adjacent list headers")
	}
	out.Account = append(out.Account, 0)
	out.FromAccounts[0] = append(out.FromAccounts[0], 0)
	out.FromAccounts = append(out.FromAccounts, []byte{9})
	if !bytes.Equal(out.FromAccounts[1], in.FromAccounts[1]) || !bytes.Equal(out.ToAccounts[0], in.ToAccounts[0]) {
		t.Fatal("appending one value changed another")
	}
}

func FuzzDelegationIndexNative(f *testing.F) {
	for _, in := range []*corepb.DelegatedResourceAccountIndex{
		{}, delegationBenchmarkMessage(1), delegationBenchmarkMessage(32),
		{FromAccounts: [][]byte{nil, {1}, {1}}, Timestamp: -1},
	} {
		data, err := Marshal(in)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(data)
	}
	f.Add(nativeFieldValue(2, shapeList|byte(protoreflect.BytesKind), []byte{0}))
	f.Add(append(bytes.Clone(magic[:]), 0, 3, 0xff, 0, 1))
	f.Fuzz(func(t *testing.T, data []byte) {
		assertDelegationUnmarshalEquivalent(t, data)
	})
}

func FuzzDelegationIndexMessage(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9})
	f.Add(bytes.Repeat([]byte{0x41}, 1_000))
	f.Fuzz(func(t *testing.T, data []byte) {
		assertDelegationMarshalEquivalent(t, delegationMessageFromBytes(data))
	})
}

// Interpret fuzz bytes as a message, keeping arbitrary lengths, duplicate/empty
// entries, signed timestamps, and opaque unknown bytes in the generated corpus.
func delegationMessageFromBytes(data []byte) *corepb.DelegatedResourceAccountIndex {
	out := new(corepb.DelegatedResourceAccountIndex)
	if len(data) >= 8 {
		out.Timestamp = int64(binary.BigEndian.Uint64(data))
		data = data[8:]
	}
	for field := 0; len(data) != 0; field++ {
		n := min(int(data[0]), len(data)-1)
		value := bytes.Clone(data[1 : 1+n])
		data = data[1+n:]
		switch field % 4 {
		case 0:
			out.Account = value
		case 1:
			out.FromAccounts = append(out.FromAccounts, value)
		case 2:
			out.ToAccounts = append(out.ToAccounts, value)
		case 3:
			out.ProtoReflect().SetUnknown(value)
		}
	}
	return out
}

func delegationBenchmarkMessage(degree int) *corepb.DelegatedResourceAccountIndex {
	out := &corepb.DelegatedResourceAccountIndex{
		Account: make([]byte, 21), FromAccounts: make([][]byte, degree),
		ToAccounts: make([][]byte, degree), Timestamp: 123456789,
	}
	out.Account[0] = 0x41
	for i := 0; i < degree; i++ {
		from, to := make([]byte, 21), make([]byte, 21)
		from[0], to[0] = 0x41, 0x41
		binary.BigEndian.PutUint64(from[13:], uint64(i+1))
		binary.BigEndian.PutUint64(to[13:], uint64(i+degree+1))
		out.FromAccounts[i], out.ToAccounts[i] = from, to
	}
	return out
}

var delegationBytesSink []byte
var delegationMessageSink *corepb.DelegatedResourceAccountIndex

func BenchmarkDelegationIndexCodec(b *testing.B) {
	for _, degree := range []int{32, 1_000, 10_000, 100_000} {
		in := delegationBenchmarkMessage(degree)
		data, err := Marshal(in)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("degree=%d", degree), func(b *testing.B) {
			b.Run("Marshal/generic", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(data)))
				for b.Loop() {
					var err error
					delegationBytesSink, err = Marshal(in)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("Marshal/specialized", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(data)))
				for b.Loop() {
					var err error
					delegationBytesSink, err = MarshalDelegationIndex(in)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("Unmarshal/generic", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(data)))
				for b.Loop() {
					out := new(corepb.DelegatedResourceAccountIndex)
					if err := Unmarshal(data, out); err != nil {
						b.Fatal(err)
					}
					delegationMessageSink = out
				}
			})
			b.Run("Unmarshal/specialized", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(data)))
				for b.Loop() {
					var err error
					delegationMessageSink, err = UnmarshalDelegationIndex(data)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
