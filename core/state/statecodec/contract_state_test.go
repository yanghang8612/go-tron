package statecodec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestContractStateCodecSchema(t *testing.T) {
	fields := new(contractpb.ContractState).ProtoReflect().Descriptor().Fields()
	if fields.Len() != 3 {
		t.Fatalf("ContractState schema changed: got %d fields, want 3", fields.Len())
	}
	for number := protoreflect.FieldNumber(1); number <= 3; number++ {
		fd := fields.ByNumber(number)
		if fd == nil || fd.Kind() != protoreflect.Int64Kind || fd.IsList() || fd.IsMap() || fd.HasPresence() {
			t.Fatalf("ContractState field %d schema changed; update specialized native codec", number)
		}
	}
}

func TestContractStateCodecDifferential(t *testing.T) {
	vals := []int64{0, 1, -1, 42, -42, math.MaxInt64, math.MinInt64}
	var cases []*contractpb.ContractState
	for _, u := range vals {
		for _, f := range vals {
			for _, c := range []int64{0, 1, -1, math.MaxInt64, math.MinInt64} {
				cases = append(cases, &contractpb.ContractState{
					EnergyUsage:  u,
					EnergyFactor: f,
					UpdateCycle:  c,
				})
			}
		}
	}
	for i, in := range cases {
		assertContractStateMarshalEquivalent(t, in)
		if i == 0 || i == len(cases)-1 {
			withUnknown := proto.Clone(in).(*contractpb.ContractState)
			withUnknown.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
			assertContractStateMarshalEquivalent(t, withUnknown)
		}
	}

	var nilMsg *contractpb.ContractState
	_, wantErr := Marshal(nilMsg)
	_, gotErr := MarshalContractState(nilMsg)
	if wantErr == nil || gotErr == nil || gotErr.Error() != wantErr.Error() {
		t.Fatalf("nil message errors differ: got %v want %v", gotErr, wantErr)
	}
}

func TestContractStateCodecRejectsMalformed(t *testing.T) {
	valid, err := Marshal(&contractpb.ContractState{EnergyUsage: 100, EnergyFactor: 20, UpdateCycle: 3})
	if err != nil {
		t.Fatal(err)
	}
	for cut := range valid {
		assertContractStateUnmarshalEquivalent(t, valid[:cut])
	}
	for i := range valid {
		for bit := byte(1); bit != 0; bit <<= 1 {
			mutated := bytes.Clone(valid)
			mutated[i] ^= bit
			assertContractStateUnmarshalEquivalent(t, mutated)
		}
	}
	var zero8 [8]byte
	for number := uint64(0); number <= 4; number++ {
		for _, tc := range []byte{0, byte(protoreflect.Int64Kind), byte(protoreflect.Int32Kind), shapeList | byte(protoreflect.Int64Kind)} {
			for _, payload := range [][]byte{nil, {0}, zero8[:]} {
				assertContractStateUnmarshalEquivalent(t, nativeFieldValue(number, tc, payload))
			}
		}
	}
	// Duplicate and out-of-order field numbers.
	var oneVal [8]byte
	binary.BigEndian.PutUint64(oneVal[:], 1)
	for _, nums := range [][]byte{{1, 1}, {2, 1}, {3, 2}} {
		raw := append(bytes.Clone(magic[:]), 2)
		for _, n := range nums {
			raw = append(raw, n, byte(protoreflect.Int64Kind), 8)
			raw = append(raw, oneVal[:]...)
		}
		assertContractStateUnmarshalEquivalent(t, append(raw, 0))
	}
}

func assertContractStateMarshalEquivalent(t testing.TB, in *contractpb.ContractState) {
	t.Helper()
	want, err := Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	got, err := MarshalContractState(in)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("native encodings differ:\ngot  %x\nwant %x", got, want)
	}
	if len(got) != cap(got) {
		t.Fatalf("output not exact-sized: len=%d cap=%d", len(got), cap(got))
	}
	assertContractStateUnmarshalEquivalent(t, want)
}

func assertContractStateUnmarshalEquivalent(t testing.TB, data []byte) {
	t.Helper()
	var want contractpb.ContractState
	wantErr := Unmarshal(data, &want)
	got, gotErr := UnmarshalContractState(data)
	if wantErr != nil {
		if gotErr == nil || gotErr.Error() != wantErr.Error() {
			t.Fatalf("decode errors differ for %x: got %v, want %v", data, gotErr, wantErr)
		}
		return
	}
	if gotErr != nil {
		t.Fatalf("specialized decoder rejected valid %x: %v", data, gotErr)
	}
	if !proto.Equal(got, &want) || !bytes.Equal(got.ProtoReflect().GetUnknown(), want.ProtoReflect().GetUnknown()) {
		t.Fatalf("decoded messages differ for %x: got %v want %v", data, got, &want)
	}
}

var (
	benchContractStateBytesSink []byte
	benchContractStateSink      *contractpb.ContractState
)

func BenchmarkMarshalContractState(b *testing.B) {
	msg := &contractpb.ContractState{
		EnergyUsage:  1234567,
		EnergyFactor: 5000,
		UpdateCycle:  8899,
	}
	b.ReportAllocs()
	for b.Loop() {
		encoded, err := MarshalContractState(msg)
		if err != nil {
			b.Fatal(err)
		}
		benchContractStateBytesSink = encoded
	}
	_ = fmt.Sprint(len(benchContractStateBytesSink))
}

func BenchmarkUnmarshalContractState(b *testing.B) {
	msg := &contractpb.ContractState{
		EnergyUsage:  1234567,
		EnergyFactor: 5000,
		UpdateCycle:  8899,
	}
	encoded, err := MarshalContractState(msg)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		decoded, err := UnmarshalContractState(encoded)
		if err != nil {
			b.Fatal(err)
		}
		benchContractStateSink = decoded
	}
}
