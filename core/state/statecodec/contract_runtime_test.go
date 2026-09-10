package statecodec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"testing"

	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestContractRuntimeSchema(t *testing.T) {
	var check func(protoreflect.MessageDescriptor, []contractRuntimeField)
	check = func(desc protoreflect.MessageDescriptor, schema []contractRuntimeField) {
		t.Helper()
		if desc.Fields().Len() != len(schema) {
			t.Fatalf("%s schema changed; update runtime reader", desc.FullName())
		}
		for i, field := range schema {
			fd := desc.Fields().ByNumber(protoreflect.FieldNumber(i + 1))
			if fd == nil || byte(fd.Kind()) != field.typeCode&0x3f || fd.IsMap() ||
				fd.IsList() != (field.typeCode&0xc0 == shapeList) ||
				fd.HasPresence() != (field.typeCode == byte(protoreflect.MessageKind)) {
				t.Fatalf("%s field %d schema changed; update runtime reader", desc.FullName(), i+1)
			}
			if fd.Kind() == protoreflect.MessageKind {
				check(fd.Message(), field.message)
			} else if field.message != nil {
				t.Fatalf("%s has unexpected child schema", fd.FullName())
			}
		}
	}
	check(new(contractpb.SmartContract).ProtoReflect().Descriptor(), contractRuntimeSchema)
}

func assertContractRuntimeEquivalent(t testing.TB, data []byte) {
	t.Helper()
	var want contractpb.SmartContract
	wantErr := Unmarshal(data, &want)
	got, gotErr := ReadContractRuntime(data)
	if wantErr != nil {
		if gotErr == nil || gotErr.Error() != wantErr.Error() {
			t.Fatalf("decode errors differ for %x: got %v, want %v", data, gotErr, wantErr)
		}
		return
	}
	if gotErr != nil {
		t.Fatalf("runtime reader rejected generic-valid %x: %v", data, gotErr)
	}
	if !bytes.Equal(got.OriginAddress, want.OriginAddress) || !bytes.Equal(got.TrxHash, want.TrxHash) ||
		got.ConsumeUserResourcePercent != want.ConsumeUserResourcePercent ||
		got.OriginEnergyLimit != want.OriginEnergyLimit || got.Version != want.Version {
		t.Fatalf("runtime fields differ: got %+v, want %v", got, &want)
	}
	var fields [11][]byte
	if !validateContractRuntimeMessage(data[len(magic):], contractRuntimeSchema, &fields) {
		t.Fatalf("valid current-schema row unexpectedly used generic fallback: %x", data)
	}
}

func contractRuntimeCodecFixture(entries int) *contractpb.SmartContract {
	out := &contractpb.SmartContract{
		OriginAddress: []byte{0x41, 1}, ContractAddress: []byte{0x41, 2},
		Bytecode: bytes.Repeat([]byte{0x60, 0x40}, 64), CallValue: 3,
		ConsumeUserResourcePercent: 37, Name: "合约", OriginEnergyLimit: 9_000_000,
		CodeHash: []byte{3, 4}, TrxHash: []byte{5, 6}, Version: 1,
		Abi: &contractpb.SmartContract_ABI{},
	}
	for i := 0; i < entries; i++ {
		out.Abi.Entrys = append(out.Abi.Entrys, &contractpb.SmartContract_ABI_Entry{
			Anonymous: true, Constant: true, Name: fmt.Sprintf("method_%d", i),
			Inputs:  []*contractpb.SmartContract_ABI_Entry_Param{{Indexed: true, Name: "to", Type: "address"}, {}},
			Outputs: []*contractpb.SmartContract_ABI_Entry_Param{{Name: "amount", Type: "uint256"}},
			Type:    contractpb.SmartContract_ABI_Entry_Function, Payable: true,
			StateMutability: contractpb.SmartContract_ABI_Entry_Payable,
		})
	}
	return out
}

func TestContractRuntimeDifferential(t *testing.T) {
	cases := []*contractpb.SmartContract{
		{}, {Abi: &contractpb.SmartContract_ABI{}}, contractRuntimeCodecFixture(1), contractRuntimeCodecFixture(64),
		{OriginAddress: []byte{0xff}, TrxHash: bytes.Repeat([]byte{0}, 80), Version: math.MinInt32,
			ConsumeUserResourcePercent: math.MinInt64, OriginEnergyLimit: math.MaxInt64},
		{Version: math.MaxInt32, ConsumeUserResourcePercent: math.MaxInt64, OriginEnergyLimit: math.MinInt64},
	}
	// The native codec preserves opaque unknown bytes even if they are invalid
	// protobuf wire data. Exercise that contract at every nesting level.
	for _, unknown := range [][]byte{{0}, {0xff, 0xff}, {0xa0, 0x06, 1}, bytes.Repeat([]byte{1}, 128)} {
		in := contractRuntimeCodecFixture(1)
		for _, msg := range []proto.Message{in, in.Abi, in.Abi.Entrys[0], in.Abi.Entrys[0].Inputs[0], in.Abi.Entrys[0].Outputs[0]} {
			msg.ProtoReflect().SetUnknown(unknown)
		}
		cases = append(cases, in)
	}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 100; i++ {
		in := contractRuntimeCodecFixture(rng.Intn(8))
		in.OriginAddress = make([]byte, rng.Intn(64))
		in.TrxHash = make([]byte, rng.Intn(80))
		_, _ = rng.Read(in.OriginAddress)
		_, _ = rng.Read(in.TrxHash)
		in.Version = int32(rng.Uint32())
		in.ConsumeUserResourcePercent = int64(rng.Uint64())
		in.OriginEnergyLimit = int64(rng.Uint64())
		for _, entry := range in.Abi.Entrys {
			entry.Type = contractpb.SmartContract_ABI_Entry_EntryType(int32(rng.Uint32()))
			entry.StateMutability = contractpb.SmartContract_ABI_Entry_StateMutabilityType(int32(rng.Uint32()))
		}
		cases = append(cases, in)
	}
	for i, in := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			data, err := Marshal(in)
			if err != nil {
				t.Fatal(err)
			}
			assertContractRuntimeEquivalent(t, data)
		})
	}
}

// Wrap a bare message body at each ABI nesting level in otherwise valid native
// framing, so malformed inputs reach the nested validator rather than failing
// at the outer field's length.
func contractRuntimeWrapBody(body []byte, depth int, output bool) []byte {
	messageField := func(number uint64, body []byte) []byte {
		return nativeFieldValue(number, byte(protoreflect.MessageKind), body)[len(magic):]
	}
	listField := func(number uint64, body []byte) []byte {
		payload := binary.AppendUvarint([]byte{1}, uint64(len(body)))
		payload = append(payload, body...)
		return nativeFieldValue(number, shapeList|byte(protoreflect.MessageKind), payload)[len(magic):]
	}
	if depth == 3 {
		number := uint64(4)
		if output {
			number = 5
		}
		body = listField(number, body)
	}
	if depth >= 2 {
		body = listField(1, body)
	}
	if depth >= 1 {
		body = messageField(3, body)
	}
	return append(bytes.Clone(magic[:]), body...)
}

func TestContractRuntimeMalformedDifferential(t *testing.T) {
	valid, err := Marshal(contractRuntimeCodecFixture(1))
	if err != nil {
		t.Fatal(err)
	}
	for cut := range valid {
		assertContractRuntimeEquivalent(t, valid[:cut])
	}
	for i := range valid {
		for bit := byte(1); bit != 0; bit <<= 1 {
			mutated := bytes.Clone(valid)
			mutated[i] ^= bit
			assertContractRuntimeEquivalent(t, mutated)
		}
	}
	assertContractRuntimeEquivalent(t, []byte{0x0a, 1, 0x41}) // protobuf is not native
	for depth, schema := range [][]contractRuntimeField{contractRuntimeSchema, contractRuntimeABISchema, contractRuntimeEntrySchema, contractRuntimeParamSchema} {
		t.Run(fmt.Sprintf("depth=%d", depth), func(t *testing.T) {
			check := func(body []byte) {
				assertContractRuntimeEquivalent(t, contractRuntimeWrapBody(body, depth, false))
				if depth == 3 {
					assertContractRuntimeEquivalent(t, contractRuntimeWrapBody(body, depth, true))
				}
			}
			for _, body := range [][]byte{
				nil, {0}, {0, 1}, {0, 0, 0}, {0x80, 0, 0}, {0, 0x80, 0},
				{1, 0x81, 0, 12, 1, 1, 0}, {1, 1, 12, 0x81, 0, 1, 0},
				binary.AppendUvarint(nil, math.MaxUint64),
			} {
				check(body)
			}
			for number := uint64(0); number <= uint64(len(schema)+1); number++ {
				for typeCode := 0; typeCode <= math.MaxUint8; typeCode++ {
					for _, payload := range [][]byte{nil, {0}, {1}, {0, 0}, make([]byte, 8), binary.BigEndian.AppendUint64(nil, 1)} {
						check(nativeFieldValue(number, byte(typeCode), payload)[len(magic):])
					}
				}
			}
			for i, field := range schema {
				for _, payload := range [][]byte{
					{0xff}, {2}, {0xc0, 0x80}, // invalid bool/string
					binary.BigEndian.AppendUint64(nil, math.MaxInt32+1),
					binary.BigEndian.AppendUint64(nil, uint64(math.MaxInt64)),
					binary.BigEndian.AppendUint64(nil, uint64(1)<<63),
					{1}, {1, 1}, {1, 0}, {1, 2, 0, 0, 0}, {2, 2, 0, 0},
					{0x81, 0, 2, 0, 0}, {1, 0x82, 0, 0, 0},
					binary.AppendUvarint(nil, math.MaxUint64),
				} {
					check(nativeFieldValue(uint64(i+1), field.typeCode, payload)[len(magic):])
				}
			}
			for _, number := range []uint64{1<<32 + 1, math.MaxUint64} {
				check(nativeFieldValue(number, byte(protoreflect.BytesKind), []byte{1})[len(magic):])
			}
		})
	}
}

func TestContractRuntimeRejectsDuplicateOrReversedFields(t *testing.T) {
	for depth, schema := range [][]contractRuntimeField{contractRuntimeSchema, contractRuntimeABISchema, contractRuntimeEntrySchema, contractRuntimeParamSchema} {
		// Obtain individually valid fields, then reverse or duplicate them while
		// keeping framing intact; this isolates field-order validation.
		var encoded [][]byte
		for i, field := range schema {
			var payload []byte
			switch field.typeCode {
			case byte(protoreflect.MessageKind):
				payload = []byte{0, 0}
			case shapeList | byte(protoreflect.MessageKind):
				payload = []byte{1, 2, 0, 0}
			case byte(protoreflect.Int64Kind), byte(protoreflect.Int32Kind), byte(protoreflect.EnumKind):
				payload = binary.BigEndian.AppendUint64(nil, 1)
			default:
				payload = []byte{1}
			}
			body := nativeFieldValue(uint64(i+1), field.typeCode, payload)[len(magic):]
			encoded = append(encoded, body[1:len(body)-1])
		}
		for i := range encoded {
			for _, j := range []int{i, 0} {
				body := append([]byte{2}, encoded[i]...)
				body = append(body, encoded[j]...)
				data := contractRuntimeWrapBody(append(body, 0), depth, false)
				if _, err := ReadContractRuntime(data); err == nil {
					t.Fatalf("accepted duplicate/reversed fields at depth %d: %x", depth, data)
				}
				assertContractRuntimeEquivalent(t, data)
			}
		}
	}
}

func FuzzContractRuntimeNative(f *testing.F) {
	for _, in := range []*contractpb.SmartContract{{}, contractRuntimeCodecFixture(1), contractRuntimeCodecFixture(8), {Version: -1, OriginEnergyLimit: math.MinInt64}} {
		data, err := Marshal(in)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(data)
	}
	for depth := 0; depth <= 3; depth++ {
		f.Add(contractRuntimeWrapBody([]byte{0, 3, 0xff, 0, 1}, depth, false))
	}
	f.Fuzz(func(t *testing.T, data []byte) { assertContractRuntimeEquivalent(t, data) })
}
