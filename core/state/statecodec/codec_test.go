package statecodec

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestRoundTripNestedRepeatedAndUnknown(t *testing.T) {
	in := &contractpb.SmartContract{
		OriginAddress:              []byte{1, 2, 3},
		ContractAddress:            []byte{4, 5, 6},
		Bytecode:                   []byte{7, 8},
		CallValue:                  99,
		ConsumeUserResourcePercent: 42,
		Abi: &contractpb.SmartContract_ABI{Entrys: []*contractpb.SmartContract_ABI_Entry{{
			Name:   "transfer",
			Inputs: []*contractpb.SmartContract_ABI_Entry_Param{{Name: "to", Type: "address"}},
		}}},
	}
	in.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	first, err := Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Marshal(proto.Clone(in).(*contractpb.SmartContract))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("encoding is not deterministic")
	}
	if !IsNative(first) {
		t.Fatal("native header missing")
	}
	var out contractpb.SmartContract
	if err := Unmarshal(first, &out); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(in, &out) {
		t.Fatalf("round trip mismatch:\nin=%v\nout=%v", in, &out)
	}
}

func TestRejectsProtobufWireValue(t *testing.T) {
	in := &corepb.Votes{Address: []byte{1}, OldVotes: []*corepb.Vote{{VoteAddress: []byte{2}, VoteCount: 3}}}
	protobufBytes, err := proto.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out corepb.Votes
	if err := Unmarshal(protobufBytes, &out); err == nil {
		t.Fatal("protobuf wire value was accepted as rooted state")
	}
}

func TestRoundTripMapFields(t *testing.T) {
	in := &corepb.Account{
		Address:             []byte{0x41, 1},
		Asset:               map[string]int64{"z": 3, "a": 1},
		FreeAssetNetUsageV2: map[string]int64{"2": 8, "1": 7},
		AssetOptimized:      true,
	}
	first, err := Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	// Rebuild with a different Go map insertion order. Durable bytes must not
	// inherit the process-random map iteration order.
	other := proto.Clone(in).(*corepb.Account)
	other.Asset = map[string]int64{}
	other.Asset["a"] = 1
	other.Asset["z"] = 3
	second, err := Marshal(other)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("map encoding is not deterministic")
	}
	var out corepb.Account
	if err := Unmarshal(first, &out); err != nil || !proto.Equal(in, &out) {
		t.Fatalf("map round trip out=%v err=%v", &out, err)
	}
}

func TestRejectsCorruptNativeRow(t *testing.T) {
	var out corepb.Votes
	if err := Unmarshal(append(magic[:], 1), &out); err == nil {
		t.Fatal("expected corrupt native row to fail")
	}
}

func TestRejectsNonCanonicalUvarint(t *testing.T) {
	// Empty message: field-count=0, unknown-length=0. Encode the first zero as
	// the overlong 0x80,0x00 representation.
	raw := append(append([]byte(nil), magic[:]...), 0x80, 0x00, 0x00)
	var out corepb.Votes
	if err := Unmarshal(raw, &out); err == nil {
		t.Fatal("non-canonical field-count uvarint was accepted")
	}
}

func nativeFieldValue(number uint64, typeCode byte, payload []byte) []byte {
	raw := append([]byte(nil), magic[:]...)
	raw = binary.AppendUvarint(raw, 1)
	raw = binary.AppendUvarint(raw, number)
	raw = append(raw, typeCode)
	raw = binary.AppendUvarint(raw, uint64(len(payload)))
	raw = append(raw, payload...)
	return append(raw, 0) // empty unknown-field trailer
}

func TestRejectsFieldNumberTruncationAlias(t *testing.T) {
	field := new(corepb.Vote).ProtoReflect().Descriptor().Fields().ByNumber(1)
	raw := nativeFieldValue(1<<32+1, shapeScalar|kindCode(field.Kind()), []byte{0x41})
	var out corepb.Vote
	if err := Unmarshal(raw, &out); err == nil {
		t.Fatal("field number above the protobuf range was truncated and accepted")
	}
}

func TestRejectsExplicitDefaultAndEmptyContainerFields(t *testing.T) {
	voteCount := new(corepb.Vote).ProtoReflect().Descriptor().Fields().ByNumber(2)
	emptyVoteCount := nativeFieldValue(2, shapeScalar|kindCode(voteCount.Kind()), make([]byte, 8))

	oldVotes := new(corepb.Votes).ProtoReflect().Descriptor().Fields().ByNumber(2)
	emptyVoteList := nativeFieldValue(2, shapeList|kindCode(oldVotes.Kind()), []byte{0})

	asset := new(corepb.Account).ProtoReflect().Descriptor().Fields().ByNumber(6)
	emptyAssetMap := nativeFieldValue(6, shapeMap|kindCode(asset.Kind()), []byte{0})

	for name, test := range map[string]struct {
		raw []byte
		msg proto.Message
	}{
		"default scalar": {emptyVoteCount, new(corepb.Vote)},
		"empty list":     {emptyVoteList, new(corepb.Votes)},
		"empty map":      {emptyAssetMap, new(corepb.Account)},
	} {
		t.Run(name, func(t *testing.T) {
			if err := Unmarshal(test.raw, test.msg); err == nil {
				t.Fatal("non-canonical empty/default field was accepted")
			}
		})
	}
}

func TestRejectsUnorderedAndDuplicateMapKeys(t *testing.T) {
	field := new(corepb.Account).ProtoReflect().Descriptor().Fields().ByName("asset")
	encodeEntries := func(keys ...string) []byte {
		payload := binary.AppendUvarint(nil, uint64(len(keys)))
		for index, key := range keys {
			encodedKey, err := appendScalar(nil, field.MapKey().Kind(), protoreflect.ValueOfString(key))
			if err != nil {
				t.Fatal(err)
			}
			encodedValue, err := appendScalar(nil, field.MapValue().Kind(), protoreflect.ValueOfInt64(int64(index+1)))
			if err != nil {
				t.Fatal(err)
			}
			payload = binary.AppendUvarint(payload, uint64(len(encodedKey)))
			payload = append(payload, encodedKey...)
			payload = binary.AppendUvarint(payload, uint64(len(encodedValue)))
			payload = append(payload, encodedValue...)
		}
		return payload
	}
	for _, keys := range [][]string{{"z", "a"}, {"a", "a"}} {
		account := new(corepb.Account)
		if err := consumeMap(encodeEntries(keys...), field, account.ProtoReflect().Mutable(field).Map()); err == nil {
			t.Fatalf("map keys %q were accepted", keys)
		}
	}
}

func TestRejectsOutOfRangeAndInvalidUTF8Scalars(t *testing.T) {
	var fixed [8]byte
	binary.BigEndian.PutUint64(fixed[:], uint64(math.MaxInt32)+1)
	if _, err := consumeScalar(fixed[:], protoreflect.Int32Kind); err == nil {
		t.Fatal("out-of-range int32 was accepted")
	}
	binary.BigEndian.PutUint64(fixed[:], uint64(math.MaxUint32)+1)
	if _, err := consumeScalar(fixed[:], protoreflect.Uint32Kind); err == nil {
		t.Fatal("out-of-range uint32 was accepted")
	}
	if _, err := consumeScalar([]byte{0xff}, protoreflect.StringKind); err == nil {
		t.Fatal("invalid UTF-8 string was accepted")
	}
	if _, err := Marshal(&contractpb.SmartContract{Name: string([]byte{0xff})}); err == nil {
		t.Fatal("invalid UTF-8 string was written")
	}
}
