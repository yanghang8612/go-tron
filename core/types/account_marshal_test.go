package types

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func setTestScalar(message protoreflect.Message, field protoreflect.FieldDescriptor) {
	switch field.Kind() {
	case protoreflect.BoolKind:
		message.Set(field, protoreflect.ValueOfBool(true))
	case protoreflect.EnumKind:
		message.Set(field, protoreflect.ValueOfEnum(1))
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		message.Set(field, protoreflect.ValueOfInt32(-7))
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		message.Set(field, protoreflect.ValueOfInt64(-7))
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		message.Set(field, protoreflect.ValueOfUint32(7))
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		message.Set(field, protoreflect.ValueOfUint64(7))
	case protoreflect.FloatKind:
		message.Set(field, protoreflect.ValueOfFloat32(1.25))
	case protoreflect.DoubleKind:
		message.Set(field, protoreflect.ValueOfFloat64(1.25))
	case protoreflect.StringKind:
		message.Set(field, protoreflect.ValueOfString("value"))
	case protoreflect.BytesKind:
		message.Set(field, protoreflect.ValueOfBytes([]byte{1, 2, 3}))
	}
}

func populateTestMessage(message protoreflect.Message, depth int) {
	if depth > 3 {
		return
	}
	fields := message.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		switch {
		case field.IsMap():
			entries := message.Mutable(field).Map()
			entries.Set(protoreflect.ValueOfString("").MapKey(), protoreflect.ValueOfInt64(0))
			entries.Set(protoreflect.ValueOfString("asset-z").MapKey(), protoreflect.ValueOfInt64(-1))
			entries.Set(protoreflect.ValueOfString("资产").MapKey(), protoreflect.ValueOfInt64(math.MaxInt64))
		case field.IsList():
			list := message.Mutable(field).List()
			if field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind {
				populateTestMessage(list.AppendMutable().Message(), depth+1)
			} else {
				value := list.NewElement()
				list.Append(value)
			}
		case field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind:
			populateTestMessage(message.Mutable(field).Message(), depth+1)
		default:
			setTestScalar(message, field)
		}
	}
}

func standardDeterministicAccount(pb *corepb.Account) ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.Marshal(pb)
}

func TestAccountDirectMapMarshalMatchesDeterministicAllFields(t *testing.T) {
	if !accountDirectMapLayoutOK {
		t.Fatal("Account schema unexpectedly disabled direct-map marshal")
	}
	pb := new(corepb.Account)
	populateTestMessage(pb.ProtoReflect(), 0)
	unknown := protowire.AppendTag(nil, 19000, protowire.BytesType)
	unknown = protowire.AppendBytes(unknown, []byte("unknown-account-field"))
	pb.ProtoReflect().SetUnknown(unknown)

	want, err := standardDeterministicAccount(pb)
	if err != nil {
		t.Fatal(err)
	}
	got, err := NewAccountFromPB(pb).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("direct-map marshal differs from protobuf deterministic output\n got: %x\nwant: %x", got, want)
	}
}

func TestAccountDirectMapMarshalRandomized(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for iteration := 0; iteration < 200; iteration++ {
		pb := &corepb.Account{
			Address:                    []byte{0x41, byte(iteration)},
			Balance:                    rng.Int63(),
			NetUsage:                   rng.Int63(),
			LatestOprationTime:         rng.Int63(),
			Asset:                      make(map[string]int64),
			AssetV2:                    make(map[string]int64),
			LatestAssetOperationTime:   make(map[string]int64),
			FreeAssetNetUsage:          make(map[string]int64),
			FreeAssetNetUsageV2:        make(map[string]int64),
			LatestAssetOperationTimeV2: make(map[string]int64),
		}
		maps := []map[string]int64{
			pb.Asset, pb.AssetV2, pb.LatestAssetOperationTime,
			pb.LatestAssetOperationTimeV2, pb.FreeAssetNetUsage, pb.FreeAssetNetUsageV2,
		}
		for mapIndex, values := range maps {
			entries := 1 + rng.Intn(24)
			for entry := 0; entry < entries; entry++ {
				key := fmt.Sprintf("%d-%03d-%08x", mapIndex, entry, rng.Uint32())
				values[key] = int64(rng.Uint64())
			}
		}
		want, err := standardDeterministicAccount(pb)
		if err != nil {
			t.Fatalf("iteration %d standard marshal: %v", iteration, err)
		}
		account := NewAccountFromPB(pb)
		got, err := account.Marshal()
		if err != nil {
			t.Fatalf("iteration %d direct marshal: %v", iteration, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("iteration %d direct-map bytes differ", iteration)
		}
		again, err := account.Marshal()
		if err != nil || !bytes.Equal(again, want) {
			t.Fatalf("iteration %d repeat marshal differs: %v", iteration, err)
		}
	}
}

func TestAccountDirectMapMarshalInvalidUTF8MatchesStandard(t *testing.T) {
	pb := &corepb.Account{
		Asset: map[string]int64{string([]byte{0xff, 0xfe}): 1},
	}
	want, standardErr := standardDeterministicAccount(pb)
	got, directErr := NewAccountFromPB(pb).Marshal()
	if (standardErr == nil) != (directErr == nil) {
		t.Fatalf("invalid UTF-8 errors differ: standard=%v direct=%v", standardErr, directErr)
	}
	if standardErr == nil && !bytes.Equal(got, want) {
		t.Fatalf("invalid UTF-8 bytes differ: got %x want %x", got, want)
	}
}

func benchmarkMapRichAccount(entries int) *corepb.Account {
	pb := &corepb.Account{
		Address:                    []byte{0x41, 0x22},
		Balance:                    1_000_000_000,
		Asset:                      make(map[string]int64, entries),
		AssetV2:                    make(map[string]int64, entries),
		LatestAssetOperationTime:   make(map[string]int64, entries),
		LatestAssetOperationTimeV2: make(map[string]int64, entries),
		FreeAssetNetUsage:          make(map[string]int64, entries),
		FreeAssetNetUsageV2:        make(map[string]int64, entries),
	}
	for i := 0; i < entries; i++ {
		legacy := "asset-" + strconv.Itoa(i)
		v2 := strconv.Itoa(1_000_000 + i)
		value := int64(i + 1)
		pb.Asset[legacy] = value
		pb.AssetV2[v2] = value
		pb.LatestAssetOperationTime[legacy] = value * 10
		pb.LatestAssetOperationTimeV2[v2] = value * 10
		pb.FreeAssetNetUsage[legacy] = value * 100
		pb.FreeAssetNetUsageV2[v2] = value * 100
	}
	return pb
}

var benchmarkAccountMarshalBytes []byte

func BenchmarkAccountDeterministicMapMarshal(b *testing.B) {
	pb := benchmarkMapRichAccount(64)
	b.Run("protobuf-reflection", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkAccountMarshalBytes, _ = standardDeterministicAccount(pb)
		}
	})
	b.Run("direct-maps", func(b *testing.B) {
		account := NewAccountFromPB(pb)
		if _, err := account.Marshal(); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			benchmarkAccountMarshalBytes, _ = account.Marshal()
		}
	})
	b.Run("storage-core-no-maps", func(b *testing.B) {
		account := NewAccountFromPB(pb)
		if _, err := account.MarshalStorageCore(); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			benchmarkAccountMarshalBytes, _ = account.MarshalStorageCore()
		}
	})
}
