package types

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func hashWireBytes(number protowire.Number, value []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(nil, number, protowire.BytesType), value)
}

func hashWireVarint(number protowire.Number, value uint64) []byte {
	return protowire.AppendVarint(protowire.AppendTag(nil, number, protowire.VarintType), value)
}

func hashWireBlock(raw []byte) []byte {
	return hashWireBytes(1, hashWireBytes(1, raw))
}

func assertBlockTransactionHashOracles(t *testing.T, wire []byte) {
	t.Helper()
	want, wantErr := UnmarshalBlockBorrowed(wire)
	var got []common.Hash
	gotErr := IterateBlockTransactionHashes(context.Background(), wire, func(ordinal int, hash common.Hash) error {
		if ordinal != len(got) {
			t.Fatalf("ordinal = %d, want %d", ordinal, len(got))
		}
		got = append(got, hash)
		return nil
	})
	if (gotErr == nil) != (wantErr == nil) {
		t.Fatalf("decode errors differ: iterate=%v borrowed=%v wire=%x", gotErr, wantErr, wire)
	}
	if wantErr != nil {
		if len(got) != 0 {
			t.Fatalf("invalid block yielded %d hashes", len(got))
		}
		return
	}
	var wantHashes []common.Hash
	for _, tx := range want.Transactions() {
		wantHashes = append(wantHashes, tx.Hash())
	}
	if !reflect.DeepEqual(got, wantHashes) {
		t.Fatalf("borrowed hashes differ: got=%x want=%x wire=%x", got, wantHashes, wire)
	}
	var generated corepb.Block
	if err := proto.Unmarshal(wire, &generated); err != nil {
		if fast, _ := transactionHashBlockWire.canonical(context.Background(), wire, 0); fast {
			t.Fatalf("fast path accepted generated-decode error: %v", err)
		}
		// Historical pre-PQ field collisions intentionally require the legacy
		// fallback. The borrowed decoder remains authoritative for that case.
		return
	}
	if fast, _ := transactionHashBlockWire.canonical(context.Background(), wire, 0); fast {
		roundTrip, err := proto.Marshal(&generated)
		if err != nil || !bytes.Equal(wire, roundTrip) {
			t.Fatalf("fast path changed on generated round trip: %v\nwire=%x\nroundtrip=%x", err, wire, roundTrip)
		}
	}
	var generatedHashes []common.Hash
	for _, tx := range generated.Transactions {
		var hash common.Hash
		if tx.RawData != nil {
			encoded, err := proto.Marshal(tx.RawData)
			if err != nil {
				t.Fatal(err)
			}
			hash = sha256.Sum256(encoded)
		}
		generatedHashes = append(generatedHashes, hash)
	}
	if !reflect.DeepEqual(got, generatedHashes) {
		t.Fatalf("generated hashes differ: got=%x want=%x wire=%x", got, generatedHashes, wire)
	}
}

// Populate every supported field of every nested generated message. This is
// independent of the recognizer's compiled rule table; the Result map is the
// intentionally unsupported shape, covered by a fallback fixture above.
func populateTransactionHashMessage(message protoreflect.Message, seed [32]byte) {
	fields := message.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.IsMap() || field.IsPacked() {
			continue
		}
		if field.Kind() == protoreflect.MessageKind {
			if field.IsList() {
				list := message.Mutable(field).List()
				for range 2 {
					child := list.NewElement()
					populateTransactionHashMessage(child.Message(), seed)
					list.Append(child)
				}
			} else {
				populateTransactionHashMessage(message.Mutable(field).Message(), seed)
			}
			continue
		}
		number := int(field.Number())
		value := int64(binary.LittleEndian.Uint64(seed[number%24:]))
		var scalar protoreflect.Value
		switch field.Kind() {
		case protoreflect.Int64Kind:
			scalar = protoreflect.ValueOfInt64(value)
		case protoreflect.Int32Kind:
			scalar = protoreflect.ValueOfInt32(int32(value))
		case protoreflect.EnumKind:
			scalar = protoreflect.ValueOfEnum(protoreflect.EnumNumber(value))
		case protoreflect.BytesKind:
			scalar = protoreflect.ValueOfBytes(bytes.Clone(seed[:1+(number+int(seed[0]))%32]))
		case protoreflect.StringKind:
			scalar = protoreflect.ValueOfString("fixture-测试/" + hex.EncodeToString(seed[:4]))
		default:
			panic("unsupported structured hash fixture field " + string(field.FullName()))
		}
		if field.IsList() {
			list := message.Mutable(field).List()
			list.Append(scalar)
			list.Append(scalar)
		} else {
			message.Set(field, scalar)
		}
	}
}

func TestIterateBlockTransactionHashesAllCanonicalFields(t *testing.T) {
	for _, seed := range [][32]byte{{}, {1}, {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, sha256.Sum256([]byte("all-fields"))} {
		var block corepb.Block
		populateTransactionHashMessage(block.ProtoReflect(), seed)
		wire, err := proto.Marshal(&block)
		if err != nil {
			t.Fatal(err)
		}
		if fast, err := transactionHashBlockWire.canonical(context.Background(), wire, 0); err != nil || !fast {
			t.Fatalf("canonical generated all-field block missed fast path: %v", err)
		}
		assertBlockTransactionHashOracles(t, wire)
	}
}

func TestIterateBlockTransactionHashesCanonicalAndFallback(t *testing.T) {
	canonicalRaw := append(hashWireBytes(1, []byte{1, 2}), hashWireVarint(14, 3)...)
	canonicalTx := hashWireBytes(1, canonicalRaw)
	unknownRaw := append(bytes.Clone(canonicalRaw), hashWireVarint(31, 42)...)
	unknownGroup := protowire.AppendTag(nil, 30, protowire.StartGroupType)
	unknownGroup = append(unknownGroup, hashWireVarint(1, 2)...)
	unknownGroup = protowire.AppendTag(unknownGroup, 30, protowire.EndGroupType)
	negativePermission := hashWireBytes(11, hashWireVarint(5, ^uint64(0)))
	truncatedPermission := hashWireBytes(11, hashWireVarint(5, uint64(1)<<32|1))
	legacyTx := append(bytes.Clone(canonicalTx), hashWireBytes(6, []byte{0xff})...)
	mapResult := hashWireBytes(1, append(bytes.Clone(canonicalTx), hashWireBytes(5, hashWireBytes(28,
		append(hashWireBytes(1, []byte("ENERGY")), hashWireVarint(2, 4)...)))...))
	for _, tc := range []struct {
		name string
		wire []byte
		fast bool
	}{
		{"canonical", hashWireBlock(canonicalRaw), true},
		{"no-transactions", nil, true},
		{"absent-raw", hashWireBytes(1, nil), true},
		{"empty-raw", hashWireBlock(nil), true},
		{"empty-repeated-bytes", hashWireBytes(1, hashWireBytes(2, nil)), true},
		{"negative-int64", hashWireBlock(hashWireVarint(14, ^uint64(0))), true},
		{"negative-int32", hashWireBlock(negativePermission), true},
		{"truncated-int32", hashWireBlock(truncatedPermission), false},
		{"reordered-raw", hashWireBlock(append(hashWireVarint(14, 3), hashWireBytes(1, []byte{1, 2})...)), false},
		{"duplicate-scalar", hashWireBlock(append(bytes.Clone(canonicalRaw), hashWireVarint(14, 4)...)), false},
		{"duplicate-raw", hashWireBytes(1, append(bytes.Clone(canonicalTx), hashWireBytes(1, hashWireVarint(14, 4))...)), false},
		{"default-scalar", hashWireBlock(hashWireVarint(14, 0)), false},
		{"default-bytes", hashWireBlock(hashWireBytes(1, nil)), false},
		{"overlong-varint", hashWireBlock([]byte{0x70, 0x81, 0x00}), false},
		{"overlong-tag", hashWireBlock([]byte{0xf0, 0x00, 0x01}), false},
		{"overlong-length", hashWireBlock([]byte{0x0a, 0x81, 0x00, 0x01}), false},
		{"wrong-wire-type", hashWireBlock(hashWireBytes(14, []byte{1})), false},
		{"unknown-raw", hashWireBlock(unknownRaw), false},
		{"unknown-group", hashWireBlock(append(bytes.Clone(canonicalRaw), unknownGroup...)), false},
		{"map-result", mapResult, false},
		{"pre-PQ", rawBlockWithTransaction(t, legacyTx, 10_476_461, 8), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fast, err := transactionHashBlockWire.canonical(context.Background(), tc.wire, 0)
			if err != nil || fast != tc.fast {
				t.Fatalf("canonical = %v, %v, want %v", fast, err, tc.fast)
			}
			assertBlockTransactionHashOracles(t, tc.wire)
		})
	}
}

func TestIterateBlockTransactionHashesValidatesCompleteBlock(t *testing.T) {
	valid := hashWireBlock(hashWireVarint(14, 1))
	for name, wire := range map[string][]byte{
		"trailing-tag":         append(bytes.Clone(valid), 0x80),
		"trailing-transaction": append(bytes.Clone(valid), hashWireBytes(1, []byte{0x0a, 0x04, 0x01})...),
		"malformed-result":     hashWireBytes(1, append(hashWireBytes(1, nil), hashWireBytes(5, []byte{0x80})...)),
		"malformed-header":     append(bytes.Clone(valid), hashWireBytes(2, []byte{0x0a, 0x01, 0xff})...),
		"invalid-utf8":         hashWireBlock(hashWireBytes(11, hashWireBytes(2, hashWireBytes(1, []byte{0xff})))),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := UnmarshalBlockBorrowed(wire); err == nil {
				t.Fatal("invalid fixture decoded")
			}
			assertBlockTransactionHashOracles(t, wire)
		})
	}
}

func TestIterateBlockTransactionHashesCancellationAndCallbackErrors(t *testing.T) {
	valid := append(hashWireBlock(hashWireVarint(14, 1)), hashWireBlock(hashWireVarint(14, 2))...)
	for _, fallback := range []bool{false, true} {
		wire := bytes.Clone(valid)
		if fallback {
			wire = append(wire, hashWireVarint(31, 1)...)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		calls := 0
		if err := IterateBlockTransactionHashes(ctx, wire, func(int, common.Hash) error { calls++; return nil }); !errors.Is(err, context.Canceled) || calls != 0 {
			t.Fatalf("initial cancellation: calls=%d err=%v", calls, err)
		}
		ctx, cancel = context.WithCancel(context.Background())
		if err := IterateBlockTransactionHashes(ctx, wire, func(int, common.Hash) error {
			calls++
			cancel()
			return nil
		}); !errors.Is(err, context.Canceled) || calls != 1 {
			t.Fatalf("mid-block cancellation: calls=%d err=%v", calls, err)
		}
		calls = 0
		ctx, cancel = context.WithCancel(context.Background())
		if err := IterateBlockTransactionHashes(ctx, wire, func(int, common.Hash) error {
			calls++
			if calls == 2 {
				cancel()
			}
			return nil
		}); !errors.Is(err, context.Canceled) || calls != 2 {
			t.Fatalf("final-callback cancellation: calls=%d err=%v", calls, err)
		}
		calls = 0
		sentinel := errors.New("callback failed")
		if err := IterateBlockTransactionHashes(nil, wire, func(int, common.Hash) error { calls++; return sentinel }); !errors.Is(err, sentinel) || calls != 1 {
			t.Fatalf("callback error: calls=%d err=%v", calls, err)
		}
	}
	if err := IterateBlockTransactionHashes(nil, nil, nil); err == nil {
		t.Fatal("nil callback accepted")
	}
}

func FuzzIterateBlockTransactionHashes(f *testing.F) {
	canonical := blockDecodeReserveTestBlock(2)
	canonical.Proto().BlockHeader.RawData.ProtoReflect().SetUnknown(nil)
	wire, err := canonical.Marshal()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(wire)
	f.Add([]byte(nil))
	f.Add(hashWireBlock([]byte{0x70, 0x81, 0x00}))
	f.Add(hashWireBlock(hashWireVarint(31, 1)))
	f.Add(hashWireBytes(1, append(hashWireBytes(1, nil), hashWireBytes(1, hashWireVarint(14, 2))...)))
	f.Fuzz(func(t *testing.T, data []byte) {
		assertBlockTransactionHashOracles(t, data)
		// Mutate raw_data as well as the outer block so the hash equivalence
		// oracle receives plenty of structurally valid transaction envelopes.
		assertBlockTransactionHashOracles(t, hashWireBlock(data))
		// Structured generation exercises every successful fast-path field,
		// then mutates one bit to probe rejection/fallback around valid input.
		seed := sha256.Sum256(data)
		var generated corepb.Block
		populateTransactionHashMessage(generated.ProtoReflect(), seed)
		canonical, err := proto.Marshal(&generated)
		if err != nil {
			t.Fatal(err)
		}
		if fast, err := transactionHashBlockWire.canonical(context.Background(), canonical, 0); err != nil || !fast {
			t.Fatalf("structured canonical input missed fast path: %v", err)
		}
		assertBlockTransactionHashOracles(t, canonical)
		bit := int(binary.LittleEndian.Uint32(seed[:4])) % (len(canonical) * 8)
		canonical[bit/8] ^= byte(1) << (bit % 8)
		assertBlockTransactionHashOracles(t, canonical)
	})
}

func BenchmarkBlockTransactionHashes(b *testing.B) {
	block := blockDecodeReserveTestBlock(200)
	block.Proto().BlockHeader.RawData.ProtoReflect().SetUnknown(nil)
	wire, err := block.Marshal()
	if err != nil {
		b.Fatal(err)
	}
	if fast, err := transactionHashBlockWire.canonical(context.Background(), wire, 0); err != nil || !fast {
		b.Fatalf("benchmark fixture does not use fast path: %v", err)
	}
	b.Run("borrowed-block-and-hash", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(wire)))
		for b.Loop() {
			decoded, err := UnmarshalBlockBorrowed(wire)
			if err != nil {
				b.Fatal(err)
			}
			for _, tx := range decoded.Transactions() {
				benchmarkBlockHash = tx.Hash()
			}
		}
	})
	b.Run("canonical-wire", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(wire)))
		for b.Loop() {
			if err := IterateBlockTransactionHashes(context.Background(), wire, func(_ int, hash common.Hash) error {
				benchmarkBlockHash = hash
				return nil
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
