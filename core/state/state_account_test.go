package state

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"testing"

	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	tcommon "github.com/tronprotocol/go-tron/common"
)

type stateAccountV2Reference StateAccountV2

var stateAccountV2DecodeSink *StateAccountV2

func decodeStateAccountV2Reference(data []byte) (*StateAccountV2, error) {
	v := new(stateAccountV2Reference)
	if err := rlp.DecodeBytes(data, v); err != nil {
		return nil, err
	}
	if v.Version != StateAccountVersion {
		return nil, fmt.Errorf("unsupported version")
	}
	return (*StateAccountV2)(v), nil
}

func TestStateAccountV2EncodeMatchesGenericRLP(t *testing.T) {
	var nilValue *StateAccountV2
	gotNil, err := nilValue.Encode()
	if err != nil {
		t.Fatalf("nil direct encode: %v", err)
	}
	wantNil, err := rlp.EncodeToBytes((*stateAccountV2Reference)(nil))
	if err != nil {
		t.Fatalf("nil reference encode: %v", err)
	}
	if !bytes.Equal(gotNil, wantNil) {
		t.Fatalf("nil encoding mismatch: got %x want %x", gotNil, wantNil)
	}

	protos := [][]byte{
		nil,
		{0x00},
		{0x7f},
		{0x80},
		bytes.Repeat([]byte{0x11}, 55),
		bytes.Repeat([]byte{0x22}, 56),
		bytes.Repeat([]byte{0x33}, 255),
		bytes.Repeat([]byte{0x44}, 256),
		bytes.Repeat([]byte{0x55}, 4096),
		bytes.Repeat([]byte{0x66}, 65536),
	}
	integers := []uint64{0, 1, 0x7f, 0x80, 0xff, 0x100, math.MaxUint64}
	for i, accountProto := range protos {
		var accountRoot, codeHash tcommon.Hash
		for j := range accountRoot {
			accountRoot[j] = byte(i + j)
			codeHash[j] = byte(0xff - i - j)
		}
		value := &StateAccountV2{
			Version:             integers[i%len(integers)],
			AccountProto:        accountProto,
			AccountKVRoot:       accountRoot,
			AccountKVGeneration: integers[(i+3)%len(integers)],
			CodeHash:            codeHash,
		}
		got, err := value.Encode()
		if err != nil {
			t.Fatalf("case %d direct encode: %v", i, err)
		}
		want, err := rlp.EncodeToBytes((*stateAccountV2Reference)(value))
		if err != nil {
			t.Fatalf("case %d reference encode: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("case %d encoding mismatch:\n got  %x\n want %x", i, got, want)
		}
		if len(got) != cap(got) {
			t.Fatalf("case %d output len/cap = %d/%d, want exact allocation", i, len(got), cap(got))
		}
	}
}

func TestStateAccountV2RoundTrip(t *testing.T) {
	in := &StateAccountV2{
		Version:             StateAccountVersion,
		AccountProto:        []byte{0x0a, 0x15, 0x41, 0x01, 0x02},
		AccountKVRoot:       tcommon.BytesToHash([]byte{0xde, 0xad, 0xbe, 0xef}),
		AccountKVGeneration: 7,
		CodeHash:            tcommon.BytesToHash([]byte{0xca, 0xfe}),
	}
	enc, err := in.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := DecodeStateAccountV2(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Version != in.Version ||
		!bytes.Equal(out.AccountProto, in.AccountProto) ||
		out.AccountKVRoot != in.AccountKVRoot ||
		out.AccountKVGeneration != in.AccountKVGeneration ||
		out.CodeHash != in.CodeHash {
		t.Fatalf("round-trip mismatch:\n in=%+v\nout=%+v", in, out)
	}
}

func TestStateAccountV2DirectDecodeMatchesGenericRLP(t *testing.T) {
	var accountRoot, codeHash tcommon.Hash
	for i := range accountRoot {
		accountRoot[i] = byte(i)
		codeHash[i] = byte(0xff - i)
	}
	protos := [][]byte{
		nil,
		{},
		{0x00},
		{0x7f},
		{0x80},
		bytes.Repeat([]byte{0x11}, 55),
		bytes.Repeat([]byte{0x22}, 56),
		bytes.Repeat([]byte{0x33}, 256),
		bytes.Repeat([]byte{0x44}, 4096),
	}
	integers := []uint64{0, 1, 0x7f, 0x80, 0xff, 0x100, math.MaxUint64}
	var validEncodings [][]byte
	for i, accountProto := range protos {
		value := &StateAccountV2{
			Version:             StateAccountVersion,
			AccountProto:        accountProto,
			AccountKVRoot:       accountRoot,
			AccountKVGeneration: integers[i%len(integers)],
			CodeHash:            codeHash,
		}
		encoded, err := value.Encode()
		if err != nil {
			t.Fatal(err)
		}
		validEncodings = append(validEncodings, encoded)
		got, gotErr := DecodeStateAccountV2(encoded)
		want, wantErr := decodeStateAccountV2Reference(encoded)
		if gotErr != nil || wantErr != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("valid case %d mismatch: got=(%+v,%v) want=(%+v,%v)", i, got, gotErr, want, wantErr)
		}
	}

	// Compare acceptance and decoded values against the former generic decoder
	// for truncations, trailing data, and byte-level corruptions. Some mutations
	// remain valid RLP; those must decode to exactly the same fields.
	malformed := [][]byte{nil, {}, {0xc0}, {0x80}}
	for _, encoded := range validEncodings {
		for end := 0; end < len(encoded); end++ {
			malformed = append(malformed, append([]byte(nil), encoded[:end]...))
		}
		withTrailing := append(append([]byte(nil), encoded...), 0x00)
		malformed = append(malformed, withTrailing)
		for i := range encoded {
			mutated := append([]byte(nil), encoded...)
			mutated[i] ^= 0x80
			malformed = append(malformed, mutated)
		}
	}
	for i, encoded := range malformed {
		got, gotErr := DecodeStateAccountV2(encoded)
		want, wantErr := decodeStateAccountV2Reference(encoded)
		if (gotErr == nil) != (wantErr == nil) {
			t.Fatalf("corpus case %d acceptance mismatch for %x: got=%v want=%v", i, encoded, gotErr, wantErr)
		}
		if gotErr == nil && !reflect.DeepEqual(got, want) {
			t.Fatalf("corpus case %d value mismatch for %x: got=%+v want=%+v", i, encoded, got, want)
		}
	}
}

func BenchmarkStateAccountV2Decode(b *testing.B) {
	value := &StateAccountV2{
		Version:             StateAccountVersion,
		AccountProto:        bytes.Repeat([]byte{0x5a}, 512),
		AccountKVRoot:       tcommon.Hash{0x11, 0x22},
		AccountKVGeneration: 7,
		CodeHash:            tcommon.Hash{0x33, 0x44},
	}
	encoded, err := value.Encode()
	if err != nil {
		b.Fatal(err)
	}
	b.Run("direct", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			stateAccountV2DecodeSink, err = DecodeStateAccountV2(encoded)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("generic-reference", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			stateAccountV2DecodeSink, err = decodeStateAccountV2Reference(encoded)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestStateAccountV2Deterministic(t *testing.T) {
	v := &StateAccountV2{Version: StateAccountVersion, AccountProto: []byte{1, 2, 3}}
	a, _ := v.Encode()
	b, _ := v.Encode()
	if !bytes.Equal(a, b) {
		t.Fatal("encoding must be deterministic")
	}
}

func TestStateAccountV2RejectsWrongVersion(t *testing.T) {
	v := &StateAccountV2{Version: 99, AccountProto: []byte{1}}
	enc, _ := v.Encode()
	if _, err := DecodeStateAccountV2(enc); err == nil {
		t.Fatal("decode must reject unknown version")
	}
}

func TestEmptyKVRootIsEmptyTrieRoot(t *testing.T) {
	if EmptyKVRoot != tcommon.Hash(ethtypes.EmptyRootHash) {
		t.Fatal("EmptyKVRoot must equal the empty trie root")
	}
}
