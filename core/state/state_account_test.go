package state

import (
	"bytes"
	"math"
	"testing"

	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	tcommon "github.com/tronprotocol/go-tron/common"
)

type stateAccountV2Reference StateAccountV2

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
