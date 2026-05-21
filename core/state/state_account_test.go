package state

import (
	"bytes"
	"testing"

	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
)

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
