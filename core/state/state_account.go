package state

import (
	"fmt"

	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	tcommon "github.com/tronprotocol/go-tron/common"
)

// StateAccountVersion is the only account-trie envelope version this build
// reads or writes. Fresh databases only; legacy raw-proto values are not
// supported.
const StateAccountVersion uint64 = 2

// EmptyKVRoot is the AccountKVRoot value for an account with no generic-KV
// entries (the empty trie root). Phase 1 always uses this.
var EmptyKVRoot = tcommon.Hash(ethtypes.EmptyRootHash)

// StateAccountV2 is the internal, versioned, RLP-encoded value stored in the
// account trie. It is deterministic and independent of java-tron protobuf
// definitions; it never leaks onto the wire, into blocks/transactions, or into
// RPC responses. The java-tron account serialization is unchanged and lives in
// AccountProto.
type StateAccountV2 struct {
	Version             uint64
	AccountProto        []byte
	AccountKVRoot       tcommon.Hash
	AccountKVGeneration uint64
	CodeHash            tcommon.Hash
}

// Encode serializes the envelope with RLP (deterministic, list-framed).
func (v *StateAccountV2) Encode() ([]byte, error) {
	return rlp.EncodeToBytes(v)
}

// DecodeStateAccountV2 parses an account-trie value and enforces the version.
func DecodeStateAccountV2(data []byte) (*StateAccountV2, error) {
	v := new(StateAccountV2)
	if err := rlp.DecodeBytes(data, v); err != nil {
		return nil, fmt.Errorf("decode StateAccountV2: %w", err)
	}
	if v.Version != StateAccountVersion {
		return nil, fmt.Errorf("unsupported StateAccountV2 version %d (want %d)", v.Version, StateAccountVersion)
	}
	return v, nil
}
