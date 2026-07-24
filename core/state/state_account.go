package state

import (
	"fmt"

	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	tcommon "github.com/tronprotocol/go-tron/common"
)

// StateAccountVersion is the only flat account-latest envelope version this
// build reads or writes. Version 3 stores a slim AccountProto; its six TRC10
// maps, Owner/Witness/Active permissions, votes, Stake V2 lists, and TRC10
// frozen supply, and AccountResource live in account-local KV domains.
// Fresh/replayed databases only: v2 envelopes are intentionally not migrated
// or accepted.
const StateAccountVersion uint64 = 3

// EmptyKVRoot is retained in the account envelope for compatibility with older
// in-process callers. Flat-state commits write this value instead of rebuilding
// per-account KV tries.
var EmptyKVRoot = tcommon.Hash(ethtypes.EmptyRootHash)

// StateAccountV3 is the internal, versioned, RLP-encoded value stored in the
// flat account latest domain. It is deterministic and independent of java-tron protobuf
// definitions; it never leaks onto the wire, into blocks/transactions, or into
// RPC responses. The java-tron account serialization is unchanged and lives in
// AccountProto.
type StateAccountV3 struct {
	Version             uint64
	AccountProto        []byte
	AccountKVRoot       tcommon.Hash
	AccountKVGeneration uint64
	CodeHash            tcommon.Hash
}

// Encode serializes the envelope with RLP (deterministic, list-framed).
func (v *StateAccountV3) Encode() ([]byte, error) {
	return rlp.EncodeToBytes(v)
}

// DecodeStateAccountV3 parses a flat account-latest envelope and enforces the version.
func DecodeStateAccountV3(data []byte) (*StateAccountV3, error) {
	v := new(StateAccountV3)
	if err := rlp.DecodeBytes(data, v); err != nil {
		return nil, fmt.Errorf("decode StateAccountV3: %w", err)
	}
	if v.Version != StateAccountVersion {
		return nil, fmt.Errorf("unsupported StateAccountV3 version %d (want %d)", v.Version, StateAccountVersion)
	}
	return v, nil
}

// StateAccountV2 is retained as a source-compatibility alias while the Erigon
// alignment branch is rebased. It does not imply v2 disk compatibility.
type StateAccountV2 = StateAccountV3

// DecodeStateAccountV2 is the source-compatible name for the strict v3 reader.
func DecodeStateAccountV2(data []byte) (*StateAccountV3, error) {
	return DecodeStateAccountV3(data)
}
