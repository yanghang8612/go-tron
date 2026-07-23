package state

import (
	"fmt"

	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	tcommon "github.com/tronprotocol/go-tron/common"
)

// StateAccountVersion is the only flat account-latest envelope version this
// build reads or writes. Fresh databases only; legacy raw-proto values are not
// supported.
const StateAccountVersion uint64 = 2

// EmptyKVRoot is retained in the account envelope for compatibility with older
// in-process callers. Flat-state commits write this value instead of rebuilding
// per-account KV tries.
var EmptyKVRoot = tcommon.Hash(ethtypes.EmptyRootHash)

// StateAccountV2 is the internal, versioned, RLP-encoded value stored in the
// flat account latest domain. It is deterministic and independent of java-tron protobuf
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

// Encode serializes the envelope with RLP (deterministic, list-framed). The
// schema is fixed, so write it directly into one exact-sized allocation rather
// than routing every dirty account through RLP's reflection encoder and an
// escaping StateAccountV2 interface value.
func (v *StateAccountV2) Encode() ([]byte, error) {
	if v == nil {
		// RLP's default nil encoding for a pointer-to-struct is an empty list.
		return []byte{0xc0}, nil
	}
	return encodeStateAccountV2Fields(v.Version, v.AccountProto, v.AccountKVRoot, v.AccountKVGeneration, v.CodeHash), nil
}

func encodeStateAccountV2Fields(version uint64, accountProto []byte, accountKVRoot tcommon.Hash, accountKVGeneration uint64, codeHash tcommon.Hash) []byte {
	contentSize := rlp.IntSize(version) +
		rlpBytesSize(accountProto) +
		1 + tcommon.HashLength +
		rlp.IntSize(accountKVGeneration) +
		1 + tcommon.HashLength
	out := make([]byte, 0, int(rlp.ListSize(uint64(contentSize))))
	out = appendRLPSize(out, 0xc0, 0xf7, contentSize)
	out = rlp.AppendUint64(out, version)
	out = appendRLPBytes(out, accountProto)
	out = append(out, 0x80+tcommon.HashLength)
	out = append(out, accountKVRoot[:]...)
	out = rlp.AppendUint64(out, accountKVGeneration)
	out = append(out, 0x80+tcommon.HashLength)
	out = append(out, codeHash[:]...)
	return out
}

func rlpBytesSize(value []byte) int {
	n := len(value)
	if n == 1 && value[0] < 0x80 {
		return 1
	}
	if n < 56 {
		return 1 + n
	}
	return 1 + encodedSizeLen(n) + n
}

func appendRLPBytes(dst, value []byte) []byte {
	if len(value) == 1 && value[0] < 0x80 {
		return append(dst, value[0])
	}
	dst = appendRLPSize(dst, 0x80, 0xb7, len(value))
	return append(dst, value...)
}

func appendRLPSize(dst []byte, shortTag, longTag byte, size int) []byte {
	if size < 56 {
		return append(dst, shortTag+byte(size))
	}
	sizeLen := encodedSizeLen(size)
	dst = append(dst, longTag+byte(sizeLen))
	for shift := (sizeLen - 1) * 8; shift >= 0; shift -= 8 {
		dst = append(dst, byte(size>>shift))
	}
	return dst
}

func encodedSizeLen(size int) int {
	n := 0
	for ; size != 0; size >>= 8 {
		n++
	}
	return n
}

// DecodeStateAccountV2 parses a flat account-latest envelope and enforces the version.
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
