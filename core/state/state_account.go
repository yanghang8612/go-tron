package state

import (
	"fmt"

	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	tcommon "github.com/tronprotocol/go-tron/common"
)

// StateAccountVersion is the only flat account-latest envelope version this
// build reads or writes. Version 3 stores a slim AccountProto; its six TRC10
// maps, Owner/Witness/Active permissions, votes, Stake V1/V2 fields, frozen
// supply, and AccountResource live in account-local KV domains.
// Fresh/replayed databases only: v2 envelopes are intentionally not migrated
// or accepted.
const StateAccountVersion uint64 = 3

// EmptyKVRoot is retained in the account envelope for compatibility with older
// in-process callers. Flat-state commits write this value instead of rebuilding
// per-account KV tries.
var EmptyKVRoot = tcommon.Hash(ethtypes.EmptyRootHash)

// StateAccountV3 is the internal, versioned, RLP-encoded value stored in the
// flat account latest domain. It never leaks onto the wire, into blocks or
// transactions, or into RPC responses. The java-tron account serialization is
// unchanged and lives in AccountProto.
type StateAccountV3 struct {
	Version             uint64
	AccountProto        []byte
	AccountKVRoot       tcommon.Hash
	AccountKVGeneration uint64
	CodeHash            tcommon.Hash
}

// StateAccountV2 remains a source-compatibility alias while callers are moved
// to the v3 name. It does not imply v2 disk compatibility.
type StateAccountV2 = StateAccountV3

// Encode serializes the fixed envelope schema directly into one exact-sized
// RLP allocation rather than using reflection for every dirty account.
func (v *StateAccountV3) Encode() ([]byte, error) {
	if v == nil {
		// RLP's default nil encoding for a pointer-to-struct is an empty list.
		return []byte{0xc0}, nil
	}
	return encodeStateAccountV2Fields(v.Version, v.AccountProto, v.AccountKVRoot, v.AccountKVGeneration, v.CodeHash), nil
}

func encodeStateAccountV2Fields(version uint64, accountProto []byte, accountKVRoot tcommon.Hash, accountKVGeneration uint64, codeHash tcommon.Hash) []byte {
	return appendStateAccountV2Fields(nil, version, accountProto, accountKVRoot, accountKVGeneration, codeHash)
}

func stateAccountV2ContentSize(version uint64, accountProto []byte, accountKVGeneration uint64) int {
	return rlp.IntSize(version) +
		rlpBytesSize(accountProto) +
		1 + tcommon.HashLength +
		rlp.IntSize(accountKVGeneration) +
		1 + tcommon.HashLength
}

func stateAccountV2EncodedSize(version uint64, accountProto []byte, accountKVGeneration uint64) int {
	return int(rlp.ListSize(uint64(stateAccountV2ContentSize(version, accountProto, accountKVGeneration))))
}

func appendStateAccountV2Fields(dst []byte, version uint64, accountProto []byte, accountKVRoot tcommon.Hash, accountKVGeneration uint64, codeHash tcommon.Hash) []byte {
	contentSize := stateAccountV2ContentSize(version, accountProto, accountKVGeneration)
	encodedSize := int(rlp.ListSize(uint64(contentSize)))
	if cap(dst)-len(dst) < encodedSize {
		grown := make([]byte, len(dst), len(dst)+encodedSize)
		copy(grown, dst)
		dst = grown
	}
	dst = appendRLPSize(dst, 0xc0, 0xf7, contentSize)
	dst = rlp.AppendUint64(dst, version)
	dst = appendRLPBytes(dst, accountProto)
	dst = append(dst, 0x80+tcommon.HashLength)
	dst = append(dst, accountKVRoot[:]...)
	dst = rlp.AppendUint64(dst, accountKVGeneration)
	dst = append(dst, 0x80+tcommon.HashLength)
	return append(dst, codeHash[:]...)
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

// DecodeStateAccountV3 parses a flat account-latest envelope and enforces the
// v3-only disk schema.
func DecodeStateAccountV3(data []byte) (*StateAccountV3, error) {
	content, trailing, err := rlp.SplitList(data)
	if err != nil {
		return nil, fmt.Errorf("decode StateAccountV3: %w", err)
	}
	if len(trailing) != 0 {
		return nil, fmt.Errorf("decode StateAccountV3: trailing bytes")
	}
	version, content, err := rlp.SplitUint64(content)
	if err != nil {
		return nil, fmt.Errorf("decode StateAccountV3 version: %w", err)
	}
	accountProto, content, err := rlp.SplitString(content)
	if err != nil {
		return nil, fmt.Errorf("decode StateAccountV3 account: %w", err)
	}
	accountKVRoot, content, err := rlp.SplitString(content)
	if err != nil {
		return nil, fmt.Errorf("decode StateAccountV3 account root: %w", err)
	}
	if len(accountKVRoot) != tcommon.HashLength {
		return nil, fmt.Errorf("decode StateAccountV3 account root: got %d bytes, want %d", len(accountKVRoot), tcommon.HashLength)
	}
	accountKVGeneration, content, err := rlp.SplitUint64(content)
	if err != nil {
		return nil, fmt.Errorf("decode StateAccountV3 generation: %w", err)
	}
	codeHash, content, err := rlp.SplitString(content)
	if err != nil {
		return nil, fmt.Errorf("decode StateAccountV3 code hash: %w", err)
	}
	if len(codeHash) != tcommon.HashLength {
		return nil, fmt.Errorf("decode StateAccountV3 code hash: got %d bytes, want %d", len(codeHash), tcommon.HashLength)
	}
	if len(content) != 0 {
		return nil, fmt.Errorf("decode StateAccountV3: too many list elements")
	}
	if version != StateAccountVersion {
		return nil, fmt.Errorf("unsupported StateAccountV3 version %d (want %d)", version, StateAccountVersion)
	}
	v := &StateAccountV3{
		Version:             version,
		AccountProto:        make([]byte, len(accountProto)),
		AccountKVGeneration: accountKVGeneration,
	}
	copy(v.AccountProto, accountProto)
	copy(v.AccountKVRoot[:], accountKVRoot)
	copy(v.CodeHash[:], codeHash)
	return v, nil
}

// DecodeStateAccountV2 is the source-compatible name for the strict v3 reader.
func DecodeStateAccountV2(data []byte) (*StateAccountV2, error) {
	return DecodeStateAccountV3(data)
}
