package state

import (
	"fmt"

	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	tcommon "github.com/tronprotocol/go-tron/common"
)

// StateAccountVersion is the flat account-latest envelope version written by
// this build. Version 4 stores a slim, explicitly encoded account core; its six TRC10
// maps, Owner/Witness/Active permissions, votes, Stake V1/V2 fields, frozen
// supply, and AccountResource live in account-local KV domains.
// Databases must be built from genesis by this codec; older protobuf-backed
// account envelopes are deliberately rejected.
const StateAccountVersion uint64 = 4

// EmptyKVRoot is retained in the account envelope for compatibility with older
// in-process callers. Flat-state commits write this value instead of rebuilding
// per-account KV tries.
var EmptyKVRoot = tcommon.Hash(ethtypes.EmptyRootHash)

// StateAccountV3 is the internal, versioned, RLP-encoded value stored in the
// flat account latest domain. It never leaks onto the wire, into blocks or
// transactions, or into RPC responses. AccountProto is a historical field name:
// v3 contains protobuf while v4 contains the internal non-protobuf core codec.
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

// stateAccountV2EncodedSizeFromProtoSize is the allocation-free size path used
// before a storage-core protobuf is appended directly into its final envelope.
// A one-byte RLP string depends on the byte value itself; callers handle that
// degenerate protobuf size through the ordinary byte-slice path.
func stateAccountV2EncodedSizeFromProtoSize(version uint64, accountProtoSize int, accountKVGeneration uint64) int {
	contentSize := stateAccountV2ContentSizeFromProtoSize(version, accountProtoSize, accountKVGeneration)
	return int(rlp.ListSize(uint64(contentSize)))
}

func stateAccountV2ContentSizeFromProtoSize(version uint64, accountProtoSize int, accountKVGeneration uint64) int {
	return rlp.IntSize(version) +
		rlpBytesSizeFromNonUnitLength(accountProtoSize) +
		1 + tcommon.HashLength +
		rlp.IntSize(accountKVGeneration) +
		1 + tcommon.HashLength
}

func rlpBytesSizeFromNonUnitLength(size int) int {
	if size < 56 {
		return 1 + size
	}
	return 1 + encodedSizeLen(size) + size
}

// appendStateAccountV2StorageCorePrefix appends the envelope through the RLP
// byte-string header for an account protobuf of accountProtoSize. The caller
// appends exactly that many protobuf bytes and then the trailer below.
func appendStateAccountV2StorageCorePrefix(dst []byte, version uint64, accountProtoSize int, accountKVGeneration uint64) []byte {
	contentSize := stateAccountV2ContentSizeFromProtoSize(version, accountProtoSize, accountKVGeneration)
	encodedSize := int(rlp.ListSize(uint64(contentSize)))
	if cap(dst)-len(dst) < encodedSize {
		grown := make([]byte, len(dst), len(dst)+encodedSize)
		copy(grown, dst)
		dst = grown
	}
	dst = appendRLPSize(dst, 0xc0, 0xf7, contentSize)
	dst = rlp.AppendUint64(dst, version)
	return appendRLPSize(dst, 0x80, 0xb7, accountProtoSize)
}

func appendStateAccountV2StorageCoreTrailer(dst []byte, accountKVRoot tcommon.Hash, accountKVGeneration uint64, codeHash tcommon.Hash) []byte {
	dst = append(dst, 0x80+tcommon.HashLength)
	dst = append(dst, accountKVRoot[:]...)
	dst = rlp.AppendUint64(dst, accountKVGeneration)
	dst = append(dst, 0x80+tcommon.HashLength)
	return append(dst, codeHash[:]...)
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
	return appendStateAccountV2StorageCoreTrailer(dst, accountKVRoot, accountKVGeneration, codeHash)
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

// DecodeStateAccountV3 parses the current flat account-latest envelope. The
// source name is retained for API stability, but only version 4 is accepted.
func DecodeStateAccountV3(data []byte) (*StateAccountV3, error) {
	v := new(StateAccountV3)
	if err := decodeStateAccountV3Into(data, v); err != nil {
		return nil, err
	}
	return v, nil
}

// decodeStateAccountV3Into is the caller-owned counterpart used by hot account
// hydration. AccountProto still receives its own durable copy: data may alias a
// pending blockbuffer layer or its bounded base-read cache and is guaranteed to
// remain valid only for this synchronous decode. Keeping the envelope itself in
// caller storage avoids a second heap object without weakening that ownership
// boundary.
func decodeStateAccountV3Into(data []byte, dst *StateAccountV3) error {
	content, trailing, err := rlp.SplitList(data)
	if err != nil {
		return fmt.Errorf("decode StateAccountV3: %w", err)
	}
	if len(trailing) != 0 {
		return fmt.Errorf("decode StateAccountV3: trailing bytes")
	}
	version, content, err := rlp.SplitUint64(content)
	if err != nil {
		return fmt.Errorf("decode StateAccountV3 version: %w", err)
	}
	accountProto, content, err := rlp.SplitString(content)
	if err != nil {
		return fmt.Errorf("decode StateAccountV3 account: %w", err)
	}
	accountKVRoot, content, err := rlp.SplitString(content)
	if err != nil {
		return fmt.Errorf("decode StateAccountV3 account root: %w", err)
	}
	if len(accountKVRoot) != tcommon.HashLength {
		return fmt.Errorf("decode StateAccountV3 account root: got %d bytes, want %d", len(accountKVRoot), tcommon.HashLength)
	}
	accountKVGeneration, content, err := rlp.SplitUint64(content)
	if err != nil {
		return fmt.Errorf("decode StateAccountV3 generation: %w", err)
	}
	codeHash, content, err := rlp.SplitString(content)
	if err != nil {
		return fmt.Errorf("decode StateAccountV3 code hash: %w", err)
	}
	if len(codeHash) != tcommon.HashLength {
		return fmt.Errorf("decode StateAccountV3 code hash: got %d bytes, want %d", len(codeHash), tcommon.HashLength)
	}
	if len(content) != 0 {
		return fmt.Errorf("decode StateAccountV3: too many list elements")
	}
	if version != StateAccountVersion {
		return fmt.Errorf("unsupported StateAccountV3 version %d (want %d)", version, StateAccountVersion)
	}
	ownedAccountProto := make([]byte, len(accountProto))
	copy(ownedAccountProto, accountProto)
	*dst = StateAccountV3{
		Version:             version,
		AccountProto:        ownedAccountProto,
		AccountKVGeneration: accountKVGeneration,
	}
	copy(dst.AccountKVRoot[:], accountKVRoot)
	copy(dst.CodeHash[:], codeHash)
	return nil
}

// DecodeStateAccountV2 is the source-compatible name for the strict v3 reader.
func DecodeStateAccountV2(data []byte) (*StateAccountV2, error) {
	return DecodeStateAccountV3(data)
}
