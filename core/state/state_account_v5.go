package state

import (
	"bytes"

	"github.com/ethereum/go-ethereum/rlp"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

// V5 retains the five-field RLP envelope and the exact storage-core bytes.
// Root: empty means EmptyKVRoot; 32 bytes preserve any explicit root.
// Code hash: empty means zero; {1} references a canonical 32-byte core CodeHash;
// {2} means Keccak256(empty code); 32 bytes preserve an independent hash.
// A core hash of a different length or
// value is never inferred or dropped. Address and unknown core fields remain
// self-contained, including for historical readers without an owner context.
const (
	stateAccountV5CoreCodeHashReference  byte = 1
	stateAccountV5EmptyCodeHashReference byte = 2
)

var stateAccountV5EmptyCodeHash = tcommon.Keccak256(nil)

func stateAccountV5ReferencesCodeHash(core []byte, hash tcommon.Hash) bool {
	if hash == (tcommon.Hash{}) || hash == stateAccountV5EmptyCodeHash || !types.IsAccountStorageCoreV4(core) {
		return false
	}
	inner, err := types.AccountStorageCoreV4CodeHash(core)
	return err == nil && bytes.Equal(inner, hash[:])
}

func stateAccountV5TrailerSize(root tcommon.Hash, generation uint64, hash tcommon.Hash, reference bool) int {
	n := 1 + rlp.IntSize(generation) + 1
	if root != EmptyKVRoot {
		n += tcommon.HashLength
	}
	if hash != (tcommon.Hash{}) && hash != stateAccountV5EmptyCodeHash && !reference {
		n += tcommon.HashLength
	}
	return n
}

func stateAccountV5EncodedSize(core []byte, root tcommon.Hash, generation uint64, hash tcommon.Hash) int {
	n := rlp.IntSize(StateAccountVersion) + rlpBytesSize(core) + stateAccountV5TrailerSize(root, generation, hash, stateAccountV5ReferencesCodeHash(core, hash))
	return int(rlp.ListSize(uint64(n)))
}

func stateAccountV5EncodedSizeFromCoreSize(coreSize int, root tcommon.Hash, generation uint64, hash tcommon.Hash, reference bool) int {
	n := rlp.IntSize(StateAccountVersion) + rlpBytesSizeFromNonUnitLength(coreSize) + stateAccountV5TrailerSize(root, generation, hash, reference)
	return int(rlp.ListSize(uint64(n)))
}

func appendStateAccountV5Prefix(dst []byte, coreSize int, root tcommon.Hash, generation uint64, hash tcommon.Hash, reference bool) []byte {
	n := rlp.IntSize(StateAccountVersion) + rlpBytesSizeFromNonUnitLength(coreSize) + stateAccountV5TrailerSize(root, generation, hash, reference)
	dst = reserveStateAccountV5(dst, int(rlp.ListSize(uint64(n))))
	dst = appendRLPSize(dst, 0xc0, 0xf7, n)
	dst = rlp.AppendUint64(dst, StateAccountVersion)
	return appendRLPSize(dst, 0x80, 0xb7, coreSize)
}

func reserveStateAccountV5(dst []byte, size int) []byte {
	if cap(dst)-len(dst) < size {
		grown := make([]byte, len(dst), len(dst)+size)
		copy(grown, dst)
		return grown
	}
	return dst
}

func appendStateAccountV5Trailer(dst []byte, root tcommon.Hash, generation uint64, hash tcommon.Hash, reference bool) []byte {
	if root == EmptyKVRoot {
		dst = append(dst, 0x80)
	} else {
		dst = append(dst, 0xa0)
		dst = append(dst, root[:]...)
	}
	dst = rlp.AppendUint64(dst, generation)
	switch {
	case hash == (tcommon.Hash{}):
		return append(dst, 0x80)
	case hash == stateAccountV5EmptyCodeHash:
		return append(dst, stateAccountV5EmptyCodeHashReference)
	case reference:
		return append(dst, stateAccountV5CoreCodeHashReference)
	default:
		dst = append(dst, 0xa0)
		return append(dst, hash[:]...)
	}
}

func appendStateAccountV5Fields(dst, core []byte, root tcommon.Hash, generation uint64, hash tcommon.Hash) []byte {
	reference := stateAccountV5ReferencesCodeHash(core, hash)
	n := rlp.IntSize(StateAccountVersion) + rlpBytesSize(core) + stateAccountV5TrailerSize(root, generation, hash, reference)
	dst = reserveStateAccountV5(dst, int(rlp.ListSize(uint64(n))))
	dst = appendRLPSize(dst, 0xc0, 0xf7, n)
	dst = rlp.AppendUint64(dst, StateAccountVersion)
	dst = appendRLPBytes(dst, core)
	return appendStateAccountV5Trailer(dst, root, generation, hash, reference)
}
