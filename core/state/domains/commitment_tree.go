package domains

import (
	"encoding/binary"
	"errors"

	"github.com/tronprotocol/go-tron/common"
)

// childKind distinguishes the two child types stored in a BranchData node.
const (
	kindHash = uint8(0) // 32-byte intermediate hash
	kindLeaf = uint8(1) // plain key bytes + 32-byte value hash
)

// branchChild holds one present child entry of a hex-trie branch node.
type branchChild struct {
	present     bool
	kind        uint8
	hashVal     common.Hash // valid when kind == kindHash
	leafKey     []byte      // valid when kind == kindLeaf
	leafValHash common.Hash // valid when kind == kindLeaf
}

// BranchData represents a hex (16-way) trie branch node.  A branch has up to
// 16 children indexed by nibble 0–15.  Each present child is either an
// intermediate hash child or a leaf (key bytes + value hash).
//
// Children are stored in a fixed 16-slot array so insertion order never
// affects encoding — Encode always iterates nibbles low→high.
type BranchData struct {
	children [16]branchChild
}

// SetHashChild marks nibble as a hash child with the given 32-byte hash.
// Overwrites any previous child at that nibble.
func (b *BranchData) SetHashChild(nibble uint8, h common.Hash) {
	b.children[nibble] = branchChild{
		present: true,
		kind:    kindHash,
		hashVal: h,
	}
}

// SetLeafChild marks nibble as a leaf child with the given key and value hash.
// Overwrites any previous child at that nibble.
func (b *BranchData) SetLeafChild(nibble uint8, key []byte, valHash common.Hash) {
	b.children[nibble] = branchChild{
		present:     true,
		kind:        kindLeaf,
		leafKey:     append([]byte(nil), key...),
		leafValHash: valHash,
	}
}

// Encode serialises the BranchData to a deterministic byte slice.
//
// Wire format:
//
//	[childMask uint16 big-endian]  — bitmask of present nibbles (bit i set ↔ child i present)
//	for each set bit i in childMask (low→high):
//	  [kind  1 byte]          0 = hash, 1 = leaf
//	  if kind == hash:
//	    [32-byte hash]
//	  if kind == leaf:
//	    [keyLen binary.Uvarint][key bytes][32-byte valHash]
func (b *BranchData) Encode() []byte {
	// Compute childMask.
	var mask uint16
	for i := uint8(0); i < 16; i++ {
		if b.children[i].present {
			mask |= 1 << i
		}
	}

	// Pre-compute required capacity for a single allocation.
	size := 2 // childMask
	for i := uint8(0); i < 16; i++ {
		c := &b.children[i]
		if !c.present {
			continue
		}
		size++ // kind byte
		if c.kind == kindHash {
			size += common.HashLength
		} else {
			// Uvarint for keyLen + key bytes + valHash
			size += binary.MaxVarintLen64 + len(c.leafKey) + common.HashLength
		}
	}

	out := make([]byte, 0, size)

	// Write childMask.
	out = append(out, byte(mask>>8), byte(mask))

	// Write children low→high nibble.
	for i := uint8(0); i < 16; i++ {
		c := &b.children[i]
		if !c.present {
			continue
		}
		out = append(out, c.kind)
		if c.kind == kindHash {
			out = append(out, c.hashVal[:]...)
		} else {
			var uvBuf [binary.MaxVarintLen64]byte
			n := binary.PutUvarint(uvBuf[:], uint64(len(c.leafKey)))
			out = append(out, uvBuf[:n]...)
			out = append(out, c.leafKey...)
			out = append(out, c.leafValHash[:]...)
		}
	}
	return out
}

// Equal reports whether b and other represent the same branch node.
// Two BranchData values are equal iff their encodings are byte-identical.
func (b BranchData) Equal(other BranchData) bool {
	enc1 := b.Encode()
	enc2 := other.Encode()
	if len(enc1) != len(enc2) {
		return false
	}
	for i := range enc1 {
		if enc1[i] != enc2[i] {
			return false
		}
	}
	return true
}

// DecodeBranchData parses a byte slice produced by BranchData.Encode.
// It returns an error on truncation, trailing bytes, invalid kind bytes, or
// a keyLen that exceeds the remaining input.
func DecodeBranchData(data []byte) (BranchData, error) {
	var b BranchData
	if len(data) < 2 {
		return b, errors.New("commitment_tree: input too short for childMask")
	}
	mask := uint16(data[0])<<8 | uint16(data[1])
	rest := data[2:]

	for i := uint8(0); i < 16; i++ {
		if mask&(1<<i) == 0 {
			continue
		}
		// Read kind byte.
		if len(rest) < 1 {
			return b, errors.New("commitment_tree: truncated at kind byte")
		}
		kind := rest[0]
		rest = rest[1:]

		switch kind {
		case kindHash:
			if len(rest) < common.HashLength {
				return b, errors.New("commitment_tree: truncated at hash child")
			}
			var h common.Hash
			copy(h[:], rest[:common.HashLength])
			rest = rest[common.HashLength:]
			b.children[i] = branchChild{present: true, kind: kindHash, hashVal: h}

		case kindLeaf:
			// Decode keyLen via Uvarint; bound by remaining slice length.
			keyLen, n := binary.Uvarint(rest)
			if n <= 0 {
				return b, errors.New("commitment_tree: invalid uvarint for keyLen")
			}
			rest = rest[n:]
			if keyLen > uint64(len(rest)) {
				return b, errors.New("commitment_tree: keyLen exceeds remaining input")
			}
			key := append([]byte(nil), rest[:keyLen]...)
			rest = rest[keyLen:]
			if len(rest) < common.HashLength {
				return b, errors.New("commitment_tree: truncated at leaf valHash")
			}
			var vh common.Hash
			copy(vh[:], rest[:common.HashLength])
			rest = rest[common.HashLength:]
			b.children[i] = branchChild{
				present:     true,
				kind:        kindLeaf,
				leafKey:     key,
				leafValHash: vh,
			}

		default:
			return b, errors.New("commitment_tree: unknown child kind byte")
		}
	}

	if len(rest) != 0 {
		return b, errors.New("commitment_tree: trailing bytes after decode")
	}
	return b, nil
}
