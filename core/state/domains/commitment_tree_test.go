package domains

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"golang.org/x/crypto/sha3"
)

func referenceLegacyKeccak(parts ...[]byte) common.Hash {
	h := sha3.NewLegacyKeccak256()
	for _, part := range parts {
		_, _ = h.Write(part)
	}
	var out common.Hash
	copy(out[:], h.Sum(nil))
	return out
}

func TestCommitmentHashFastPathsMatchReference(t *testing.T) {
	key := []byte("commitment-key")
	value := []byte("commitment-value")
	var keyLen, valueLen [8]byte
	binary.BigEndian.PutUint64(keyLen[:], uint64(len(key)))
	binary.BigEndian.PutUint64(valueLen[:], uint64(len(value)))

	pathDigest := referenceLegacyKeccak(keyLen[:], key)
	var wantPath [pathLen]byte
	for i, b := range pathDigest {
		wantPath[2*i] = b >> 4
		wantPath[2*i+1] = b & 0x0f
	}
	if got := keyPath(key); got != wantPath {
		t.Fatalf("keyPath = %x, want %x", got, wantPath)
	}

	wantLeaf := referenceLegacyKeccak([]byte{0x00}, keyLen[:], key, valueLen[:], value)
	if got := leafValueHash(key, value); got != wantLeaf {
		t.Fatalf("leafValueHash = %x, want %x", got, wantLeaf)
	}

	hashChild := common.Hash{0x11, 0x22}
	leafChild := common.Hash{0x33, 0x44}
	var branch BranchData
	branch.SetHashChild(2, hashChild)
	branch.SetLeafChild(9, []byte("ignored-by-node-hash"), leafChild)
	wantNode := referenceLegacyKeccak(
		[]byte{0x01},
		[]byte{0x02}, hashChild[:],
		[]byte{0x09}, leafChild[:],
	)
	if got := branch.nodeHash(); got != wantNode {
		t.Fatalf("nodeHash = %x, want %x", got, wantNode)
	}

	// Exercise the maximum-size 529-byte node preimage as well as the sparse
	// case above. Alternate child kinds; leaf keys are deliberately excluded
	// from the node hash by the commitment format.
	parts := make([][]byte, 1, 1+16*2)
	parts[0] = []byte{0x01}
	var full BranchData
	for nibble := uint8(0); nibble < 16; nibble++ {
		childHash := common.Hash{nibble, nibble + 1, nibble + 2}
		if nibble%2 == 0 {
			full.SetHashChild(nibble, childHash)
		} else {
			full.SetLeafChild(nibble, []byte{0xff, nibble}, childHash)
		}
		parts = append(parts, []byte{nibble}, childHash[:])
	}
	wantFull := referenceLegacyKeccak(parts...)
	if got := full.nodeHash(); got != wantFull {
		t.Fatalf("full nodeHash = %x, want %x", got, wantFull)
	}
}

func TestBranchDataRoundTrip(t *testing.T) {
	var b BranchData

	h := common.Hash{0xAB, 0xCD}
	b.SetHashChild(0x3, h)

	key := []byte("somekey")
	valHash := common.Hash{0x12, 0x34}
	b.SetLeafChild(0xf, key, valHash)

	enc := b.Encode()
	got, err := DecodeBranchData(enc)
	if err != nil {
		t.Fatalf("DecodeBranchData: %v", err)
	}
	if !b.Equal(got) {
		t.Fatalf("decoded branch not Equal to original")
	}
}

func TestBranchDataDecodeLeafKeyOwnership(t *testing.T) {
	var source BranchData
	source.SetLeafChild(4, []byte("borrow-me"), common.Hash{0xaa})
	encoded := source.Encode()

	var copied, borrowed BranchData
	if err := DecodeBranchDataInto(encoded, &copied); err != nil {
		t.Fatal(err)
	}
	if err := decodeBranchDataIntoNoCopy(encoded, &borrowed); err != nil {
		t.Fatal(err)
	}
	keyOffset := bytes.Index(encoded, []byte("borrow-me"))
	if keyOffset < 0 {
		t.Fatal("encoded leaf key not found")
	}
	encoded[keyOffset] = 'B'

	copiedKey, _ := copied.leafChildAt(4)
	if string(copiedKey) != "borrow-me" {
		t.Fatalf("public decoder retained input: %q", copiedKey)
	}
	borrowedKey, _ := borrowed.leafChildAt(4)
	if string(borrowedKey) != "Borrow-me" {
		t.Fatalf("no-copy decoder did not alias input: %q", borrowedKey)
	}
}

func TestBranchDataDeterministicAndProperty(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < 50; i++ {
		// Build a reference branch with random children.
		var ref BranchData
		for nibble := uint8(0); nibble < 16; nibble++ {
			if rng.Intn(2) == 0 {
				continue
			}
			if rng.Intn(2) == 0 {
				var h common.Hash
				rng.Read(h[:])
				ref.SetHashChild(nibble, h)
			} else {
				keyLen := rng.Intn(32) + 1
				key := make([]byte, keyLen)
				rng.Read(key)
				var vh common.Hash
				rng.Read(vh[:])
				ref.SetLeafChild(nibble, key, vh)
			}
		}

		// Encode → decode → must be Equal.
		enc := ref.Encode()
		got, err := DecodeBranchData(enc)
		if err != nil {
			t.Fatalf("iter %d: DecodeBranchData: %v", i, err)
		}
		if !ref.Equal(got) {
			t.Fatalf("iter %d: decoded branch not Equal", i)
		}

		// Insert same children in a different (reverse) order into a second branch;
		// Encode must be byte-identical.
		var ref2 BranchData
		for nibble := int(15); nibble >= 0; nibble-- {
			c := ref.children[nibble]
			if !c.present {
				continue
			}
			if c.kind == 0 {
				ref2.SetHashChild(uint8(nibble), c.valueHash)
			} else {
				ref2.SetLeafChild(uint8(nibble), c.leafKey, c.valueHash)
			}
		}
		enc2 := ref2.Encode()
		if !bytes.Equal(enc, enc2) {
			t.Fatalf("iter %d: encoding not deterministic: enc=%x enc2=%x", i, enc, enc2)
		}
	}
}

func TestBranchDataDecodeSafety(t *testing.T) {
	var b BranchData
	b.SetHashChild(0x1, common.Hash{0x11})
	b.SetLeafChild(0x5, []byte("key"), common.Hash{0x55})
	valid := b.Encode()

	// Truncate at every possible length — must not panic.
	for i := 0; i < len(valid); i++ {
		_, err := DecodeBranchData(valid[:i])
		if err == nil {
			// Only the full-length decode should succeed.
			t.Fatalf("truncated at %d bytes unexpectedly succeeded", i)
		}
	}

	// Garbage bytes.
	if _, err := DecodeBranchData([]byte{0xFF, 0xFF, 0xFF, 0x00}); err == nil {
		t.Fatal("garbage decode should fail")
	}

	// Trailing bytes after valid data.
	trailing := append(append([]byte{}, valid...), 0x00)
	if _, err := DecodeBranchData(trailing); err == nil {
		t.Fatal("trailing bytes should fail")
	}

	// Invalid kind byte.
	bad := append([]byte{}, valid...)
	// After the 2-byte childMask, the first child entry starts at byte 2.
	// Set kind to 0xFF.
	bad[2] = 0xFF
	if _, err := DecodeBranchData(bad); err == nil {
		t.Fatal("invalid kind byte should fail")
	}
}
