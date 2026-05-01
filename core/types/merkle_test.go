package types

import (
	"crypto/sha256"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

func leafHash(b byte) common.Hash {
	var h common.Hash
	h[31] = b
	return h
}

// TestMerkleRoot_Empty: java-tron stores 32 bytes of zero in `tx_trie_root`
// for blocks with no transactions (verified live: block #1 of the local
// java-tron private chain has txTrieRoot == 32×0).
func TestMerkleRoot_Empty(t *testing.T) {
	if got := MerkleRoot(nil); got != (common.Hash{}) {
		t.Fatalf("empty: want zero hash, got %x", got)
	}
}

// TestMerkleRoot_Single: single leaf carries up unchanged
// (java-tron MerkleTree returns the leaf as the root when count == 1).
func TestMerkleRoot_Single(t *testing.T) {
	leaf := leafHash(0x42)
	if got := MerkleRoot([]common.Hash{leaf}); got != leaf {
		t.Fatalf("single: want %x, got %x", leaf, got)
	}
}

// TestMerkleRoot_TwoLeaves: parent = SHA256(left || right).
func TestMerkleRoot_TwoLeaves(t *testing.T) {
	a, b := leafHash(0xAA), leafHash(0xBB)
	h := sha256.New()
	h.Write(a[:])
	h.Write(b[:])
	var want common.Hash
	copy(want[:], h.Sum(nil))
	if got := MerkleRoot([]common.Hash{a, b}); got != want {
		t.Fatalf("two: want %x, got %x", want, got)
	}
}

// TestMerkleRoot_Three_OddCarriesUnchanged: with odd count at any level,
// the trailing leaf carries up unchanged (no doubling). This is the
// detail that diverges from naive Bitcoin-style merkle and is critical
// for matching java-tron.
func TestMerkleRoot_Three_OddCarriesUnchanged(t *testing.T) {
	a, b, c := leafHash(1), leafHash(2), leafHash(3)
	// Level 1: [SHA(a||b), c]
	h1 := sha256.New()
	h1.Write(a[:])
	h1.Write(b[:])
	var ab common.Hash
	copy(ab[:], h1.Sum(nil))
	// Level 2: SHA(ab || c)
	h2 := sha256.New()
	h2.Write(ab[:])
	h2.Write(c[:])
	var want common.Hash
	copy(want[:], h2.Sum(nil))
	if got := MerkleRoot([]common.Hash{a, b, c}); got != want {
		t.Fatalf("three: want %x, got %x", want, got)
	}
}

// TestMerkleRoot_Four_FullPaired: paired all the way up.
func TestMerkleRoot_Four_FullPaired(t *testing.T) {
	a, b, c, d := leafHash(1), leafHash(2), leafHash(3), leafHash(4)
	// Level 1: [SHA(a||b), SHA(c||d)]
	h := sha256.New()
	h.Write(a[:])
	h.Write(b[:])
	var ab common.Hash
	copy(ab[:], h.Sum(nil))
	h = sha256.New()
	h.Write(c[:])
	h.Write(d[:])
	var cd common.Hash
	copy(cd[:], h.Sum(nil))
	// Level 2: SHA(ab || cd)
	h = sha256.New()
	h.Write(ab[:])
	h.Write(cd[:])
	var want common.Hash
	copy(want[:], h.Sum(nil))
	if got := MerkleRoot([]common.Hash{a, b, c, d}); got != want {
		t.Fatalf("four: want %x, got %x", want, got)
	}
}
