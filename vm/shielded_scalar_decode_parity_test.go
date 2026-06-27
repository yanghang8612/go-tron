package vm

import "testing"

// java PrecompiledContracts shielded verifyMint/Transfer/Burn decode the leafCount
// and valueBalance scalars via parseLong -> DataWord.longValueSafe() (saturate to
// Long.MAX_VALUE for a >8-byte or negative word). gtron's shielded precompiles read
// them with parseUint64FromWord / parseInt64FromWord (raw low-64), so a crafted word
// like 2^64+k slipped a small value past the `leafCount >= TREE_WIDTH(2^32)` gate and
// fed a wrapped valueBalance to the binding-signature check. The shieldedLeafCount /
// shieldedValueBalance helpers now used at every shielded site must saturate.
//
// Note: a full verifyMint/Transfer replay needs the librustzcash Pedersen backend
// (build tag `sapling`); without it zksnark.EmptyRoots() fails and insertLeaves
// returns the failure payload regardless of the gate, so the gate divergence is not
// observable hermetically. This test locks the decode the sites depend on.
func TestShieldedScalarDecodeSaturatesLikeJava(t *testing.T) {
	const maxInt64 = int64(^uint64(0) >> 1) // java Long.MAX_VALUE

	// A 2^64+5 word: low-64 == 5, but a high byte (word index 23) is set, so
	// longValueSafe saturates (bytesOccupied 9 > 8).
	w := make([]byte, 32)
	w[23] = 0x01
	w[31] = 0x05

	if got := shieldedLeafCount(w, 0); got < shieldedTreeWidth {
		t.Fatalf("shieldedLeafCount(2^64+5) = %d, want >= TREE_WIDTH %d (java longValueSafe saturates)", got, shieldedTreeWidth)
	}
	if got := shieldedValueBalance(w, 0); got != maxInt64 {
		t.Fatalf("shieldedValueBalance(2^64+5) = %d, want Long.MAX_VALUE %d", got, maxInt64)
	}

	// Red guard: the OLD raw low-64 decode read 5 and slipped past the TREE_WIDTH gate.
	if raw := parseUint64FromWord(w, 0); raw != 5 || raw >= shieldedTreeWidth {
		t.Fatalf("fixture: raw low-64 decode = %d, want 5 (< TREE_WIDTH) to prove the divergence", raw)
	}

	// Clean small scalars decode unchanged.
	clean := make([]byte, 32)
	clean[31] = 0x05
	if got := shieldedLeafCount(clean, 0); got != 5 {
		t.Fatalf("shieldedLeafCount(5) = %d, want 5", got)
	}
	if got := shieldedValueBalance(clean, 0); got != 5 {
		t.Fatalf("shieldedValueBalance(5) = %d, want 5", got)
	}
}
