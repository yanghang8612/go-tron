package vm

import "testing"

// TestCombinedMemoryExpansionCost pins the CALL-family memory charge against java
// EnergyCost.getCalculateCallCost: `calcMemEnergy(oldMemSize, in.max(out))` — a
// SINGLE expansion to max(inEnd, retEnd). gtron previously charged the input and
// return expansions separately, both baselined on the un-resized memory, which
// double-counted the overlapping region (a consensus over-charge that lowered
// contract.Energy before the 63/64 split).
func TestCombinedMemoryExpansionCost(t *testing.T) {
	// Audit scenario: empty memory, in=[0,32], ret=[32,32]. java charges
	// calcMemEnergy(0, 64) = 6; the old double-charge gave 3 + 6 = 9.
	mem := newMemory()
	if got := combinedMemoryExpansionCost(mem, 0, 32, 32, 32); got != 6 {
		t.Fatalf("empty mem in=[0,32] ret=[32,32]: got %d, want 6 (java in.max(out)=64)", got)
	}
	if got := memoryExpansionCost(mem, 0, 64); got != 6 {
		t.Fatalf("sanity: single expansion to 64 = %d, want 6", got)
	}

	// Identical/overlapping regions charged once, not twice.
	if got := combinedMemoryExpansionCost(mem, 0, 32, 0, 32); got != 3 {
		t.Fatalf("overlapping in=ret=[0,32]: got %d, want 3 (one 32-byte word)", got)
	}

	// Common Solidity case (input already inside memory): combined == the return
	// expansion alone, so the hot path is unchanged by the fix.
	warm := newMemory()
	warm.resize(64)
	gotCommon := combinedMemoryExpansionCost(warm, 0, 32, 64, 32) // in fits; ret grows 64->96
	wantCommon := memoryExpansionCost(warm, 64, 32)               // ret region alone
	if gotCommon != wantCommon {
		t.Fatalf("common case: combined %d != ret-only %d (hot path must be unchanged)", gotCommon, wantCommon)
	}

	// Zero-size return region contributes nothing (java memNeeded(_,0)==0).
	if got := combinedMemoryExpansionCost(newMemory(), 0, 32, 999, 0); got != 3 {
		t.Fatalf("zero-size ret: got %d, want 3 (in=[0,32] only)", got)
	}
}
