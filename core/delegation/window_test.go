package delegation

import "testing"

// TestComputeResourceIncrease_PerAccountWindow proves the recovery uses the
// account's STORED window, not the fixed global 28800 — the core of the
// #D-undelegate fix. Same usage+elapsed, different stored window → different
// recovered usage. Hand-derived against java ResourceProcessor.increase.
func TestComputeResourceIncrease_PerAccountWindow(t *testing.T) {
	// Stored window 14400 (half), usage 600, elapsed 7200 (now=7200, last=0):
	//   averageLastUsage = ceil(600*1e6/14400) = 41667
	//   decay (14400-7200)/14400 = 0.5 → round(41667*0.5)=round(20833.5)=20834
	//   newUsage = (20834*14400)/1e6 = 300
	if got, _, _ := computeResourceIncrease(14400, false, 600, 0, 0, 7200, false, false); got != 300 {
		t.Fatalf("per-account window=14400 recovery: got %d, want 300", got)
	}
	// Default/global window (raw=0 → 28800 view), same inputs → 450, proving the
	// stored window is what's read (450 != 300).
	if got, _, _ := computeResourceIncrease(0, false, 600, 0, 0, 7200, false, false); got != 450 {
		t.Fatalf("global window=28800 recovery: got %d, want 450", got)
	}
}

// TestComputeResourceIncrease_MatchesValidatedCore cross-checks this package's
// copy of computeResourceIncrease against the java-CI-validated values pinned in
// core/resource_window_test.go (org.tron.core.EnergyProcessorTest
// .testUseEnergyInWindowSizeV2 + the single-step settle golden), guarding against
// copy drift from the validated core/resource_window.go.
func TestComputeResourceIncrease_MatchesValidatedCore(t *testing.T) {
	// V2 single-step settle: window 14_400_000 (V2), usage 1_000_000 + 50_000,
	// 7_155 slots later → usage 553_125, window 9_193_462, optimized.
	if u, w, opt := computeResourceIncrease(14_400_000, true, 1_000_000, 50_000, 9_999, 17_154, false, true); u != 553_125 || w != 9_193_462 || !opt {
		t.Fatalf("V2 single-step: got (%d,%d,%v), want (553125,9193462,true)", u, w, opt)
	}
	// Recovery (usage=0), no elapsed → identity.
	if u, _, _ := computeResourceIncrease(0, false, 852_710_572, 0, 5, 5, false, false); u != 852_710_572 {
		t.Fatalf("recovery(delta=0): got %d, want 852710572", u)
	}
	// Recovery, elapsed >= window → fully decayed.
	if u, _, _ := computeResourceIncrease(0, false, 852_710_572, 0, 0, 28_800, false, false); u != 0 {
		t.Fatalf("recovery(delta>=window): got %d, want 0", u)
	}
}

// TestCombineOwnerWindow_V1 pins java ResourceProcessor.getNewWindowSize used by
// unDelegateIncrease: (ownerUsage*ownerWindow + transferUsage*recvWindow)/newOwnerUsage.
func TestCombineOwnerWindow_V1(t *testing.T) {
	// (300*20000 + 100*10000) / 400 = 7_000_000 / 400 = 17500.
	raw, opt := combineOwnerWindow(300, 20000, false, 100, 10000, false, 400, false, false)
	if raw != 17500 || opt {
		t.Fatalf("V1 combine: got (%d,%v), want (17500,false)", raw, opt)
	}
}

// TestCombineOwnerWindow_V2 pins unDelegateIncreaseV2: divideCeil over the V2-view
// windows, clamped to windowSize*WINDOW_SIZE_PRECISION, optimized=true.
func TestCombineOwnerWindow_V2(t *testing.T) {
	// ceil((300*20_000_000 + 100*10_000_000) / 400) = ceil(7_000_000_000/400) = 17_500_000
	// < 28_800*1000 = 28_800_000 → no clamp.
	raw, opt := combineOwnerWindow(300, 20_000_000, true, 100, 10_000_000, true, 400, false, true)
	if raw != 17_500_000 || !opt {
		t.Fatalf("V2 combine: got (%d,%v), want (17500000,true)", raw, opt)
	}
}
