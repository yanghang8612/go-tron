package types

import (
	"testing"
)

func TestCatchUpToCycle_SameCycleNoOp(t *testing.T) {
	cs := NewContractState(5)
	cs.SetEnergyFactor(1000)
	changed := cs.CatchUpToCycle(5, 100, 2000, 10000, false)
	if changed {
		t.Fatal("same cycle should be no-op")
	}
	if cs.EnergyFactor() != 1000 {
		t.Fatalf("factor mutated: got %d, want 1000", cs.EnergyFactor())
	}
}

func TestCatchUpToCycle_UninitializedResets(t *testing.T) {
	cs := NewContractState(0) // update_cycle = 0 → uninitialized
	cs.AddEnergyUsage(999)
	cs.SetEnergyFactor(500)

	if !cs.CatchUpToCycle(10, 100, 2000, 10000, false) {
		t.Fatal("expected mutation")
	}
	if cs.UpdateCycle() != 10 {
		t.Fatalf("cycle: got %d, want 10", cs.UpdateCycle())
	}
	if cs.EnergyFactor() != 0 || cs.EnergyUsage() != 0 {
		t.Fatalf("state should be zeroed: factor=%d usage=%d", cs.EnergyFactor(), cs.EnergyUsage())
	}
}

func TestCatchUpToCycle_IncreaseAboveThreshold(t *testing.T) {
	// Java-tron worked example: cycle 1000 → 1001, usage 1M > threshold 900K,
	// increaseFactor=2000, maxFactor=10000, oldFactor=5000.
	// newFactor = min(10000, (5000+10000) * 1.2 - 10000) = min(10000, 8000) = 8000.
	cs := NewContractState(1000)
	cs.SetEnergyFactor(5000)
	cs.AddEnergyUsage(1_000_000)

	if !cs.CatchUpToCycle(1001, 900_000, 2000, 10000, false) {
		t.Fatal("expected mutation")
	}
	if cs.EnergyFactor() != 8000 {
		t.Fatalf("factor: got %d, want 8000", cs.EnergyFactor())
	}
	if cs.UpdateCycle() != 1001 {
		t.Fatalf("cycle: got %d, want 1001", cs.UpdateCycle())
	}
	if cs.EnergyUsage() != 0 {
		t.Fatalf("usage should reset: got %d", cs.EnergyUsage())
	}
}

func TestCatchUpToCycle_IncreaseClampedToMax(t *testing.T) {
	// oldFactor already at 9000; step push would go to 12000 but max is 10000.
	cs := NewContractState(100)
	cs.SetEnergyFactor(9000)
	cs.AddEnergyUsage(500_000)

	if !cs.CatchUpToCycle(101, 100_000, 2000, 10000, false) {
		t.Fatal("expected mutation")
	}
	// (9000+10000) * 1.2 = 22800; -10000 = 12800; clamped to 10000.
	if cs.EnergyFactor() != 10000 {
		t.Fatalf("factor: got %d, want 10000", cs.EnergyFactor())
	}
}

func TestCatchUpToCycle_DecreaseOverMultipleCycles(t *testing.T) {
	// Skip ahead 3 cycles with no high-usage event; factor decays.
	// decreaseBase = 1 - 2000/4/10000 = 0.95
	// decreasePct = 0.95^3 = 0.857375
	// raw = (5000+10000) * 0.857375 - 10000 = 12860.625 - 10000 = 2860
	cs := NewContractState(10)
	cs.SetEnergyFactor(5000)
	// No usage → skip increase branch.

	if !cs.CatchUpToCycle(13, 100_000, 2000, 10000, false) {
		t.Fatal("expected mutation")
	}
	got := cs.EnergyFactor()
	// Allow ±1 for float rounding.
	if got < 2859 || got > 2861 {
		t.Fatalf("decayed factor: got %d, want ~2860", got)
	}
	if cs.UpdateCycle() != 13 {
		t.Fatalf("cycle: got %d, want 13", cs.UpdateCycle())
	}
}

func TestCatchUpToCycle_IncreaseThenDecay(t *testing.T) {
	// cycle 10 with usage above threshold → bump to 11; then decay across 12, 13.
	// Step 1 (cycle 11): (5000+10000)*1.2 - 10000 = 8000 (below maxFactor).
	// Step 2 (cycles 12, 13): decay 2 cycles.
	//   base = 0.95, pct = 0.9025
	//   raw = (8000+10000)*0.9025 - 10000 = 16245 - 10000 = 6245
	cs := NewContractState(10)
	cs.SetEnergyFactor(5000)
	cs.AddEnergyUsage(1_000_000)

	if !cs.CatchUpToCycle(13, 900_000, 2000, 10000, false) {
		t.Fatal("expected mutation")
	}
	got := cs.EnergyFactor()
	if got < 6244 || got > 6246 {
		t.Fatalf("hybrid factor: got %d, want ~6245", got)
	}
}

func TestCatchUpToCycle_DecreaseFloorsAtZero(t *testing.T) {
	cs := NewContractState(1)
	cs.SetEnergyFactor(100) // tiny factor

	// Very long quiet stretch — 1000 cycles of decay.
	if !cs.CatchUpToCycle(1001, 100_000, 2000, 10000, false) {
		t.Fatal("expected mutation")
	}
	if cs.EnergyFactor() < 0 {
		t.Fatalf("factor should floor at 0: got %d", cs.EnergyFactor())
	}
}

func TestCatchUpToCycle_UsageAtThresholdNoIncrease(t *testing.T) {
	// strictly greater-than, so usage == threshold → no increase
	cs := NewContractState(5)
	cs.SetEnergyFactor(0)
	cs.AddEnergyUsage(1000)

	if !cs.CatchUpToCycle(6, 1000, 2000, 10000, false) {
		t.Fatal("expected mutation")
	}
	// No increase, 1 cycle of decay on factor=0 → stays 0.
	if cs.EnergyFactor() != 0 {
		t.Fatalf("factor: got %d, want 0", cs.EnergyFactor())
	}
}

func TestBytesRoundTrip(t *testing.T) {
	cs := NewContractState(42)
	cs.SetEnergyFactor(7777)
	cs.AddEnergyUsage(555)

	data, err := cs.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := NewContractStateFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.UpdateCycle() != 42 || loaded.EnergyFactor() != 7777 || loaded.EnergyUsage() != 555 {
		t.Fatalf("round-trip lost data: cycle=%d factor=%d usage=%d",
			loaded.UpdateCycle(), loaded.EnergyFactor(), loaded.EnergyUsage())
	}
}
