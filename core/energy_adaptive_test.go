package core

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/params"
)

func TestHeadSlot(t *testing.T) {
	genesis := int64(1529891469000)
	blockTs := genesis + 3*params.BlockProducedInterval
	if got := HeadSlot(blockTs, genesis); got != 3 {
		t.Fatalf("HeadSlot: got %d, want 3", got)
	}
}

func TestIncrease_NoDecay(t *testing.T) {
	// Same slot — no decay applied, just adds new usage.
	got := increase(100, 50, 10, 10, 20)
	// averageLastUsage = ceil(100*1_000_000/20) = 5_000_000
	// averageUsage     = ceil(50*1_000_000/20) = 2_500_000
	// lastTime == now → no decay
	// total = 7_500_000 * 20 / 1_000_000 = 150
	if got != 150 {
		t.Fatalf("increase same-slot: got %d, want 150", got)
	}
}

func TestIncrease_PartialDecay(t *testing.T) {
	// Elapsed < windowSize — partial decay.
	got := increase(100, 0, 5, 15, 20)
	// averageLastUsage = ceil(100*1M/20) = 5_000_000
	// delta = 10, decay = (20-10)/20 = 0.5
	// averageLastUsage = round(5_000_000 * 0.5) = 2_500_000
	// averageUsage = 0
	// result = 2_500_000 * 20 / 1_000_000 = 50
	if got != 50 {
		t.Fatalf("increase partial-decay: got %d, want 50", got)
	}
}

func TestIncrease_FullDecay(t *testing.T) {
	// Elapsed >= windowSize — old usage fully decayed.
	got := increase(1000, 10, 0, 25, 20)
	// averageLastUsage decays to 0
	// averageUsage = ceil(10*1M/20) = 500_000
	// result = 500_000 * 20 / 1_000_000 = 10
	if got != 10 {
		t.Fatalf("increase full-decay: got %d, want 10", got)
	}
}

func TestIncrease_ZeroUsage(t *testing.T) {
	got := increase(0, 0, 0, 5, 20)
	if got != 0 {
		t.Fatalf("increase zero: got %d, want 0", got)
	}
}

func TestUpdateAdaptiveTotalEnergyLimit_ExpandWhenUnderTarget(t *testing.T) {
	dp := state.NewDynamicProperties()
	dp.SetTotalEnergyLimit(50_000_000_000)
	dp.SetTotalEnergyCurrentLimit(50_000_000_000)
	dp.SetTotalEnergyTargetLimit(3_472_222) // 50B / 14400
	dp.SetTotalEnergyAverageUsage(0)        // well under target
	dp.SetAdaptiveResourceLimitMultiplier(1000)

	UpdateAdaptiveTotalEnergyLimit(dp)

	got := dp.TotalEnergyCurrentLimit()
	// Expand: 50_000_000_000 * 1000 / 999 = 50_050_050_050
	want := int64(50_000_000_000) * 1000 / 999
	if got != want {
		t.Fatalf("expand: got %d, want %d", got, want)
	}
}

func TestUpdateAdaptiveTotalEnergyLimit_ContractWhenOverTarget(t *testing.T) {
	dp := state.NewDynamicProperties()
	dp.SetTotalEnergyLimit(50_000_000_000)
	dp.SetTotalEnergyCurrentLimit(60_000_000_000)
	dp.SetTotalEnergyTargetLimit(3_472_222)
	dp.SetTotalEnergyAverageUsage(5_000_000) // above target
	dp.SetAdaptiveResourceLimitMultiplier(1000)

	UpdateAdaptiveTotalEnergyLimit(dp)

	got := dp.TotalEnergyCurrentLimit()
	want := int64(60_000_000_000) * 99 / 100 // 59_400_000_000
	if got != want {
		t.Fatalf("contract: got %d, want %d", got, want)
	}
}

func TestUpdateAdaptiveTotalEnergyLimit_ClampsToFloor(t *testing.T) {
	dp := state.NewDynamicProperties()
	dp.SetTotalEnergyLimit(50_000_000_000)
	dp.SetTotalEnergyCurrentLimit(50_000_000_000) // already at floor
	dp.SetTotalEnergyTargetLimit(3_472_222)
	dp.SetTotalEnergyAverageUsage(5_000_000) // above target → contract
	dp.SetAdaptiveResourceLimitMultiplier(1000)

	UpdateAdaptiveTotalEnergyLimit(dp)

	got := dp.TotalEnergyCurrentLimit()
	// 50B * 99/100 = 49.5B, but floor is 50B
	if got != 50_000_000_000 {
		t.Fatalf("floor clamp: got %d, want %d", got, int64(50_000_000_000))
	}
}

func TestUpdateAdaptiveTotalEnergyLimit_ClampsToCeiling(t *testing.T) {
	dp := state.NewDynamicProperties()
	dp.SetTotalEnergyLimit(50_000_000_000)
	ceiling := int64(50_000_000_000) * 50 // multiplier = 50
	dp.SetTotalEnergyCurrentLimit(ceiling) // already at ceiling
	dp.SetTotalEnergyTargetLimit(3_472_222)
	dp.SetTotalEnergyAverageUsage(0) // under target → expand
	dp.SetAdaptiveResourceLimitMultiplier(50)

	UpdateAdaptiveTotalEnergyLimit(dp)

	got := dp.TotalEnergyCurrentLimit()
	if got != ceiling {
		t.Fatalf("ceiling clamp: got %d, want %d", got, ceiling)
	}
}

func TestUpdateTotalEnergyAverageUsage(t *testing.T) {
	dp := state.NewDynamicProperties()
	genesis := int64(1529891469000)

	// Simulate block at slot 100 with 1000 energy used.
	dp.SetLatestBlockHeaderTimestamp(genesis + 100*params.BlockProducedInterval)
	dp.SetBlockEnergyUsage(1000)
	dp.SetTotalEnergyAverageUsage(0)
	dp.SetTotalEnergyAverageTime(0)

	UpdateTotalEnergyAverageUsage(dp, genesis)

	avgUsage := dp.TotalEnergyAverageUsage()
	avgTime := dp.TotalEnergyAverageTime()

	if avgTime != 100 {
		t.Fatalf("avgTime: got %d, want 100", avgTime)
	}
	// First call: old usage fully decayed (slot 0 + 20 window < slot 100)
	// averageUsage = ceil(1000*1M/20) = 50_000_000
	// result = 50_000_000 * 20 / 1_000_000 = 1000
	if avgUsage != 1000 {
		t.Fatalf("avgUsage: got %d, want 1000", avgUsage)
	}
}

func TestUpdateTotalEnergyAverageUsage_Decay(t *testing.T) {
	dp := state.NewDynamicProperties()
	genesis := int64(1529891469000)

	// Set existing average to 1000 at slot 100.
	dp.SetTotalEnergyAverageUsage(1000)
	dp.SetTotalEnergyAverageTime(100)
	dp.SetBlockEnergyUsage(0)

	// Now at slot 110 (10 slots later, within 20-slot window).
	dp.SetLatestBlockHeaderTimestamp(genesis + 110*params.BlockProducedInterval)

	UpdateTotalEnergyAverageUsage(dp, genesis)

	avgUsage := dp.TotalEnergyAverageUsage()
	// decay = (20-10)/20 = 0.5
	// averageLastUsage = ceil(1000*1M/20) = 50_000_000
	// decayed = round(50_000_000 * 0.5) = 25_000_000
	// result = 25_000_000 * 20 / 1_000_000 = 500
	if avgUsage != 500 {
		t.Fatalf("decayed avg: got %d, want 500", avgUsage)
	}
}

func TestSetTotalEnergyLimit_SideEffects(t *testing.T) {
	dp := state.NewDynamicProperties()
	dp.SetAdaptiveResourceLimitTargetRatio(14400)

	// When adaptive energy is OFF, setting limit also sets currentLimit.
	dp.SetTotalEnergyLimit(100_000_000_000)

	if got := dp.TotalEnergyCurrentLimit(); got != 100_000_000_000 {
		t.Fatalf("currentLimit: got %d, want %d", got, int64(100_000_000_000))
	}
	if got := dp.TotalEnergyTargetLimit(); got != 100_000_000_000/14400 {
		t.Fatalf("targetLimit: got %d, want %d", got, 100_000_000_000/14400)
	}

	// When adaptive energy is ON, currentLimit is NOT touched.
	dp.SetAllowAdaptiveEnergy(true)
	dp.SetTotalEnergyCurrentLimit(80_000_000_000)
	dp.SetTotalEnergyLimit(90_000_000_000)

	if got := dp.TotalEnergyCurrentLimit(); got != 80_000_000_000 {
		t.Fatalf("currentLimit should be unchanged: got %d, want %d", got, int64(80_000_000_000))
	}
}
