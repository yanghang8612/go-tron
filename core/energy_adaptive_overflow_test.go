package core

import (
	"math"
	"testing"

	"github.com/tronprotocol/go-tron/core/state"
)

// Golden standard: java-tron EnergyProcessor.updateAdaptiveTotalEnergyLimit
// (chainbase/.../db/EnergyProcessor.java:83-88) computes
//
//	upperBound = BigInteger.valueOf(totalEnergyLimit)
//	    .multiply(BigInteger.valueOf(multiplier)).longValueExact()
//
// BEFORE the min/max clamp. longValueExact() throws ArithmeticException when the
// product does not fit in a signed 64-bit long, which unwinds the block-processing
// stack and rejects the block. The CI oracle
// framework/.../db/CalculateGlobalLimitHardenTest.java:319-345 pins this with two
// assertThrows(ArithmeticException.class, ...) cases — crucially, the throw depends
// only on the ceiling overflowing, NOT on the final clamped result's size.

// TestUpdateAdaptiveTotalEnergyLimit_OverflowRejects mirrors
// CalculateGlobalLimitHardenTest.testUpdateAdaptiveTotalEnergyLimitOverflowDetected:
// limit=1e16 × multiplier=1000 = 1e19 > 2^63-1 (≈9.22e18) ⇒ reject. Note targetLimit
// is MAX so the contract branch yields a small result, yet java still throws purely
// from the ceiling overflow.
func TestUpdateAdaptiveTotalEnergyLimit_OverflowRejects(t *testing.T) {
	dp := state.NewDynamicProperties()
	dp.SetAllowHardenResourceCalculation(true)
	dp.SetTotalEnergyAverageUsage(0)
	dp.SetTotalEnergyTargetLimit(math.MaxInt64)
	dp.SetTotalEnergyCurrentLimit(10_000_000_000_000_000) // 1e16
	dp.SetTotalEnergyLimit(10_000_000_000_000_000)        // 1e16
	dp.SetAdaptiveResourceLimitMultiplier(1000)

	if err := UpdateAdaptiveTotalEnergyLimit(dp); err == nil {
		t.Fatalf("expected overflow error (java ArithmeticException), got nil; "+
			"currentLimit silently wrapped to %d", dp.TotalEnergyCurrentLimit())
	}
}

// TestUpdateAdaptiveTotalEnergyLimit_MultiplierOverflowRejects mirrors
// CalculateGlobalLimitHardenTest.testUpdateAdaptiveLimitMultiplierOverflowDetected:
// limit=MaxInt64/100 × multiplier=1000 ≈ 10×MaxInt64 ⇒ reject.
func TestUpdateAdaptiveTotalEnergyLimit_MultiplierOverflowRejects(t *testing.T) {
	dp := state.NewDynamicProperties()
	dp.SetAllowHardenResourceCalculation(true)
	dp.SetTotalEnergyAverageUsage(0)
	dp.SetTotalEnergyTargetLimit(math.MaxInt64)
	dp.SetTotalEnergyCurrentLimit(1_000_000)
	dp.SetTotalEnergyLimit(math.MaxInt64 / 100)
	dp.SetAdaptiveResourceLimitMultiplier(1000)

	if err := UpdateAdaptiveTotalEnergyLimit(dp); err == nil {
		t.Fatalf("expected overflow error (java ArithmeticException), got nil; "+
			"currentLimit silently wrapped to %d", dp.TotalEnergyCurrentLimit())
	}
}

// TestUpdateAdaptiveTotalEnergyLimit_ReachableProposalMaxima proves the overflow is
// reachable with inputs all inside the proposal validators' accepted ranges:
// total_energy_limit = 1e17 (proposal #19 upper bound) and multiplier = 10000
// (proposal #29 upper bound) ⇒ 1e21 ≫ 2^63-1 ⇒ reject.
func TestUpdateAdaptiveTotalEnergyLimit_ReachableProposalMaxima(t *testing.T) {
	dp := state.NewDynamicProperties()
	dp.SetAllowHardenResourceCalculation(true)
	dp.SetTotalEnergyAverageUsage(0)
	dp.SetTotalEnergyTargetLimit(math.MaxInt64)
	dp.SetTotalEnergyCurrentLimit(100_000_000_000_000_000) // 1e17
	dp.SetTotalEnergyLimit(100_000_000_000_000_000)        // 1e17, proposal #19 max
	dp.SetAdaptiveResourceLimitMultiplier(10000)           // proposal #29 max

	if err := UpdateAdaptiveTotalEnergyLimit(dp); err == nil {
		t.Fatalf("expected overflow error at proposal maxima, got nil; "+
			"currentLimit silently wrapped to %d", dp.TotalEnergyCurrentLimit())
	}
}

// TestUpdateAdaptiveTotalEnergyLimit_HardenNormalNoReject guards against
// false-positive block rejection: with mainnet-scale params (limit=1.8e11,
// multiplier=1000 ⇒ ceiling 1.8e14, no overflow) harden=true must succeed and
// produce the correctly-clamped expansion, identical to harden=false.
// This is the go analogue of CalculateGlobalLimitHardenTest
// .testUpdateAdaptiveTotalEnergyLimitParity (harden on==off for normal params).
func TestUpdateAdaptiveTotalEnergyLimit_HardenNormalNoReject(t *testing.T) {
	const (
		limit      = int64(180_000_000_000) // 1.8e11, mainnet total_energy_limit
		multiplier = int64(1000)
	)

	run := func(harden bool) int64 {
		dp := state.NewDynamicProperties()
		dp.SetAllowHardenResourceCalculation(harden)
		dp.SetTotalEnergyLimit(limit)
		dp.SetTotalEnergyCurrentLimit(limit)
		dp.SetTotalEnergyTargetLimit(limit / 14400)
		dp.SetTotalEnergyAverageUsage(0) // under target → expand
		dp.SetAdaptiveResourceLimitMultiplier(multiplier)
		if err := UpdateAdaptiveTotalEnergyLimit(dp); err != nil {
			t.Fatalf("harden=%v: unexpected error on normal params: %v", harden, err)
		}
		return dp.TotalEnergyCurrentLimit()
	}

	hardenOff := run(false)
	hardenOn := run(true)

	wantExpand := limit * expandRateNumerator / expandRateDenominator // 180_180_180_180
	if hardenOff != wantExpand {
		t.Fatalf("harden=false expand: got %d, want %d", hardenOff, wantExpand)
	}
	if hardenOn != hardenOff {
		t.Fatalf("harden parity: on=%d != off=%d", hardenOn, hardenOff)
	}
}

// TestUpdateAdaptiveTotalEnergyLimit_PreHardenNeverRejects guards the fork gate:
// with harden=false (pre-proposal-#97) the legacy int64 multiply path is retained
// byte-for-byte and MUST NOT return an error even when the product overflows — the
// exact-multiply check is harden-only. (Pre-#97 wraps silently, matching java's
// non-harden `totalEnergyLimit * multiplier` branch at EnergyProcessor.java:86.)
func TestUpdateAdaptiveTotalEnergyLimit_PreHardenNeverRejects(t *testing.T) {
	dp := state.NewDynamicProperties()
	dp.SetAllowHardenResourceCalculation(false)
	dp.SetTotalEnergyAverageUsage(0)
	dp.SetTotalEnergyTargetLimit(math.MaxInt64)
	dp.SetTotalEnergyCurrentLimit(10_000_000_000_000_000) // 1e16
	dp.SetTotalEnergyLimit(10_000_000_000_000_000)        // 1e16 × 1000 overflows
	dp.SetAdaptiveResourceLimitMultiplier(1000)

	// Capture the legacy wrapped ceiling so we prove behaviour is byte-identical.
	wantCeiling := dp.TotalEnergyLimit() * dp.AdaptiveResourceLimitMultiplier() // wraps
	// avgUsage(0) <= target(MAX) → expand branch (harden=false → ok always true).
	wantResult, _ := scaleByRate(dp.TotalEnergyCurrentLimit(),
		expandRateNumerator, expandRateDenominator, false)
	floor := dp.TotalEnergyLimit()
	if wantResult < floor {
		wantResult = floor
	}
	if wantResult > wantCeiling {
		wantResult = wantCeiling
	}

	if err := UpdateAdaptiveTotalEnergyLimit(dp); err != nil {
		t.Fatalf("harden=false must never reject (legacy wraps silently), got: %v", err)
	}
	if got := dp.TotalEnergyCurrentLimit(); got != wantResult {
		t.Fatalf("harden=false legacy result changed: got %d, want %d", got, wantResult)
	}
}
