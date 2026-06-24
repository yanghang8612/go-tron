package core

import (
	"fmt"
	"math"
	"math/big"

	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/params"
)

const (
	contractRateNumerator   = 99
	contractRateDenominator = 100
	expandRateNumerator     = 1000
	expandRateDenominator   = 999
	adaptivePeriodMs        = 60_000
	adaptiveWindowSize      = adaptivePeriodMs / params.BlockProducedInterval // 20 blocks
	resourcePrecision       = 1_000_000
)

// HeadSlot returns the absolute slot number for the current head block,
// matching java-tron's EnergyProcessor.getHeadSlot.
func HeadSlot(latestBlockTimestamp, genesisTimestamp int64) int64 {
	return (latestBlockTimestamp - genesisTimestamp) / params.BlockProducedInterval
}

// UpdateTotalEnergyAverageUsage recomputes the global EWMA of per-block
// energy usage, matching java-tron's EnergyProcessor.updateTotalEnergyAverageUsage.
func UpdateTotalEnergyAverageUsage(dp *state.DynamicProperties, genesisTimestamp int64) {
	now := HeadSlot(dp.LatestBlockHeaderTimestamp(), genesisTimestamp)
	blockUsage := dp.BlockEnergyUsage()
	avgUsage := dp.TotalEnergyAverageUsage()
	avgTime := dp.TotalEnergyAverageTime()

	newAvg := increase(avgUsage, blockUsage, avgTime, now, adaptiveWindowSize)
	if dp.AllowHardenResourceCalculation() {
		newAvg = increaseHardened(avgUsage, blockUsage, avgTime, now, adaptiveWindowSize)
	}

	dp.SetTotalEnergyAverageUsage(newAvg)
	dp.SetTotalEnergyAverageTime(now)
}

// UpdateAdaptiveTotalEnergyLimit adjusts total_energy_current_limit per-block,
// matching java-tron's EnergyProcessor.updateAdaptiveTotalEnergyLimit.
//
// If average usage exceeds target: contract by 1% (×99/100).
// If average usage is at or below target: expand by ~0.1% (×1000/999).
// Result is clamped to [totalEnergyLimit, totalEnergyLimit × multiplier].
//
// Under allow_harden_resource_calculation (proposal #97) java computes the upper
// bound as BigInteger(totalEnergyLimit).multiply(multiplier).longValueExact(),
// which throws ArithmeticException — rejecting the block — when the product does
// not fit in a signed int64. That throw happens BEFORE the min/max clamp and
// depends only on the ceiling overflowing, independent of the final result's size
// (chainbase/.../db/EnergyProcessor.java:83-88; pinned by the assertThrows oracle
// in framework/.../db/CalculateGlobalLimitHardenTest.java:319-345). We reproduce
// that semantics by computing the ceiling with mulExactInt64 first and returning
// an error on overflow. Pre-#97 retains the legacy wrapping int64 multiply and
// never returns an error.
func UpdateAdaptiveTotalEnergyLimit(dp *state.DynamicProperties) error {
	avgUsage := dp.TotalEnergyAverageUsage()
	targetLimit := dp.TotalEnergyTargetLimit()
	currentLimit := dp.TotalEnergyCurrentLimit()
	baseLimit := dp.TotalEnergyLimit()

	var (
		result int64
		ok     bool
	)
	harden := dp.AllowHardenResourceCalculation()
	if avgUsage > targetLimit {
		result, ok = scaleByRate(currentLimit, contractRateNumerator, contractRateDenominator, harden)
	} else {
		result, ok = scaleByRate(currentLimit, expandRateNumerator, expandRateDenominator, harden)
	}
	if !ok {
		// java scaleByRate longValueExact() → ArithmeticException → reject block.
		return fmt.Errorf("adaptive total energy scaleByRate overflow: "+
			"currentLimit(%d) scaled exceeds int64", currentLimit)
	}

	multiplier := dp.AdaptiveResourceLimitMultiplier()
	floor := baseLimit
	// Compute the ceiling BEFORE clamping, matching java's evaluation order: under
	// harden, an overflow here is a block-rejection trigger regardless of result.
	ceiling := baseLimit * multiplier
	if harden {
		exact, ok := mulExactInt64(baseLimit, multiplier)
		if !ok {
			// java longValueExact() → ArithmeticException → reject block.
			return fmt.Errorf("adaptive total energy ceiling overflow: "+
				"totalEnergyLimit(%d) × multiplier(%d) exceeds int64", baseLimit, multiplier)
		}
		ceiling = exact
	}

	if result < floor {
		result = floor
	}
	if result > ceiling {
		result = ceiling
	}

	dp.SetTotalEnergyCurrentLimit(result)
	return nil
}

// mulExactInt64 multiplies a×b in big.Int and reports whether the product fits in
// a signed int64. It is the go analogue of java BigInteger.longValueExact(): false
// marks the exact ArithmeticException point java uses to reject a block.
func mulExactInt64(a, b int64) (int64, bool) {
	n := new(big.Int).Mul(big.NewInt(a), big.NewInt(b))
	if !n.IsInt64() {
		return 0, false
	}
	return n.Int64(), true
}

// increase implements java-tron's ResourceProcessor.increase(lastUsage, usage,
// lastTime, now, windowSize) — a sliding-window average with linear decay.
func increase(lastUsage, usage, lastTime, now, windowSize int64) int64 {
	averageLastUsage := divideCeil(lastUsage*resourcePrecision, windowSize)
	averageUsage := divideCeil(usage*resourcePrecision, windowSize)

	if lastTime != now {
		if lastTime+windowSize > now {
			delta := now - lastTime
			decay := float64(windowSize-delta) / float64(windowSize)
			averageLastUsage = int64(math.Round(float64(averageLastUsage) * decay))
		} else {
			averageLastUsage = 0
		}
	}

	averageLastUsage += averageUsage
	return averageLastUsage * windowSize / resourcePrecision
}

func increaseHardened(lastUsage, usage, lastTime, now, windowSize int64) int64 {
	averageLastUsage := divideCeilBig(
		new(big.Int).Mul(big.NewInt(lastUsage), big.NewInt(resourcePrecision)),
		big.NewInt(windowSize),
	)
	averageUsage := divideCeilBig(
		new(big.Int).Mul(big.NewInt(usage), big.NewInt(resourcePrecision)),
		big.NewInt(windowSize),
	)

	if lastTime != now {
		if lastTime+windowSize > now {
			delta := now - lastTime
			decay := float64(windowSize-delta) / float64(windowSize)
			averageLastUsage = int64(math.Round(float64(averageLastUsage) * decay))
		} else {
			averageLastUsage = 0
		}
	}

	return bigMulDivInt64(averageLastUsage+averageUsage, windowSize, resourcePrecision)
}

func divideCeil(numerator, denominator int64) int64 {
	result := numerator / denominator
	if numerator%denominator > 0 {
		result++
	}
	return result
}

// scaleByRate computes value × numerator / denominator. Under harden it mirrors
// java EnergyProcessor.scaleByRate (EnergyProcessor.java:197-205):
// BigInteger(value).multiply(numerator).divide(denominator).longValueExact(), so
// it reports ok=false when the divided result does not fit in int64 (java's
// ArithmeticException point). Pre-harden retains the legacy wrapping int64 path.
//
// On reachable inputs ok is always true (currentLimit ≤ totalEnergyLimit ≤ 1e17 by
// proposal #19, ×1000/999 stays ~1e17), so this never trips for valid chains; it is
// kept for structural parity and defence in depth, preserving the "harden ⇒ every
// multiplication is exact" invariant.
func scaleByRate(value, numerator, denominator int64, harden bool) (int64, bool) {
	if harden {
		return mulDivExactInt64(value, numerator, denominator)
	}
	return value * numerator / denominator, true
}

// mulDivExactInt64 computes a×b/c (big.Int, truncating division like java's
// BigInteger.divide) and reports whether the quotient fits in a signed int64 —
// the go analogue of BigInteger.longValueExact() applied to a*b/c.
func mulDivExactInt64(a, b, c int64) (int64, bool) {
	n := new(big.Int).Mul(big.NewInt(a), big.NewInt(b))
	n.Quo(n, big.NewInt(c))
	if !n.IsInt64() {
		return 0, false
	}
	return n.Int64(), true
}
