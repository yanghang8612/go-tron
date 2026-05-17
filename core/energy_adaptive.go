package core

import (
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
func UpdateAdaptiveTotalEnergyLimit(dp *state.DynamicProperties) {
	avgUsage := dp.TotalEnergyAverageUsage()
	targetLimit := dp.TotalEnergyTargetLimit()
	currentLimit := dp.TotalEnergyCurrentLimit()
	baseLimit := dp.TotalEnergyLimit()

	var result int64
	harden := dp.AllowHardenResourceCalculation()
	if avgUsage > targetLimit {
		result = scaleByRate(currentLimit, contractRateNumerator, contractRateDenominator, harden)
	} else {
		result = scaleByRate(currentLimit, expandRateNumerator, expandRateDenominator, harden)
	}

	multiplier := dp.AdaptiveResourceLimitMultiplier()
	floor := baseLimit
	ceiling := baseLimit * multiplier
	if harden {
		ceiling = bigMulDivInt64(baseLimit, multiplier, 1)
	}

	if result < floor {
		result = floor
	}
	if result > ceiling {
		result = ceiling
	}

	dp.SetTotalEnergyCurrentLimit(result)
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

func scaleByRate(value, numerator, denominator int64, harden bool) int64 {
	if harden {
		return bigMulDivInt64(value, numerator, denominator)
	}
	return value * numerator / denominator
}
