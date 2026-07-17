package actuator

import (
	"math"
	"math/big"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/params"
)

// computeEnergyIncrease ports java-tron ResourceProcessor.increase (V1, the
// supportUnfreezeDelay path) and increaseV2 (the supportAllowCancelAllUnfreezeV2
// path) for the ENERGY resource.
//
// Given the account's stored window (raw field + optimized flag) and a
// (lastUsage, usage, lastTime, now) tuple, it returns the new energy usage and
// the new stored window (raw + optimized). It is pure: the caller persists the
// results. `cancelAllV2` selects the increaseV2 (V2-units, optimized) path.
//
// The recovery formula and window-renormalization match java byte-for-byte;
// golden values are pinned in energy_window_divergence_test.go.
func computeEnergyIncrease(rawWindow int64, optimized bool, lastUsage, usage, lastTime, now int64, harden, cancelAllV2 bool) (newUsage, newRawWindow int64, newOptimized bool) {
	const precision = resourcePrecisionForEnergy
	windowSize := int64(params.WindowSizeSlots) // global default window (slots)
	oldWindowSize := windowSizeV1View(rawWindow, optimized)

	var averageLastUsage, averageUsage int64
	if harden {
		averageLastUsage = divideCeilBigInt(
			new(big.Int).Mul(big.NewInt(lastUsage), big.NewInt(precision)), big.NewInt(oldWindowSize))
		averageUsage = divideCeilBigInt(
			new(big.Int).Mul(big.NewInt(usage), big.NewInt(precision)), big.NewInt(windowSize))
	} else {
		averageLastUsage = divideCeilInt(lastUsage*precision, oldWindowSize)
		averageUsage = divideCeilInt(usage*precision, windowSize)
	}

	if lastTime != now {
		if lastTime+oldWindowSize > now {
			delta := now - lastTime
			decay := float64(oldWindowSize-delta) / float64(oldWindowSize)
			averageLastUsage = int64(math.Round(float64(averageLastUsage) * decay))
		} else {
			averageLastUsage = 0
		}
	}

	newUsage = energyGetUsage2(averageLastUsage, oldWindowSize, averageUsage, windowSize, harden)
	remainUsage := energyGetUsage1(averageLastUsage, oldWindowSize, harden)

	if cancelAllV2 {
		if remainUsage == 0 {
			return newUsage, windowSize * params.WindowSizePrecision, true
		}
		oldWindowSizeV2 := windowSizeV2View(rawWindow, optimized)
		remainWindowSize := oldWindowSizeV2 - (now-lastTime)*params.WindowSizePrecision
		var nw int64
		if harden {
			bi := new(big.Int).Add(
				new(big.Int).Mul(big.NewInt(remainUsage), big.NewInt(remainWindowSize)),
				new(big.Int).Mul(new(big.Int).Mul(big.NewInt(usage), big.NewInt(windowSize)),
					big.NewInt(params.WindowSizePrecision)),
			)
			nw = divideCeilBigInt(bi, big.NewInt(newUsage))
		} else {
			nw = divideCeilInt(
				remainUsage*remainWindowSize+usage*windowSize*params.WindowSizePrecision, newUsage)
		}
		if maxWindow := windowSize * params.WindowSizePrecision; nw > maxWindow {
			nw = maxWindow
		}
		return newUsage, nw, true
	}

	// V1 (increase) path.
	if remainUsage == 0 {
		return newUsage, windowSize, optimized
	}
	remainWindowSize := oldWindowSize - (now - lastTime)
	var nw int64
	if harden {
		bi := new(big.Int).Add(
			new(big.Int).Mul(big.NewInt(remainUsage), big.NewInt(remainWindowSize)),
			new(big.Int).Mul(big.NewInt(usage), big.NewInt(windowSize)),
		)
		nw = common.BigInt64Exact(new(big.Int).Quo(bi, big.NewInt(newUsage)), "energy window size")
	} else {
		nw = (remainUsage*remainWindowSize + usage*windowSize) / newUsage
	}
	return newUsage, nw, optimized
}

// computeEnergyIncreaseGlobal ports java-tron ResourceProcessor.increase(lastUsage,
// usage, lastTime, now) — the 4-arg overload that runs over the GLOBAL windowSize
// (WINDOW_SIZE_MS / BLOCK_INTERVAL = 28800 slots) with no per-account window. This
// is the pre-Stake-2.0 (supportUnfreezeDelay == false) recovery/settle formula:
// EnergyProcessor.useEnergy calls increase(usage,0,…) to recover, then
// increase(R,energy,now,now) to add — both reduce to this helper.
//
// It is the precision-averaging formula (divideCeil*PRECISION + round(decay) +
// getUsage), NOT the plain truncate gtron used before; the truncate drifted ~1
// unit per recovered block, compounding to a consensus fork on busy contracts
// (see project_pre_stake2_energy_recovery_drift). It degenerates from
// computeEnergyIncrease with rawWindow==0 (oldWindowSize==windowSize==28800), so
// newUsage == getUsage(averageLastUsage+averageUsage, 28800) == java's increase.
// The recomputed window is discarded — the global window is never persisted
// per-account before Stake 2.0.
func computeEnergyIncreaseGlobal(lastUsage, usage, lastTime, now int64, harden bool) int64 {
	newUsage, _, _ := computeEnergyIncrease(0, false, lastUsage, usage, lastTime, now, harden, false)
	return newUsage
}

// windowSizeV1View mirrors AccountCapsule.getWindowSize(ENERGY) on a raw field.
func windowSizeV1View(raw int64, optimized bool) int64 {
	if raw == 0 {
		return params.WindowSizeSlots
	}
	if optimized {
		if raw < params.WindowSizePrecision {
			return params.WindowSizeSlots
		}
		return raw / params.WindowSizePrecision
	}
	return raw
}

// windowSizeV2View mirrors AccountCapsule.getWindowSizeV2(ENERGY) on a raw field.
func windowSizeV2View(raw int64, optimized bool) int64 {
	if raw == 0 {
		return params.WindowSizeSlots * params.WindowSizePrecision
	}
	if optimized {
		return raw
	}
	return raw * params.WindowSizePrecision
}

func divideCeilInt(num, den int64) int64 {
	q := num / den
	if num%den > 0 {
		q++
	}
	return q
}

// energyGetUsage1 mirrors java ResourceProcessor.getUsage(usage, windowSize).
func energyGetUsage1(usage, windowSize int64, harden bool) int64 {
	if harden {
		return common.BigInt64Exact(new(big.Int).Quo(
			new(big.Int).Mul(big.NewInt(usage), big.NewInt(windowSize)),
			big.NewInt(resourcePrecisionForEnergy)), "energy usage")
	}
	return usage * windowSize / resourcePrecisionForEnergy
}

// energyGetUsage2 mirrors java getUsage(oldUsage, oldWindowSize, newUsage, newWindowSize).
func energyGetUsage2(oldUsage, oldWindowSize, newUsage, newWindowSize int64, harden bool) int64 {
	if harden {
		bi := new(big.Int).Add(
			new(big.Int).Mul(big.NewInt(oldUsage), big.NewInt(oldWindowSize)),
			new(big.Int).Mul(big.NewInt(newUsage), big.NewInt(newWindowSize)),
		)
		return common.BigInt64Exact(new(big.Int).Quo(bi, big.NewInt(resourcePrecisionForEnergy)), "combined energy usage")
	}
	return (oldUsage*oldWindowSize + newUsage*newWindowSize) / resourcePrecisionForEnergy
}
