package core

import (
	"math"
	"math/big"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/params"
)

// computeResourceIncrease ports java-tron ResourceProcessor.increase (V1, the
// supportUnfreezeDelay path) and increaseV2 (the supportAllowCancelAllUnfreezeV2
// path). The window math is resource-agnostic — it is the same code java runs
// for BANDWIDTH and ENERGY via getWindowSize(ResourceCode) — so this mirrors the
// energy port in actuator/energy_window.go (computeEnergyIncrease) exactly.
//
// Given the account's stored window (raw field + optimized flag) and a
// (lastUsage, usage, lastTime, now) tuple, it returns the new usage and the new
// stored window (raw + optimized). It is pure: the caller persists the results.
// `cancelAllV2` selects the increaseV2 (V2-units, optimized) path.
//
// Used by the bandwidth consume path (core/bandwidth.go) once supportUnfreezeDelay
// is active. Golden values are pinned in resource_window_test.go against the same
// java-CI oracles as the energy port.
func computeResourceIncrease(rawWindow int64, optimized bool, lastUsage, usage, lastTime, now int64, harden, cancelAllV2 bool) (newUsage, newRawWindow int64, newOptimized bool) {
	const precision = resourcePrecision
	windowSize := int64(params.WindowSizeSlots) // global default window (slots)
	oldWindowSize := resWindowSizeV1View(rawWindow, optimized)

	var averageLastUsage, averageUsage int64
	if harden {
		averageLastUsage = divideCeilBig(
			new(big.Int).Mul(big.NewInt(lastUsage), big.NewInt(precision)), big.NewInt(oldWindowSize))
		averageUsage = divideCeilBig(
			new(big.Int).Mul(big.NewInt(usage), big.NewInt(precision)), big.NewInt(windowSize))
	} else {
		averageLastUsage = divideCeil(lastUsage*precision, oldWindowSize)
		averageUsage = divideCeil(usage*precision, windowSize)
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

	newUsage = resGetUsage2(averageLastUsage, oldWindowSize, averageUsage, windowSize, harden)
	remainUsage := resGetUsage1(averageLastUsage, oldWindowSize, harden)

	if cancelAllV2 {
		if remainUsage == 0 {
			return newUsage, windowSize * params.WindowSizePrecision, true
		}
		oldWindowSizeV2 := resWindowSizeV2View(rawWindow, optimized)
		remainWindowSize := oldWindowSizeV2 - (now-lastTime)*params.WindowSizePrecision
		var nw int64
		if harden {
			bi := new(big.Int).Add(
				new(big.Int).Mul(big.NewInt(remainUsage), big.NewInt(remainWindowSize)),
				new(big.Int).Mul(new(big.Int).Mul(big.NewInt(usage), big.NewInt(windowSize)),
					big.NewInt(params.WindowSizePrecision)),
			)
			nw = divideCeilBig(bi, big.NewInt(newUsage))
		} else {
			nw = divideCeil(
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
		nw = tcommon.BigInt64Exact(new(big.Int).Quo(bi, big.NewInt(newUsage)), "resource window size")
	} else {
		nw = (remainUsage*remainWindowSize + usage*windowSize) / newUsage
	}
	return newUsage, nw, optimized
}

// resWindowSizeV1View mirrors AccountCapsule.getWindowSize(rc) on a raw field.
func resWindowSizeV1View(raw int64, optimized bool) int64 {
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

// resWindowSizeV2View mirrors AccountCapsule.getWindowSizeV2(rc) on a raw field.
func resWindowSizeV2View(raw int64, optimized bool) int64 {
	if raw == 0 {
		return params.WindowSizeSlots * params.WindowSizePrecision
	}
	if optimized {
		return raw
	}
	return raw * params.WindowSizePrecision
}

// resGetUsage1 mirrors java ResourceProcessor.getUsage(usage, windowSize).
func resGetUsage1(usage, windowSize int64, harden bool) int64 {
	if harden {
		return tcommon.BigInt64Exact(new(big.Int).Quo(
			new(big.Int).Mul(big.NewInt(usage), big.NewInt(windowSize)),
			big.NewInt(resourcePrecision)), "resource usage")
	}
	return usage * windowSize / resourcePrecision
}

// resGetUsage2 mirrors java getUsage(oldUsage, oldWindowSize, newUsage, newWindowSize).
func resGetUsage2(oldUsage, oldWindowSize, newUsage, newWindowSize int64, harden bool) int64 {
	if harden {
		bi := new(big.Int).Add(
			new(big.Int).Mul(big.NewInt(oldUsage), big.NewInt(oldWindowSize)),
			new(big.Int).Mul(big.NewInt(newUsage), big.NewInt(newWindowSize)),
		)
		return tcommon.BigInt64Exact(new(big.Int).Quo(bi, big.NewInt(resourcePrecision)), "combined resource usage")
	}
	return (oldUsage*oldWindowSize + newUsage*newWindowSize) / resourcePrecision
}
