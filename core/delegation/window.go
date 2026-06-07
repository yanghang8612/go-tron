package delegation

import (
	"math"
	"math/big"

	"github.com/tronprotocol/go-tron/params"
)

// This file ports java-tron's per-account resource-window math for the V2
// delegation path (ResourceProcessor.increase / increaseV2 recovery + the
// unDelegateIncrease / unDelegateIncreaseV2 owner+receiver window COMBINE).
// core/delegation cannot import core/actuator (it is imported BY them), so the
// recovery half is a 1:1 copy of core/resource_window.go::computeResourceIncrease
// (validated against the testUseEnergyInWindowSizeV2 java-CI oracle); the combine
// (getNewWindowSize) is the same formula that recovery already uses on the consume
// path, applied here with the owner/receiver window arguments java passes.

const resourcePrecision = 1_000_000

// computeResourceIncrease mirrors core/resource_window.go::computeResourceIncrease
// (java ResourceProcessor.increase V1 / increaseV2). Returns the new usage and the
// renormalized stored window (raw + optimized). With usage==0 it is a pure
// recovery that also renormalizes the window (java's recovery()).
func computeResourceIncrease(rawWindow int64, optimized bool, lastUsage, usage, lastTime, now int64, harden, cancelAllV2 bool) (newUsage, newRawWindow int64, newOptimized bool) {
	const precision = resourcePrecision
	windowSize := int64(params.WindowSizeSlots)
	oldWindowSize := windowSizeV1View(rawWindow, optimized)

	var averageLastUsage, averageUsage int64
	if harden {
		averageLastUsage = divideCeilBig(new(big.Int).Mul(big.NewInt(lastUsage), big.NewInt(precision)), big.NewInt(oldWindowSize))
		averageUsage = divideCeilBig(new(big.Int).Mul(big.NewInt(usage), big.NewInt(precision)), big.NewInt(windowSize))
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

	newUsage = getUsage2(averageLastUsage, oldWindowSize, averageUsage, windowSize, harden)
	remainUsage := getUsage1(averageLastUsage, oldWindowSize, harden)

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
				new(big.Int).Mul(new(big.Int).Mul(big.NewInt(usage), big.NewInt(windowSize)), big.NewInt(params.WindowSizePrecision)),
			)
			nw = divideCeilBig(bi, big.NewInt(newUsage))
		} else {
			nw = divideCeil(remainUsage*remainWindowSize+usage*windowSize*params.WindowSizePrecision, newUsage)
		}
		if maxWindow := windowSize * params.WindowSizePrecision; nw > maxWindow {
			nw = maxWindow
		}
		return newUsage, nw, true
	}

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
		nw = new(big.Int).Quo(bi, big.NewInt(newUsage)).Int64()
	} else {
		nw = (remainUsage*remainWindowSize + usage*windowSize) / newUsage
	}
	return newUsage, nw, optimized
}

// combineOwnerWindow ports java ResourceProcessor.unDelegateIncrease's owner-side
// window combine: the new owner window is the usage-weighted blend of the owner's
// (post-recovery) window and the receiver's window. V1 uses getNewWindowSize
// (plain divide, V1-view windows); V2 (supportAllowCancelAllUnfreezeV2) uses
// divideCeil over the V2-view windows, clamped to windowSize*WINDOW_SIZE_PRECISION.
// Returns the new owner (rawWindow, optimized) to persist.
//
//	ownerUsage    = the owner's recovered usage (before adding transferUsage)
//	ownerRaw/Opt  = the owner's window after its own recovery (computeResourceIncrease)
//	recvRaw/Opt   = the receiver's window after its recovery
//	newOwnerUsage = ownerUsage + transferUsage (caller-checked > 0)
func combineOwnerWindow(ownerUsage, ownerRaw int64, ownerOpt bool, transferUsage, recvRaw int64, recvOpt bool, newOwnerUsage int64, cancelAllV2 bool) (int64, bool) {
	windowSize := int64(params.WindowSizeSlots)
	if cancelAllV2 {
		ownerWin := windowSizeV2View(ownerRaw, ownerOpt)
		recvWin := windowSizeV2View(recvRaw, recvOpt)
		if ownerWin < 0 {
			ownerWin = 0
		}
		if recvWin < 0 {
			recvWin = 0
		}
		nw := divideCeil(ownerUsage*ownerWin+transferUsage*recvWin, newOwnerUsage)
		if maxWindow := windowSize * params.WindowSizePrecision; nw > maxWindow {
			nw = maxWindow
		}
		return nw, true // setNewWindowSizeV2 marks optimized
	}
	ownerWin := windowSizeV1View(ownerRaw, ownerOpt)
	recvWin := windowSizeV1View(recvRaw, recvOpt)
	if ownerWin < 0 {
		ownerWin = 0
	}
	if recvWin < 0 {
		recvWin = 0
	}
	nw := (ownerUsage*ownerWin + transferUsage*recvWin) / newOwnerUsage
	return nw, ownerOpt // setNewWindowSize leaves optimized untouched
}

// zeroOwnerWindow ports the newOwnerUsage==0 branch (setNewWindowSize(windowSize)
// V1, setNewWindowSizeV2(windowSize*PRECISION) V2).
func zeroOwnerWindow(ownerOpt, cancelAllV2 bool) (int64, bool) {
	windowSize := int64(params.WindowSizeSlots)
	if cancelAllV2 {
		return windowSize * params.WindowSizePrecision, true
	}
	return windowSize, ownerOpt
}

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

func windowSizeV2View(raw int64, optimized bool) int64 {
	if raw == 0 {
		return params.WindowSizeSlots * params.WindowSizePrecision
	}
	if optimized {
		return raw
	}
	return raw * params.WindowSizePrecision
}

func getUsage1(usage, windowSize int64, harden bool) int64 {
	if harden {
		return new(big.Int).Quo(new(big.Int).Mul(big.NewInt(usage), big.NewInt(windowSize)), big.NewInt(resourcePrecision)).Int64()
	}
	return usage * windowSize / resourcePrecision
}

func getUsage2(oldUsage, oldWindowSize, newUsage, newWindowSize int64, harden bool) int64 {
	if harden {
		bi := new(big.Int).Add(
			new(big.Int).Mul(big.NewInt(oldUsage), big.NewInt(oldWindowSize)),
			new(big.Int).Mul(big.NewInt(newUsage), big.NewInt(newWindowSize)),
		)
		return new(big.Int).Quo(bi, big.NewInt(resourcePrecision)).Int64()
	}
	return (oldUsage*oldWindowSize + newUsage*newWindowSize) / resourcePrecision
}

func divideCeil(num, den int64) int64 {
	q := num / den
	if num%den > 0 {
		q++
	}
	return q
}

func divideCeilBig(num, den *big.Int) int64 {
	q, r := new(big.Int).QuoRem(num, den, new(big.Int))
	if r.Sign() > 0 {
		q.Add(q, big.NewInt(1))
	}
	return q.Int64()
}
