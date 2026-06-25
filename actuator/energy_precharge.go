package actuator

import (
	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// energyPreCharge captures the inputs java-tron's TransactionTrace.resetAccountUsage
// needs to undo the pre-VM energy pre-charge after execution. The fields mirror the
// java receipt's caller/origin energy bookkeeping:
//
//	r            -> receipt.{Caller,Origin}EnergyUsage      (recovered usage at `now`)
//	sizeV1/sizeV2-> receipt.{Caller,Origin}EnergyWindowSize{,V2} (recovered window views)
//	merged       -> receipt.{Caller,Origin}EnergyMergedUsage (usage after the pre-charge increase)
//	mergedSizeV1 -> receipt.{Caller,Origin}EnergyMergedWindowSize
//
// old* hold the pristine state captured before the pre-charge so a reverted VM (where
// java never commits rootRepository and skips resetAccountUsage) discards the charge.
type energyPreCharge struct {
	addr         common.Address
	r            int64
	sizeV1       int64
	sizeV2       int64
	merged       int64
	mergedSizeV1 int64
	oldUsage     int64
	oldRawWindow int64
	oldOptimized bool
	oldTime      int64
}

// preChargeEnergyUsage mirrors the `if (allowTvmFreezeV2())` pre-charge block in
// java-tron VMActuator.getAccountEnergyLimitWithFixRatio /
// getTotalEnergyLimitWithFixRatio, executed BEFORE VM.play:
//
//	energyProcessor.updateUsage(account);                 // recover usage -> now (R)
//	account.setLatestConsumeTimeForEnergy(now);
//	account.setEnergyUsage(energyProcessor.increase(account, ENERGY,
//	    account.getEnergyUsage(), charge, now, now));      // merge in `charge`
//	rootRepository.updateAccount(account);                 // persist into VM-visible state
//
// `charge` is min(leftFrozenEnergy, energyFromFeeLimit) for the caller, or the origin's
// creatorEnergyLimit for the split path. Persisting the merged usage means a contract
// that reads this account's own energy usage mid-VM (via the staking-query precompiles)
// sees the charged value, exactly as java does. The pre-charge is recorded on `result`
// and undone by restoreEnergyPreCharges after the VM.
func preChargeEnergyUsage(ctx *Context, addr common.Address, charge int64, result *Result) {
	if ctx == nil || ctx.DynProps == nil || result == nil {
		return
	}
	// Gate identical to java VMConfig.allowTvmFreezeV2() (ConfigLoader sets it from
	// DynamicPropertiesStore.supportUnfreezeDelay()).
	if !ctx.DynProps.SupportUnfreezeDelay() {
		return
	}
	acct := ctx.State.GetAccount(addr)
	if acct == nil {
		return
	}
	now := ctx.ResourceTime()
	oldUsage := ctx.State.GetEnergyUsage(addr)
	oldTime := ctx.State.GetLatestConsumeTimeForEnergy(addr)
	oldRaw, oldOpt := acct.RawEnergyWindowSize(), acct.EnergyWindowOptimized()
	harden := ctx.DynProps.AllowHardenResourceCalculation()
	cancelAllV2 := ctx.DynProps.SupportCancelAllUnfreezeV2()

	// Step 1 — java EnergyProcessor.updateUsage: recover usage to `now`.
	r, rRaw, rOpt := computeEnergyIncrease(oldRaw, oldOpt, oldUsage, 0, oldTime, now, harden, cancelAllV2)
	// Step 2 — java increase(account, ENERGY, R, charge, now, now): merge in the charge.
	merged, mRaw, mOpt := computeEnergyIncrease(rRaw, rOpt, r, charge, now, now, harden, cancelAllV2)

	result.energyPreCharges = append(result.energyPreCharges, energyPreCharge{
		addr:         addr,
		r:            r,
		sizeV1:       windowSizeV1View(rRaw, rOpt),
		sizeV2:       windowSizeV2View(rRaw, rOpt),
		merged:       merged,
		mergedSizeV1: windowSizeV1View(mRaw, mOpt),
		oldUsage:     oldUsage,
		oldRawWindow: oldRaw,
		oldOptimized: oldOpt,
		oldTime:      oldTime,
	})

	ctx.State.SetEnergyUsage(addr, merged)
	ctx.State.SetEnergyWindow(addr, mRaw, mOpt)
	ctx.State.SetLatestConsumeTimeForEnergy(addr, now)
}

// restoreEnergyPreCharges undoes every pre-charge recorded on `result` after the VM,
// mirroring java-tron TransactionTrace.pay():
//
//   - SUCCESS (java commits rootRepository, then calls resetAccountUsage): subtract the
//     pre-charge's area contribution from the CURRENT (post-VM) usage, preserving any
//     change the VM itself made to this account's energy_usage.
//   - REVERT / exception / OOE (java never commits rootRepository and skips
//     resetAccountUsage): discard the pre-charge entirely — restore the pristine state.
//
// Either way the post-VM energy settle (PayEnergyBill) is left byte-identical to the
// no-pre-charge path, so the final on-chain energy_usage is unchanged.
func restoreEnergyPreCharges(ctx *Context, result *Result) {
	if ctx == nil || result == nil || len(result.energyPreCharges) == 0 {
		return
	}
	success := result.ContractRet == int32(corepb.Transaction_Result_SUCCESS)
	cancelAllV2 := ctx.DynProps != nil && ctx.DynProps.SupportCancelAllUnfreezeV2()
	for _, pc := range result.energyPreCharges {
		if !success {
			// Discard: java's rootRepository was never committed.
			ctx.State.SetEnergyUsage(pc.addr, pc.oldUsage)
			ctx.State.SetEnergyWindow(pc.addr, pc.oldRawWindow, pc.oldOptimized)
			ctx.State.SetLatestConsumeTimeForEnergy(pc.addr, pc.oldTime)
			continue
		}
		acct := ctx.State.GetAccount(pc.addr)
		if acct == nil {
			continue
		}
		currentUsage := ctx.State.GetEnergyUsage(pc.addr)
		currentSizeV1 := acct.EnergyWindowSize()
		currentOpt := acct.EnergyWindowOptimized()
		// java resetAccountUsage: newArea = currentUsage*currentSize
		//                                   - (mergedUsage*mergedSize - usage*size)
		newArea := currentUsage*currentSizeV1 - (pc.merged*pc.mergedSizeV1 - pc.r*pc.sizeV1)
		newSizeV1 := pc.sizeV1
		if pc.mergedSizeV1 != currentSizeV1 {
			newSizeV1 = currentSizeV1
		}
		newUsage := newArea / newSizeV1
		if newUsage < 0 {
			newUsage = 0
		}
		ctx.State.SetEnergyUsage(pc.addr, newUsage)
		if cancelAllV2 {
			// java resetAccountUsageV2: setNewWindowSizeV2(newUsage==0 ? 0 : newSize2).
			newSizeV2 := pc.sizeV2
			if pc.mergedSizeV1 != currentSizeV1 {
				newSizeV2 = acct.EnergyWindowSizeV2()
			}
			if newUsage == 0 {
				ctx.State.SetEnergyWindow(pc.addr, 0, true)
			} else {
				ctx.State.SetEnergyWindow(pc.addr, newSizeV2, true)
			}
		} else {
			// java resetAccountUsage: setNewWindowSize(newUsage==0 ? 0 : newSize),
			// which writes the V1 raw field and leaves the optimized flag untouched.
			if newUsage == 0 {
				ctx.State.SetEnergyWindow(pc.addr, 0, currentOpt)
			} else {
				ctx.State.SetEnergyWindow(pc.addr, newSizeV1, currentOpt)
			}
		}
	}
	result.energyPreCharges = nil
}
