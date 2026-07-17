// Package delegation holds V2 delegation usage-transfer helpers shared
// between the actuator path (DelegateResource / UnDelegateResource) and
// the VM path (opcodes 0xDE / 0xDF). No actuator/vm imports — only state
// + types + params.
package delegation

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// resState reads the per-account usage, last-consume-time and the stored
// recovery window (raw + optimized) for a resource.
func resState(statedb *state.StateDB, acct *types.Account, addr tcommon.Address, resource corepb.ResourceCode) (usage, lastTime, rawWindow int64, optimized bool) {
	if resource == corepb.ResourceCode_BANDWIDTH {
		return statedb.GetNetUsage(addr), statedb.GetLatestConsumeTime(addr), acct.RawNetWindowSize(), acct.NetWindowOptimized()
	}
	return statedb.GetEnergyUsage(addr), statedb.GetLatestConsumeTimeForEnergy(addr), acct.RawEnergyWindowSize(), acct.EnergyWindowOptimized()
}

// writeResState persists usage, the recovery window and the consume time.
func writeResState(statedb *state.StateDB, addr tcommon.Address, resource corepb.ResourceCode, usage, rawWindow int64, optimized bool, now int64) {
	if resource == corepb.ResourceCode_BANDWIDTH {
		statedb.SetNetUsage(addr, usage)
		statedb.SetNetWindow(addr, rawWindow, optimized)
		statedb.SetLatestConsumeTime(addr, now)
	} else {
		statedb.SetEnergyUsage(addr, usage)
		statedb.SetEnergyWindow(addr, rawWindow, optimized)
		statedb.SetLatestConsumeTimeForEnergy(addr, now)
	}
}

// TransferUsageFromReceiver mirrors java-tron UnDelegateResourceActuator.execute's
// receiver side: BandwidthProcessor.updateUsageForDelegated(receiver) recovers the
// receiver's usage against its PER-ACCOUNT window (renormalizing + writing the
// window), then the actuator peels off the portion proportional to the undelegated
// balance (capped at unDelegateBalance/TRX × totalLimit/totalWeight) and writes the
// remainder back. Returns the transferable amount AND the receiver's post-recovery
// window so the owner-side combine (FoldUsageIntoOwner) can blend it in — exactly
// java's unDelegateIncrease reading receiver.getWindowSize().
// tvmForm selects the unDelegateMaxUsage float evaluation order: java's
// UnDelegateResourceProcessor (the 0xDF opcode, tvmForm=true) computes it
// left-to-right `(ub/TRX) * limit / weight`, while UnDelegateResourceActuator
// (tvmForm=false) groups the ratio `(ub/TRX) * (limit/weight)`. The two IEEE-754
// orders differ by 1 at rare ULP boundaries, and when that cap binds it flips the
// committed transferUsage — so the shared helper must use the form matching its
// caller's java path.
func TransferUsageFromReceiver(statedb *state.StateDB, dp *state.DynamicProperties, receiver tcommon.Address, resource corepb.ResourceCode, unDelegateBalance, now int64, tvmForm bool) (transfer, recvRawWindow int64, recvOptimized bool) {
	acct := statedb.GetAccount(receiver)
	if acct == nil {
		return 0, 0, false
	}
	harden := dp.AllowHardenResourceCalculation()
	cancelAllV2 := dp.AllowCancelAllUnfreezeV2()

	usage, lastTime, rawWindow, optimized := resState(statedb, acct, receiver, resource)
	var totalLimit, totalWeight, totalFrozen, acquiredV2 int64
	if resource == corepb.ResourceCode_BANDWIDTH {
		totalLimit = dp.TotalNetLimit()
		totalWeight = dp.TotalNetWeight()
		totalFrozen = totalBandwidthFrozen(acct)
		acquiredV2 = acct.AcquiredDelegatedFrozenV2BalanceForBandwidth()
	} else {
		totalLimit = dp.TotalEnergyCurrentLimit()
		totalWeight = dp.TotalEnergyWeight()
		totalFrozen = totalEnergyFrozen(acct)
		acquiredV2 = acct.AcquiredDelegatedFrozenV2BalanceForEnergy()
	}

	// Per-account window recovery (usage arg = 0 → pure recovery + window renorm).
	recovered, newRaw, newOpt := computeResourceIncrease(rawWindow, optimized, usage, 0, lastTime, now, harden, cancelAllV2)

	// java UnDelegateResourceActuator/Processor: when the receiver's acquired V2
	// delegated balance is below the undelegated amount (a TVM contract suicide then
	// re-create clears acquired while a stale delegation record survives), it sets
	// acquired=0 and SKIPS the proportional usage transfer — transferUsage stays 0
	// and the receiver keeps its full recovered usage. The caller's
	// SubAcquiredDelegatedFrozenV2 already clamps the balance to 0 (== setAcquired(0)).
	if acquiredV2 >= unDelegateBalance && totalFrozen > 0 && recovered > 0 {
		maxTransfer := int64(0)
		if totalWeight > 0 {
			if tvmForm {
				// java UnDelegateResourceProcessor: (ub/TRX) * limit / weight (left-to-right).
				maxTransfer = int64(float64(unDelegateBalance) / float64(params.TRXPrecision) * float64(totalLimit) / float64(totalWeight))
			} else {
				// java UnDelegateResourceActuator: (ub/TRX) * (limit/weight) (grouped ratio).
				maxTransfer = int64(float64(unDelegateBalance) / float64(params.TRXPrecision) * (float64(totalLimit) / float64(totalWeight)))
			}
		}
		transfer = int64(float64(recovered) * (float64(unDelegateBalance) / float64(totalFrozen)))
		if transfer > maxTransfer {
			transfer = maxTransfer
		}
	}

	newUsage := recovered - transfer
	if newUsage < 0 {
		newUsage = 0
	}
	writeResState(statedb, receiver, resource, newUsage, newRaw, newOpt, now)
	return transfer, newRaw, newOpt
}

// FoldUsageIntoOwner mirrors java-tron ResourceProcessor.unDelegateIncrease /
// unDelegateIncreaseV2: recover the owner's usage against its PER-ACCOUNT window,
// add transferUsage, and set the new owner window to the usage-weighted blend of
// the owner's (post-recovery) window and the receiver's window. java gates this on
// `Objects.nonNull(receiverCapsule) && transferUsage > 0`, so the undelegate
// callers must only invoke it when transferUsage > 0 (the selfdestruct-inheritor
// merge calls it with the owner's full recovered usage, also > 0).
func FoldUsageIntoOwner(statedb *state.StateDB, dp *state.DynamicProperties, owner tcommon.Address, resource corepb.ResourceCode, transferUsage, recvRawWindow int64, recvOptimized bool, now int64) {
	acct := statedb.GetAccount(owner)
	if acct == nil {
		return
	}
	harden := dp.AllowHardenResourceCalculation()
	cancelAllV2 := dp.AllowCancelAllUnfreezeV2()

	usage, lastTime, ownerRaw, ownerOpt := resState(statedb, acct, owner, resource)
	ownerRecovered, ownerNewRaw, ownerNewOpt := computeResourceIncrease(ownerRaw, ownerOpt, usage, 0, lastTime, now, harden, cancelAllV2)

	newOwnerUsage := ownerRecovered + transferUsage
	var finalRaw int64
	var finalOpt bool
	if newOwnerUsage == 0 {
		finalRaw, finalOpt = zeroOwnerWindow(ownerNewOpt, cancelAllV2)
	} else {
		finalRaw, finalOpt = combineOwnerWindow(ownerRecovered, ownerNewRaw, ownerNewOpt, transferUsage, recvRawWindow, recvOptimized, newOwnerUsage, harden, cancelAllV2)
	}
	writeResState(statedb, owner, resource, newOwnerUsage, finalRaw, finalOpt, now)
}

// MergeUsageToInheritor mirrors java-tron Program.transferFrozenV2BalanceToInheritor's
// per-resource usage merge for a self-destructing contract: recover the owner's
// usage to `now` (BandwidthProcessor.updateUsageForDelegated / EnergyProcessor
// .updateUsage — pure recovery that also renormalizes + persists the window and
// stamps the owner's consume time = now), then, when that recovered usage is
// positive, fold ALL of it into the inheritor's window
// (unDelegateIncrease(inheritor, owner, owner.usage, resource, now)). java guards
// the fold on `owner.getUsage() > 0` after the recovery, so callers that later
// clearOwnerFreezeV2 still see the owner's consume time advanced to now.
func MergeUsageToInheritor(statedb *state.StateDB, dp *state.DynamicProperties, owner, inheritor tcommon.Address, resource corepb.ResourceCode, now int64) {
	acct := statedb.GetAccount(owner)
	if acct == nil {
		return
	}
	harden := dp.AllowHardenResourceCalculation()
	cancelAllV2 := dp.AllowCancelAllUnfreezeV2()

	usage, lastTime, rawWindow, optimized := resState(statedb, acct, owner, resource)
	recovered, newRaw, newOpt := computeResourceIncrease(rawWindow, optimized, usage, 0, lastTime, now, harden, cancelAllV2)
	// java updateUsageForDelegated/updateUsage persists the recovered usage+window
	// and the suicide body sets the owner's consume time to now.
	writeResState(statedb, owner, resource, recovered, newRaw, newOpt, now)
	if recovered > 0 {
		FoldUsageIntoOwner(statedb, dp, inheritor, resource, recovered, newRaw, newOpt, now)
	}
}

// AvailableFrozenV2ForDelegation returns the owner's self-frozen V2 balance
// that can still be delegated after accounting for already-consumed resource
// usage. This mirrors java-tron's DelegateResourceActuator.validate:
//
//	available = selfFrozenV2 - max(0, usageAsFrozenBalance
//	    - ownV1Frozen - acquiredV1Delegation - acquiredV2Delegation)
//
// The recovery uses the owner's PER-ACCOUNT window (java validate calls
// updateUsageForDelegated/updateUsage on the owner first); validate is
// non-persisting, so the recovered window is discarded.
func AvailableFrozenV2ForDelegation(statedb *state.StateDB, dp *state.DynamicProperties, owner tcommon.Address, resource corepb.ResourceCode, now int64) int64 {
	acct := statedb.GetAccount(owner)
	if acct == nil {
		return 0
	}

	selfFrozen := acct.GetFrozenV2Amount(resource)
	if selfFrozen <= 0 {
		return 0
	}

	harden := dp.AllowHardenResourceCalculation()
	cancelAllV2 := dp.AllowCancelAllUnfreezeV2()
	usageRaw, lastTime, rawWindow, optimized := resState(statedb, acct, owner, resource)
	usage, _, _ := computeResourceIncrease(rawWindow, optimized, usageRaw, 0, lastTime, now, harden, cancelAllV2)

	var v1OwnFrozen, v1AcquiredFrozen, v2AcquiredFrozen, totalLimit, totalWeight int64
	switch resource {
	case corepb.ResourceCode_BANDWIDTH:
		v1OwnFrozen = acct.TotalFrozenBandwidth()
		v1AcquiredFrozen = acct.AcquiredDelegatedFrozenBandwidth()
		v2AcquiredFrozen = acct.AcquiredDelegatedFrozenV2BalanceForBandwidth()
		totalLimit = dp.TotalNetLimit()
		totalWeight = dp.TotalNetWeight()
	case corepb.ResourceCode_ENERGY:
		v1OwnFrozen = acct.FrozenEnergyAmount()
		v1AcquiredFrozen = acct.AcquiredDelegatedFrozenEnergy()
		v2AcquiredFrozen = acct.AcquiredDelegatedFrozenV2BalanceForEnergy()
		totalLimit = dp.TotalEnergyCurrentLimit()
		totalWeight = dp.TotalEnergyWeight()
	default:
		return 0
	}

	usageAsFrozen := resourceUsageToFrozenBalance(usage, totalLimit, totalWeight)
	v2Usage := usageAsFrozen - v1OwnFrozen - v1AcquiredFrozen - v2AcquiredFrozen
	if v2Usage < 0 {
		v2Usage = 0
	}
	available := selfFrozen - v2Usage
	if available < 0 {
		return 0
	}
	return available
}

// resourceUsageToFrozenBalance converts recovered usage back into the frozen
// balance it represents. Mirrors java DelegateResourceActuator.validate and
// DelegateResourceProcessor: (long)(usage * TRX_PRECISION * ((double)totalWeight
// / totalLimit)) — a plain double cast on BOTH the actuator and VM paths,
// UNGATED. AllowHardenResourceCalculation only switches the recovery averaging in
// ResourceProcessor.increase (mirrored by computeResourceIncrease); java never
// applies big.Int / harden to this product, so neither must go.
func resourceUsageToFrozenBalance(usage, totalLimit, totalWeight int64) int64 {
	if usage <= 0 || totalLimit <= 0 || totalWeight <= 0 {
		return 0
	}
	return int64(float64(usage) * float64(params.TRXPrecision) * (float64(totalWeight) / float64(totalLimit)))
}

func totalBandwidthFrozen(acct *types.Account) int64 {
	frozen := acct.TotalFrozenBandwidth()
	frozen += acct.AcquiredDelegatedFrozenBandwidth()
	frozen += acct.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH)
	frozen += acct.AcquiredDelegatedFrozenV2BalanceForBandwidth()
	return frozen
}

func totalEnergyFrozen(acct *types.Account) int64 {
	frozen := acct.FrozenEnergyAmount()
	frozen += acct.AcquiredDelegatedFrozenEnergy()
	frozen += acct.GetFrozenV2Amount(corepb.ResourceCode_ENERGY)
	frozen += acct.AcquiredDelegatedFrozenV2BalanceForEnergy()
	return frozen
}
