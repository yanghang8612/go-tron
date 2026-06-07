// Package delegation holds V2 delegation usage-transfer helpers shared
// between the actuator path (DelegateResource / UnDelegateResource) and
// the VM path (opcodes 0xDE / 0xDF). No actuator/vm imports — only state
// + types + params.
package delegation

import (
	"math"
	"math/big"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// TransferUsageFromReceiver mirrors java-tron's usage-transfer math in
// UnDelegateResourceActuator.execute. Recovers the receiver's current
// usage, peels off the portion proportional to the undelegated balance
// (capped at unDelegateBalance/TRX × totalLimit/totalWeight), writes the
// remainder back and returns the transferable amount.
//
// Callers then feed the returned value into FoldUsageIntoOwner so the
// owner inherits that consumption.
func TransferUsageFromReceiver(statedb *state.StateDB, dp *state.DynamicProperties, receiver tcommon.Address, resource corepb.ResourceCode, unDelegateBalance, now int64) int64 {
	acct := statedb.GetAccount(receiver)
	if acct == nil {
		return 0
	}

	var usage, lastTime, totalLimit, totalWeight, totalFrozen int64
	if resource == corepb.ResourceCode_BANDWIDTH {
		usage = statedb.GetNetUsage(receiver)
		lastTime = statedb.GetLatestConsumeTime(receiver)
		totalLimit = dp.TotalNetLimit()
		totalWeight = dp.TotalNetWeight()
		totalFrozen = totalBandwidthFrozen(acct)
	} else {
		usage = statedb.GetEnergyUsage(receiver)
		lastTime = statedb.GetLatestConsumeTimeForEnergy(receiver)
		totalLimit = dp.TotalEnergyCurrentLimit()
		totalWeight = dp.TotalEnergyWeight()
		totalFrozen = totalEnergyFrozen(acct)
	}

	recovered := recoverUsageWindowForDP(usage, lastTime, now, dp)

	var transfer int64
	if totalFrozen > 0 && recovered > 0 {
		maxTransfer := int64(0)
		if totalWeight > 0 {
			maxTransfer = int64(float64(unDelegateBalance) / float64(params.TRXPrecision) * (float64(totalLimit) / float64(totalWeight)))
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
	if resource == corepb.ResourceCode_BANDWIDTH {
		statedb.SetNetUsage(receiver, newUsage)
		statedb.SetLatestConsumeTime(receiver, now)
	} else {
		statedb.SetEnergyUsage(receiver, newUsage)
		statedb.SetLatestConsumeTimeForEnergy(receiver, now)
	}
	return transfer
}

// FoldUsageIntoOwner recovers owner's current usage and adds transferUsage.
// Mirrors java-tron ResourceProcessor.unDelegateIncrease's "owner add"
// side. Passing transferUsage == 0 refreshes owner's recovered usage
// without adding anything — useful when delegating (mirrors java-tron's
// processor.updateUsageForDelegated(ownerCapsule) call).
func FoldUsageIntoOwner(statedb *state.StateDB, dp *state.DynamicProperties, owner tcommon.Address, resource corepb.ResourceCode, transferUsage, now int64) {
	var usage, lastTime int64
	if resource == corepb.ResourceCode_BANDWIDTH {
		usage = statedb.GetNetUsage(owner)
		lastTime = statedb.GetLatestConsumeTime(owner)
	} else {
		usage = statedb.GetEnergyUsage(owner)
		lastTime = statedb.GetLatestConsumeTimeForEnergy(owner)
	}
	recovered := recoverUsageWindowForDP(usage, lastTime, now, dp)
	newUsage := recovered + transferUsage
	if resource == corepb.ResourceCode_BANDWIDTH {
		statedb.SetNetUsage(owner, newUsage)
		statedb.SetLatestConsumeTime(owner, now)
	} else {
		statedb.SetEnergyUsage(owner, newUsage)
		statedb.SetLatestConsumeTimeForEnergy(owner, now)
	}
}

// AvailableFrozenV2ForDelegation returns the owner's self-frozen V2 balance
// that can still be delegated after accounting for already-consumed resource
// usage. This mirrors java-tron's DelegateResourceActuator.validate:
//
//	available = selfFrozenV2 - max(0, usageAsFrozenBalance
//	    - ownV1Frozen - acquiredV1Delegation - acquiredV2Delegation)
//
// The usage recovery follows go-tron's existing resource timestamp model so
// the check is consistent with bandwidth/energy charging in this codebase.
func AvailableFrozenV2ForDelegation(statedb *state.StateDB, dp *state.DynamicProperties, owner tcommon.Address, resource corepb.ResourceCode, now int64) int64 {
	acct := statedb.GetAccount(owner)
	if acct == nil {
		return 0
	}

	selfFrozen := acct.GetFrozenV2Amount(resource)
	if selfFrozen <= 0 {
		return 0
	}

	var usage, v1OwnFrozen, v1AcquiredFrozen, v2AcquiredFrozen, totalLimit, totalWeight int64
	switch resource {
	case corepb.ResourceCode_BANDWIDTH:
		usage = recoverUsageWindowForDP(statedb.GetNetUsage(owner), statedb.GetLatestConsumeTime(owner), now, dp)
		v1OwnFrozen = acct.TotalFrozenBandwidth()
		v1AcquiredFrozen = acct.AcquiredDelegatedFrozenBandwidth()
		v2AcquiredFrozen = acct.AcquiredDelegatedFrozenV2BalanceForBandwidth()
		totalLimit = dp.TotalNetLimit()
		totalWeight = dp.TotalNetWeight()
	case corepb.ResourceCode_ENERGY:
		usage = recoverUsageWindowForDP(statedb.GetEnergyUsage(owner), statedb.GetLatestConsumeTimeForEnergy(owner), now, dp)
		v1OwnFrozen = acct.FrozenEnergyAmount()
		v1AcquiredFrozen = acct.AcquiredDelegatedFrozenEnergy()
		v2AcquiredFrozen = acct.AcquiredDelegatedFrozenV2BalanceForEnergy()
		totalLimit = dp.TotalEnergyCurrentLimit()
		totalWeight = dp.TotalEnergyWeight()
	default:
		return 0
	}

	usageAsFrozen := resourceUsageToFrozenBalance(usage, totalLimit, totalWeight, dp.AllowHardenResourceCalculation())
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

// RecoverUsageWindow applies java-tron's precision-averaging global-window
// recovery for resource usage. `lastTime` and `now` are head-slot values, not
// millisecond timestamps (duplicated here to avoid import cycles).
func RecoverUsageWindow(oldUsage, lastTime, now int64) int64 {
	return RecoverUsageWindowWithHarden(oldUsage, lastTime, now, false)
}

// RecoverUsageWindowWithHarden mirrors ResourceProcessor.increase(oldUsage, 0,
// lastTime, now, windowSize) over the global 28,800-slot window. The hardened
// branch uses BigInteger-style arithmetic, matching java's
// allow_harden_resource_calculation path.
func RecoverUsageWindowWithHarden(oldUsage, lastTime, now int64, harden bool) int64 {
	if oldUsage <= 0 {
		return 0
	}
	windowSize := int64(params.WindowSizeSlots)
	elapsed := now - lastTime
	if elapsed >= windowSize {
		return 0
	}
	if elapsed <= 0 {
		return oldUsage
	}

	var averageLastUsage int64
	if harden {
		averageLastUsage = divideCeilBigInt(
			new(big.Int).Mul(big.NewInt(oldUsage), big.NewInt(resourceWindowPrecision)),
			big.NewInt(windowSize),
		)
	} else {
		averageLastUsage = divideCeilInt(oldUsage*resourceWindowPrecision, windowSize)
	}
	decay := float64(windowSize-elapsed) / float64(windowSize)
	averageLastUsage = int64(math.Round(float64(averageLastUsage) * decay))

	if harden {
		n := new(big.Int).Mul(big.NewInt(averageLastUsage), big.NewInt(windowSize))
		n.Quo(n, big.NewInt(resourceWindowPrecision))
		return n.Int64()
	}
	return averageLastUsage * windowSize / resourceWindowPrecision
}

func recoverUsageWindowForDP(oldUsage, lastTime, now int64, dp *state.DynamicProperties) int64 {
	return RecoverUsageWindowWithHarden(oldUsage, lastTime, now, dp != nil && dp.AllowHardenResourceCalculation())
}

const resourceWindowPrecision = int64(1_000_000)

func divideCeilInt(numerator, denominator int64) int64 {
	result := numerator / denominator
	if numerator%denominator > 0 {
		result++
	}
	return result
}

func divideCeilBigInt(numerator, denominator *big.Int) int64 {
	q, r := new(big.Int).QuoRem(numerator, denominator, new(big.Int))
	if r.Sign() > 0 {
		q.Add(q, big.NewInt(1))
	}
	return q.Int64()
}

func resourceUsageToFrozenBalance(usage, totalLimit, totalWeight int64, harden bool) int64 {
	if usage <= 0 || totalLimit <= 0 || totalWeight <= 0 {
		return 0
	}
	if !harden {
		return int64(float64(usage) * float64(params.TRXPrecision) * (float64(totalWeight) / float64(totalLimit)))
	}
	n := new(big.Int).Mul(big.NewInt(usage), big.NewInt(params.TRXPrecision))
	n.Mul(n, big.NewInt(totalWeight))
	n.Quo(n, big.NewInt(totalLimit))
	return n.Int64()
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
