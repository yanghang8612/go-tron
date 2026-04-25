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

	recovered := RecoverUsageWindow(usage, lastTime, now)

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
func FoldUsageIntoOwner(statedb *state.StateDB, owner tcommon.Address, resource corepb.ResourceCode, transferUsage, now int64) {
	var usage, lastTime int64
	if resource == corepb.ResourceCode_BANDWIDTH {
		usage = statedb.GetNetUsage(owner)
		lastTime = statedb.GetLatestConsumeTime(owner)
	} else {
		usage = statedb.GetEnergyUsage(owner)
		lastTime = statedb.GetLatestConsumeTimeForEnergy(owner)
	}
	recovered := RecoverUsageWindow(usage, lastTime, now)
	newUsage := recovered + transferUsage
	if resource == corepb.ResourceCode_BANDWIDTH {
		statedb.SetNetUsage(owner, newUsage)
		statedb.SetLatestConsumeTime(owner, now)
	} else {
		statedb.SetEnergyUsage(owner, newUsage)
		statedb.SetLatestConsumeTimeForEnergy(owner, now)
	}
}

// RecoverUsageWindow applies the sliding-window linear decay go-tron uses
// for resource usage (mirrors core.recoverUsage — duplicated here to
// avoid import cycles).
func RecoverUsageWindow(oldUsage, lastTime, now int64) int64 {
	if oldUsage <= 0 {
		return 0
	}
	elapsed := now - lastTime
	if elapsed >= int64(params.WindowSizeMs) {
		return 0
	}
	if elapsed <= 0 {
		return oldUsage
	}
	remaining := int64(params.WindowSizeMs) - elapsed
	return oldUsage * remaining / int64(params.WindowSizeMs)
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
