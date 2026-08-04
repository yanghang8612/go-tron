package core

import (
	"github.com/tronprotocol/go-tron/actuator"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/state"
)

// blockEnergyUsageForkVersion is VERSION_3_6_5 — the SR version after which
// java-tron's `ReceiptCapsule.payEnergyBill` adds the balance-paid energy
// overflow into `block_energy_usage`. Stake-paid energy is always added
// when adaptive energy is on (via `EnergyProcessor.useEnergy`), regardless
// of this fork.
const blockEnergyUsageForkVersion int32 = 9

// accumulateBlockEnergyUsage mirrors the two-tier accumulation java-tron
// runs out of `EnergyProcessor.useEnergy` + `ReceiptCapsule.payEnergyBill`:
//
//   - stake-paid energy (`EnergyUsed + OriginEnergyUsage`) is always added
//     to `block_energy_usage` when adaptive energy is on
//     (EnergyProcessor.java:137-139, runs once per useEnergy call).
//   - the balance-paid overflow (`EnergyUsageTotal - stake`) is added too,
//     but only after VERSION_3_6_5 passes
//     (ReceiptCapsule.java:281-285).
//
// So post-3_6_5 the total bump is `EnergyUsageTotal`; pre-3_6_5 only the stake
// portion counts.
func accumulateBlockEnergyUsage(dp *state.DynamicProperties, forkStats forks.ForkStatsReader, prevBlockTime int64, result *actuator.Result, forkPassCache *forks.VersionPassCache) {
	if dp == nil || result == nil {
		return
	}
	delta := blockEnergyUsageDelta(dp, forkStats, prevBlockTime, result.EnergyUsageTotal, result.EnergyUsed, result.OriginEnergyUsage, forkPassCache)
	if delta <= 0 {
		return
	}
	dp.SetBlockEnergyUsage(dp.BlockEnergyUsage() + delta)
}

// blockEnergyUsageDelta is the shared transaction-boundary rule for canonical
// accumulation and observe-only parallel-result projection. Keeping the fork
// decision here prevents the projected carrier from drifting from serial
// execution as the resource rules evolve.
func blockEnergyUsageDelta(dp *state.DynamicProperties, forkStats forks.ForkStatsReader, prevBlockTime, energyUsageTotal, energyUsed, originEnergyUsage int64, forkPassCache *forks.VersionPassCache) int64 {
	if dp == nil || !dp.AllowAdaptiveEnergy() || energyUsageTotal <= 0 {
		return 0
	}
	delta := energyUsed + originEnergyUsage
	if forkPassCache.Pass(forkStats, blockEnergyUsageForkVersion, prevBlockTime, dp.MaintenanceTimeInterval()) {
		delta = energyUsageTotal
	}
	if delta <= 0 {
		return 0
	}
	return delta
}
