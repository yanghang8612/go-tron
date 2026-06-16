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
	if !dp.AllowAdaptiveEnergy() || result.EnergyUsageTotal <= 0 {
		return
	}
	delta := result.EnergyUsed + result.OriginEnergyUsage
	if forkPassCache.Pass(forkStats, blockEnergyUsageForkVersion, prevBlockTime, dp.MaintenanceTimeInterval()) {
		delta = result.EnergyUsageTotal
	}
	if delta <= 0 {
		return
	}
	dp.SetBlockEnergyUsage(dp.BlockEnergyUsage() + delta)
}
