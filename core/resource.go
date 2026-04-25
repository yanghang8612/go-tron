package core

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// availableAccountEnergy returns this account's share of the global energy
// pool, mirroring java-tron's EnergyProcessor.calculateGlobalEnergyLimit.
// Uses total_energy_current_limit (which is dynamically adjusted when
// adaptive energy is active) rather than the static total_energy_limit.
func availableAccountEnergy(acct *types.Account, dp *state.DynamicProperties) int64 {
	if acct == nil {
		return 0
	}
	frozen := acct.FrozenEnergyAmount()
	frozen += acct.AcquiredDelegatedFrozenEnergy()
	frozen += acct.GetFrozenV2Amount(corepb.ResourceCode_ENERGY)
	frozen += acct.AcquiredDelegatedFrozenV2BalanceForEnergy()

	totalWeight := dp.TotalEnergyWeight()
	if totalWeight <= 0 {
		return 0
	}
	totalLimit := dp.TotalEnergyCurrentLimit()

	if dp.UnfreezeDelayDays() > 0 {
		netWeight := float64(frozen) / float64(trxPrecision)
		return int64(netWeight * (float64(totalLimit) / float64(totalWeight)))
	}
	if frozen < trxPrecision {
		return 0
	}
	netWeight := frozen / trxPrecision
	return int64(float64(netWeight) * (float64(totalLimit) / float64(totalWeight)))
}

// ResourceProcessor handles bandwidth and energy consumption/recovery.
type ResourceProcessor struct {
	statedb *state.StateDB
}

// NewResourceProcessor creates a new ResourceProcessor.
func NewResourceProcessor(statedb *state.StateDB) *ResourceProcessor {
	return &ResourceProcessor{statedb: statedb}
}

// RecoverBandwidth applies sliding window recovery to frozen bandwidth usage.
func (r *ResourceProcessor) RecoverBandwidth(addr tcommon.Address, now int64) {
	oldUsage := r.statedb.GetNetUsage(addr)
	lastTime := r.statedb.GetLatestConsumeTime(addr)
	newUsage := recoverUsage(oldUsage, lastTime, now)
	if newUsage != oldUsage {
		r.statedb.SetNetUsage(addr, newUsage)
	}
}

// RecoverFreeBandwidth applies sliding window recovery to free bandwidth usage.
func (r *ResourceProcessor) RecoverFreeBandwidth(addr tcommon.Address, now int64) {
	oldUsage := r.statedb.GetFreeNetUsage(addr)
	lastTime := r.statedb.GetLatestConsumeFreeTime(addr)
	newUsage := recoverUsage(oldUsage, lastTime, now)
	if newUsage != oldUsage {
		r.statedb.SetFreeNetUsage(addr, newUsage)
	}
}

// RecoverEnergy applies sliding window recovery to energy usage.
func (r *ResourceProcessor) RecoverEnergy(addr tcommon.Address, now int64) {
	oldUsage := r.statedb.GetEnergyUsage(addr)
	lastTime := r.statedb.GetLatestConsumeTimeForEnergy(addr)
	newUsage := recoverUsage(oldUsage, lastTime, now)
	if newUsage != oldUsage {
		r.statedb.SetEnergyUsage(addr, newUsage)
	}
}

// recoverUsage computes new usage after sliding window recovery.
func recoverUsage(oldUsage int64, lastTime int64, now int64) int64 {
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
