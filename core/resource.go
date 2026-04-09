package core

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/params"
)

// ResourceProcessor handles bandwidth and energy consumption/recovery.
type ResourceProcessor struct {
	statedb *state.StateDB
}

// NewResourceProcessor creates a new ResourceProcessor.
func NewResourceProcessor(statedb *state.StateDB) *ResourceProcessor {
	return &ResourceProcessor{statedb: statedb}
}

// RecoverBandwidth applies sliding window recovery to frozen bandwidth usage.
// The LatestConsumeTime is not modified here; it is updated only when bandwidth is consumed.
func (r *ResourceProcessor) RecoverBandwidth(addr tcommon.Address, now int64) {
	acc := r.statedb.GetAccount(addr)
	if acc == nil {
		return
	}
	newUsage := recoverUsage(acc.NetUsage(), acc.LatestConsumeTime(), now)
	acc.SetNetUsage(newUsage)
}

// RecoverFreeBandwidth applies sliding window recovery to free bandwidth usage.
// The LatestConsumeFreeTime is not modified here; it is updated only when bandwidth is consumed.
func (r *ResourceProcessor) RecoverFreeBandwidth(addr tcommon.Address, now int64) {
	acc := r.statedb.GetAccount(addr)
	if acc == nil {
		return
	}
	newUsage := recoverUsage(acc.FreeNetUsage(), acc.LatestConsumeFreeTime(), now)
	acc.SetFreeNetUsage(newUsage)
}

// RecoverEnergy applies sliding window recovery to energy usage.
// The LatestConsumeTimeForEnergy is not modified here; it is updated only when energy is consumed.
func (r *ResourceProcessor) RecoverEnergy(addr tcommon.Address, now int64) {
	acc := r.statedb.GetAccount(addr)
	if acc == nil {
		return
	}
	newUsage := recoverUsage(acc.EnergyUsage(), acc.LatestConsumeTimeForEnergy(), now)
	acc.SetEnergyUsage(newUsage)
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
