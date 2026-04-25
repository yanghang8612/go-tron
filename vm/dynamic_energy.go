package vm

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
)

// updateContractEnergyFactor loads the ContractState for addr, fast-
// forwards its factor to the current cycle, persists the update if any,
// and returns the multiplier to apply to this execution's opcode costs.
//
// The returned factor is the *effective* multiplier (stored offset +
// DynamicEnergyFactorDecimal), so factor=10_000 means 1.0×, factor=18_000
// means 1.8×. Only factors strictly above 10_000 cause a penalty on each
// opcode.
//
// Mirrors java-tron Program.updateContextContractFactor.
func updateContractEnergyFactor(tvm *TVM, addr tcommon.Address) int64 {
	if tvm.DB == nil {
		return types.DynamicEnergyFactorDecimal
	}
	dp := tvm.StateDB.DynamicProperties()
	if dp == nil {
		return types.DynamicEnergyFactorDecimal
	}
	currentCycle := dp.CurrentCycleNumber()

	cs := rawdb.ReadContractState(tvm.DB, addr)
	if cs == nil {
		cs = types.NewContractState(currentCycle)
		_ = rawdb.WriteContractState(tvm.DB, addr, cs)
		return types.DynamicEnergyFactorDecimal
	}

	threshold := dp.DynamicEnergyThreshold()
	increaseFactor := dp.DynamicEnergyIncreaseFactor()
	maxFactor := dp.DynamicEnergyMaxFactor()

	if cs.CatchUpToCycle(currentCycle, threshold, increaseFactor, maxFactor) {
		_ = rawdb.WriteContractState(tvm.DB, addr, cs)
	}
	return cs.EnergyFactor() + types.DynamicEnergyFactorDecimal
}

// recordContractEnergyUsage adds rawUsage (the pre-factor base energy
// cost) to the contract's rolling energy_usage counter. This is the
// counter compared against DynamicEnergyThreshold at the next catch-up
// call; the scaled (post-factor) usage is not what drives the feedback.
func recordContractEnergyUsage(tvm *TVM, addr tcommon.Address, rawUsage int64) {
	if tvm.DB == nil || rawUsage <= 0 {
		return
	}
	cs := rawdb.ReadContractState(tvm.DB, addr)
	if cs == nil {
		dp := tvm.StateDB.DynamicProperties()
		var cycle int64
		if dp != nil {
			cycle = dp.CurrentCycleNumber()
		}
		cs = types.NewContractState(cycle)
	}
	cs.AddEnergyUsage(rawUsage)
	_ = rawdb.WriteContractState(tvm.DB, addr, cs)
}

// applyDynamicEnergyPenalty returns the penalty (additional cost) to
// charge on top of baseCost given the current factor. Returns 0 when the
// factor is at-or-below 1.0× (i.e., factor <= DynamicEnergyFactorDecimal).
//
// Formula: penalty = baseCost × factor / DECIMAL − baseCost. Mirrors
// java-tron VM.play's non-CALL branch.
func applyDynamicEnergyPenalty(baseCost uint64, factor int64) uint64 {
	decimal := uint64(types.DynamicEnergyFactorDecimal)
	if factor <= int64(decimal) || baseCost == 0 {
		return 0
	}
	scaled := baseCost * uint64(factor) / decimal
	if scaled <= baseCost {
		return 0
	}
	return scaled - baseCost
}
