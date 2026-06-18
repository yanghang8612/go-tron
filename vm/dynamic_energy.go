package vm

import (
	tcommon "github.com/tronprotocol/go-tron/common"
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
	if tvm.StateDB == nil {
		return types.DynamicEnergyFactorDecimal
	}
	// Read the dynamic properties from the wired tvm.DynProps, NOT from
	// tvm.StateDB.DynamicProperties(): the production block-execution path
	// (vm_actuator.go / tron_backend.go) carries the live dp as a separate
	// NewTVM argument and never calls StateDB.SetDynamicProperties, so the
	// StateDB's own dynProps is the empty default from state.New (cycle 0,
	// thresholds 0) — a distinct object from the live dp. Reading it made the
	// catch-up see currentCycle==0 < the contract's UpdateCycle and reset the
	// factor every call, so the dynamic-energy penalty never accrued on-chain.
	// Every other VM site already uses tvm.DynProps.
	dp := tvm.DynProps
	if dp == nil {
		return types.DynamicEnergyFactorDecimal
	}
	currentCycle := dp.CurrentCycleNumber()

	cs := tvm.StateDB.ReadContractState(addr)
	if cs == nil {
		cs = types.NewContractState(currentCycle)
		_ = tvm.StateDB.WriteContractState(addr, cs)
		return types.DynamicEnergyFactorDecimal
	}

	threshold := dp.DynamicEnergyThreshold()
	increaseFactor := dp.DynamicEnergyIncreaseFactor()
	maxFactor := dp.DynamicEnergyMaxFactor()

	if cs.CatchUpToCycle(currentCycle, threshold, increaseFactor, maxFactor, dp.AllowStrictMath()) {
		_ = tvm.StateDB.WriteContractState(addr, cs)
	}
	return cs.EnergyFactor() + types.DynamicEnergyFactorDecimal
}

// recordContractEnergyUsage adds rawUsage (the pre-factor base energy
// cost) to the contract's rolling energy_usage counter. This is the
// counter compared against DynamicEnergyThreshold at the next catch-up
// call; the scaled (post-factor) usage is not what drives the feedback.
func recordContractEnergyUsage(tvm *TVM, addr tcommon.Address, rawUsage int64) {
	if tvm.StateDB == nil || rawUsage <= 0 {
		return
	}
	cs := tvm.StateDB.ReadContractState(addr)
	if cs == nil {
		// Same dp source as updateContractEnergyFactor: the wired tvm.DynProps,
		// not the production-nil StateDB.DynamicProperties().
		dp := tvm.DynProps
		var cycle int64
		if dp != nil {
			cycle = dp.CurrentCycleNumber()
		}
		cs = types.NewContractState(cycle)
	}
	cs.AddEnergyUsage(rawUsage)
	_ = tvm.StateDB.WriteContractState(addr, cs)
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
