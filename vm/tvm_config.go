package vm

import (
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/state"
)

// TVMConfig holds per-transaction fork flags for the TVM interpreter.
// Computed once in VMActuator before constructing the TVM.
type TVMConfig struct {
	TransferTrc10       bool // allow_tvm_transfer_trc10
	Constantinople      bool // allow_tvm_constantinople: CREATE2, EXTCODEHASH, SHL/SHR/SAR
	Solidity059         bool // allow_tvm_solidity059
	Istanbul            bool // allow_tvm_istanbul: CHAINID, SELFBALANCE
	Freeze              bool // allow_tvm_freeze: TRON freeze precompiles
	ShieldedToken       bool // allow_tvm_shielded_token
	Vote                bool // allow_tvm_vote
	StakingV2           bool // allow_staking_v2: FreezeV2/DelegateV2 precompiles
	London              bool // allow_tvm_london: BASEFEE
	Compatibility       bool // allow_tvm_compatibility
	DynamicEnergy       bool // allow_dynamic_energy
	Blob                bool // allow_tvm_blob
	Cancun              bool // allow_tvm_cancun: TLOAD, TSTORE, MCOPY
	// HigherLimitForMaxCpuTimeOfOneTx mirrors java-tron proposal #65
	// (`allow_higher_limit_for_max_cpu_time_of_one_tx`). When active,
	// java-tron's `OperationRegistry.adjustMemOperations` rebases
	// MLOAD/MSTORE/MSTORE8 from the default `0 + memDelta` to
	// `SPECIAL_TIER (1) + memDelta` (see EnergyCost.java:170-196).
	HigherLimitForMaxCpuTimeOfOneTx bool
	// NewResourceModelPower mirrors java-tron's joint check
	// `supportUnfreezeDelay() && supportAllowNewResourceModel()` used in the
	// TotalVoteCount precompile to select getAllTronPower() vs getTronPower().
	NewResourceModelPower bool
}

// NewTVMConfig builds a TVMConfig from the current DynamicProperties and block number.
func NewTVMConfig(blockNum uint64, dp *state.DynamicProperties) TVMConfig {
	isActive := func(flag forks.AllowFlag) bool {
		return forks.IsActive(flag, blockNum, dp)
	}
	higherLimit := false
	unfreezeDelay := false
	if dp != nil {
		higherLimit = dp.AllowHigherLimitForMaxCpuTimeOfOneTx()
		unfreezeDelay = dp.UnfreezeDelayDays() > 0
	}
	return TVMConfig{
		TransferTrc10:         isActive(forks.AllowTvmTransferTrc10),
		Constantinople:        isActive(forks.AllowTvmConstantinople),
		Solidity059:           isActive(forks.AllowTvmSolidity059),
		Istanbul:              isActive(forks.AllowTvmIstanbul),
		Freeze:                isActive(forks.AllowTvmFreeze),
		ShieldedToken:         isActive(forks.AllowTvmShieldedToken),
		Vote:                  isActive(forks.AllowTvmVote),
		StakingV2:             isActive(forks.AllowStakingV2),
		London:                isActive(forks.AllowTvmLondon),
		Compatibility:         isActive(forks.AllowTvmCompatibleEvm),
		DynamicEnergy:         isActive(forks.AllowDynamicEnergy),
		Blob:                  isActive(forks.AllowTvmBlob),
		Cancun:                isActive(forks.AllowTvmCancun),
		// AllowHigherLimitForMaxCpuTimeOfOneTx is read directly off DP.
		// It is governed by proposal #65 but does not have an `AllowFlag`
		// entry — only the VM consumes it, and the gating is on the DP
		// boolean rather than on a fork-controller version vote.
		HigherLimitForMaxCpuTimeOfOneTx: higherLimit,
		NewResourceModelPower:           isActive(forks.AllowNewResourceModel) && unfreezeDelay,
	}
}
