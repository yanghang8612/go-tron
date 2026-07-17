package vm

import (
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/state"
)

// TVMConfig holds per-transaction fork flags for the TVM interpreter.
// Computed once in VMActuator before constructing the TVM.
type TVMConfig struct {
	TransferTrc10        bool // allow_tvm_transfer_trc10
	Constantinople       bool // allow_tvm_constantinople: CREATE2, EXTCODEHASH, SHL/SHR/SAR
	Solidity059          bool // allow_tvm_solidity059
	Istanbul             bool // allow_tvm_istanbul: CHAINID, SELFBALANCE
	Freeze               bool // allow_tvm_freeze: TRON freeze precompiles
	ShieldedToken        bool // allow_tvm_shielded_token
	Vote                 bool // allow_tvm_vote
	StakingV2            bool // support_unfreeze_delay: FreezeV2/DelegateV2 TVM ops/precompiles
	London               bool // allow_tvm_london: BASEFEE
	Compatibility        bool // allow_tvm_compatibility
	DynamicEnergy        bool // allow_dynamic_energy
	EnergyAdjustment     bool // allow_energy_adjustment
	Shanghai             bool // allow_tvm_shanghai: PUSH0
	Blob                 bool // allow_tvm_blob
	Cancun               bool // allow_tvm_cancun: TLOAD, TSTORE, MCOPY
	SelfdestructRestrict bool // allow_tvm_selfdestruct_restriction
	Prague               bool // allow_tvm_prague
	Osaka                bool // allow_tvm_osaka: CLZ, P256VERIFY
	FnDsa512             bool // allow_fn_dsa_512: PQ signatures/precompiles
	MlDsa44              bool // allow_ml_dsa_44: PQ signatures/precompiles
	// HigherLimitForMaxCpuTimeOfOneTx mirrors java-tron proposal #65
	// (`allow_higher_limit_for_max_cpu_time_of_one_tx`). When active,
	// java-tron's `OperationRegistry.adjustMemOperations` rebases
	// MLOAD/MSTORE/MSTORE8 from the default `0 + memDelta` to
	// `SPECIAL_TIER (1) + memDelta` (see EnergyCost.java:170-196).
	HigherLimitForMaxCpuTimeOfOneTx bool
	MultiSign                       bool // allow_multi_sign
	OptimizedReturnValueOfChainId   bool // allow_optimized_return_value_of_chain_id
	// NewResourceModelPower mirrors java-tron's joint check
	// `supportUnfreezeDelay() && supportAllowNewResourceModel()` used in the
	// TotalVoteCount precompile to select getAllTronPower() vs getTronPower().
	NewResourceModelPower bool
	// MultiSigCheckV2 is true once VERSION_4_7_1 SR vote passed. Currently
	// consumed only by the 0x0a ValidateMultiSign precompile to switch its
	// duplicate-signer behaviour: pre-fork it silently skipped exact-byte
	// signature duplicates from the same address; post-fork it must report
	// failure (java-tron MUtil.checkCPUTime → OutOfTimeException). This is NOT derived from an
	// AllowFlag — VERSION_4_7_1 is a pure SR-version vote, no DP key — so
	// it cannot be set by `NewTVMConfig`; the caller wires it from the rooted
	// fork-vote store for the current execution state.
	MultiSigCheckV2 bool
	// CpuTimeGuard is true once VERSION_4_8_1_1 (block version 35) SR vote passed.
	// java-tron's MUtil.checkCPUTimeForCreate2 / checkCPUTimeForModExp throw
	// OutOfTimeException under this fork: a CREATE2 at MAX_DEPTH and the degenerate
	// ModExp input (baseLen==0 && modLen==0 && expLen>1024) abort the tx. Like
	// MultiSigCheckV2 this is a pure SR-version vote (no DP key / AllowFlag), so it
	// is caller-wired from the rooted fork-vote store, not by NewTVMConfig.
	CpuTimeGuard bool

	// Tracer, when non-nil, receives the EIP-3155 hook stream during execution
	// (per-opcode CaptureState plus frame CaptureStart/Enter/Exit/End). It is
	// left nil by NewTVMConfig — only the debug_trace* backends and the
	// GTRON_TVM_TRACE diagnostic path install one — so production execution
	// pays a single nil check per opcode.
	Tracer Tracer
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
		TransferTrc10:        isActive(forks.AllowTvmTransferTrc10),
		Constantinople:       isActive(forks.AllowTvmConstantinople),
		Solidity059:          isActive(forks.AllowTvmSolidity059),
		Istanbul:             isActive(forks.AllowTvmIstanbul),
		Freeze:               isActive(forks.AllowTvmFreeze),
		ShieldedToken:        isActive(forks.AllowTvmShieldedToken),
		Vote:                 isActive(forks.AllowTvmVote),
		StakingV2:            unfreezeDelay,
		London:               isActive(forks.AllowTvmLondon),
		Compatibility:        isActive(forks.AllowTvmCompatibleEvm),
		DynamicEnergy:        isActive(forks.AllowDynamicEnergy),
		EnergyAdjustment:     isActive(forks.AllowEnergyAdjustment),
		Shanghai:             isActive(forks.AllowTvmShanghai),
		Blob:                 isActive(forks.AllowTvmBlob),
		Cancun:               isActive(forks.AllowTvmCancun),
		SelfdestructRestrict: isActive(forks.AllowTvmSelfdestructRestriction),
		Prague:               isActive(forks.AllowTvmPrague),
		Osaka:                isActive(forks.AllowTvmOsaka),
		FnDsa512:             dp != nil && dp.AllowFnDsa512(),
		MlDsa44:              dp != nil && dp.AllowMlDsa44(),
		// AllowHigherLimitForMaxCpuTimeOfOneTx is read directly off DP.
		// It is governed by proposal #65 but does not have an `AllowFlag`
		// entry — only the VM consumes it, and the gating is on the DP
		// boolean rather than on a fork-controller version vote.
		HigherLimitForMaxCpuTimeOfOneTx: higherLimit,
		MultiSign:                       isActive(forks.AllowMultiSign),
		OptimizedReturnValueOfChainId:   dp != nil && dp.AllowOptimizedReturnValueOfChainId(),
		NewResourceModelPower:           isActive(forks.AllowNewResourceModel) && unfreezeDelay,
	}
}
