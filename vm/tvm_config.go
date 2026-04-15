package vm

import (
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/state"
)

// TVMConfig holds per-transaction fork flags for the TVM interpreter.
// Computed once in VMActuator before constructing the TVM.
type TVMConfig struct {
	TransferTrc10  bool // allow_tvm_transfer_trc10
	Constantinople bool // allow_tvm_constantinople: CREATE2, EXTCODEHASH, SHL/SHR/SAR
	Solidity059    bool // allow_tvm_solidity059
	Istanbul       bool // allow_tvm_istanbul: CHAINID, SELFBALANCE
	Freeze         bool // allow_tvm_freeze: TRON freeze precompiles
	ShieldedToken  bool // allow_tvm_shielded_token
	Vote           bool // allow_tvm_vote
	StakingV2      bool // allow_staking_v2: FreezeV2/DelegateV2 precompiles
	London         bool // allow_tvm_london: BASEFEE
	Compatibility  bool // allow_tvm_compatibility
	DynamicEnergy  bool // allow_dynamic_energy
	BigInteger     bool // allow_tvm_big_integer
	Blob           bool // allow_tvm_blob
	Cancun         bool // allow_tvm_cancun: TLOAD, TSTORE, MCOPY
}

// NewTVMConfig builds a TVMConfig from the current DynamicProperties and block number.
func NewTVMConfig(blockNum uint64, dp *state.DynamicProperties) TVMConfig {
	isActive := func(flag forks.AllowFlag) bool {
		return forks.IsActive(flag, blockNum, dp)
	}
	return TVMConfig{
		TransferTrc10:  isActive(forks.AllowTvmTransferTrc10),
		Constantinople: isActive(forks.AllowTvmConstantinople),
		Solidity059:    isActive(forks.AllowTvmSolidity059),
		Istanbul:       isActive(forks.AllowTvmIstanbul),
		Freeze:         isActive(forks.AllowTvmFreeze),
		ShieldedToken:  isActive(forks.AllowTvmShieldedToken),
		Vote:           isActive(forks.AllowTvmVote),
		StakingV2:      isActive(forks.AllowStakingV2),
		London:         isActive(forks.AllowTvmLondon),
		Compatibility:  isActive(forks.AllowTvmCompatibleEvm),
		DynamicEnergy:  isActive(forks.AllowDynamicEnergy),
		BigInteger:     isActive(forks.AllowTvmBigInteger),
		Blob:           isActive(forks.AllowTvmBlob),
		Cancun:         isActive(forks.AllowTvmCancun),
	}
}
