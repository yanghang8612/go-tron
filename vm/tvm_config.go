package vm

import (
	"github.com/tronprotocol/go-tron/core/hardfork"
	"github.com/tronprotocol/go-tron/core/state"
)

// TVMConfig holds per-transaction fork flags for the TVM interpreter.
// Computed once in VMActuator before constructing the EVM.
type TVMConfig struct {
	TransferTrc10  bool // allow_tvm_transfer_trc10
	Constantinople bool // allow_tvm_constantinople: CREATE2, EXTCODEHASH, SHL/SHR/SAR
	Solidity059    bool // allow_tvm_solidity059
	Istanbul       bool // allow_tvm_istanbul: CHAINID, SELFBALANCE
	Freeze         bool // allow_tvm_freeze: TRON freeze precompiles
	ShieldedToken  bool // allow_tvm_shielded_token
	Vote           bool // allow_tvm_vote
	London         bool // allow_tvm_london: BASEFEE
	Compatibility  bool // allow_tvm_compatibility
	DynamicEnergy  bool // allow_dynamic_energy
	BigInteger     bool // allow_tvm_big_integer
	Blob           bool // allow_tvm_blob
	Cancun         bool // allow_tvm_cancun: TLOAD, TSTORE, MCOPY
}

// NewTVMConfig builds a TVMConfig from the current DynamicProperties and block number.
func NewTVMConfig(blockNum uint64, dp *state.DynamicProperties) TVMConfig {
	isActive := func(flag hardfork.AllowFlag) bool {
		return hardfork.IsActive(flag, blockNum, dp)
	}
	return TVMConfig{
		TransferTrc10:  isActive(hardfork.AllowTvmTransferTrc10),
		Constantinople: isActive(hardfork.AllowTvmConstantinople),
		Solidity059:    isActive(hardfork.AllowTvmSolidity059),
		Istanbul:       isActive(hardfork.AllowTvmIstanbul),
		Freeze:         isActive(hardfork.AllowTvmFreeze),
		ShieldedToken:  isActive(hardfork.AllowTvmShieldedToken),
		Vote:           isActive(hardfork.AllowTvmVote),
		London:         isActive(hardfork.AllowTvmLondon),
		Compatibility:  isActive(hardfork.AllowTvmCompatibility),
		DynamicEnergy:  isActive(hardfork.AllowDynamicEnergy),
		BigInteger:     isActive(hardfork.AllowTvmBigInteger),
		Blob:           isActive(hardfork.AllowTvmBlob),
		Cancun:         isActive(hardfork.AllowTvmCancun),
	}
}
