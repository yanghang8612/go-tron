package forks

import "github.com/tronprotocol/go-tron/core/state"

// AllowFlag identifies a feature that can be toggled via governance proposal.
type AllowFlag int

const (
	AllowSameTokenName AllowFlag = iota
	AllowDelegateResource
	AllowAdaptiveEnergyLimit
	AllowMultiSign
	AllowChangeDelegation
	AllowTvmTransferTrc10
	AllowTvmConstantinople
	AllowTvmSolidity059
	AllowTvmSolidity058
	AllowTvmIstanbul
	AllowMarketTransaction
	AllowTvmFreeze
	AllowTvmShieldedToken
	AllowTvmVote
	AllowAccountHistory
	AllowPbft
	AllowStakingV2
	AllowTvmLondon
	AllowTvmCompatibility
	AllowDynamicEnergy
	AllowNewResourceModel
	AllowEnergyAdjustment
	AllowTvmBigInteger
	AllowTvmBlob
	AllowTvmCancun
)

// dynKey maps each AllowFlag to its DynamicProperties string key.
var dynKey = map[AllowFlag]string{
	AllowSameTokenName:       "allow_same_token_name",
	AllowDelegateResource:    "allow_delegate_resource",
	AllowAdaptiveEnergyLimit: "allow_adaptive_energy_limit",
	AllowMultiSign:           "allow_multi_sign",
	AllowChangeDelegation:    "allow_change_delegation",
	AllowTvmTransferTrc10:    "allow_tvm_transfer_trc10",
	AllowTvmConstantinople:   "allow_tvm_constantinople",
	AllowTvmSolidity059:      "allow_tvm_solidity059",
	AllowTvmSolidity058:      "allow_tvm_solidity058",
	AllowTvmIstanbul:         "allow_tvm_istanbul",
	AllowMarketTransaction:   "allow_market_transaction",
	AllowTvmFreeze:           "allow_tvm_freeze",
	AllowTvmShieldedToken:    "allow_tvm_shielded_token",
	AllowTvmVote:             "allow_tvm_vote",
	AllowAccountHistory:      "allow_account_history",
	AllowPbft:                "allow_pbft",
	AllowStakingV2:           "allow_staking_v2",
	AllowTvmLondon:           "allow_tvm_london",
	AllowTvmCompatibility:    "allow_tvm_compatibility",
	AllowDynamicEnergy:       "allow_dynamic_energy",
	AllowNewResourceModel:    "allow_new_resource_model",
	AllowEnergyAdjustment:    "allow_energy_adjustment",
	AllowTvmBigInteger:       "allow_tvm_big_integer",
	AllowTvmBlob:             "allow_tvm_blob",
	AllowTvmCancun:           "allow_tvm_cancun",
}

// IsActive returns true if the flag is activated in the DynamicProperties.
// blockNum is available for future block-height-based activation; currently unused.
func IsActive(flag AllowFlag, blockNum uint64, dp *state.DynamicProperties) bool {
	if dp == nil {
		return false
	}
	key, ok := dynKey[flag]
	if !ok {
		return false
	}
	v, _ := dp.Get(key)
	return v != 0
}
