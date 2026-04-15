package forks

import "github.com/tronprotocol/go-tron/core/state"

// AllowFlag identifies a feature that can be toggled via governance proposal.
type AllowFlag int

const (
	AllowSameTokenName AllowFlag = iota
	AllowDelegateResource
	AllowAdaptiveEnergy
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
	AllowTvmCompatibleEvm
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
	AllowAdaptiveEnergy:      "allow_adaptive_energy",
	AllowMultiSign:           "allow_multi_sign",
	AllowChangeDelegation:    "change_delegation",
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
	AllowTvmCompatibleEvm:    "allow_tvm_compatible_evm",
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

// ProposalParamKey maps a governance proposal parameter ID to its DynamicProperties key.
// Returns "" for unknown IDs. This is the single source of truth for all proposal parameter IDs.
func ProposalParamKey(id int64) string {
	mapping := map[int64]string{
		// Numeric parameters
		0:  "maintenance_time_interval",
		1:  "account_upgrade_cost",
		2:  "create_account_fee",
		3:  "transaction_fee",
		4:  "asset_issue_fee",
		5:  "witness_pay_per_block",
		6:  "witness_standby_allowance",
		9:  "create_new_account_fee_in_system_contract",
		10: "create_new_account_bandwidth_rate",
		11: "energy_fee",
		13: "max_cpu_time_of_one_tx",
		19: "total_energy_current_limit",
		22: "total_net_limit",
		27: "unfreeze_delay_days",
		65: "free_net_limit",
		// Allow flags
		14: "allow_same_token_name",
		16: "allow_delegate_resource",
		17: "allow_adaptive_energy",
		18: "allow_tvm_transfer_trc10",
		20: "allow_multi_sign",
		21: "change_delegation",
		23: "allow_new_resource_model",
		25: "allow_tvm_constantinople",
		26: "allow_tvm_solidity059",
		28: "allow_tvm_freeze",
		29: "allow_tvm_shielded_token",
		30: "allow_pbft",
		31: "allow_tvm_istanbul",
		33: "allow_market_transaction",
		34: "allow_tvm_compatible_evm",
		35: "allow_account_history",
		36: "allow_tvm_vote",
		66: "allow_tvm_london",
		67: "allow_energy_adjustment",
		70: "allow_dynamic_energy",
		74: "allow_staking_v2",
		78: "allow_tvm_big_integer",
		82: "allow_tvm_cancun",
		83: "allow_tvm_blob",
	}
	return mapping[id]
}
