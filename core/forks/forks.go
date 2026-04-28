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
	AllowTvmIstanbul
	AllowMarketTransaction
	AllowTvmFreeze
	AllowTvmShieldedToken
	AllowTvmVote
	AllowPbft
	AllowStakingV2
	AllowTvmLondon
	AllowTvmCompatibleEvm
	AllowDynamicEnergy
	AllowNewResourceModel
	AllowEnergyAdjustment
	AllowTvmBlob
	AllowTvmCancun
	AllowBlackholeOptimization
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
	AllowTvmIstanbul:         "allow_tvm_istanbul",
	AllowMarketTransaction:   "allow_market_transaction",
	AllowTvmFreeze:           "allow_tvm_freeze",
	AllowTvmShieldedToken:    "allow_shielded_trc20_transaction", // alias: proposal #39 gates the VM shielded token precompiles
	AllowTvmVote:             "allow_tvm_vote",
	AllowPbft:                "allow_pbft",
	AllowStakingV2:           "allow_new_resource_model", // alias: V2 staking rides the same proposal #62 as the state-layer V2 gate
	AllowTvmLondon:           "allow_tvm_london",
	AllowTvmCompatibleEvm:    "allow_tvm_compatible_evm",
	AllowDynamicEnergy:       "allow_dynamic_energy",
	AllowNewResourceModel:    "allow_new_resource_model",
	AllowEnergyAdjustment:    "allow_energy_adjustment",
	AllowTvmBlob:                 "allow_tvm_blob",
	AllowTvmCancun:               "allow_tvm_cancun",
	AllowBlackholeOptimization:   "allow_blackhole_optimization",
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

// ProposalParamKey maps a governance proposal parameter ID to its
// DynamicProperties key. Returns "" for unknown IDs.
//
// Authoritative source: the enum
//   org.tron.core.utils.ProposalUtil.ProposalType
// in java-tron (actuator/src/main/java/...). Every ACTIVE entry there
// (i.e. not commented out) must be present here, mapped to the go-tron
// key with the same semantics.
//
// Commented-out historical IDs in ProposalType (27, 28, 34, 42, 43, 58)
// are intentionally absent — they were either never activated on mainnet
// or have been retired. Do not add them back without re-reading the
// corresponding proto / ProposalUtil comment.
func ProposalParamKey(id int64) string {
	return proposalParamKey[id]
}

var proposalParamKey = map[int64]string{
	0:  "maintenance_time_interval",
	1:  "account_upgrade_cost",
	2:  "create_account_fee",
	3:  "transaction_fee",
	4:  "asset_issue_fee",
	5:  "witness_pay_per_block",
	6:  "witness_standby_allowance",
	7:  "create_new_account_fee_in_system_contract",
	8:  "create_new_account_bandwidth_rate",
	9:  "allow_creation_of_contracts",
	10: "remove_the_power_of_the_gr",
	11: "energy_fee",
	12: "exchange_create_fee",
	13: "max_cpu_time_of_one_tx",
	14: "allow_update_account_name",
	15: "allow_same_token_name",
	16: "allow_delegate_resource",
	17: "total_energy_limit",
	18: "allow_tvm_transfer_trc10",
	19: "total_energy_limit", // java `TOTAL_CURRENT_ENERGY_LIMIT` writes to the same DP key as #17 via saveTotalEnergyLimit2
	20: "allow_multi_sign",
	21: "allow_adaptive_energy",
	22: "update_account_permission_fee",
	23: "multi_sign_fee",
	24: "allow_proto_filter_num",
	25: "allow_account_state_root",
	26: "allow_tvm_constantinople",
	29: "adaptive_resource_limit_multiplier",
	30: "change_delegation",
	31: "witness_127_pay_per_block",
	32: "allow_tvm_solidity059",
	33: "adaptive_resource_limit_target_ratio",
	35: "forbid_transfer_to_contract",
	39: "allow_shielded_trc20_transaction",
	40: "allow_pbft",
	41: "allow_tvm_istanbul",
	44: "allow_market_transaction",
	45: "market_sell_fee",
	46: "market_cancel_fee",
	47: "max_fee_limit",
	48: "allow_transaction_fee_pool",
	49: "allow_blackhole_optimization",
	51: "allow_new_resource_model",
	52: "allow_tvm_freeze",
	53: "allow_account_asset_optimization",
	59: "allow_tvm_vote",
	60: "allow_tvm_compatible_evm",
	61: "free_net_limit",
	62: "total_net_limit",
	63: "allow_tvm_london",
	65: "allow_higher_limit_for_max_cpu_time_of_one_tx",
	66: "allow_asset_optimization",
	67: "allow_new_reward",
	68: "memo_fee",
	69: "allow_delegate_optimization",
	70: "unfreeze_delay_days",
	71: "allow_optimized_return_value_of_chain_id",
	72: "allow_dynamic_energy",
	73: "dynamic_energy_threshold",
	74: "dynamic_energy_increase_factor",
	75: "dynamic_energy_max_factor",
	76: "allow_tvm_shanghai",
	77: "allow_cancel_all_unfreeze_v2",
	78: "max_delegate_lock_period",
	79: "allow_old_reward_opt",
	81: "allow_energy_adjustment",
	82: "max_create_account_tx_size",
	83: "allow_tvm_cancun",
	87: "allow_strict_math",
	88: "consensus_logic_optimization",
	89: "allow_tvm_blob",
	92: "proposal_expire_time",
	94: "allow_tvm_selfdestruct_restriction",
	96: "allow_tvm_osaka",
}
