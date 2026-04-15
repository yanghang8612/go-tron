package state

// javaGetterToGoKeyMap maps /wallet/getchainparameters key names (java-tron
// getter conventions) to go-tron snake_case DP keys. This is the single
// source of truth for the 76 chain parameters exposed to governance.
//
// Authoritative reference: the enum
//   org.tron.core.utils.ProposalUtil.ProposalType
// in java-tron, cross-checked against the golden fixture
//   test/fixtures/00-genesis-dp-mainnet/fixture.json
// produced by the M0' extraction tooling.
//
// Extending this table:
//   1. Confirm the key appears in a fresh java-tron /wallet/getchainparameters
//      (regenerate the fixture if needed).
//   2. Add the map entry with the canonical snake_case name. Fix acronyms
//      manually (PBFT→pbft, TRC20→trc20, CPU→cpu) rather than rely on a
//      naive camel→snake convertor.
//   3. Add the default value to defaultProps in dynamic_properties.go.
//   4. Add a getter (and setter if mutated by actuators / proposals).
//   5. If the parameter is governance-voted, add the ProposalType ID
//      mapping in core/forks/forks.go.
var javaGetterToGoKeyMap = map[string]string{
	// Baseline chain parameters (present in go-tron today; defaults to be
	// validated against the fixture by the conformance test).
	"getMaintenanceTimeInterval":              "maintenance_time_interval",
	"getAccountUpgradeCost":                   "account_upgrade_cost",
	"getCreateAccountFee":                     "create_account_fee",
	"getTransactionFee":                       "transaction_fee",
	"getAssetIssueFee":                        "asset_issue_fee",
	"getWitnessPayPerBlock":                   "witness_pay_per_block",
	"getWitnessStandbyAllowance":              "witness_standby_allowance",
	"getCreateNewAccountFeeInSystemContract":  "create_new_account_fee_in_system_contract",
	"getCreateNewAccountBandwidthRate":        "create_new_account_bandwidth_rate",
	"getEnergyFee":                            "energy_fee",
	"getExchangeCreateFee":                    "exchange_create_fee",
	"getMaxCpuTimeOfOneTx":                    "max_cpu_time_of_one_tx",
	"getTotalEnergyCurrentLimit":              "total_energy_current_limit",
	"getTotalNetLimit":                        "total_net_limit",
	"getUnfreezeDelayDays":                    "unfreeze_delay_days",
	"getFreeNetLimit":                         "free_net_limit",
	"getProposalExpireTime":                   "proposal_expire_time",
	"getAllowSameTokenName":                   "allow_same_token_name",
	"getAllowDelegateResource":                "allow_delegate_resource",
	"getAllowMultiSign":                       "allow_multi_sign",
	"getAllowTvmTransferTrc10":                "allow_tvm_transfer_trc10",
	"getAllowTvmConstantinople":               "allow_tvm_constantinople",
	"getAllowTvmSolidity059":                  "allow_tvm_solidity059",
	"getAllowTvmIstanbul":                     "allow_tvm_istanbul",
	"getAllowMarketTransaction":               "allow_market_transaction",
	"getAllowTvmFreeze":                       "allow_tvm_freeze",
	"getAllowTvmVote":                         "allow_tvm_vote",
	"getAllowPBFT":                            "allow_pbft",
	"getAllowTvmLondon":                       "allow_tvm_london",
	"getAllowDynamicEnergy":                   "allow_dynamic_energy",
	"getAllowTvmBlob":                         "allow_tvm_blob",
	"getAllowTvmCancun":                       "allow_tvm_cancun",
	"getAllowEnergyAdjustment":                "allow_energy_adjustment",
	"getForbidTransferToContract":             "forbid_transfer_to_contract",
	"getAllowNewResourceModel":                "allow_new_resource_model",

	// Keys below correspond to DP entries that do not yet exist in go-tron
	// or whose go-tron name does not match java-tron's snake_case.
	// The conformance test reports each such case; Task 2–4 of the M1.1
	// plan closes the gaps.
	"getUpdateAccountPermissionFee":            "update_account_permission_fee",
	"getAllowAdaptiveEnergy":                   "allow_adaptive_energy",
	"getChangeDelegation":                      "change_delegation",
	"getAllowTvmCompatibleEvm":                 "allow_tvm_compatible_evm",
	"getAdaptiveResourceLimitMultiplier":       "adaptive_resource_limit_multiplier",
	"getAdaptiveResourceLimitTargetRatio":      "adaptive_resource_limit_target_ratio",
	"getAllowAccountAssetOptimization":         "allow_account_asset_optimization",
	"getAllowAccountStateRoot":                 "allow_account_state_root",
	"getAllowAssetOptimization":                "allow_asset_optimization",
	"getAllowCancelAllUnfreezeV2":              "allow_cancel_all_unfreeze_v2",
	"getAllowCreationOfContracts":              "allow_creation_of_contracts",
	"getAllowDelegateOptimization":             "allow_delegate_optimization",
	"getAllowHigherLimitForMaxCpuTimeOfOneTx":  "allow_higher_limit_for_max_cpu_time_of_one_tx",
	"getAllowNewReward":                        "allow_new_reward",
	"getAllowOldRewardOpt":                     "allow_old_reward_opt",
	"getAllowOptimizeBlackHole":                "allow_optimize_black_hole",
	"getAllowOptimizedReturnValueOfChainId":    "allow_optimized_return_value_of_chain_id",
	"getAllowProtoFilterNum":                   "allow_proto_filter_num",
	"getAllowShieldedTRC20Transaction":         "allow_shielded_trc20_transaction",
	"getAllowStrictMath":                       "allow_strict_math",
	"getAllowTransactionFeePool":               "allow_transaction_fee_pool",
	"getAllowTvmOsaka":                         "allow_tvm_osaka",
	"getAllowTvmSelfdestructRestriction":       "allow_tvm_selfdestruct_restriction",
	"getAllowTvmShangHai":                      "allow_tvm_shanghai",
	"getAllowUpdateAccountName":                "allow_update_account_name",
	"getConsensusLogicOptimization":            "consensus_logic_optimization",
	"getDynamicEnergyIncreaseFactor":           "dynamic_energy_increase_factor",
	"getDynamicEnergyMaxFactor":                "dynamic_energy_max_factor",
	"getDynamicEnergyThreshold":                "dynamic_energy_threshold",
	"getMarketCancelFee":                       "market_cancel_fee",
	"getMarketSellFee":                         "market_sell_fee",
	"getMaxCreateAccountTxSize":                "max_create_account_tx_size",
	"getMaxDelegateLockPeriod":                 "max_delegate_lock_period",
	"getMaxFeeLimit":                           "max_fee_limit",
	"getMemoFee":                               "memo_fee",
	"getMultiSignFee":                          "multi_sign_fee",
	"getRemoveThePowerOfTheGr":                 "remove_the_power_of_the_gr",
	"getTotalEnergyAverageUsage":               "total_energy_average_usage",
	"getTotalEnergyLimit":                      "total_energy_limit",
	"getTotalEnergyTargetLimit":                "total_energy_target_limit",
	"getWitness127PayPerBlock":                 "witness_127_pay_per_block",
}

// JavaGetterToGoKey returns the go-tron DP key for a given java-tron
// getter name, or "" if no mapping is registered. Exported for use by the
// fixture-based conformance test and by tooling that consumes fixtures.
func JavaGetterToGoKey(javaGetter string) string {
	return javaGetterToGoKeyMap[javaGetter]
}
