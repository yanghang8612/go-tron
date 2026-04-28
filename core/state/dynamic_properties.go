package state

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

var defaultProps = map[string]int64{
	"maintenance_time_interval":                 21600000,
	"account_upgrade_cost":                      9999000000,
	"create_account_fee":                        100000,
	"transaction_fee":                           10,
	"asset_issue_fee":                           1024000000,
	"witness_pay_per_block":                     32000000,
	"witness_127_pay_per_block":                 16000000,
	"witness_standby_allowance":                 115200000000,
	"create_new_account_fee_in_system_contract": 0,
	"create_new_account_bandwidth_rate":         1,
	"energy_fee":                                100,
	"max_cpu_time_of_one_tx":                    50,
	"total_energy_current_limit":                50000000000,
	"total_energy_limit":                        50000000000,
	"total_energy_target_limit":                 3472222,
	"total_energy_average_usage":                0,
	"total_net_limit":                           43200000000,
	// Internal resource-weight counters (in TRX units, per java-tron
	// DynamicPropertiesStore.addTotalNetWeight comment "The unit is trx").
	// Default 0 — grown by freeze actuators, shrunk by unfreeze.
	"total_net_weight":                          0,
	"total_energy_weight":                       0,
	"total_tron_power_weight":                   0,
	"unfreeze_delay_days":                       0,
	"latest_block_header_timestamp":             0,
	"latest_block_header_number":                0,
	"latest_solidified_block_num":               0,
	"next_maintenance_time":                     0,
	"allow_new_resource_model":                  0,
	"free_net_limit":                            5000,
	"next_proposal_id":                          0,
	"next_token_id":                             1_000_001,
	"next_exchange_id":                          1,
	"exchange_create_fee":                       1_024_000_000,
	"exchange_balance_limit":                    1_000_000_000_000_000,
	"allow_same_token_name":                     0,
	"allow_delegate_resource":                   0,
	"allow_adaptive_energy":               0,
	"allow_multi_sign":                          0,
	"change_delegation":                   0,
	"allow_tvm_transfer_trc10":                  0,
	"allow_tvm_constantinople":                  0,
	"allow_tvm_solidity059":                     0,
	"allow_tvm_istanbul":                        0,
	"allow_market_transaction":                  0,
	"allow_tvm_freeze":                          0,
	"allow_tvm_vote":           0,
	"allow_pbft":               0,
	"allow_tvm_london":                          0,
	"allow_tvm_compatible_evm":                   0,
	"allow_dynamic_energy":                      0,
	"allow_tvm_blob":                            0,
	"allow_tvm_cancun":                          0,
	"allow_energy_adjustment":                   0,
	"forbid_transfer_to_contract":              0,
	"update_account_permission_fee":            100_000_000,
	"total_sign_num":                           5,
	"proposal_expire_time":                     259_200_000,
	"allow_shielded_transaction":               0,
	"zen_token_id":                             1_000_016,
	"total_shielded_pool_value":                0,
	"shielded_transaction_fee":                 100_000,
	"shielded_transaction_create_account_fee":  1_000_000,

	// M1.1: chain parameters backfilled from java-tron mainnet fixture
	// (test/fixtures/00-genesis-dp-mainnet/fixture.json). Defaults align
	// to java-tron's DynamicPropertiesStore initial values. Adaptive /
	// reward / dynamic-energy *behaviour* is not yet implemented — these
	// keys only ensure that initial state and governance proposals
	// serialize to the same DB shape as java-tron.
	"adaptive_resource_limit_multiplier":            1000,
	"adaptive_resource_limit_target_ratio":          10,
	"block_energy_usage":                            0,
	"total_energy_average_time":                     0,
	"current_cycle_number":                          0,
	"new_reward_algorithm_effective_cycle":          9_223_372_036_854_775_807, // Long.MAX_VALUE — disabled until set by proposal
	"allow_account_asset_optimization":              0,
	"allow_account_state_root":                      0,
	"allow_asset_optimization":                      0,
	"allow_cancel_all_unfreeze_v2":                  0,
	"allow_creation_of_contracts":                   0,
	"allow_delegate_optimization":                   0,
	"allow_higher_limit_for_max_cpu_time_of_one_tx": 0,
	"allow_new_reward":                              0,
	"allow_old_reward_opt":                          0,
	"allow_blackhole_optimization":                     0,
	"allow_optimized_return_value_of_chain_id":      0,
	"allow_proto_filter_num":                        0,
	"allow_shielded_trc20_transaction":              0,
	"allow_strict_math":                             0,
	"allow_transaction_fee_pool":                    0,
	"allow_tvm_osaka":                               0,
	"allow_tvm_selfdestruct_restriction":            0,
	"allow_tvm_shanghai":                            0,
	"allow_update_account_name":                     0,
	"consensus_logic_optimization":                  0,
	"dynamic_energy_increase_factor":                0,
	"dynamic_energy_max_factor":                     0,
	"dynamic_energy_threshold":                      0,
	"market_cancel_fee":                             0,
	"market_sell_fee":                               0,
	"max_create_account_tx_size":                    1000,
	"max_delegate_lock_period":                      86400,
	"max_fee_limit":                                 1_000_000_000,
	"memo_fee":                                      0,
	"multi_sign_fee":                                1_000_000,
	"remove_the_power_of_the_gr":                    0,

	// M1.6: storage market keys — initialized by java-tron DynamicPropertiesStore
	// but never modified on mainnet (feature was never activated). Present for
	// completeness so DP serializes identically to java-tron's initial state.
	"total_storage_pool":          100_000_000_000_000, // 100 TRX in sun
	"total_storage_tax":           0,
	"total_storage_reserved":      137_438_953_472, // 128 GiB in bytes
	"storage_exchange_tax_rate":   10,

	// §1.6: freeze/supply/bandwidth/accounting keys missing from earlier
	// backfills. Defaults verified against java-tron DynamicPropertiesStore.
	"max_frozen_time":              3,
	"min_frozen_time":              3,
	"max_frozen_supply_number":     10,
	"max_frozen_supply_time":       3652,
	"min_frozen_supply_time":       1,
	"witness_allowance_frozen_time": 1,
	"one_day_net_limit":            57_600_000_000,
	"public_net_limit":             14_400_000_000,
	"public_net_usage":             0,
	"public_net_time":              0,
	"transaction_fee_pool":         0,
	"total_transaction_cost":       0,
	"total_create_account_cost":    0,
	"block_filled_slots_index":     0,
	"version_number":               0,
}

// DynamicProperties holds runtime-adjustable chain parameters stored as key-value pairs.
type DynamicProperties struct {
	props                 map[string]int64
	dirty                 map[string]struct{}
	latestBlockHeaderHash common.Hash
	hashDirty             bool
}

// NewDynamicProperties creates a DynamicProperties with default values.
func NewDynamicProperties() *DynamicProperties {
	dp := &DynamicProperties{
		props: make(map[string]int64, len(defaultProps)),
		dirty: make(map[string]struct{}),
	}
	for k, v := range defaultProps {
		dp.props[k] = v
	}
	return dp
}

// LoadDynamicProperties creates a DynamicProperties with defaults, overriding from DB.
func LoadDynamicProperties(db ethdb.KeyValueReader) *DynamicProperties {
	dp := NewDynamicProperties()
	for k := range defaultProps {
		data := rawdb.ReadDynamicProperty(db, k)
		if len(data) == 8 {
			dp.props[k] = int64(binary.BigEndian.Uint64(data))
		}
	}
	hashData := rawdb.ReadDynamicProperty(db, "latest_block_header_hash")
	if len(hashData) == common.HashLength {
		dp.latestBlockHeaderHash = common.BytesToHash(hashData)
	}
	return dp
}

// Flush writes only dirty props to db as 8-byte big-endian, then clears dirty state.
func (dp *DynamicProperties) Flush(db ethdb.KeyValueWriter) {
	buf := make([]byte, 8)
	for k := range dp.dirty {
		binary.BigEndian.PutUint64(buf, uint64(dp.props[k]))
		rawdb.WriteDynamicProperty(db, k, append([]byte{}, buf...))
	}
	if dp.hashDirty {
		rawdb.WriteDynamicProperty(db, "latest_block_header_hash", dp.latestBlockHeaderHash.Bytes())
		dp.hashDirty = false
	}
	dp.dirty = make(map[string]struct{})
}

// Get returns the value for a key and whether it was found.
func (dp *DynamicProperties) Get(key string) (int64, bool) {
	v, ok := dp.props[key]
	return v, ok
}

// Set sets a value and marks the key dirty.
func (dp *DynamicProperties) Set(key string, value int64) {
	dp.props[key] = value
	dp.dirty[key] = struct{}{}
}

// Keys returns every currently-known DP key (defaults plus anything Set()).
// The result is unsorted; callers that need stability (e.g. the conformance
// digest) must sort it themselves.
func (dp *DynamicProperties) Keys() []string {
	out := make([]string, 0, len(dp.props))
	for k := range dp.props {
		out = append(out, k)
	}
	return out
}

// --- Typed getters ---

func (dp *DynamicProperties) MaintenanceTimeInterval() int64 {
	return dp.props["maintenance_time_interval"]
}

func (dp *DynamicProperties) NextMaintenanceTime() int64 {
	return dp.props["next_maintenance_time"]
}

func (dp *DynamicProperties) LatestBlockHeaderNumber() int64 {
	return dp.props["latest_block_header_number"]
}

func (dp *DynamicProperties) LatestBlockHeaderTimestamp() int64 {
	return dp.props["latest_block_header_timestamp"]
}

func (dp *DynamicProperties) LatestSolidifiedBlockNum() int64 {
	return dp.props["latest_solidified_block_num"]
}

func (dp *DynamicProperties) WitnessPayPerBlock() int64 {
	return dp.props["witness_pay_per_block"]
}

func (dp *DynamicProperties) WitnessStandbyAllowance() int64 {
	return dp.props["witness_standby_allowance"]
}

func (dp *DynamicProperties) TransactionFee() int64 {
	return dp.props["transaction_fee"]
}

func (dp *DynamicProperties) EnergyFee() int64 {
	return dp.props["energy_fee"]
}

func (dp *DynamicProperties) CreateAccountFee() int64 {
	return dp.props["create_account_fee"]
}

func (dp *DynamicProperties) CreateNewAccountFeeInSystemContract() int64 {
	return dp.props["create_new_account_fee_in_system_contract"]
}

func (dp *DynamicProperties) TotalEnergyCurrentLimit() int64 {
	return dp.props["total_energy_current_limit"]
}
func (dp *DynamicProperties) SetTotalEnergyCurrentLimit(v int64) {
	dp.Set("total_energy_current_limit", v)
}

func (dp *DynamicProperties) BlockEnergyUsage() int64 { return dp.props["block_energy_usage"] }
func (dp *DynamicProperties) SetBlockEnergyUsage(v int64) {
	dp.Set("block_energy_usage", v)
}

func (dp *DynamicProperties) TotalEnergyAverageTime() int64 {
	return dp.props["total_energy_average_time"]
}
func (dp *DynamicProperties) SetTotalEnergyAverageTime(v int64) {
	dp.Set("total_energy_average_time", v)
}

func (dp *DynamicProperties) TotalNetLimit() int64 {
	return dp.props["total_net_limit"]
}

// --- Resource weight counters ---
// Mirror java-tron's TOTAL_{NET,ENERGY,TRON_POWER}_WEIGHT in
// DynamicPropertiesStore. Unit is TRX (not SUN); actuators convert
// frozen SUN amounts via `amount / TRX_PRECISION` before calling Add*.

func (dp *DynamicProperties) TotalNetWeight() int64 { return dp.props["total_net_weight"] }
func (dp *DynamicProperties) SetTotalNetWeight(v int64) {
	dp.Set("total_net_weight", v)
}

func (dp *DynamicProperties) TotalEnergyWeight() int64 { return dp.props["total_energy_weight"] }
func (dp *DynamicProperties) SetTotalEnergyWeight(v int64) {
	dp.Set("total_energy_weight", v)
}

func (dp *DynamicProperties) TotalTronPowerWeight() int64 {
	return dp.props["total_tron_power_weight"]
}
func (dp *DynamicProperties) SetTotalTronPowerWeight(v int64) {
	dp.Set("total_tron_power_weight", v)
}

// AddTotalNetWeight adds delta to total_net_weight and clamps the result
// to >= 0 when allow_new_reward is active, mirroring java-tron's
// DynamicPropertiesStore.addTotalNetWeight. delta == 0 is a no-op and
// leaves the dirty flag unset.
func (dp *DynamicProperties) AddTotalNetWeight(delta int64) {
	if delta == 0 {
		return
	}
	next := dp.TotalNetWeight() + delta
	if dp.AllowNewReward() && next < 0 {
		next = 0
	}
	dp.SetTotalNetWeight(next)
}

// AddTotalEnergyWeight — see AddTotalNetWeight.
func (dp *DynamicProperties) AddTotalEnergyWeight(delta int64) {
	if delta == 0 {
		return
	}
	next := dp.TotalEnergyWeight() + delta
	if dp.AllowNewReward() && next < 0 {
		next = 0
	}
	dp.SetTotalEnergyWeight(next)
}

// AddTotalTronPowerWeight — see AddTotalNetWeight.
func (dp *DynamicProperties) AddTotalTronPowerWeight(delta int64) {
	if delta == 0 {
		return
	}
	next := dp.TotalTronPowerWeight() + delta
	if dp.AllowNewReward() && next < 0 {
		next = 0
	}
	dp.SetTotalTronPowerWeight(next)
}

func (dp *DynamicProperties) UnfreezeDelayDays() int64 {
	return dp.props["unfreeze_delay_days"]
}

func (dp *DynamicProperties) MaxCpuTimeOfOneTx() int64 {
	return dp.props["max_cpu_time_of_one_tx"]
}

func (dp *DynamicProperties) AllowNewResourceModel() bool {
	return dp.props["allow_new_resource_model"] != 0
}
func (dp *DynamicProperties) SetAllowNewResourceModel(v bool) {
	if v {
		dp.Set("allow_new_resource_model", 1)
	} else {
		dp.Set("allow_new_resource_model", 0)
	}
}

func (dp *DynamicProperties) AllowSameTokenName() bool {
	return dp.props["allow_same_token_name"] != 0
}
func (dp *DynamicProperties) SetAllowSameTokenName(v bool) {
	if v {
		dp.Set("allow_same_token_name", 1)
	} else {
		dp.Set("allow_same_token_name", 0)
	}
}

func (dp *DynamicProperties) AllowDelegateResource() bool {
	return dp.props["allow_delegate_resource"] != 0
}
func (dp *DynamicProperties) SetAllowDelegateResource(v bool) {
	if v {
		dp.Set("allow_delegate_resource", 1)
	} else {
		dp.Set("allow_delegate_resource", 0)
	}
}

func (dp *DynamicProperties) AllowAdaptiveEnergy() bool {
	return dp.props["allow_adaptive_energy"] != 0
}
func (dp *DynamicProperties) SetAllowAdaptiveEnergy(v bool) {
	if v {
		dp.Set("allow_adaptive_energy", 1)
	} else {
		dp.Set("allow_adaptive_energy", 0)
	}
}

func (dp *DynamicProperties) AllowMultiSign() bool {
	return dp.props["allow_multi_sign"] != 0
}
func (dp *DynamicProperties) SetAllowMultiSign(v bool) {
	if v {
		dp.Set("allow_multi_sign", 1)
	} else {
		dp.Set("allow_multi_sign", 0)
	}
}

func (dp *DynamicProperties) ChangeDelegation() bool {
	return dp.props["change_delegation"] != 0
}
func (dp *DynamicProperties) SetChangeDelegation(v bool) {
	if v {
		dp.Set("change_delegation", 1)
	} else {
		dp.Set("change_delegation", 0)
	}
}

func (dp *DynamicProperties) AllowTvmTransferTrc10() bool {
	return dp.props["allow_tvm_transfer_trc10"] != 0
}
func (dp *DynamicProperties) SetAllowTvmTransferTrc10(v bool) {
	if v {
		dp.Set("allow_tvm_transfer_trc10", 1)
	} else {
		dp.Set("allow_tvm_transfer_trc10", 0)
	}
}

func (dp *DynamicProperties) AllowTvmConstantinople() bool {
	return dp.props["allow_tvm_constantinople"] != 0
}
func (dp *DynamicProperties) SetAllowTvmConstantinople(v bool) {
	if v {
		dp.Set("allow_tvm_constantinople", 1)
	} else {
		dp.Set("allow_tvm_constantinople", 0)
	}
}

func (dp *DynamicProperties) AllowTvmSolidity059() bool {
	return dp.props["allow_tvm_solidity059"] != 0
}
func (dp *DynamicProperties) SetAllowTvmSolidity059(v bool) {
	if v {
		dp.Set("allow_tvm_solidity059", 1)
	} else {
		dp.Set("allow_tvm_solidity059", 0)
	}
}

func (dp *DynamicProperties) AllowTvmIstanbul() bool {
	return dp.props["allow_tvm_istanbul"] != 0
}
func (dp *DynamicProperties) SetAllowTvmIstanbul(v bool) {
	if v {
		dp.Set("allow_tvm_istanbul", 1)
	} else {
		dp.Set("allow_tvm_istanbul", 0)
	}
}

func (dp *DynamicProperties) AllowMarketTransaction() bool {
	return dp.props["allow_market_transaction"] != 0
}
func (dp *DynamicProperties) SetAllowMarketTransaction(v bool) {
	if v {
		dp.Set("allow_market_transaction", 1)
	} else {
		dp.Set("allow_market_transaction", 0)
	}
}

func (dp *DynamicProperties) AllowTvmFreeze() bool {
	return dp.props["allow_tvm_freeze"] != 0
}
func (dp *DynamicProperties) SetAllowTvmFreeze(v bool) {
	if v {
		dp.Set("allow_tvm_freeze", 1)
	} else {
		dp.Set("allow_tvm_freeze", 0)
	}
}

// AllowTvmShieldedToken / SetAllowTvmShieldedToken are compatibility wrappers.
// VM precompiles named this historically; java-tron gates them on
// allow_shielded_trc20_transaction (proposal #39). Delegate so the flag
// flips when proposal #39 passes.
func (dp *DynamicProperties) AllowTvmShieldedToken() bool { return dp.AllowShieldedTrc20Transaction() }
func (dp *DynamicProperties) SetAllowTvmShieldedToken(v bool) {
	dp.SetAllowShieldedTrc20Transaction(v)
}

func (dp *DynamicProperties) AllowTvmVote() bool {
	return dp.props["allow_tvm_vote"] != 0
}
func (dp *DynamicProperties) SetAllowTvmVote(v bool) {
	if v {
		dp.Set("allow_tvm_vote", 1)
	} else {
		dp.Set("allow_tvm_vote", 0)
	}
}

func (dp *DynamicProperties) AllowPbft() bool {
	return dp.props["allow_pbft"] != 0
}
func (dp *DynamicProperties) SetAllowPbft(v bool) {
	if v {
		dp.Set("allow_pbft", 1)
	} else {
		dp.Set("allow_pbft", 0)
	}
}

// AllowStakingV2 and SetAllowStakingV2 are compatibility wrappers for the
// V2 staking feature gate. java-tron uses a single flag for both the
// state-layer V2 rules (freeze/unfreeze/delegate) and the VM V2 precompiles:
// `allow_new_resource_model` (proposal #62). These wrappers delegate there
// so every go-tron gate flips together when proposal #62 passes.
func (dp *DynamicProperties) AllowStakingV2() bool { return dp.AllowNewResourceModel() }
func (dp *DynamicProperties) SetAllowStakingV2(v bool) { dp.SetAllowNewResourceModel(v) }

func (dp *DynamicProperties) AllowTvmLondon() bool {
	return dp.props["allow_tvm_london"] != 0
}
func (dp *DynamicProperties) SetAllowTvmLondon(v bool) {
	if v {
		dp.Set("allow_tvm_london", 1)
	} else {
		dp.Set("allow_tvm_london", 0)
	}
}

func (dp *DynamicProperties) AllowTvmCompatibleEvm() bool {
	return dp.props["allow_tvm_compatible_evm"] != 0
}
func (dp *DynamicProperties) SetAllowTvmCompatibleEvm(v bool) {
	if v {
		dp.Set("allow_tvm_compatible_evm", 1)
	} else {
		dp.Set("allow_tvm_compatible_evm", 0)
	}
}

func (dp *DynamicProperties) AllowDynamicEnergy() bool {
	return dp.props["allow_dynamic_energy"] != 0
}
func (dp *DynamicProperties) SetAllowDynamicEnergy(v bool) {
	if v {
		dp.Set("allow_dynamic_energy", 1)
	} else {
		dp.Set("allow_dynamic_energy", 0)
	}
}

func (dp *DynamicProperties) AllowTvmBlob() bool {
	return dp.props["allow_tvm_blob"] != 0
}
func (dp *DynamicProperties) SetAllowTvmBlob(v bool) {
	if v {
		dp.Set("allow_tvm_blob", 1)
	} else {
		dp.Set("allow_tvm_blob", 0)
	}
}

func (dp *DynamicProperties) AllowTvmCancun() bool {
	return dp.props["allow_tvm_cancun"] != 0
}
func (dp *DynamicProperties) SetAllowTvmCancun(v bool) {
	if v {
		dp.Set("allow_tvm_cancun", 1)
	} else {
		dp.Set("allow_tvm_cancun", 0)
	}
}

func (dp *DynamicProperties) AllowEnergyAdjustment() bool {
	return dp.props["allow_energy_adjustment"] != 0
}
func (dp *DynamicProperties) SetAllowEnergyAdjustment(v bool) {
	if v {
		dp.Set("allow_energy_adjustment", 1)
	} else {
		dp.Set("allow_energy_adjustment", 0)
	}
}

func (dp *DynamicProperties) FreeNetLimit() int64 {
	return dp.props["free_net_limit"]
}

func (dp *DynamicProperties) LatestBlockHeaderHash() common.Hash {
	return dp.latestBlockHeaderHash
}

// --- Typed setters ---

func (dp *DynamicProperties) SetNextMaintenanceTime(t int64) {
	dp.Set("next_maintenance_time", t)
}

func (dp *DynamicProperties) SetLatestBlockHeaderNumber(n int64) {
	dp.Set("latest_block_header_number", n)
}

func (dp *DynamicProperties) SetLatestBlockHeaderTimestamp(t int64) {
	dp.Set("latest_block_header_timestamp", t)
}

func (dp *DynamicProperties) SetLatestSolidifiedBlockNum(n int64) {
	dp.Set("latest_solidified_block_num", n)
}

func (dp *DynamicProperties) SetLatestBlockHeaderHash(h common.Hash) {
	dp.latestBlockHeaderHash = h
	dp.hashDirty = true
}

func (dp *DynamicProperties) NextProposalID() int64 {
	return dp.props["next_proposal_id"]
}

func (dp *DynamicProperties) SetNextProposalID(id int64) {
	dp.Set("next_proposal_id", id)
}

// NextTokenID returns the next token ID to assign (starts at 1_000_001).
func (dp *DynamicProperties) NextTokenID() int64 { return dp.props["next_token_id"] }

// SetNextTokenID updates the next token ID counter.
func (dp *DynamicProperties) SetNextTokenID(id int64) { dp.Set("next_token_id", id) }

// NextExchangeID returns the next exchange ID to assign (starts at 1).
func (dp *DynamicProperties) NextExchangeID() int64 { return dp.props["next_exchange_id"] }

// SetNextExchangeID updates the next exchange ID counter.
func (dp *DynamicProperties) SetNextExchangeID(id int64) { dp.Set("next_exchange_id", id) }

// AssetIssueFee returns the fee (in SUN) required to issue a TRC10 token.
func (dp *DynamicProperties) AssetIssueFee() int64 { return dp.props["asset_issue_fee"] }

// ExchangeCreateFee returns the fee (in SUN) required to create a DEX exchange.
// Matches java-tron DynamicPropertiesStore.getExchangeCreateFee (default 1024 TRX).
func (dp *DynamicProperties) ExchangeCreateFee() int64 { return dp.props["exchange_create_fee"] }

// SetExchangeCreateFee updates the exchange creation fee.
func (dp *DynamicProperties) SetExchangeCreateFee(fee int64) { dp.Set("exchange_create_fee", fee) }

// ExchangeBalanceLimit returns the maximum per-token balance an exchange may hold.
// Matches java-tron DynamicPropertiesStore.getExchangeBalanceLimit (default 1e15).
func (dp *DynamicProperties) ExchangeBalanceLimit() int64 {
	return dp.props["exchange_balance_limit"]
}

// SetExchangeBalanceLimit updates the exchange balance limit.
func (dp *DynamicProperties) SetExchangeBalanceLimit(limit int64) {
	dp.Set("exchange_balance_limit", limit)
}

// AccountUpgradeCost returns the fee (in SUN) to upgrade an account to witness.
func (dp *DynamicProperties) AccountUpgradeCost() int64 { return dp.props["account_upgrade_cost"] }

// ForbidTransferToContract returns true if TRX/TRC10 transfers to smart contracts are forbidden.
func (dp *DynamicProperties) ForbidTransferToContract() bool {
	return dp.props["forbid_transfer_to_contract"] != 0
}

// UpdateAccountPermissionFee returns the fee (in SUN) to update account permissions.
func (dp *DynamicProperties) UpdateAccountPermissionFee() int64 {
	return dp.props["update_account_permission_fee"]
}

// TotalSignNum returns the maximum total number of keys across all permissions.
func (dp *DynamicProperties) TotalSignNum() int64 { return dp.props["total_sign_num"] }

// ProposalExpireTime returns the proposal expiration window in milliseconds (default 3 days).
func (dp *DynamicProperties) ProposalExpireTime() int64 { return dp.props["proposal_expire_time"] }

// AllowShieldedTransaction returns true if shielded (Sapling) transactions are enabled.
func (dp *DynamicProperties) AllowShieldedTransaction() bool {
	return dp.props["allow_shielded_transaction"] != 0
}
func (dp *DynamicProperties) SetAllowShieldedTransaction(v bool) {
	if v {
		dp.Set("allow_shielded_transaction", 1)
	} else {
		dp.Set("allow_shielded_transaction", 0)
	}
}

// ZenTokenID returns the TRC10 token ID of the ZEN token (default 1000016).
func (dp *DynamicProperties) ZenTokenID() int64 { return dp.props["zen_token_id"] }

// TotalShieldedPoolValue returns the total ZEN value currently held in the shielded pool.
func (dp *DynamicProperties) TotalShieldedPoolValue() int64 {
	return dp.props["total_shielded_pool_value"]
}

// AdjustTotalShieldedPoolValue adds delta to the total shielded pool value.
func (dp *DynamicProperties) AdjustTotalShieldedPoolValue(delta int64) {
	dp.Set("total_shielded_pool_value", dp.props["total_shielded_pool_value"]+delta)
}

// ShieldedTransactionFee returns the fee (in ZEN smallest unit) for a shielded transaction.
func (dp *DynamicProperties) ShieldedTransactionFee() int64 {
	return dp.props["shielded_transaction_fee"]
}

// ShieldedTransactionCreateAccountFee returns the fee when a shielded tx creates a new account.
func (dp *DynamicProperties) ShieldedTransactionCreateAccountFee() int64 {
	return dp.props["shielded_transaction_create_account_fee"]
}

// All returns a read-only copy of all dynamic properties.
func (dp *DynamicProperties) All() map[string]int64 {
	result := make(map[string]int64, len(dp.props))
	for k, v := range dp.props {
		result[k] = v
	}
	return result
}

// ---------------------------------------------------------------------------
// M1.1 backfilled accessors.
//
// These mirror java-tron's DynamicPropertiesStore getter/setter names; the
// backing string keys are enforced by dynamic_properties_fixture_test.go.
// Behaviour coupled to these flags (adaptive energy recalc, reward v2,
// dynamic energy scaling, storage rent, ...) is out of M1.1 scope — see
// M1.4–M1.8 of PLAN.md.
// ---------------------------------------------------------------------------

func boolGet(dp *DynamicProperties, key string) bool { return dp.props[key] != 0 }
func boolSet(dp *DynamicProperties, key string, v bool) {
	if v {
		dp.Set(key, 1)
	} else {
		dp.Set(key, 0)
	}
}

// Numeric params.

func (dp *DynamicProperties) AdaptiveResourceLimitMultiplier() int64 {
	return dp.props["adaptive_resource_limit_multiplier"]
}
func (dp *DynamicProperties) SetAdaptiveResourceLimitMultiplier(v int64) {
	dp.Set("adaptive_resource_limit_multiplier", v)
}

func (dp *DynamicProperties) AdaptiveResourceLimitTargetRatio() int64 {
	return dp.props["adaptive_resource_limit_target_ratio"]
}
func (dp *DynamicProperties) SetAdaptiveResourceLimitTargetRatio(v int64) {
	dp.Set("adaptive_resource_limit_target_ratio", v)
}

func (dp *DynamicProperties) TotalEnergyLimit() int64 { return dp.props["total_energy_limit"] }

// SetTotalEnergyLimit mirrors java-tron's saveTotalEnergyLimit2: updates the
// base limit, recomputes targetLimit, and (only when adaptive energy is OFF)
// also sets currentLimit to match.
func (dp *DynamicProperties) SetTotalEnergyLimit(v int64) {
	dp.Set("total_energy_limit", v)
	ratio := dp.AdaptiveResourceLimitTargetRatio()
	if ratio > 0 {
		dp.SetTotalEnergyTargetLimit(v / ratio)
	}
	if !dp.AllowAdaptiveEnergy() {
		dp.SetTotalEnergyCurrentLimit(v)
	}
}

func (dp *DynamicProperties) TotalEnergyTargetLimit() int64 {
	return dp.props["total_energy_target_limit"]
}
func (dp *DynamicProperties) SetTotalEnergyTargetLimit(v int64) {
	dp.Set("total_energy_target_limit", v)
}

func (dp *DynamicProperties) TotalEnergyAverageUsage() int64 {
	return dp.props["total_energy_average_usage"]
}
func (dp *DynamicProperties) SetTotalEnergyAverageUsage(v int64) {
	dp.Set("total_energy_average_usage", v)
}

func (dp *DynamicProperties) DynamicEnergyIncreaseFactor() int64 {
	return dp.props["dynamic_energy_increase_factor"]
}
func (dp *DynamicProperties) SetDynamicEnergyIncreaseFactor(v int64) {
	dp.Set("dynamic_energy_increase_factor", v)
}

func (dp *DynamicProperties) DynamicEnergyMaxFactor() int64 {
	return dp.props["dynamic_energy_max_factor"]
}
func (dp *DynamicProperties) SetDynamicEnergyMaxFactor(v int64) {
	dp.Set("dynamic_energy_max_factor", v)
}

func (dp *DynamicProperties) DynamicEnergyThreshold() int64 {
	return dp.props["dynamic_energy_threshold"]
}
func (dp *DynamicProperties) SetDynamicEnergyThreshold(v int64) {
	dp.Set("dynamic_energy_threshold", v)
}

func (dp *DynamicProperties) MarketCancelFee() int64 { return dp.props["market_cancel_fee"] }
func (dp *DynamicProperties) SetMarketCancelFee(v int64) {
	dp.Set("market_cancel_fee", v)
}

func (dp *DynamicProperties) MarketSellFee() int64 { return dp.props["market_sell_fee"] }
func (dp *DynamicProperties) SetMarketSellFee(v int64) {
	dp.Set("market_sell_fee", v)
}

func (dp *DynamicProperties) MaxCreateAccountTxSize() int64 {
	return dp.props["max_create_account_tx_size"]
}
func (dp *DynamicProperties) SetMaxCreateAccountTxSize(v int64) {
	dp.Set("max_create_account_tx_size", v)
}

func (dp *DynamicProperties) MaxDelegateLockPeriod() int64 {
	return dp.props["max_delegate_lock_period"]
}
func (dp *DynamicProperties) SetMaxDelegateLockPeriod(v int64) {
	dp.Set("max_delegate_lock_period", v)
}

func (dp *DynamicProperties) MaxFeeLimit() int64 { return dp.props["max_fee_limit"] }
func (dp *DynamicProperties) SetMaxFeeLimit(v int64) { dp.Set("max_fee_limit", v) }

func (dp *DynamicProperties) MemoFee() int64 { return dp.props["memo_fee"] }
func (dp *DynamicProperties) SetMemoFee(v int64) { dp.Set("memo_fee", v) }

func (dp *DynamicProperties) MultiSignFee() int64 { return dp.props["multi_sign_fee"] }
func (dp *DynamicProperties) SetMultiSignFee(v int64) { dp.Set("multi_sign_fee", v) }

func (dp *DynamicProperties) Witness127PayPerBlock() int64 {
	return dp.props["witness_127_pay_per_block"]
}
func (dp *DynamicProperties) SetWitness127PayPerBlock(v int64) {
	dp.Set("witness_127_pay_per_block", v)
}

// Boolean / integer flag params.

func (dp *DynamicProperties) AllowAccountAssetOptimization() bool {
	return boolGet(dp, "allow_account_asset_optimization")
}
func (dp *DynamicProperties) SetAllowAccountAssetOptimization(v bool) {
	boolSet(dp, "allow_account_asset_optimization", v)
}

func (dp *DynamicProperties) AllowAccountStateRoot() bool {
	return boolGet(dp, "allow_account_state_root")
}
func (dp *DynamicProperties) SetAllowAccountStateRoot(v bool) {
	boolSet(dp, "allow_account_state_root", v)
}

func (dp *DynamicProperties) AllowAssetOptimization() bool {
	return boolGet(dp, "allow_asset_optimization")
}
func (dp *DynamicProperties) SetAllowAssetOptimization(v bool) {
	boolSet(dp, "allow_asset_optimization", v)
}

func (dp *DynamicProperties) AllowCancelAllUnfreezeV2() bool {
	return boolGet(dp, "allow_cancel_all_unfreeze_v2")
}
func (dp *DynamicProperties) SetAllowCancelAllUnfreezeV2(v bool) {
	boolSet(dp, "allow_cancel_all_unfreeze_v2", v)
}

func (dp *DynamicProperties) AllowCreationOfContracts() bool {
	return boolGet(dp, "allow_creation_of_contracts")
}
func (dp *DynamicProperties) SetAllowCreationOfContracts(v bool) {
	boolSet(dp, "allow_creation_of_contracts", v)
}

func (dp *DynamicProperties) AllowDelegateOptimization() bool {
	return boolGet(dp, "allow_delegate_optimization")
}
func (dp *DynamicProperties) SetAllowDelegateOptimization(v bool) {
	boolSet(dp, "allow_delegate_optimization", v)
}

func (dp *DynamicProperties) AllowHigherLimitForMaxCpuTimeOfOneTx() bool {
	return boolGet(dp, "allow_higher_limit_for_max_cpu_time_of_one_tx")
}
func (dp *DynamicProperties) SetAllowHigherLimitForMaxCpuTimeOfOneTx(v bool) {
	boolSet(dp, "allow_higher_limit_for_max_cpu_time_of_one_tx", v)
}

func (dp *DynamicProperties) AllowNewReward() bool {
	return boolGet(dp, "allow_new_reward")
}
func (dp *DynamicProperties) SetAllowNewReward(v bool) {
	boolSet(dp, "allow_new_reward", v)
}

func (dp *DynamicProperties) AllowOldRewardOpt() bool {
	return boolGet(dp, "allow_old_reward_opt")
}
func (dp *DynamicProperties) SetAllowOldRewardOpt(v bool) {
	boolSet(dp, "allow_old_reward_opt", v)
}

func (dp *DynamicProperties) CurrentCycleNumber() int64 {
	return dp.props["current_cycle_number"]
}
func (dp *DynamicProperties) SetCurrentCycleNumber(v int64) {
	dp.Set("current_cycle_number", v)
}

// NewRewardAlgorithmEffectiveCycle returns the cycle at which the VI-based
// reward algorithm takes over. Defaults to math.MaxInt64 (disabled) until
// the proposal activates; then set by proposal application to the current
// cycle + 1.
func (dp *DynamicProperties) NewRewardAlgorithmEffectiveCycle() int64 {
	return dp.props["new_reward_algorithm_effective_cycle"]
}
func (dp *DynamicProperties) SetNewRewardAlgorithmEffectiveCycle(v int64) {
	dp.Set("new_reward_algorithm_effective_cycle", v)
}

// UseNewRewardAlgorithm mirrors java-tron's useNewRewardAlgorithm:
// true once the effective cycle has been set (i.e., != MaxInt64).
func (dp *DynamicProperties) UseNewRewardAlgorithm() bool {
	return dp.NewRewardAlgorithmEffectiveCycle() != 9_223_372_036_854_775_807
}

// AllowBlackHoleOptimization mirrors java-tron's getAllowBlackHoleOptimization
// (DP key ALLOW_BLACKHOLE_OPTIMIZATION). Note: the /wallet/getchainparameters
// HTTP API still exposes this value under the legacy label
// "getAllowOptimizeBlackHole" for SDK compatibility (see Wallet.java:1354).
func (dp *DynamicProperties) AllowBlackHoleOptimization() bool {
	return boolGet(dp, "allow_blackhole_optimization")
}
func (dp *DynamicProperties) SetAllowBlackHoleOptimization(v bool) {
	boolSet(dp, "allow_blackhole_optimization", v)
}

func (dp *DynamicProperties) AllowOptimizedReturnValueOfChainId() bool {
	return boolGet(dp, "allow_optimized_return_value_of_chain_id")
}
func (dp *DynamicProperties) SetAllowOptimizedReturnValueOfChainId(v bool) {
	boolSet(dp, "allow_optimized_return_value_of_chain_id", v)
}

func (dp *DynamicProperties) AllowProtoFilterNum() int64 {
	return dp.props["allow_proto_filter_num"]
}
func (dp *DynamicProperties) SetAllowProtoFilterNum(v int64) {
	dp.Set("allow_proto_filter_num", v)
}

func (dp *DynamicProperties) AllowShieldedTrc20Transaction() bool {
	return boolGet(dp, "allow_shielded_trc20_transaction")
}
func (dp *DynamicProperties) SetAllowShieldedTrc20Transaction(v bool) {
	boolSet(dp, "allow_shielded_trc20_transaction", v)
}

func (dp *DynamicProperties) AllowStrictMath() bool {
	return boolGet(dp, "allow_strict_math")
}
func (dp *DynamicProperties) SetAllowStrictMath(v bool) {
	boolSet(dp, "allow_strict_math", v)
}

func (dp *DynamicProperties) AllowTransactionFeePool() bool {
	return boolGet(dp, "allow_transaction_fee_pool")
}
func (dp *DynamicProperties) SetAllowTransactionFeePool(v bool) {
	boolSet(dp, "allow_transaction_fee_pool", v)
}

func (dp *DynamicProperties) AllowTvmOsaka() bool {
	return boolGet(dp, "allow_tvm_osaka")
}
func (dp *DynamicProperties) SetAllowTvmOsaka(v bool) {
	boolSet(dp, "allow_tvm_osaka", v)
}

func (dp *DynamicProperties) AllowTvmSelfdestructRestriction() bool {
	return boolGet(dp, "allow_tvm_selfdestruct_restriction")
}
func (dp *DynamicProperties) SetAllowTvmSelfdestructRestriction(v bool) {
	boolSet(dp, "allow_tvm_selfdestruct_restriction", v)
}

func (dp *DynamicProperties) AllowTvmShanghai() bool {
	return boolGet(dp, "allow_tvm_shanghai")
}
func (dp *DynamicProperties) SetAllowTvmShanghai(v bool) {
	boolSet(dp, "allow_tvm_shanghai", v)
}

func (dp *DynamicProperties) AllowUpdateAccountName() bool {
	return boolGet(dp, "allow_update_account_name")
}
func (dp *DynamicProperties) SetAllowUpdateAccountName(v bool) {
	boolSet(dp, "allow_update_account_name", v)
}

func (dp *DynamicProperties) ConsensusLogicOptimization() bool {
	return boolGet(dp, "consensus_logic_optimization")
}
func (dp *DynamicProperties) SetConsensusLogicOptimization(v bool) {
	boolSet(dp, "consensus_logic_optimization", v)
}

// RemoveThePowerOfTheGr mirrors java-tron's getRemoveThePowerOfTheGr,
// which stores 0 (initial) or -1 (executed) — it is not a classic bool.
func (dp *DynamicProperties) RemoveThePowerOfTheGr() int64 {
	return dp.props["remove_the_power_of_the_gr"]
}
func (dp *DynamicProperties) SetRemoveThePowerOfTheGr(v int64) {
	dp.Set("remove_the_power_of_the_gr", v)
}

// M1.6: storage market — dormant on mainnet (feature never activated).
// Keys are initialized to java-tron defaults and never modified at runtime.

func (dp *DynamicProperties) TotalStoragePool() int64 { return dp.props["total_storage_pool"] }
func (dp *DynamicProperties) SetTotalStoragePool(v int64) {
	dp.Set("total_storage_pool", v)
}

func (dp *DynamicProperties) TotalStorageTax() int64 { return dp.props["total_storage_tax"] }
func (dp *DynamicProperties) SetTotalStorageTax(v int64) {
	dp.Set("total_storage_tax", v)
}

func (dp *DynamicProperties) TotalStorageReserved() int64 { return dp.props["total_storage_reserved"] }
func (dp *DynamicProperties) SetTotalStorageReserved(v int64) {
	dp.Set("total_storage_reserved", v)
}

func (dp *DynamicProperties) StorageExchangeTaxRate() int64 {
	return dp.props["storage_exchange_tax_rate"]
}
func (dp *DynamicProperties) SetStorageExchangeTaxRate(v int64) {
	dp.Set("storage_exchange_tax_rate", v)
}

// §1.6: freeze/supply/bandwidth/accounting accessors.

func (dp *DynamicProperties) MaxFrozenTime() int64 { return dp.props["max_frozen_time"] }
func (dp *DynamicProperties) SetMaxFrozenTime(v int64) {
	dp.Set("max_frozen_time", v)
}

func (dp *DynamicProperties) MinFrozenTime() int64 { return dp.props["min_frozen_time"] }
func (dp *DynamicProperties) SetMinFrozenTime(v int64) {
	dp.Set("min_frozen_time", v)
}

func (dp *DynamicProperties) MaxFrozenSupplyNumber() int64 {
	return dp.props["max_frozen_supply_number"]
}
func (dp *DynamicProperties) SetMaxFrozenSupplyNumber(v int64) {
	dp.Set("max_frozen_supply_number", v)
}

func (dp *DynamicProperties) MaxFrozenSupplyTime() int64 {
	return dp.props["max_frozen_supply_time"]
}
func (dp *DynamicProperties) SetMaxFrozenSupplyTime(v int64) {
	dp.Set("max_frozen_supply_time", v)
}

func (dp *DynamicProperties) MinFrozenSupplyTime() int64 {
	return dp.props["min_frozen_supply_time"]
}
func (dp *DynamicProperties) SetMinFrozenSupplyTime(v int64) {
	dp.Set("min_frozen_supply_time", v)
}

func (dp *DynamicProperties) WitnessAllowanceFrozenTime() int64 {
	return dp.props["witness_allowance_frozen_time"]
}
func (dp *DynamicProperties) SetWitnessAllowanceFrozenTime(v int64) {
	dp.Set("witness_allowance_frozen_time", v)
}

func (dp *DynamicProperties) OneDayNetLimit() int64 { return dp.props["one_day_net_limit"] }
func (dp *DynamicProperties) SetOneDayNetLimit(v int64) {
	dp.Set("one_day_net_limit", v)
}

func (dp *DynamicProperties) PublicNetLimit() int64 { return dp.props["public_net_limit"] }
func (dp *DynamicProperties) SetPublicNetLimit(v int64) {
	dp.Set("public_net_limit", v)
}

func (dp *DynamicProperties) PublicNetUsage() int64 { return dp.props["public_net_usage"] }
func (dp *DynamicProperties) SetPublicNetUsage(v int64) {
	dp.Set("public_net_usage", v)
}

func (dp *DynamicProperties) PublicNetTime() int64 { return dp.props["public_net_time"] }
func (dp *DynamicProperties) SetPublicNetTime(v int64) {
	dp.Set("public_net_time", v)
}

func (dp *DynamicProperties) TransactionFeePool() int64 { return dp.props["transaction_fee_pool"] }
func (dp *DynamicProperties) SetTransactionFeePool(v int64) {
	dp.Set("transaction_fee_pool", v)
}

func (dp *DynamicProperties) TotalTransactionCost() int64 {
	return dp.props["total_transaction_cost"]
}
func (dp *DynamicProperties) SetTotalTransactionCost(v int64) {
	dp.Set("total_transaction_cost", v)
}

func (dp *DynamicProperties) TotalCreateAccountCost() int64 {
	return dp.props["total_create_account_cost"]
}
func (dp *DynamicProperties) SetTotalCreateAccountCost(v int64) {
	dp.Set("total_create_account_cost", v)
}

func (dp *DynamicProperties) BlockFilledSlotsIndex() int64 {
	return dp.props["block_filled_slots_index"]
}
func (dp *DynamicProperties) SetBlockFilledSlotsIndex(v int64) {
	dp.Set("block_filled_slots_index", v)
}

func (dp *DynamicProperties) VersionNumber() int64 { return dp.props["version_number"] }
func (dp *DynamicProperties) SetVersionNumber(v int64) {
	dp.Set("version_number", v)
}
