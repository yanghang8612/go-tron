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
	"witness_pay_per_block":                     16000000,
	"witness_standby_allowance":                 115200000000,
	"create_new_account_fee_in_system_contract": 0,
	"create_new_account_bandwidth_rate":         1,
	"energy_fee":                                100,
	"max_cpu_time_of_one_tx":                    80,
	"total_energy_current_limit":                50000000000,
	"total_net_limit":                           43200000000,
	"unfreeze_delay_days":                       14,
	"latest_block_header_timestamp":             0,
	"latest_block_header_number":                0,
	"latest_solidified_block_num":               0,
	"next_maintenance_time":                     0,
	"allow_new_resource_model":                  0,
	"free_net_limit":                            1500,
	"next_proposal_id":                          0,
	"next_token_id":                             1_000_001,
	"allow_same_token_name":                     0,
	"allow_delegate_resource":                   0,
	"allow_adaptive_energy_limit":               0,
	"allow_multi_sign":                          0,
	"allow_change_delegation":                   0,
	"allow_tvm_transfer_trc10":                  0,
	"allow_tvm_constantinople":                  0,
	"allow_tvm_solidity059":                     0,
	"allow_tvm_istanbul":                        0,
	"allow_market_transaction":                  0,
	"allow_tvm_freeze":                          0,
	"allow_tvm_shielded_token":                  0,
	"allow_tvm_vote":                            0,
	"allow_account_history":                     0,
	"allow_pbft":                                0,
	"allow_staking_v2":                          0,
	"allow_tvm_london":                          0,
	"allow_tvm_compatibility":                   0,
	"allow_dynamic_energy":                      0,
	"allow_tvm_big_integer":                     0,
	"allow_tvm_blob":                            0,
	"allow_tvm_cancun":                          0,
	"allow_energy_adjustment":                   0,
	"allow_tvm_solidity058":                     0,
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

func (dp *DynamicProperties) TotalNetLimit() int64 {
	return dp.props["total_net_limit"]
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

func (dp *DynamicProperties) AllowAdaptiveEnergyLimit() bool {
	return dp.props["allow_adaptive_energy_limit"] != 0
}
func (dp *DynamicProperties) SetAllowAdaptiveEnergyLimit(v bool) {
	if v {
		dp.Set("allow_adaptive_energy_limit", 1)
	} else {
		dp.Set("allow_adaptive_energy_limit", 0)
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

func (dp *DynamicProperties) AllowChangeDelegation() bool {
	return dp.props["allow_change_delegation"] != 0
}
func (dp *DynamicProperties) SetAllowChangeDelegation(v bool) {
	if v {
		dp.Set("allow_change_delegation", 1)
	} else {
		dp.Set("allow_change_delegation", 0)
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

func (dp *DynamicProperties) AllowTvmShieldedToken() bool {
	return dp.props["allow_tvm_shielded_token"] != 0
}
func (dp *DynamicProperties) SetAllowTvmShieldedToken(v bool) {
	if v {
		dp.Set("allow_tvm_shielded_token", 1)
	} else {
		dp.Set("allow_tvm_shielded_token", 0)
	}
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

func (dp *DynamicProperties) AllowAccountHistory() bool {
	return dp.props["allow_account_history"] != 0
}
func (dp *DynamicProperties) SetAllowAccountHistory(v bool) {
	if v {
		dp.Set("allow_account_history", 1)
	} else {
		dp.Set("allow_account_history", 0)
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

func (dp *DynamicProperties) AllowStakingV2() bool {
	return dp.props["allow_staking_v2"] != 0
}
func (dp *DynamicProperties) SetAllowStakingV2(v bool) {
	if v {
		dp.Set("allow_staking_v2", 1)
	} else {
		dp.Set("allow_staking_v2", 0)
	}
}

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

func (dp *DynamicProperties) AllowTvmCompatibility() bool {
	return dp.props["allow_tvm_compatibility"] != 0
}
func (dp *DynamicProperties) SetAllowTvmCompatibility(v bool) {
	if v {
		dp.Set("allow_tvm_compatibility", 1)
	} else {
		dp.Set("allow_tvm_compatibility", 0)
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

func (dp *DynamicProperties) AllowTvmBigInteger() bool {
	return dp.props["allow_tvm_big_integer"] != 0
}
func (dp *DynamicProperties) SetAllowTvmBigInteger(v bool) {
	if v {
		dp.Set("allow_tvm_big_integer", 1)
	} else {
		dp.Set("allow_tvm_big_integer", 0)
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

// AssetIssueFee returns the fee (in SUN) required to issue a TRC10 token.
func (dp *DynamicProperties) AssetIssueFee() int64 { return dp.props["asset_issue_fee"] }

// All returns a read-only copy of all dynamic properties.
func (dp *DynamicProperties) All() map[string]int64 {
	result := make(map[string]int64, len(dp.props))
	for k, v := range dp.props {
		result[k] = v
	}
	return result
}
