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

// All returns a read-only copy of all dynamic properties.
func (dp *DynamicProperties) All() map[string]int64 {
	result := make(map[string]int64, len(dp.props))
	for k, v := range dp.props {
		result[k] = v
	}
	return result
}
