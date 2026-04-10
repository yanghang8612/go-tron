package core

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
)

// ProcessProposals checks all pending proposals and approves or cancels them
// based on the approval count vs active SR count.
func ProcessProposals(db ethdb.KeyValueStore, dynProps *state.DynamicProperties, activeCount int, maintenanceTime int64) {
	ids := rawdb.ReadProposalIndex(db)
	for _, id := range ids {
		p := rawdb.ReadProposal(db, id)
		if p == nil || p.State != rawdb.ProposalStatePending {
			continue
		}
		if p.ExpirationTime > maintenanceTime {
			continue // not yet expired
		}

		approvalCount := len(p.Approvals)
		// 70% threshold: approvals * 10 >= activeCount * 7
		if approvalCount*10 >= activeCount*7 {
			// Apply parameters
			for _, k := range sortedKeys(p.Parameters) {
				name := paramIDToName(k)
				if name != "" {
					dynProps.Set(name, p.Parameters[k])
				}
			}
			p.State = rawdb.ProposalStateApproved
		} else {
			p.State = rawdb.ProposalStateCanceled
		}
		rawdb.WriteProposal(db, id, p)
	}
}

// paramIDToName maps a TRON proposal parameter ID to its DynProps key name.
func paramIDToName(id int64) string {
	mapping := map[int64]string{
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
		15: "max_cpu_time_of_one_tx",
		19: "total_energy_current_limit",
		22: "total_net_limit",
		27: "unfreeze_delay_days",
		65: "free_net_limit",
	}
	if name, ok := mapping[id]; ok {
		return name
	}
	return ""
}

func sortedKeys(m map[int64]int64) []int64 {
	keys := make([]int64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// Simple insertion sort for small maps
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}
