package core

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/forks"
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
		if activeCount == 0 {
			continue // cannot compute threshold with zero active witnesses
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
	return forks.ProposalParamKey(id)
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
