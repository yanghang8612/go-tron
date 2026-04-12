package core

import (
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/hardfork"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
)

// ProcessProposals checks all pending proposals and approves or cancels them
// based on the approval count vs active SR count.
// activeWitnesses is the current active super-representative set; only approvals
// from current witnesses are counted (matches java-tron's hasMostApprovals logic).
func ProcessProposals(db ethdb.KeyValueStore, dynProps *state.DynamicProperties, activeWitnesses []tcommon.Address, maintenanceTime int64) error {
	activeCount := len(activeWitnesses)
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

		// Count only approvals from currently-active witnesses.
		activeApprovals := 0
		for _, approval := range p.Approvals {
			for _, w := range activeWitnesses {
				if approval == w {
					activeApprovals++
					break
				}
			}
		}
		// 70% threshold: matches java-tron's `count >= activeWitnesses.size() * 7 / 10`
		if activeApprovals >= activeCount*7/10 {
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
		if err := rawdb.WriteProposal(db, id, p); err != nil {
			return err
		}
	}
	return nil
}

// paramIDToName maps a TRON proposal parameter ID to its DynProps key name.
func paramIDToName(id int64) string {
	return hardfork.ProposalParamKey(id)
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
