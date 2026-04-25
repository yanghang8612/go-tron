package core

import (
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
)

const version3_6_5 int32 = 9

// ProcessProposals checks all pending proposals and approves or cancels them
// based on the approval count vs active SR count.
// activeWitnesses is the current active super-representative set; only approvals
// from current witnesses are counted (matches java-tron's hasMostApprovals logic).
func ProcessProposals(db ethdb.KeyValueStore, dynProps *state.DynamicProperties, activeWitnesses []tcommon.Address, maintenanceTime int64, fc *forks.ForkController) error {
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
			applyProposalSideEffects(p, dynProps, fc, maintenanceTime)
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
	return forks.ProposalParamKey(id)
}

// applyProposalSideEffects handles java-tron ProposalService-style
// activation hooks that go beyond setting a single DP key.
func applyProposalSideEffects(p *rawdb.Proposal, dynProps *state.DynamicProperties, fc *forks.ForkController, maintenanceTime int64) {
	for paramID, value := range p.Parameters {
		switch paramID {
		case 21: // ALLOW_ADAPTIVE_ENERGY
			if fc != nil && fc.Pass(version3_6_5, maintenanceTime, dynProps.MaintenanceTimeInterval()) {
				dynProps.SetAdaptiveResourceLimitTargetRatio(2880)
				dynProps.SetAdaptiveResourceLimitMultiplier(50)
				totalEnergyLimit := dynProps.TotalEnergyLimit()
				dynProps.SetTotalEnergyTargetLimit(totalEnergyLimit / 2880)
			}
		case 67: // ALLOW_NEW_REWARD — set effective cycle so VI path starts
			if value != 0 {
				// Mirrors java-tron ProposalService: saves effective cycle
				// as currentCycleNumber+1 so the new-algorithm window begins
				// at the NEXT maintenance boundary.
				dynProps.SetNewRewardAlgorithmEffectiveCycle(dynProps.CurrentCycleNumber() + 1)
			}
		}
	}
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
