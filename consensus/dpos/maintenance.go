package dpos

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus"
	"github.com/tronprotocol/go-tron/params"
)

// DoMaintenance performs maintenance period operations:
// 1. Distribute standby allowance to top-127 witnesses
// 2. Compute and set next maintenance time
func DoMaintenance(chain consensus.ChainHeaderWriter, blockTime int64, allWitnesses []WitnessVote) {
	sorted := SortWitnessesByVotes(allWitnesses)

	standbyCount := params.WitnessStandbyLength
	if len(sorted) < standbyCount {
		standbyCount = len(sorted)
	}
	if standbyCount > 0 {
		allowancePerWitness := chain.WitnessStandbyAllowance() / int64(standbyCount)
		for i := 0; i < standbyCount; i++ {
			chain.AddAllowance(sorted[i].Address, allowancePerWitness)
		}
	}

	nextMaint := calcNextMaintenanceTime(blockTime, chain.NextMaintenanceTime(), chain.MaintenanceTimeInterval())
	chain.SetNextMaintenanceTime(nextMaint)
}

// calcNextMaintenanceTime computes the next maintenance timestamp after blockTime.
func calcNextMaintenanceTime(blockTime, currentMaint, interval int64) int64 {
	if interval <= 0 {
		return currentMaint
	}
	round := (blockTime - currentMaint) / interval
	return currentMaint + (round+1)*interval
}

// SelectActiveWitnesses returns the top N witnesses by vote count,
// using deterministic tiebreaking via SortWitnessesByVotes.
func SelectActiveWitnesses(allWitnesses []WitnessVote) []common.Address {
	sorted := SortWitnessesByVotes(allWitnesses)
	count := params.MaxActiveWitnessNum
	if len(sorted) < count {
		count = len(sorted)
	}
	result := make([]common.Address, count)
	for i := 0; i < count; i++ {
		result[i] = sorted[i].Address
	}
	return result
}
