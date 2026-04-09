package dpos

import (
	"sort"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus"
	"github.com/tronprotocol/go-tron/params"
)

func DoMaintenance(chain consensus.ChainHeaderWriter, allWitnesses []WitnessVote) {
	sort.Slice(allWitnesses, func(i, j int) bool {
		return allWitnesses[i].Votes > allWitnesses[j].Votes
	})

	standbyCount := params.WitnessStandbyLength
	if len(allWitnesses) < standbyCount {
		standbyCount = len(allWitnesses)
	}
	if standbyCount > 0 {
		allowancePerWitness := chain.WitnessStandbyAllowance() / int64(standbyCount)
		for i := 0; i < standbyCount; i++ {
			chain.AddAllowance(allWitnesses[i].Address, allowancePerWitness)
		}
	}

	nextMaint := chain.MaintenanceTimeInterval()
	chain.SetNextMaintenanceTime(nextMaint)
}

func SelectActiveWitnesses(allWitnesses []WitnessVote) []common.Address {
	sort.Slice(allWitnesses, func(i, j int) bool {
		return allWitnesses[i].Votes > allWitnesses[j].Votes
	})
	count := params.MaxActiveWitnessNum
	if len(allWitnesses) < count {
		count = len(allWitnesses)
	}
	result := make([]common.Address, count)
	for i := 0; i < count; i++ {
		result[i] = allWitnesses[i].Address
	}
	return result
}
