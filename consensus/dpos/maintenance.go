package dpos

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus"
	"github.com/tronprotocol/go-tron/params"
)

// DoMaintenance performs legacy maintenance-time operations:
// 1. Distribute witness_standby_allowance pro-rata by votes to top-127
//    witnesses (only when change_delegation is OFF — the per-block
//    payStandbyWitness flow replaces this once the new path activates).
// 2. Compute and set next maintenance time.
//
// The M1.5 new-reward path (VI accumulation + cycle rollover) is handled
// separately by core.applyRewardMaintenance after this call returns.
func DoMaintenance(chain consensus.ChainHeaderWriter, blockTime int64, allWitnesses []WitnessVote) {
	sorted := SortWitnessesByVotes(allWitnesses)

	if !chain.ChangeDelegation() {
		distributeLegacyStandby(chain, sorted)
	}

	nextMaint := calcNextMaintenanceTime(blockTime, chain.NextMaintenanceTime(), chain.MaintenanceTimeInterval())
	chain.SetNextMaintenanceTime(nextMaint)
}

// distributeLegacyStandby mirrors java-tron's IncentiveManager.reward:
// witness_standby_allowance is split pro-rata by vote share among the top
// WitnessStandbyLength witnesses. Float math mirrors the upstream
// expression tree for byte-for-byte parity.
func distributeLegacyStandby(chain consensus.ChainHeaderWriter, sorted []WitnessVote) {
	standbyCount := params.WitnessStandbyLength
	if len(sorted) < standbyCount {
		standbyCount = len(sorted)
	}
	if standbyCount <= 0 {
		return
	}
	standby := sorted[:standbyCount]

	var voteSum int64
	for _, w := range standby {
		voteSum += w.Votes
	}
	if voteSum <= 0 {
		return
	}
	totalPay := chain.WitnessStandbyAllowance()
	if totalPay <= 0 {
		return
	}
	for _, w := range standby {
		pay := int64(float64(w.Votes) * (float64(totalPay) / float64(voteSum)))
		if pay > 0 {
			chain.AddAllowance(w.Address, pay)
		}
	}
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
