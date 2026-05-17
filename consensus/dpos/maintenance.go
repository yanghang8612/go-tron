package dpos

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus"
	"github.com/tronprotocol/go-tron/params"
)

// DoMaintenance performs legacy maintenance-time operations:
//  1. Distribute witness_standby_allowance pro-rata by votes to top-127
//     witnesses (only when change_delegation is OFF — the per-block
//     payStandbyWitness flow replaces this once the new path activates).
//  2. Compute and set next maintenance time.
//
// The M1.5 new-reward path (VI accumulation + cycle rollover) is handled
// separately by core.applyRewardMaintenance after this call returns.
func DoMaintenance(chain consensus.ChainHeaderWriter, blockTime int64, allWitnesses []WitnessVote) {
	tryRemoveThePowerOfTheGr(chain, allWitnesses)

	sorted := SortWitnessesByVotes(allWitnesses)

	if !chain.ChangeDelegation() {
		distributeLegacyStandby(chain, sorted)
	}

	nextMaint := CalcNextMaintenanceTime(blockTime, chain.NextMaintenanceTime(), chain.MaintenanceTimeInterval())
	chain.SetNextMaintenanceTime(nextMaint)
}

func TryRemoveThePowerOfTheGr(chain consensus.ChainHeaderWriter, allWitnesses []WitnessVote) {
	tryRemoveThePowerOfTheGr(chain, allWitnesses)
}

func DistributeLegacyStandby(chain consensus.ChainHeaderWriter, sorted []WitnessVote) {
	distributeLegacyStandby(chain, sorted)
}

// tryRemoveThePowerOfTheGr ports java-tron
// MaintenanceManager.tryRemoveThePowerOfTheGr. When the
// REMOVE_THE_POWER_OF_THE_GR DP flag is set to 1 (by a successful proposal
// activation in core.applyProposalSideEffects), this strips each genesis
// representative's *initial* vote count from their current vote total and
// flips the flag to -1 so it never fires again. Subtraction is applied to
// both the persistent witness store (via the chain adapter) and the
// in-memory `allWitnesses` slice — the latter so SelectActiveWitnesses and
// distributeLegacyStandby downstream see the stripped totals, matching
// java-tron's `tryRemove → countVote → updateWitness` ordering.
func tryRemoveThePowerOfTheGr(chain consensus.ChainHeaderWriter, allWitnesses []WitnessVote) {
	if chain.RemoveThePowerOfTheGr() != 1 {
		return
	}
	for _, gw := range chain.GenesisWitnesses() {
		w := chain.GetWitness(gw.Address)
		if w == nil {
			continue
		}
		chain.AddWitnessVoteCount(gw.Address, -gw.VoteCount)
		for i := range allWitnesses {
			if allWitnesses[i].Address == gw.Address {
				allWitnesses[i].Votes -= gw.VoteCount
				break
			}
		}
	}
	chain.SetRemoveThePowerOfTheGr(-1)
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

// CalcNextMaintenanceTime computes the next maintenance timestamp after blockTime.
// Mirrors java-tron DynamicPropertiesStore.updateNextMaintenanceTime: the result
// preserves `currentMaint mod interval`, so the maintenance grid alignment is
// stable across cycles once seeded at genesis.
func CalcNextMaintenanceTime(blockTime, currentMaint, interval int64) int64 {
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
