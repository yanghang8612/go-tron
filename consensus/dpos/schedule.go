package dpos

import (
	"sort"

	"github.com/tronprotocol/go-tron/common"
)

type WitnessVote struct {
	Address common.Address
	Votes   int64
}

func GetScheduledWitness(slot int64, headTimestamp, genesisTime int64, activeWitnesses []common.Address, isMaintenance bool, maintenanceSkipSlots int64) common.Address {
	if len(activeWitnesses) == 0 {
		return common.Address{}
	}
	currentAbsSlot := AbsoluteSlot(headTimestamp, genesisTime) + slot
	if isMaintenance {
		currentAbsSlot += maintenanceSkipSlots
	}
	idx := WitnessIndex(currentAbsSlot, len(activeWitnesses))
	return activeWitnesses[idx]
}

func SortWitnessesByVotes(witnesses []WitnessVote) []WitnessVote {
	sorted := make([]WitnessVote, len(witnesses))
	copy(sorted, witnesses)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Votes != sorted[j].Votes {
			return sorted[i].Votes > sorted[j].Votes
		}
		return sorted[i].Address.Hex() > sorted[j].Address.Hex()
	})
	return sorted
}
