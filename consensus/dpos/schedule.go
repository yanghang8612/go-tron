package dpos

import (
	"sort"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/params"
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
	return SortWitnessesByVotesWithOptimization(witnesses, true)
}

func SortWitnessesByVotesWithOptimization(witnesses []WitnessVote, sortOpt bool) []WitnessVote {
	sorted := make([]WitnessVote, len(witnesses))
	copy(sorted, witnesses)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Votes != sorted[j].Votes {
			return sorted[i].Votes > sorted[j].Votes
		}
		if !sortOpt {
			return javaByteStringHash(sorted[i].Address.Bytes()) > javaByteStringHash(sorted[j].Address.Bytes())
		}
		return sorted[i].Address.Hex() > sorted[j].Address.Hex()
	})
	return sorted
}

func SelectActiveWitnesses(allWitnesses []WitnessVote) []common.Address {
	return SelectActiveWitnessesWithOptimization(allWitnesses, true)
}

func SelectActiveWitnessesWithOptimization(allWitnesses []WitnessVote, sortOpt bool) []common.Address {
	sorted := SortWitnessesByVotesWithOptimization(allWitnesses, sortOpt)
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

// javaByteStringHash mirrors com.google.protobuf.ByteString.hashCode for a
// LiteralByteString: seed with size, multiply by 31, and add each signed byte.
func javaByteStringHash(data []byte) int32 {
	h := int32(len(data))
	for _, b := range data {
		h = h*31 + int32(int8(b))
	}
	if h == 0 {
		h = 1
	}
	return h
}
