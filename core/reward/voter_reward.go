// Package reward holds pure reward-math helpers shared between the block-
// processing path (core package) and the withdraw actuator (actuator
// package). No core/actuator dependencies — only state plus a narrow reward
// snapshot reader.
package reward

import (
	"math/big"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
)

// DecimalOfViReward mirrors java-tron DelegationStore.DECIMAL_OF_VI_REWARD:
// 10^18. The VI metric tracks cumulative (reward × 10^18 / voteCount) per
// witness; voter shares divide by this to get the per-voter reward.
var DecimalOfViReward = new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)

// VoteEntry is a single {witness, voteCount} pair on a voter's ballot.
type VoteEntry struct {
	Witness tcommon.Address
	Count   int64
}

// SnapshotReader is the minimal rooted reward-v2 read surface needed for
// reward settlement. StateDB implements it via the SystemReward account domain.
type SnapshotReader interface {
	ReadCycleReward(cycle int64, addr []byte) int64
	ReadCycleVote(cycle int64, addr []byte) int64
	ReadWitnessVI(cycle int64, addr []byte) *big.Int
}

// ComputeVoterReward computes the pending reward for a voter across cycles
// [beginCycle, endCycle). Uses the old pro-rata algorithm for cycles before
// new_reward_algorithm_effective_cycle and the VI-based algorithm after.
// Mirrors java-tron's MortgageService.computeReward.
func ComputeVoterReward(store SnapshotReader, dp *state.DynamicProperties, votes []VoteEntry, beginCycle, endCycle int64) int64 {
	if beginCycle >= endCycle {
		return 0
	}
	var reward int64
	newAlgoCycle := dp.NewRewardAlgorithmEffectiveCycle()

	if beginCycle < newAlgoCycle {
		oldEnd := endCycle
		if newAlgoCycle < oldEnd {
			oldEnd = newAlgoCycle
		}
		reward += oldRewardSum(store, votes, beginCycle, oldEnd)
		beginCycle = oldEnd
	}
	if beginCycle < endCycle {
		for _, v := range votes {
			beginVi := store.ReadWitnessVI(beginCycle-1, v.Witness.Bytes())
			endVi := store.ReadWitnessVI(endCycle-1, v.Witness.Bytes())
			delta := new(big.Int).Sub(endVi, beginVi)
			if delta.Sign() <= 0 {
				continue
			}
			share := new(big.Int).Mul(delta, big.NewInt(v.Count))
			share.Quo(share, DecimalOfViReward)
			reward += share.Int64()
		}
	}
	return reward
}

// oldRewardSum sums per-cycle pro-rata voter reward using the pre-new
// algorithm path. Mirrors java-tron's MortgageService.computeReward(cycle, votes).
func oldRewardSum(store SnapshotReader, votes []VoteEntry, begin, end int64) int64 {
	var reward int64
	for cycle := begin; cycle < end; cycle++ {
		for _, v := range votes {
			totalReward := store.ReadCycleReward(cycle, v.Witness.Bytes())
			if totalReward <= 0 {
				continue
			}
			totalVote := store.ReadCycleVote(cycle, v.Witness.Bytes())
			if totalVote == rawdb.RewardRemark || totalVote == 0 {
				continue
			}
			voteRate := float64(v.Count) / float64(totalVote)
			reward += int64(voteRate * float64(totalReward))
		}
	}
	return reward
}
