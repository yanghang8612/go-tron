package actuator

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/reward"
	"github.com/tronprotocol/go-tron/core/state"

	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// withdrawReward settles a voter's pending cycle rewards into allowance,
// then advances the voter's beginCycle / endCycle cursors.
//
// Mirrors java-tron MortgageService.withdrawReward:
//  1. Gated on change_delegation (only runs once the new path is on).
//  2. Handles the "latest finalized cycle" edge case where beginCycle+1 ==
//     endCycle and a stale accountVote snapshot must be settled first.
//  3. Computes the remainder across (beginCycle, currentCycle) with
//     whichever algorithm applies per cycle.
//  4. Writes the new cursors and snapshots the current account-vote for
//     the next withdrawal.
func withdrawReward(db BufferedKVStore, statedb *state.StateDB, dp *state.DynamicProperties, addr common.Address) {
	if dp == nil || statedb == nil || !dp.ChangeDelegation() {
		return
	}

	currentCycle := dp.CurrentCycleNumber()
	beginCycle := statedb.ReadBeginCycle(addr.Bytes())
	endCycle := statedb.ReadEndCycle(addr.Bytes())
	if beginCycle > currentCycle || !statedb.Exist(addr) {
		return
	}

	// Current-cycle edge: a snapshot exists for beginCycle — voter's vote
	// was already counted there — skip.
	if beginCycle == currentCycle {
		if _, present := statedb.ReadCycleAccountVotes(beginCycle, addr.Bytes()); present {
			return
		}
	}

	currentVoteList := statedb.GetVotes(addr)
	currentVotes := voteEntriesFromVotes(currentVoteList)
	if statedb.Error() != nil {
		return
	}
	// V4 preserves the exact pre-settlement ballot only. Reward adjustments do
	// not mutate Votes; unrelated assets/permissions/allowance are not snapshots.

	// Finalize the most-recent recorded-but-not-yet-settled cycle.
	if beginCycle+1 == endCycle && beginCycle < currentCycle {
		if votes := readSnapshotVotes(statedb, beginCycle, addr); len(votes) > 0 {
			paid := reward.ComputeVoterReward(statedb, dp, votes, beginCycle, endCycle)
			if paid > 0 {
				statedb.AddAllowance(addr, paid)
			}
		}
		beginCycle++
	}

	endCycle = currentCycle

	if len(currentVotes) == 0 {
		_ = statedb.WriteBeginCycle(addr.Bytes(), endCycle+1)
		return
	}

	if beginCycle < endCycle {
		paid := reward.ComputeVoterReward(statedb, dp, currentVotes, beginCycle, endCycle)
		if paid > 0 {
			statedb.AddAllowance(addr, paid)
		}
	}

	_ = statedb.WriteBeginCycle(addr.Bytes(), endCycle)
	_ = statedb.WriteEndCycle(addr.Bytes(), endCycle+1)
	_ = statedb.WriteCycleAccountVotes(endCycle, addr.Bytes(), currentVoteList)
}

// queryReward returns the pending reward a voter would settle on withdraw,
// without mutating state. Mirrors MortgageService.queryReward.
func queryReward(db BufferedKVStore, statedb *state.StateDB, dp *state.DynamicProperties, addr common.Address) int64 {
	if dp == nil || statedb == nil || !dp.ChangeDelegation() {
		return 0
	}
	if !statedb.Exist(addr) {
		return 0
	}
	allowance := statedb.GetAllowance(addr)

	currentCycle := dp.CurrentCycleNumber()
	beginCycle := statedb.ReadBeginCycle(addr.Bytes())
	endCycle := statedb.ReadEndCycle(addr.Bytes())
	if beginCycle > currentCycle {
		return allowance
	}

	var pending int64
	if beginCycle+1 == endCycle && beginCycle < currentCycle {
		if votes := readSnapshotVotes(statedb, beginCycle, addr); len(votes) > 0 {
			pending += reward.ComputeVoterReward(statedb, dp, votes, beginCycle, endCycle)
		}
		beginCycle++
	}
	endCycle = currentCycle

	currentVotes := voteEntriesFromVotes(statedb.GetVotes(addr))
	if statedb.Error() != nil {
		return 0
	}
	if len(currentVotes) == 0 {
		return pending + allowance
	}
	if beginCycle < endCycle {
		pending += reward.ComputeVoterReward(statedb, dp, currentVotes, beginCycle, endCycle)
	}
	return pending + allowance
}

// voteEntriesFromVotes converts a protobuf vote list into reward.VoteEntry slice.
func voteEntriesFromVotes(pb []*corepb.Vote) []reward.VoteEntry {
	out := make([]reward.VoteEntry, 0, len(pb))
	for _, v := range pb {
		out = append(out, reward.VoteEntry{
			Witness: common.BytesToAddress(v.VoteAddress),
			Count:   v.VoteCount,
		})
	}
	return out
}

// readSnapshotVotes loads the typed cycle snapshot without a full Account
// protobuf encode/decode round trip. Presence is handled independently above.
func readSnapshotVotes(statedb *state.StateDB, cycle int64, addr common.Address) []reward.VoteEntry {
	votes, present := statedb.ReadCycleAccountVotes(cycle, addr.Bytes())
	if !present {
		return nil
	}
	return voteEntriesFromVotes(votes)
}
