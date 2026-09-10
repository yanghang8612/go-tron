package actuator

// These copies freeze the pre-optimization reward paths as independent oracles
// for selective account hydration. Keep the eager GetAccount and snapshot order.
import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/reward"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
)

func voteEntriesFromAccount(acct *types.Account) []reward.VoteEntry {
	if acct == nil {
		return nil
	}
	out := make([]reward.VoteEntry, 0, len(acct.Votes()))
	for _, vote := range acct.Votes() {
		out = append(out, reward.VoteEntry{Witness: common.BytesToAddress(vote.VoteAddress), Count: vote.VoteCount})
	}
	return out
}

func withdrawRewardBeforeSelectiveAccountRead(db BufferedKVStore, statedb *state.StateDB, dp *state.DynamicProperties, addr common.Address) {
	if dp == nil || statedb == nil || !dp.ChangeDelegation() {
		return
	}

	currentCycle := dp.CurrentCycleNumber()
	beginCycle := statedb.ReadBeginCycle(addr.Bytes())
	endCycle := statedb.ReadEndCycle(addr.Bytes())
	acct := statedb.GetAccount(addr)
	if acct == nil || beginCycle > currentCycle {
		return
	}

	// Current-cycle edge: a snapshot exists for beginCycle — voter's vote
	// was already counted there — skip.
	if beginCycle == currentCycle {
		if snap := statedb.ReadCycleAccountVote(beginCycle, addr.Bytes()); snap != nil {
			return
		}
	}

	// java-tron's accountStore.get returns a detached AccountCapsule. Later
	// adjustAllowance calls read and write a different capsule, so the
	// account-vote row saved at the end contains the allowance from before this
	// settlement. StateDB returns a mutable cached account instead; serialize it
	// now so AddAllowance below cannot leak the newly paid reward into the
	// historical snapshot.
	currentVotes := voteEntriesFromAccount(acct)
	accountVoteSnapshot := marshalAccountVote(acct)

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
	if accountVoteSnapshot != nil {
		_ = statedb.WriteCycleAccountVote(endCycle, addr.Bytes(), accountVoteSnapshot)
	}
}

// queryReward returns the pending reward a voter would settle on withdraw,
// without mutating state. Mirrors MortgageService.queryReward.
func queryRewardBeforeSelectiveAccountRead(db BufferedKVStore, statedb *state.StateDB, dp *state.DynamicProperties, addr common.Address) int64 {
	if dp == nil || statedb == nil || !dp.ChangeDelegation() {
		return 0
	}
	acct := statedb.GetAccount(addr)
	if acct == nil {
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

	currentVotes := voteEntriesFromAccount(acct)
	if len(currentVotes) == 0 {
		return pending + allowance
	}
	if beginCycle < endCycle {
		pending += reward.ComputeVoterReward(statedb, dp, currentVotes, beginCycle, endCycle)
	}
	return pending + allowance
}
