package actuator

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/reward"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"google.golang.org/protobuf/proto"

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

	currentVotes := voteEntriesFromAccount(acct)
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
	if snap := marshalAccountVote(acct); snap != nil {
		_ = statedb.WriteCycleAccountVote(endCycle, addr.Bytes(), snap)
	}
}

// queryReward returns the pending reward a voter would settle on withdraw,
// without mutating state. Mirrors MortgageService.queryReward.
func queryReward(db BufferedKVStore, statedb *state.StateDB, dp *state.DynamicProperties, addr common.Address) int64 {
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

// voteEntriesFromAccount converts an account's protobuf vote list into
// reward.VoteEntry slice.
func voteEntriesFromAccount(acct *types.Account) []reward.VoteEntry {
	if acct == nil {
		return nil
	}
	pb := acct.Votes()
	out := make([]reward.VoteEntry, 0, len(pb))
	for _, v := range pb {
		out = append(out, reward.VoteEntry{
			Witness: common.BytesToAddress(v.VoteAddress),
			Count:   v.VoteCount,
		})
	}
	return out
}

// readSnapshotVotes loads a voter's cycle-snapshot vote list (written via
// WriteCycleAccountVote) into reward.VoteEntry slice.
func readSnapshotVotes(statedb *state.StateDB, cycle int64, addr common.Address) []reward.VoteEntry {
	raw := statedb.ReadCycleAccountVote(cycle, addr.Bytes())
	if len(raw) == 0 {
		return nil
	}
	snap := &corepb.Account{}
	if err := proto.Unmarshal(raw, snap); err != nil {
		return nil
	}
	out := make([]reward.VoteEntry, 0, len(snap.Votes))
	for _, v := range snap.Votes {
		out = append(out, reward.VoteEntry{
			Witness: common.BytesToAddress(v.VoteAddress),
			Count:   v.VoteCount,
		})
	}
	return out
}

// marshalAccountVote serializes a voter's account (votes + allowance) for
// later recall during the beginCycle+1==endCycle edge-case settlement.
// Mirrors java-tron's setAccountVote(cycle, addr, account).
func marshalAccountVote(acct *types.Account) []byte {
	if acct == nil {
		return nil
	}
	raw, err := proto.Marshal(acct.Proto())
	if err != nil {
		return nil
	}
	return raw
}
