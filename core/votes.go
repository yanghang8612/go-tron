package core

import (
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
)

// pendingVoteDeltas reads the rooted VotesStore (WitnessVoteState KV) on statedb
// and nets each voter's old/new vote lists into per-witness deltas. Reading
// through statedb means deltas reflect any same-block actuator/TVM votes staged
// in the shared overlay.
func pendingVoteDeltas(statedb *state.StateDB) (map[tcommon.Address]int64, bool) {
	deltas, ok, _ := pendingVoteDeltasStrict(statedb)
	return deltas, ok
}

func pendingVoteDeltasStrict(statedb *state.StateDB) (map[tcommon.Address]int64, bool, error) {
	voters, _, err := statedb.ReadVotesIndexStrict()
	if err != nil || len(voters) == 0 {
		return nil, false, err
	}
	return pendingVoteDeltasForVoters(statedb, voters)
}

func pendingVoteDeltasForVoters(statedb *state.StateDB, voters []tcommon.Address) (map[tcommon.Address]int64, bool, error) {
	deltas := make(map[tcommon.Address]int64)
	hasRecords := false
	for _, voter := range voters {
		votes, _, err := statedb.ReadVotesStrict(voter)
		if err != nil {
			return nil, false, fmt.Errorf("read pending votes for %s: %w", voter.Hex(), err)
		}
		if votes == nil {
			continue
		}
		for _, vote := range votes.OldVotes {
			if vote == nil {
				continue
			}
			addr := tcommon.BytesToAddress(vote.VoteAddress)
			deltas[addr] -= vote.VoteCount
		}
		for _, vote := range votes.NewVotes {
			if vote == nil {
				continue
			}
			addr := tcommon.BytesToAddress(vote.VoteAddress)
			deltas[addr] += vote.VoteCount
		}
		hasRecords = true
	}
	return deltas, hasRecords, nil
}

// applyPendingVotes drains java-tron-style VotesStore records into the
// WitnessStore view at maintenance time. Each record contains the vote list at
// the start of the epoch plus the voter's latest vote list; the net deltas are
// applied once and the rooted pending store is cleared. Both the records and
// the clear happen on statedb, so the drain rewinds with the full state root and
// observes same-block actuator/TVM writes through the shared overlay.
func applyPendingVotes(statedb *state.StateDB) (bool, error) {
	voters, _, err := statedb.ReadVotesIndexStrict()
	if err != nil || len(voters) == 0 {
		return false, err
	}
	deltas, applied, err := pendingVoteDeltasForVoters(statedb, voters)
	if err != nil {
		return false, err
	}
	for _, voter := range voters {
		if err := statedb.DeleteVotes(voter); err != nil {
			return false, fmt.Errorf("delete pending votes for %s: %w", voter.Hex(), err)
		}
	}
	if err := statedb.WriteVotesIndex(nil); err != nil {
		return false, fmt.Errorf("clear pending votes index: %w", err)
	}

	for addr, delta := range deltas {
		if delta != 0 {
			statedb.AddWitnessVoteCount(addr, delta)
		}
	}
	return applied, nil
}
