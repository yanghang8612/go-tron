package core

import (
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
)

func pendingVoteDeltas(db ethdb.KeyValueReader) (map[tcommon.Address]int64, bool) {
	voters := rawdb.ReadVotesIndex(db)
	if len(voters) == 0 {
		return nil, false
	}
	deltas := make(map[tcommon.Address]int64)
	hasRecords := false
	for _, voter := range voters {
		votes := rawdb.ReadVotes(db, voter)
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
	return deltas, hasRecords
}

// applyPendingVotes drains java-tron-style VotesStore records into the
// WitnessStore view at maintenance time. Each record contains the vote list at
// the start of the epoch plus the voter's latest vote list; the net deltas are
// applied once and the pending store is cleared.
func applyPendingVotes(db kvReadWriter, statedb *state.StateDB) bool {
	voters := rawdb.ReadVotesIndex(db)
	if len(voters) == 0 {
		return false
	}
	deltas, applied := pendingVoteDeltas(db)
	for _, voter := range voters {
		_ = rawdb.DeleteVotes(db, voter)
	}
	rawdb.WriteVotesIndex(db, nil)

	for addr, delta := range deltas {
		if delta != 0 {
			statedb.AddWitnessVoteCount(addr, delta)
		}
	}
	return applied
}
