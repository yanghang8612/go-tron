package state

import (
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// WitnessIndexAt reconstructs the registered witness index at the end of
// blockNum.
func (r *PersistentHistoryReader) WitnessIndexAt(blockNum uint64) ([]tcommon.Address, error) {
	raw, ok, err := r.AccountKVAt(tcommon.SystemAccountAddress, kvdomains.SystemWitnessSchedule, witnessScheduleIndexKey, blockNum)
	if err != nil || !ok {
		return nil, err
	}
	return decodeAddressList(raw), nil
}

// ActiveWitnessesAt reconstructs the active witness set at the end of blockNum.
func (r *PersistentHistoryReader) ActiveWitnessesAt(blockNum uint64) ([]tcommon.Address, error) {
	raw, ok, err := r.AccountKVAt(tcommon.SystemAccountAddress, kvdomains.SystemWitnessSchedule, witnessScheduleActiveKey, blockNum)
	if err != nil || !ok {
		return nil, err
	}
	return decodeAddressList(raw), nil
}

// WitnessAt reconstructs a witness capsule at the end of blockNum.
func (r *PersistentHistoryReader) WitnessAt(addr tcommon.Address, blockNum uint64) (*types.Witness, error) {
	raw, ok, err := r.AccountKVAt(addr, kvdomains.WitnessCapsule, rawdb.WitnessCapsuleStateKey(addr), blockNum)
	if err != nil || !ok || len(raw) == 0 {
		return nil, err
	}
	w, err := types.UnmarshalWitness(raw)
	if err != nil {
		return nil, fmt.Errorf("decode witness at block %d: %w", blockNum, err)
	}
	return w, nil
}

// VotesIndexAt reconstructs the pending vote-state voter index at the end of
// blockNum.
func (r *PersistentHistoryReader) VotesIndexAt(blockNum uint64) ([]tcommon.Address, error) {
	raw, ok, err := r.AccountKVAt(tcommon.SystemAccountAddress, kvdomains.WitnessVoteState, votesStoreIndexKey, blockNum)
	if err != nil || !ok {
		return nil, err
	}
	return decodeAddressList(raw), nil
}

// VotesAt reconstructs a voter's pending vote-state record at the end of
// blockNum.
func (r *PersistentHistoryReader) VotesAt(addr tcommon.Address, blockNum uint64) (*corepb.Votes, error) {
	raw, ok, err := r.AccountKVAt(tcommon.SystemAccountAddress, kvdomains.WitnessVoteState, votesStoreKey(addr), blockNum)
	if err != nil || !ok || len(raw) == 0 {
		return nil, err
	}
	votes := &corepb.Votes{}
	if err := proto.Unmarshal(raw, votes); err != nil {
		return nil, fmt.Errorf("decode votes at block %d: %w", blockNum, err)
	}
	return votes, nil
}

// PendingVoteDeltasAt nets the rooted pending vote-state records into
// per-witness vote deltas at the end of blockNum.
func (r *PersistentHistoryReader) PendingVoteDeltasAt(blockNum uint64) (map[tcommon.Address]int64, bool, error) {
	voters, err := r.VotesIndexAt(blockNum)
	if err != nil || len(voters) == 0 {
		return nil, false, err
	}
	deltas := make(map[tcommon.Address]int64)
	hasRecords := false
	for _, voter := range voters {
		votes, err := r.VotesAt(voter, blockNum)
		if err != nil {
			return nil, false, err
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
