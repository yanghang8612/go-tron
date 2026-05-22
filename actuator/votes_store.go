package actuator

import (
	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// recordPendingVotes stages a voter's epoch delta into the rooted VotesStore
// (WitnessVoteState KV) on the in-scope statedb, so the maintenance drain later
// in the SAME block reads it through the shared overlay and it rewinds with the
// full state root.
func recordPendingVotes(ctx *Context, owner common.Address, oldVotes, newVotes []*corepb.Vote) error {
	if ctx.State == nil {
		return nil
	}
	votes := ctx.State.ReadVotes(owner)
	if votes == nil {
		votes = &corepb.Votes{
			Address:  owner.Bytes(),
			OldVotes: cloneVotes(oldVotes),
		}
	}
	votes.NewVotes = cloneVotes(newVotes)
	return ctx.State.WriteVotes(owner, votes)
}

func cloneVotes(votes []*corepb.Vote) []*corepb.Vote {
	if len(votes) == 0 {
		return nil
	}
	out := make([]*corepb.Vote, 0, len(votes))
	for _, vote := range votes {
		if vote == nil {
			continue
		}
		out = append(out, &corepb.Vote{
			VoteAddress: append([]byte(nil), vote.VoteAddress...),
			VoteCount:   vote.VoteCount,
		})
	}
	return out
}
