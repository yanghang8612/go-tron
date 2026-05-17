package actuator

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func recordPendingVotes(ctx *Context, owner common.Address, oldVotes, newVotes []*corepb.Vote) error {
	if ctx.DB == nil {
		return nil
	}
	votes := rawdb.ReadVotes(ctx.DB, owner)
	if votes == nil {
		votes = &corepb.Votes{
			Address:  owner.Bytes(),
			OldVotes: cloneVotes(oldVotes),
		}
	}
	votes.NewVotes = cloneVotes(newVotes)
	return rawdb.WriteVotes(ctx.DB, owner, votes)
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
