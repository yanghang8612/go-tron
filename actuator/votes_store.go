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
			OldVotes: oldVotes,
		}
	}
	// WriteVotes synchronously marshals the complete record into StateDB-owned
	// bytes before returning. The temporary protobuf does not retain either
	// input slice, so cloning every vote and address here only duplicated the
	// serialization ownership boundary.
	votes.NewVotes = newVotes
	return ctx.State.WriteVotes(owner, votes)
}
