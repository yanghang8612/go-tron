package actuator

import (
	"errors"
	"sync"

	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type VoteWitnessActuator struct{}

type voteConversionBuffer struct {
	refs   [params.MaxVoteNumber]*corepb.Vote
	values [params.MaxVoteNumber]corepb.Vote
}

var voteConversionBufferPool = sync.Pool{
	New: func() any { return new(voteConversionBuffer) },
}

func coreVotesFromContract(votes []*contractpb.VoteWitnessContract_Vote) (*voteConversionBuffer, []*corepb.Vote) {
	if len(votes) > params.MaxVoteNumber {
		converted := make([]*corepb.Vote, len(votes))
		for i, vote := range votes {
			converted[i] = &corepb.Vote{VoteAddress: vote.VoteAddress, VoteCount: vote.VoteCount}
		}
		return nil, converted
	}
	buffer := voteConversionBufferPool.Get().(*voteConversionBuffer)
	for i, vote := range votes {
		entry := &buffer.values[i]
		entry.VoteAddress = vote.VoteAddress
		entry.VoteCount = vote.VoteCount
		buffer.refs[i] = entry
	}
	return buffer, buffer.refs[:len(votes)]
}

func (b *voteConversionBuffer) release(n int) {
	if b == nil {
		return
	}
	for i := 0; i < n; i++ {
		b.refs[i] = nil
		b.values[i] = corepb.Vote{}
	}
	voteConversionBufferPool.Put(b)
}

func (a *VoteWitnessActuator) getContract(ctx *Context) (*contractpb.VoteWitnessContract, error) {
	return decodedContract[*contractpb.VoteWitnessContract](ctx, "VoteWitnessContract")
}

func (a *VoteWitnessActuator) Validate(ctx *Context) error {
	vc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	if len(vc.Votes) == 0 {
		return errors.New("no votes provided")
	}
	if len(vc.Votes) > params.MaxVoteNumber {
		return errors.New("too many votes")
	}

	ownerAddr, err := checkedAddress(vc.OwnerAddress, "ownerAddress")
	if err != nil {
		return err
	}
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}

	var tronPower int64
	if forks.IsActive(forks.AllowNewResourceModel, ctx.BlockNumber, ctx.DynProps) {
		tronPower = ctx.State.GetAllTronPower(ownerAddr)
	} else {
		tronPower = ctx.State.GetLegacyTronPower(ownerAddr)
	}
	tronPower /= int64(params.TRXPrecision)
	var totalVoteCount int64
	for _, v := range vc.Votes {
		targetAddr, err := checkedAddress(v.VoteAddress, "voteAddress")
		if err != nil {
			return err
		}
		if v.VoteCount <= 0 {
			return errors.New("vote count must be positive")
		}
		var ok bool
		totalVoteCount, ok = checkedAddInt64(totalVoteCount, v.VoteCount)
		if !ok {
			return errors.New("vote count overflow")
		}
		if !ctx.State.AccountExists(targetAddr) {
			return errors.New("vote target account does not exist")
		}
		if !witnessExists(ctx, targetAddr) {
			return errors.New("vote target is not a witness")
		}
	}
	if totalVoteCount > tronPower {
		return errors.New("total votes exceed tron power")
	}
	return nil
}

func (a *VoteWitnessActuator) Execute(ctx *Context) (*Result, error) {
	vc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr, err := checkedAddress(vc.OwnerAddress, "ownerAddress")
	if err != nil {
		return nil, err
	}

	// Settle pending voter rewards BEFORE changing votes. Java-tron's
	// VoteWitnessActuator.execute calls mortgageService.withdrawReward
	// first so that the voter's accumulated rewards are locked in against
	// the OLD vote list before the new vote list applies. Without this,
	// a voter who changes targets would retroactively earn the new
	// witness's rewards for past cycles.
	withdrawReward(ctx.DB, ctx.State, ctx.DynProps, ownerAddr)

	// AllowNewResourceModel: snapshot legacy tron power into old_tron_power on the
	// first vote after the fork, so subsequent getAllTronPower() calls return a
	// stable value independent of new non-TRON_POWER-typed V2 freezes.
	if forks.IsActive(forks.AllowNewResourceModel, ctx.BlockNumber, ctx.DynProps) {
		ctx.State.InitializeOldTronPowerIfNeeded(ownerAddr)
	}

	// Store the epoch delta in VotesStore. java-tron does not mutate
	// WitnessStore here; MaintenanceManager.countVote applies old/new
	// vote deltas at the next maintenance boundary.
	oldVotes := ctx.State.GetVotes(ownerAddr)

	// Set new votes on account
	voteBuffer, newVotes := coreVotesFromContract(vc.Votes)
	defer voteBuffer.release(len(newVotes))
	if err := recordPendingVotes(ctx, ownerAddr, oldVotes, newVotes); err != nil {
		return nil, err
	}
	ctx.State.SetVotes(ownerAddr, newVotes)

	return &Result{Fee: 0, ContractRet: 1}, nil
}
