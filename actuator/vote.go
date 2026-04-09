package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type VoteWitnessActuator struct{}

func (a *VoteWitnessActuator) getContract(ctx *Context) (*contractpb.VoteWitnessContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	vc := &contractpb.VoteWitnessContract{}
	if err := contract.Parameter.UnmarshalTo(vc); err != nil {
		return nil, errors.New("failed to unmarshal VoteWitnessContract")
	}
	return vc, nil
}

func (a *VoteWitnessActuator) Validate(ctx *Context) error {
	vc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(vc.OwnerAddress)
	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	if ownerAcc == nil {
		return errors.New("owner account does not exist")
	}
	if len(vc.Votes) > params.MaxVoteNumber {
		return errors.New("too many votes")
	}
	tronPower := ownerAcc.GetFrozenV2Amount(corepb.ResourceCode_TRON_POWER) / int64(params.TRXPrecision)
	var totalVotes int64
	seen := make(map[common.Address]bool)
	for _, v := range vc.Votes {
		if v.VoteCount <= 0 {
			return errors.New("vote count must be positive")
		}
		totalVotes += v.VoteCount
		witnessAddr := common.BytesToAddress(v.VoteAddress)
		if seen[witnessAddr] {
			return errors.New("duplicate vote address")
		}
		seen[witnessAddr] = true
		if rawdb.ReadWitness(ctx.DB, witnessAddr) == nil {
			return errors.New("vote target is not a witness")
		}
	}
	if totalVotes > tronPower {
		return errors.New("total votes exceed TRON power")
	}
	return nil
}

func (a *VoteWitnessActuator) Execute(ctx *Context) (*Result, error) {
	vc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(vc.OwnerAddress)
	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)

	// Remove old votes from witnesses
	for _, oldVote := range ownerAcc.Votes() {
		wAddr := common.BytesToAddress(oldVote.VoteAddress)
		w := rawdb.ReadWitness(ctx.DB, wAddr)
		if w != nil {
			w.SetVoteCount(w.VoteCount() - oldVote.VoteCount)
			rawdb.WriteWitness(ctx.DB, wAddr, w)
		}
	}

	// Set new votes on owner
	newVotes := make([]*corepb.Vote, len(vc.Votes))
	for i, v := range vc.Votes {
		newVotes[i] = &corepb.Vote{
			VoteAddress: v.VoteAddress,
			VoteCount:   v.VoteCount,
		}
		wAddr := common.BytesToAddress(v.VoteAddress)
		w := rawdb.ReadWitness(ctx.DB, wAddr)
		if w != nil {
			w.SetVoteCount(w.VoteCount() + v.VoteCount)
			rawdb.WriteWitness(ctx.DB, wAddr, w)
		}
	}
	ownerAcc.SetVotes(newVotes)
	rawdb.WriteAccount(ctx.DB, ownerAddr, ownerAcc)

	return &Result{Fee: 0}, nil
}
