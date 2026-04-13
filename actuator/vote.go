package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
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
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if len(vc.Votes) == 0 {
		return errors.New("no votes provided")
	}
	if len(vc.Votes) > params.MaxVoteNumber {
		return errors.New("too many votes")
	}

	tronPower := ctx.State.TotalFrozenV2(ownerAddr) / int64(params.TRXPrecision)
	var totalVoteCount int64
	seen := make(map[common.Address]bool)
	for _, v := range vc.Votes {
		targetAddr := common.BytesToAddress(v.VoteAddress)
		if seen[targetAddr] {
			return errors.New("duplicate vote target")
		}
		seen[targetAddr] = true
		if v.VoteCount <= 0 {
			return errors.New("vote count must be positive")
		}
		totalVoteCount += v.VoteCount
		if !ctx.State.AccountExists(targetAddr) {
			return errors.New("vote target account does not exist")
		}
		if ctx.State.GetWitness(targetAddr) == nil {
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
	ownerAddr := common.BytesToAddress(vc.OwnerAddress)

	// Remove old votes from witnesses
	oldVotes := ctx.State.GetVotes(ownerAddr)
	for _, v := range oldVotes {
		targetAddr := common.BytesToAddress(v.VoteAddress)
		ctx.State.AddWitnessVoteCount(targetAddr, -v.VoteCount)
	}

	// Set new votes on account
	newVotes := make([]*corepb.Vote, len(vc.Votes))
	for i, v := range vc.Votes {
		newVotes[i] = &corepb.Vote{
			VoteAddress: v.VoteAddress,
			VoteCount:   v.VoteCount,
		}
	}
	ctx.State.SetVotes(ownerAddr, newVotes)

	// Add new votes to witnesses
	for _, v := range vc.Votes {
		targetAddr := common.BytesToAddress(v.VoteAddress)
		ctx.State.AddWitnessVoteCount(targetAddr, v.VoteCount)
	}

	return &Result{Fee: 0, ContractRet: 1}, nil
}
