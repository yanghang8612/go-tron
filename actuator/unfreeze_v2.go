package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/params"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

const maxUnfreezeCount = 32

type UnfreezeBalanceV2Actuator struct{}

func (a *UnfreezeBalanceV2Actuator) getContract(ctx *Context) (*contractpb.UnfreezeBalanceV2Contract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	uc := &contractpb.UnfreezeBalanceV2Contract{}
	if err := contract.Parameter.UnmarshalTo(uc); err != nil {
		return nil, errors.New("failed to unmarshal UnfreezeBalanceV2Contract")
	}
	return uc, nil
}

func (a *UnfreezeBalanceV2Actuator) Validate(ctx *Context) error {
	if !forks.IsActive(forks.AllowStakingV2, ctx.BlockNumber, ctx.DynProps) {
		return errors.New("staking v2 not yet enabled")
	}
	uc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(uc.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if uc.UnfreezeBalance <= 0 {
		return errors.New("unfreeze balance must be positive")
	}
	frozen := ctx.State.GetFrozenV2Amount(ownerAddr, uc.Resource)
	if frozen < uc.UnfreezeBalance {
		return errors.New("insufficient frozen balance")
	}
	if ctx.State.UnfreezeV2Count(ownerAddr) >= maxUnfreezeCount {
		return errors.New("too many pending unfreezes")
	}
	return nil
}

func (a *UnfreezeBalanceV2Actuator) Execute(ctx *Context) (*Result, error) {
	uc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(uc.OwnerAddress)
	ctx.State.ReduceFreezeV2(ownerAddr, uc.Resource, uc.UnfreezeBalance)
	expireTime := ctx.BlockTime + ctx.DynProps.UnfreezeDelayDays()*86_400_000
	ctx.State.AddUnfreezeV2(ownerAddr, uc.Resource, uc.UnfreezeBalance, expireTime)

	// Invalidate votes if remaining Tron Power < total votes cast
	newTP := ctx.State.TotalFrozenV2(ownerAddr) / int64(params.TRXPrecision)
	votes := ctx.State.GetVotes(ownerAddr)
	var totalVotes int64
	for _, v := range votes {
		totalVotes += v.VoteCount
	}
	if totalVotes > newTP {
		for _, v := range votes {
			ctx.State.AddWitnessVoteCount(common.BytesToAddress(v.VoteAddress), -v.VoteCount)
		}
		ctx.State.ClearVotes(ownerAddr)
	}

	return &Result{Fee: 0, ContractRet: 1}, nil
}
