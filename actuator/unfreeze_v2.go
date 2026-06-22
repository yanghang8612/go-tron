package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
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
	if !ctx.DynProps.SupportUnfreezeDelay() {
		return errors.New("staking v2 not yet enabled")
	}
	uc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr, err := checkedAddress(uc.OwnerAddress, "address")
	if err != nil {
		return err
	}
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if uc.UnfreezeBalance <= 0 {
		return errors.New("unfreeze balance must be positive")
	}
	newResourceModel := forks.IsActive(forks.AllowNewResourceModel, ctx.BlockNumber, ctx.DynProps)
	switch uc.Resource {
	case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_ENERGY:
		// always valid under StakingV2
	case corepb.ResourceCode_TRON_POWER:
		if !newResourceModel {
			return errors.New("TRON_POWER unfreeze requires AllowNewResourceModel")
		}
	default:
		return errors.New("invalid resource type")
	}
	frozen := ctx.State.GetFrozenV2Amount(ownerAddr, uc.Resource)
	if frozen < uc.UnfreezeBalance {
		return errors.New("insufficient frozen balance")
	}
	if unfreezingV2Count(ctx.State.GetAccount(ownerAddr), ctx.PrevBlockTime) >= maxUnfreezeCount {
		return errors.New("too many pending unfreezes")
	}
	return nil
}

func (a *UnfreezeBalanceV2Actuator) Execute(ctx *Context) (*Result, error) {
	uc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr, err := checkedAddress(uc.OwnerAddress, "address")
	if err != nil {
		return nil, err
	}

	withdrawReward(ctx.DB, ctx.State, ctx.DynProps, ownerAddr)
	withdrawnExpired := ctx.State.RemoveExpiredUnfreezeV2(ownerAddr, ctx.PrevBlockTime)
	if withdrawnExpired > 0 {
		ctx.State.AddBalance(ownerAddr, withdrawnExpired)
	}

	// AllowNewResourceModel: snapshot legacy tron power before the unfreeze so
	// that old_tron_power captures the pre-unfreeze state.
	if forks.IsActive(forks.AllowNewResourceModel, ctx.BlockNumber, ctx.DynProps) {
		ctx.State.InitializeOldTronPowerIfNeeded(ownerAddr)
	}

	// Track the total_{net,energy,tron_power}_weight delta around the
	// frozenV2 mutation, mirroring java-tron's
	// UnfreezeBalanceV2Actuator.updateTotalResourceWeight.
	oldWeight := frozenV2WithDelegatedWeight(ctx.State, ownerAddr, uc.Resource)
	ctx.State.ReduceFreezeV2(ownerAddr, uc.Resource, uc.UnfreezeBalance)
	newWeight := frozenV2WithDelegatedWeight(ctx.State, ownerAddr, uc.Resource)
	addResourceWeight(ctx.DynProps, uc.Resource, newWeight-oldWeight)

	expireTime := ctx.PrevBlockTime + ctx.DynProps.UnfreezeDelayDays()*86_400_000
	ctx.State.AddUnfreezeV2(ownerAddr, uc.Resource, uc.UnfreezeBalance, expireTime)

	if err := updateVotesAfterUnfreezeV2(ctx, ownerAddr, uc.Resource); err != nil {
		return nil, err
	}

	// AllowNewResourceModel: any V2 unfreeze consumes the legacy snapshot,
	// so only explicit TRON_POWER-typed frozen counts going forward.
	if forks.IsActive(forks.AllowNewResourceModel, ctx.BlockNumber, ctx.DynProps) {
		ctx.State.InvalidateOldTronPower(ownerAddr)
	}

	return &Result{Fee: 0, WithdrawExpireAmount: withdrawnExpired, ContractRet: 1}, nil
}

func unfreezingV2Count(account interface {
	UnfrozenV2() []*corepb.Account_UnFreezeV2
}, now int64) int {
	if account == nil {
		return 0
	}
	var count int
	for _, u := range account.UnfrozenV2() {
		if u.UnfreezeExpireTime > now {
			count++
		}
	}
	return count
}

func updateVotesAfterUnfreezeV2(ctx *Context, ownerAddr common.Address, resource corepb.ResourceCode) error {
	votes := ctx.State.GetVotes(ownerAddr)
	if len(votes) == 0 {
		return nil
	}
	// Mirror java UnfreezeBalanceV2Actuator.updateVote control flow.
	newResourceModel := forks.IsActive(forks.AllowNewResourceModel, ctx.BlockNumber, ctx.DynProps)
	if newResourceModel {
		account := ctx.State.GetAccount(ownerAddr)
		if account != nil && account.OldTronPowerIsInvalid() {
			// old_tron_power already invalid: a BANDWIDTH/ENERGY unfreeze leaves
			// votes alone; a TRON_POWER unfreeze falls through to the proportional
			// recompute below (java `default: break`), which must use getAllTronPower.
			if resource == corepb.ResourceCode_BANDWIDTH || resource == corepb.ResourceCode_ENERGY {
				return nil
			}
		} else {
			// Not invalid yet: clear all votes at once (new-resource-model start).
			return clearVotesWithPendingDelta(ctx, ownerAddr, votes)
		}
	}

	var totalVotes int64
	for _, v := range votes {
		totalVotes += v.VoteCount
	}
	// java: ownedTronPower = supportAllowNewResourceModel ? getAllTronPower() :
	// getTronPower(); return when it covers the votes, then when totalVote == 0.
	var ownedTronPower int64
	if newResourceModel {
		ownedTronPower = ctx.State.GetAllTronPower(ownerAddr)
	} else {
		ownedTronPower = ctx.State.GetLegacyTronPower(ownerAddr)
	}
	if ownedTronPower >= totalVotes*int64(params.TRXPrecision) {
		return nil
	}
	if totalVotes == 0 {
		return nil
	}
	newVotes := make([]*corepb.Vote, 0, len(votes))
	for _, v := range votes {
		newVoteCount := int64(float64(v.VoteCount) / float64(totalVotes) * float64(ownedTronPower) / float64(params.TRXPrecision))
		if newVoteCount > 0 {
			newVotes = append(newVotes, &corepb.Vote{
				VoteAddress: v.VoteAddress,
				VoteCount:   newVoteCount,
			})
		}
	}
	if err := recordPendingVotes(ctx, ownerAddr, votes, newVotes); err != nil {
		return err
	}
	if len(newVotes) == 0 {
		ctx.State.ClearVotes(ownerAddr)
		return nil
	}
	ctx.State.SetVotes(ownerAddr, newVotes)
	return nil
}

func clearVotesWithPendingDelta(ctx *Context, ownerAddr common.Address, votes []*corepb.Vote) error {
	if err := recordPendingVotes(ctx, ownerAddr, votes, nil); err != nil {
		return err
	}
	ctx.State.ClearVotes(ownerAddr)
	return nil
}
