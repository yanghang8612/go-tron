package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type FreezeBalanceV2Actuator struct{}

func (a *FreezeBalanceV2Actuator) getContract(ctx *Context) (*contractpb.FreezeBalanceV2Contract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	fc := &contractpb.FreezeBalanceV2Contract{}
	if err := contract.Parameter.UnmarshalTo(fc); err != nil {
		return nil, errors.New("failed to unmarshal FreezeBalanceV2Contract")
	}
	return fc, nil
}

func (a *FreezeBalanceV2Actuator) Validate(ctx *Context) error {
	if !ctx.DynProps.SupportUnfreezeDelay() {
		return errors.New("staking v2 not yet enabled")
	}
	fc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(fc.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if fc.FrozenBalance <= 0 {
		return errors.New("frozen balance must be positive")
	}
	if ctx.State.GetBalance(ownerAddr) < fc.FrozenBalance {
		return errors.New("insufficient balance to freeze")
	}
	newResourceModel := forks.IsActive(forks.AllowNewResourceModel, ctx.BlockNumber, ctx.DynProps)
	switch fc.Resource {
	case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_ENERGY:
		// always allowed under StakingV2
	case corepb.ResourceCode_TRON_POWER:
		if !newResourceModel {
			return errors.New("TRON_POWER freeze requires AllowNewResourceModel")
		}
	default:
		if newResourceModel {
			return errors.New("invalid resource type; valid: BANDWIDTH, ENERGY, TRON_POWER")
		}
		return errors.New("invalid resource type; valid: BANDWIDTH, ENERGY")
	}
	return nil
}

func (a *FreezeBalanceV2Actuator) Execute(ctx *Context) (*Result, error) {
	fc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(fc.OwnerAddress)

	// AllowNewResourceModel: snapshot legacy tron power on the first V2 freeze
	// after the fork, so getAllTronPower() remains stable going forward.
	if forks.IsActive(forks.AllowNewResourceModel, ctx.BlockNumber, ctx.DynProps) {
		ctx.State.InitializeOldTronPowerIfNeeded(ownerAddr)
	}

	// total_{net,energy,tron_power}_weight tracks the (frozenV2+delegatedV2)/TRX
	// share — read it before the mutation, then again after, and persist the
	// delta. Mirrors java-tron's FreezeBalanceV2Actuator.execute switch block.
	oldWeight := frozenV2WithDelegatedWeight(ctx.State, ownerAddr, fc.Resource)
	if err := ctx.State.SubBalance(ownerAddr, fc.FrozenBalance); err != nil {
		return nil, err
	}
	ctx.State.AddFreezeV2(ownerAddr, fc.Resource, fc.FrozenBalance)
	newWeight := frozenV2WithDelegatedWeight(ctx.State, ownerAddr, fc.Resource)
	addResourceWeight(ctx.DynProps, fc.Resource, newWeight-oldWeight)

	return &Result{Fee: 0, ContractRet: 1}, nil
}
