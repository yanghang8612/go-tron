package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
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
	expireTime := ctx.BlockTime + 14*86_400_000
	ctx.State.AddUnfreezeV2(ownerAddr, uc.Resource, uc.UnfreezeBalance, expireTime)
	return &Result{Fee: 0}, nil
}
