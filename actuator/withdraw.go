package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

const withdrawCooldown = 86_400_000 // 24 hours in ms

type WithdrawBalanceActuator struct{}

func (a *WithdrawBalanceActuator) getContract(ctx *Context) (*contractpb.WithdrawBalanceContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	wc := &contractpb.WithdrawBalanceContract{}
	if err := contract.Parameter.UnmarshalTo(wc); err != nil {
		return nil, errors.New("failed to unmarshal WithdrawBalanceContract")
	}
	return wc, nil
}

func (a *WithdrawBalanceActuator) Validate(ctx *Context) error {
	wc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(wc.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if !ctx.State.IsWitness(ownerAddr) {
		return errors.New("account is not a witness")
	}
	if ctx.State.GetAllowance(ownerAddr) <= 0 {
		return errors.New("no allowance to withdraw")
	}
	lastWithdraw := ctx.State.GetLatestWithdrawTime(ownerAddr)
	if ctx.BlockTime-lastWithdraw < withdrawCooldown {
		return errors.New("withdraw too frequent")
	}
	return nil
}

func (a *WithdrawBalanceActuator) Execute(ctx *Context) (*Result, error) {
	wc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(wc.OwnerAddress)
	allowance := ctx.State.GetAllowance(ownerAddr)
	ctx.State.AddBalance(ownerAddr, allowance)
	ctx.State.SetAllowance(ownerAddr, 0)
	ctx.State.SetLatestWithdrawTime(ownerAddr, ctx.BlockTime)
	return &Result{Fee: 0, ContractRet: 1}, nil
}
