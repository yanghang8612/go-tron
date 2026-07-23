package actuator

import (
	"errors"

	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type CreateAccountActuator struct{}

func (a *CreateAccountActuator) getContract(ctx *Context) (*contractpb.AccountCreateContract, error) {
	return decodedContract[*contractpb.AccountCreateContract](ctx, "AccountCreateContract")
}

func (a *CreateAccountActuator) Validate(ctx *Context) error {
	ac, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr, err := checkedAddress(ac.OwnerAddress, "ownerAddress")
	if err != nil {
		return err
	}
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if ctx.State.GetBalance(ownerAddr) < ctx.DynProps.CreateNewAccountFeeInSystemContract() {
		return errors.New("Validate CreateAccountActuator error, insufficient fee.")
	}
	newAddr, err := checkedAddress(ac.AccountAddress, "account address")
	if err != nil {
		return err
	}
	if ctx.State.AccountExists(newAddr) {
		return errors.New("account already exists")
	}
	return nil
}

func (a *CreateAccountActuator) Execute(ctx *Context) (*Result, error) {
	ac, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr, err := checkedAddress(ac.OwnerAddress, "ownerAddress")
	if err != nil {
		return nil, err
	}
	newAddr, err := checkedAddress(ac.AccountAddress, "account address")
	if err != nil {
		return nil, err
	}
	ctx.State.CreateAccountWithTime(newAddr, ac.Type, ctx.DynProps.LatestBlockHeaderTimestamp())
	if ctx.DynProps.AllowMultiSign() {
		ctx.State.ApplyDefaultAccountPermissions(newAddr, ctx.DynProps)
	}
	fee := ctx.DynProps.CreateNewAccountFeeInSystemContract()
	if err := burnFee(ctx, ownerAddr, fee); err != nil {
		return nil, err
	}
	return &Result{Fee: fee, ContractRet: 1}, nil
}
