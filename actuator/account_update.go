package actuator

import (
	"errors"

	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type AccountUpdateActuator struct{}

func (a *AccountUpdateActuator) getContract(ctx *Context) (*contractpb.AccountUpdateContract, error) {
	return decodedContract[*contractpb.AccountUpdateContract](ctx, "AccountUpdateContract")
}

func (a *AccountUpdateActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	if !validBytesLen(c.AccountName, 200, true) {
		return errors.New("invalid accountName")
	}
	ownerAddr, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return err
	}
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if ctx.State.GetAccountName(ownerAddr) != "" && !ctx.DynProps.AllowUpdateAccountName() {
		return errors.New("account name already set")
	}
	if ctx.State != nil && ctx.State.HasAccountNameIndex(c.AccountName) && !ctx.DynProps.AllowUpdateAccountName() {
		return errors.New("account name already exists")
	}
	return nil
}

func (a *AccountUpdateActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return nil, err
	}
	ctx.State.SetAccountName(ownerAddr, string(c.AccountName))
	if err := ctx.State.WriteAccountNameIndex(c.AccountName, ownerAddr); err != nil {
		return nil, err
	}
	return &Result{Fee: 0, ContractRet: 1}, nil
}
