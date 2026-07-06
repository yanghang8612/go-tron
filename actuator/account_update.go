package actuator

import (
	"errors"
	"fmt"

	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type AccountUpdateActuator struct{}

func (a *AccountUpdateActuator) getContract(ctx *Context) (*contractpb.AccountUpdateContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.AccountUpdateContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal AccountUpdateContract")
	}
	return c, nil
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
	if ctx.State != nil && !ctx.DynProps.AllowUpdateAccountName() {
		exists, err := ctx.State.HasAccountNameIndexStrict(c.AccountName)
		if err != nil {
			return fmt.Errorf("read account name index: %w", err)
		}
		if exists {
			return errors.New("account name already exists")
		}
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
