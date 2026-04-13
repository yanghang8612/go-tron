package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
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
	if len(c.AccountName) == 0 {
		return errors.New("account name is empty")
	}
	if len(c.AccountName) > 32 {
		return errors.New("account name too long (max 32 bytes)")
	}
	for _, b := range c.AccountName {
		if b < 0x20 || b > 0x7e {
			return errors.New("account name must contain only printable ASCII characters")
		}
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if ctx.State.GetAccountName(ownerAddr) != "" {
		return errors.New("account name already set")
	}
	return nil
}

func (a *AccountUpdateActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	ctx.State.SetAccountName(ownerAddr, string(c.AccountName))
	return &Result{Fee: 0, ContractRet: 1}, nil
}
