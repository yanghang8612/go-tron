package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type SetAccountIdActuator struct{}

func (a *SetAccountIdActuator) getContract(ctx *Context) (*contractpb.SetAccountIdContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.SetAccountIdContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal SetAccountIdContract")
	}
	return c, nil
}

func (a *SetAccountIdActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	if len(c.AccountId) < 8 {
		return errors.New("account id too short (min 8 bytes)")
	}
	if len(c.AccountId) > 32 {
		return errors.New("account id too long (max 32 bytes)")
	}
	for _, b := range c.AccountId {
		if b < 0x21 || b > 0x7e {
			return errors.New("account id must contain only printable non-space ASCII characters")
		}
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if ctx.State.GetAccountId(ownerAddr) != "" {
		return errors.New("account id already set")
	}
	return nil
}

func (a *SetAccountIdActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	ctx.State.SetAccountId(ownerAddr, string(c.AccountId))
	return &Result{Fee: 0, ContractRet: 1}, nil
}
