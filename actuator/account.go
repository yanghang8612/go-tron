package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type CreateAccountActuator struct{}

func (a *CreateAccountActuator) getContract(ctx *Context) (*contractpb.AccountCreateContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	ac := &contractpb.AccountCreateContract{}
	if err := contract.Parameter.UnmarshalTo(ac); err != nil {
		return nil, errors.New("failed to unmarshal AccountCreateContract")
	}
	return ac, nil
}

func (a *CreateAccountActuator) Validate(ctx *Context) error {
	ac, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(ac.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	newAddr := common.BytesToAddress(ac.AccountAddress)
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
	newAddr := common.BytesToAddress(ac.AccountAddress)
	ctx.State.CreateAccountWithTime(newAddr, ac.Type, ctx.DynProps.LatestBlockHeaderTimestamp())
	if ctx.DynProps.AllowMultiSign() {
		ctx.State.ApplyDefaultAccountPermissions(newAddr, ctx.DynProps)
	}
	return &Result{Fee: 0, ContractRet: 1}, nil
}
