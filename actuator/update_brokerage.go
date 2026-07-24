package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/core/forks"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type UpdateBrokerageActuator struct{}

func (a *UpdateBrokerageActuator) getContract(ctx *Context) (*contractpb.UpdateBrokerageContract, error) {
	return decodedContract[*contractpb.UpdateBrokerageContract](ctx, "UpdateBrokerageContract")
}

func (a *UpdateBrokerageActuator) Validate(ctx *Context) error {
	if !forks.IsActive(forks.AllowChangeDelegation, ctx.BlockNumber, ctx.DynProps) {
		return errors.New("brokerage update not yet enabled")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return err
	}
	if c.Brokerage < 0 || c.Brokerage > 100 {
		return errors.New("brokerage must be 0-100")
	}
	if !witnessExists(ctx, ownerAddr) {
		return errors.New("owner is not a witness")
	}
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	return nil
}

func (a *UpdateBrokerageActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return nil, err
	}
	if err := ctx.State.WriteWitnessBrokerage(ownerAddr, int64(c.Brokerage)); err != nil {
		return nil, err
	}
	return &Result{Fee: 0, ContractRet: 1}, nil
}
