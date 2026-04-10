package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type UpdateBrokerageActuator struct{}

func (a *UpdateBrokerageActuator) getContract(ctx *Context) (*contractpb.UpdateBrokerageContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.UpdateBrokerageContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal UpdateBrokerageContract")
	}
	return c, nil
}

func (a *UpdateBrokerageActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if ctx.State.GetWitness(ownerAddr) == nil {
		return errors.New("owner is not a witness")
	}
	if c.Brokerage < 0 || c.Brokerage > 100 {
		return errors.New("brokerage must be 0-100")
	}
	return nil
}

func (a *UpdateBrokerageActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	if ctx.DB != nil {
		rawdb.WriteWitnessBrokerage(ctx.DB, ownerAddr, int64(c.Brokerage))
	}
	return &Result{Fee: 0, ContractRet: 1}, nil
}
