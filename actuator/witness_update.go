package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type WitnessUpdateActuator struct{}

func (a *WitnessUpdateActuator) getContract(ctx *Context) (*contractpb.WitnessUpdateContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.WitnessUpdateContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal WitnessUpdateContract")
	}
	return c, nil
}

func (a *WitnessUpdateActuator) Validate(ctx *Context) error {
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
	if len(c.UpdateUrl) == 0 {
		return errors.New("witness URL is empty")
	}
	if len(c.UpdateUrl) > 256 {
		return errors.New("witness URL too long (max 256 bytes)")
	}
	return nil
}

func (a *WitnessUpdateActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	ctx.State.PutWitness(ownerAddr, string(c.UpdateUrl))
	return &Result{Fee: 0, ContractRet: 1}, nil
}
