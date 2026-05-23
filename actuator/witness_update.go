package actuator

import (
	"errors"

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
	ownerAddr, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return err
	}
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if !witnessExists(ctx, ownerAddr) {
		return errors.New("owner is not a witness")
	}
	if !validBytesLen(c.UpdateUrl, 256, false) {
		return errors.New("invalid witness URL")
	}
	return nil
}

func (a *WitnessUpdateActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return nil, err
	}
	url := string(c.UpdateUrl)
	ctx.State.SetWitnessURL(ownerAddr, url)
	return &Result{Fee: 0, ContractRet: 1}, nil
}
