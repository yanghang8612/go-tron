package actuator

import (
	"errors"

	tcommon "github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type UpdateSettingActuator struct{}

func (a *UpdateSettingActuator) getContract(ctx *Context) (*contractpb.UpdateSettingContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.UpdateSettingContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal UpdateSettingContract")
	}
	return c, nil
}

func (a *UpdateSettingActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	contractAddr := tcommon.BytesToAddress(c.ContractAddress)

	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	meta := ctx.State.GetContract(contractAddr)
	if meta == nil {
		return errors.New("contract does not exist")
	}
	originAddr := tcommon.BytesToAddress(meta.OriginAddress)
	if originAddr != ownerAddr {
		return errors.New("sender is not the contract origin")
	}
	if c.ConsumeUserResourcePercent < 0 || c.ConsumeUserResourcePercent > 100 {
		return errors.New("consume_user_resource_percent must be in [0, 100]")
	}
	return nil
}

func (a *UpdateSettingActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	contractAddr := tcommon.BytesToAddress(c.ContractAddress)
	meta := ctx.State.GetContract(contractAddr)
	if meta == nil {
		return nil, errors.New("contract not found")
	}
	meta.ConsumeUserResourcePercent = c.ConsumeUserResourcePercent
	ctx.State.SetContract(contractAddr, meta)
	return &Result{Fee: 0, ContractRet: 1}, nil
}
