package actuator

import (
	"errors"

	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

type UpdateSettingActuator struct{}

func (a *UpdateSettingActuator) getContract(ctx *Context) (*contractpb.UpdateSettingContract, error) {
	return decodedContract[*contractpb.UpdateSettingContract](ctx, "UpdateSettingContract")
}

func (a *UpdateSettingActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return err
	}
	contractAddr, err := checkedAddress(c.ContractAddress, "contractAddress")
	if err != nil {
		return err
	}

	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	meta := ctx.State.GetContract(contractAddr)
	if meta == nil {
		return errors.New("contract does not exist")
	}
	originAddr, err := checkedAddress(meta.OriginAddress, "contract originAddress")
	if err != nil {
		return err
	}
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
	contractAddr, err := checkedAddress(c.ContractAddress, "contractAddress")
	if err != nil {
		return nil, err
	}
	raw := ctx.State.GetContract(contractAddr)
	if raw == nil {
		return nil, errors.New("contract not found")
	}
	meta := proto.Clone(raw).(*contractpb.SmartContract)
	meta.ConsumeUserResourcePercent = c.ConsumeUserResourcePercent
	ctx.State.SetContract(contractAddr, meta)
	return &Result{Fee: 0, ContractRet: 1}, nil
}
