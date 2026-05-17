package actuator

import (
	"errors"

	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

const blockNumForEnergyLimit int64 = 4_727_890

type UpdateEnergyLimitActuator struct{}

func (a *UpdateEnergyLimitActuator) getContract(ctx *Context) (*contractpb.UpdateEnergyLimitContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.UpdateEnergyLimitContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal UpdateEnergyLimitContract")
	}
	return c, nil
}

func (a *UpdateEnergyLimitActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	if !energyLimitHardForkActive(ctx) {
		return errors.New("energy limit update not yet enabled")
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
	if c.OriginEnergyLimit <= 0 {
		return errors.New("origin_energy_limit must be positive")
	}
	return nil
}

func (a *UpdateEnergyLimitActuator) Execute(ctx *Context) (*Result, error) {
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
	meta.OriginEnergyLimit = c.OriginEnergyLimit
	ctx.State.SetContract(contractAddr, meta)
	return &Result{Fee: 0, ContractRet: 1}, nil
}
