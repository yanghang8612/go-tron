package actuator

import (
	"errors"

	tcommon "github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

type ClearABIActuator struct{}

func (a *ClearABIActuator) getContract(ctx *Context) (*contractpb.ClearABIContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.ClearABIContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal ClearABIContract")
	}
	return c, nil
}

func (a *ClearABIActuator) Validate(ctx *Context) error {
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
	return nil
}

func (a *ClearABIActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	contractAddr := tcommon.BytesToAddress(c.ContractAddress)
	raw := ctx.State.GetContract(contractAddr)
	if raw == nil {
		return nil, errors.New("contract not found")
	}
	meta := proto.Clone(raw).(*contractpb.SmartContract)
	meta.Abi = nil
	ctx.State.SetContract(contractAddr, meta)
	return &Result{Fee: 0, ContractRet: 1}, nil
}
