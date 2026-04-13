package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type WitnessCreateActuator struct{}

func (a *WitnessCreateActuator) getContract(ctx *Context) (*contractpb.WitnessCreateContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	wc := &contractpb.WitnessCreateContract{}
	if err := contract.Parameter.UnmarshalTo(wc); err != nil {
		return nil, errors.New("failed to unmarshal WitnessCreateContract")
	}
	return wc, nil
}

func (a *WitnessCreateActuator) Validate(ctx *Context) error {
	wc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(wc.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if ctx.State.GetWitness(ownerAddr) != nil {
		return errors.New("witness already exists")
	}
	if len(wc.Url) == 0 {
		return errors.New("witness URL is empty")
	}
	fee := ctx.DynProps.AccountUpgradeCost()
	if ctx.State.GetBalance(ownerAddr) < fee {
		return errors.New("insufficient balance for witness creation fee")
	}
	return nil
}

func (a *WitnessCreateActuator) Execute(ctx *Context) (*Result, error) {
	wc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(wc.OwnerAddress)
	fee := ctx.DynProps.AccountUpgradeCost()
	if err := ctx.State.SubBalance(ownerAddr, fee); err != nil {
		return nil, err
	}
	ctx.State.PutWitness(ownerAddr, string(wc.Url))
	ctx.State.SetIsWitness(ownerAddr, true)
	return &Result{Fee: fee, ContractRet: 1}, nil
}
