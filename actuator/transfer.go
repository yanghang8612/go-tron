package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type TransferActuator struct{}

func (a *TransferActuator) getContract(ctx *Context) (*contractpb.TransferContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	tc := &contractpb.TransferContract{}
	if err := contract.Parameter.UnmarshalTo(tc); err != nil {
		return nil, errors.New("failed to unmarshal TransferContract")
	}
	return tc, nil
}

func (a *TransferActuator) Validate(ctx *Context) error {
	tc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(tc.OwnerAddress)
	toAddr := common.BytesToAddress(tc.ToAddress)
	if ownerAddr == toAddr {
		return errors.New("cannot transfer to self")
	}
	if tc.Amount <= 0 {
		return errors.New("transfer amount must be positive")
	}
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if ctx.State.GetBalance(ownerAddr) < tc.Amount {
		return errors.New("insufficient balance")
	}
	return nil
}

func (a *TransferActuator) Execute(ctx *Context) (*Result, error) {
	tc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(tc.OwnerAddress)
	toAddr := common.BytesToAddress(tc.ToAddress)

	if err := ctx.State.SubBalance(ownerAddr, tc.Amount); err != nil {
		return nil, err
	}
	if !ctx.State.AccountExists(toAddr) {
		ctx.State.CreateAccount(toAddr, corepb.AccountType_Normal)
	}
	ctx.State.AddBalance(toAddr, tc.Amount)
	return &Result{Fee: 0, ContractRet: 1}, nil
}
