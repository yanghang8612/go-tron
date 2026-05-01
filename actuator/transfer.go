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
	if ctx.DynProps.ForbidTransferToContract() && ctx.State.AccountExists(toAddr) {
		if len(ctx.State.GetCode(toAddr)) > 0 {
			return errors.New("cannot transfer TRX to a smart contract")
		}
	}
	fee := int64(0)
	if !ctx.State.AccountExists(toAddr) {
		fee = ctx.DynProps.CreateNewAccountFeeInSystemContract()
	}
	if ctx.State.GetBalance(ownerAddr) < tc.Amount+fee {
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

	fee := int64(0)
	if !ctx.State.AccountExists(toAddr) {
		ctx.State.CreateAccountWithTime(toAddr, corepb.AccountType_Normal, ctx.DynProps.LatestBlockHeaderTimestamp())
		if ctx.DynProps.AllowMultiSign() {
			ctx.State.ApplyDefaultAccountPermissions(toAddr, ctx.DynProps)
		}
		fee = ctx.DynProps.CreateNewAccountFeeInSystemContract()
		if err := burnFee(ctx, ownerAddr, fee); err != nil {
			return nil, err
		}
		ctx.DynProps.AddTotalCreateAccountCost(fee)
	}
	if err := ctx.State.SubBalance(ownerAddr, tc.Amount); err != nil {
		return nil, err
	}
	ctx.State.AddBalance(toAddr, tc.Amount)
	return &Result{Fee: fee, ContractRet: 1}, nil
}
