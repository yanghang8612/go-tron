package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

const withdrawCooldown = 86400000 // 24 hours in ms

type WithdrawBalanceActuator struct{}

func (a *WithdrawBalanceActuator) getContract(ctx *Context) (*contractpb.WithdrawBalanceContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	wc := &contractpb.WithdrawBalanceContract{}
	if err := contract.Parameter.UnmarshalTo(wc); err != nil {
		return nil, errors.New("failed to unmarshal WithdrawBalanceContract")
	}
	return wc, nil
}

func (a *WithdrawBalanceActuator) Validate(ctx *Context) error {
	wc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(wc.OwnerAddress)
	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	if ownerAcc == nil {
		return errors.New("owner account does not exist")
	}
	if ownerAcc.Allowance() <= 0 {
		return errors.New("no allowance to withdraw")
	}
	if ctx.BlockTime-ownerAcc.LatestWithdrawTime() < withdrawCooldown {
		return errors.New("withdraw too frequent, must wait 24 hours")
	}
	return nil
}

func (a *WithdrawBalanceActuator) Execute(ctx *Context) (*Result, error) {
	wc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(wc.OwnerAddress)
	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	allowance := ownerAcc.Allowance()
	ownerAcc.SetBalance(ownerAcc.Balance() + allowance)
	ownerAcc.SetAllowance(0)
	ownerAcc.SetLatestWithdrawTime(ctx.BlockTime)
	rawdb.WriteAccount(ctx.DB, ownerAddr, ownerAcc)
	return &Result{Fee: 0}, nil
}
