package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type FreezeBalanceV2Actuator struct{}

func (a *FreezeBalanceV2Actuator) getContract(ctx *Context) (*contractpb.FreezeBalanceV2Contract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	fc := &contractpb.FreezeBalanceV2Contract{}
	if err := contract.Parameter.UnmarshalTo(fc); err != nil {
		return nil, errors.New("failed to unmarshal FreezeBalanceV2Contract")
	}
	return fc, nil
}

func (a *FreezeBalanceV2Actuator) Validate(ctx *Context) error {
	fc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(fc.OwnerAddress)
	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	if ownerAcc == nil {
		return errors.New("owner account does not exist")
	}
	if fc.FrozenBalance <= 0 {
		return errors.New("frozen balance must be positive")
	}
	if ownerAcc.Balance() < fc.FrozenBalance {
		return errors.New("insufficient balance to freeze")
	}
	if fc.Resource != corepb.ResourceCode_BANDWIDTH && fc.Resource != corepb.ResourceCode_ENERGY {
		return errors.New("invalid resource type")
	}
	return nil
}

func (a *FreezeBalanceV2Actuator) Execute(ctx *Context) (*Result, error) {
	fc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(fc.OwnerAddress)
	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	ownerAcc.SetBalance(ownerAcc.Balance() - fc.FrozenBalance)
	ownerAcc.AddFreezeV2(fc.Resource, fc.FrozenBalance)
	rawdb.WriteAccount(ctx.DB, ownerAddr, ownerAcc)
	return &Result{Fee: 0}, nil
}
