package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
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

	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	if ownerAcc == nil {
		return errors.New("owner account does not exist")
	}
	if ownerAcc.Balance() < tc.Amount {
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

	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	ownerAcc.SetBalance(ownerAcc.Balance() - tc.Amount)
	rawdb.WriteAccount(ctx.DB, ownerAddr, ownerAcc)

	toAcc := rawdb.ReadAccount(ctx.DB, toAddr)
	if toAcc == nil {
		toAcc = types.NewAccount(toAddr, corepb.AccountType_Normal)
	}
	toAcc.SetBalance(toAcc.Balance() + tc.Amount)
	rawdb.WriteAccount(ctx.DB, toAddr, toAcc)

	return &Result{Fee: 0}, nil
}
