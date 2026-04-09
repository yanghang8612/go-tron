package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type CreateAccountActuator struct{}

func (a *CreateAccountActuator) getContract(ctx *Context) (*contractpb.AccountCreateContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	ac := &contractpb.AccountCreateContract{}
	if err := contract.Parameter.UnmarshalTo(ac); err != nil {
		return nil, errors.New("failed to unmarshal AccountCreateContract")
	}
	return ac, nil
}

func (a *CreateAccountActuator) Validate(ctx *Context) error {
	ac, err := a.getContract(ctx)
	if err != nil {
		return err
	}

	ownerAddr := common.BytesToAddress(ac.OwnerAddress)
	newAddr := common.BytesToAddress(ac.AccountAddress)

	if ownerAddr.IsEmpty() || newAddr.IsEmpty() {
		return errors.New("invalid address")
	}

	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	if ownerAcc == nil {
		return errors.New("owner account does not exist")
	}

	if rawdb.HasAccount(ctx.DB, newAddr) {
		return errors.New("account already exists")
	}

	return nil
}

func (a *CreateAccountActuator) Execute(ctx *Context) (*Result, error) {
	ac, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}

	newAddr := common.BytesToAddress(ac.AccountAddress)
	accType := ac.Type
	if accType == 0 {
		accType = corepb.AccountType_Normal
	}
	newAcc := types.NewAccount(newAddr, accType)
	rawdb.WriteAccount(ctx.DB, newAddr, newAcc)

	return &Result{Fee: 0}, nil
}
