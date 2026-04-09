package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
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

	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	if ownerAcc == nil {
		return errors.New("owner account does not exist")
	}

	if rawdb.ReadWitness(ctx.DB, ownerAddr) != nil {
		return errors.New("witness already exists")
	}

	if len(wc.Url) == 0 {
		return errors.New("witness URL is empty")
	}

	return nil
}

func (a *WitnessCreateActuator) Execute(ctx *Context) (*Result, error) {
	wc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}

	ownerAddr := common.BytesToAddress(wc.OwnerAddress)

	witness := types.NewWitness(ownerAddr, string(wc.Url))
	rawdb.WriteWitness(ctx.DB, ownerAddr, witness)

	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	ownerAcc.SetIsWitness(true)
	rawdb.WriteAccount(ctx.DB, ownerAddr, ownerAcc)

	return &Result{Fee: 0}, nil
}
