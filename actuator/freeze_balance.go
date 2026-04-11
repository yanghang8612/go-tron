package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type FreezeBalanceActuator struct{}

func (a *FreezeBalanceActuator) getContract(ctx *Context) (*contractpb.FreezeBalanceContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	fc := &contractpb.FreezeBalanceContract{}
	if err := contract.Parameter.UnmarshalTo(fc); err != nil {
		return nil, errors.New("failed to unmarshal FreezeBalanceContract")
	}
	return fc, nil
}

func (a *FreezeBalanceActuator) Validate(ctx *Context) error {
	fc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(fc.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if fc.FrozenBalance < 1_000_000 {
		return errors.New("frozen balance must be at least 1 TRX")
	}
	if fc.FrozenDuration < 3 {
		return errors.New("frozen duration must be at least 3 days")
	}
	if ctx.State.GetBalance(ownerAddr) < fc.FrozenBalance {
		return errors.New("insufficient balance")
	}
	if fc.Resource != corepb.ResourceCode_BANDWIDTH &&
		fc.Resource != corepb.ResourceCode_ENERGY &&
		fc.Resource != corepb.ResourceCode_TRON_POWER {
		return errors.New("invalid resource type")
	}
	if len(fc.ReceiverAddress) > 0 {
		receiverAddr := common.BytesToAddress(fc.ReceiverAddress)
		if !ctx.State.AccountExists(receiverAddr) {
			return errors.New("receiver account does not exist")
		}
	}
	return nil
}

func (a *FreezeBalanceActuator) Execute(ctx *Context) (*Result, error) {
	fc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(fc.OwnerAddress)
	if err := ctx.State.SubBalance(ownerAddr, fc.FrozenBalance); err != nil {
		return nil, err
	}

	expireTimeMs := ctx.BlockTime + fc.FrozenDuration*86_400_000
	delegated := len(fc.ReceiverAddress) > 0

	if !delegated {
		switch fc.Resource {
		case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_TRON_POWER:
			ctx.State.FreezeV1Bandwidth(ownerAddr, fc.FrozenBalance, expireTimeMs)
		case corepb.ResourceCode_ENERGY:
			ctx.State.FreezeV1Energy(ownerAddr, fc.FrozenBalance, expireTimeMs)
		}
	} else {
		receiverAddr := common.BytesToAddress(fc.ReceiverAddress)
		switch fc.Resource {
		case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_TRON_POWER:
			ctx.State.FreezeV1DelegatedBandwidth(ownerAddr, receiverAddr, fc.FrozenBalance)
		case corepb.ResourceCode_ENERGY:
			ctx.State.FreezeV1DelegatedEnergy(ownerAddr, receiverAddr, fc.FrozenBalance)
		}
	}

	return &Result{Fee: 0, ContractRet: 1}, nil
}
