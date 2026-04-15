package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type UnfreezeBalanceActuator struct{}

func (a *UnfreezeBalanceActuator) getContract(ctx *Context) (*contractpb.UnfreezeBalanceContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	uc := &contractpb.UnfreezeBalanceContract{}
	if err := contract.Parameter.UnmarshalTo(uc); err != nil {
		return nil, errors.New("failed to unmarshal UnfreezeBalanceContract")
	}
	return uc, nil
}

func (a *UnfreezeBalanceActuator) Validate(ctx *Context) error {
	uc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(uc.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	delegated := len(uc.ReceiverAddress) > 0
	acct := ctx.State.GetStateObject(ownerAddr)
	if acct == nil {
		return errors.New("account not found")
	}
	if !delegated {
		switch uc.Resource {
		case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_TRON_POWER:
			hasExpired := false
			for _, f := range acct.FrozenBandwidthList() {
				if f.ExpireTime <= ctx.BlockTime {
					hasExpired = true
					break
				}
			}
			if !hasExpired {
				return errors.New("no expired frozen bandwidth")
			}
		case corepb.ResourceCode_ENERGY:
			if acct.FrozenEnergyAmount() == 0 {
				return errors.New("no frozen energy")
			}
			if acct.FrozenEnergyExpireTime() > ctx.BlockTime {
				return errors.New("frozen energy not expired")
			}
		default:
			return errors.New("invalid resource type")
		}
	} else {
		switch uc.Resource {
		case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_TRON_POWER:
			if acct.DelegatedFrozenBandwidth() <= 0 {
				return errors.New("no delegated frozen bandwidth")
			}
		case corepb.ResourceCode_ENERGY:
			if acct.DelegatedFrozenEnergy() <= 0 {
				return errors.New("no delegated frozen energy")
			}
		default:
			return errors.New("invalid resource type")
		}
	}
	return nil
}

func (a *UnfreezeBalanceActuator) Execute(ctx *Context) (*Result, error) {
	uc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(uc.OwnerAddress)
	delegated := len(uc.ReceiverAddress) > 0

	var removed int64
	if !delegated {
		switch uc.Resource {
		case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_TRON_POWER:
			removed = ctx.State.UnfreezeV1Bandwidth(ownerAddr, ctx.BlockTime)
		case corepb.ResourceCode_ENERGY:
			removed = ctx.State.UnfreezeV1Energy(ownerAddr, ctx.BlockTime)
		}
		ctx.State.AddBalance(ownerAddr, removed)
	} else {
		receiverAddr := common.BytesToAddress(uc.ReceiverAddress)
		switch uc.Resource {
		case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_TRON_POWER:
			removed = ctx.State.GetDelegatedFrozenV1Bandwidth(ownerAddr)
			ctx.State.UnfreezeV1DelegatedBandwidth(ownerAddr, receiverAddr, removed)
		case corepb.ResourceCode_ENERGY:
			removed = ctx.State.GetDelegatedFrozenV1Energy(ownerAddr)
			ctx.State.UnfreezeV1DelegatedEnergy(ownerAddr, receiverAddr, removed)
		}
		ctx.State.AddBalance(ownerAddr, removed)
	}

	// Shrink global weight by the amount returned to liquid balance.
	// Intentionally NOT gated on allow_new_resource_model — historical V1
	// unfreezes must stay reachable post-fork.
	addV1ResourceWeight(ctx.DynProps, uc.Resource, -removed)

	return &Result{Fee: 0, ContractRet: 1}, nil
}
