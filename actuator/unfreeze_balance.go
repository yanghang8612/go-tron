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
	ownerAddr, err := checkedAddress(uc.OwnerAddress, "ownerAddress")
	if err != nil {
		return err
	}
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	delegated := len(uc.ReceiverAddress) > 0 && ctx.DynProps.AllowDelegateResource()
	acct := ctx.State.GetStateObject(ownerAddr)
	if acct == nil {
		return errors.New("account not found")
	}
	if !delegated {
		switch uc.Resource {
		case corepb.ResourceCode_BANDWIDTH:
			hasExpired := false
			for _, f := range acct.FrozenBandwidthList() {
				if f.ExpireTime <= ctx.PrevBlockTime {
					hasExpired = true
					break
				}
			}
			if !hasExpired {
				return errors.New("no expired frozen bandwidth")
			}
		case corepb.ResourceCode_TRON_POWER:
			if !ctx.DynProps.AllowNewResourceModel() {
				return errors.New("invalid resource type")
			}
			account := ctx.State.GetAccount(ownerAddr)
			if account == nil || account.V1TronPowerFrozen() <= 0 {
				return errors.New("no frozenBalance(TronPower)")
			}
			if account.V1TronPowerExpireTime() > ctx.PrevBlockTime {
				return errors.New("It's not time to unfreeze(TronPower).")
			}
		case corepb.ResourceCode_ENERGY:
			if acct.FrozenEnergyAmount() == 0 {
				return errors.New("no frozen energy")
			}
			if acct.FrozenEnergyExpireTime() > ctx.PrevBlockTime {
				return errors.New("frozen energy not expired")
			}
		default:
			return errors.New("invalid resource type")
		}
	} else {
		receiverAddr, err := checkedAddress(uc.ReceiverAddress, "receiverAddress")
		if err != nil {
			return err
		}
		if receiverAddr == ownerAddr {
			return errors.New("receiverAddress must not be the same as ownerAddress")
		}
		if !ctx.DynProps.AllowTvmConstantinople() && !ctx.State.AccountExists(receiverAddr) {
			return errors.New("receiver account does not exist")
		}
		dr := ctx.State.ReadDelegatedResourceLegacy(ownerAddr, receiverAddr)
		if dr == nil {
			return errors.New("delegated Resource does not exist")
		}
		switch uc.Resource {
		case corepb.ResourceCode_BANDWIDTH:
			if dr.FrozenBalanceForBandwidth <= 0 {
				return errors.New("no delegated frozen bandwidth")
			}
			if dr.ExpireTimeForBandwidth > ctx.PrevBlockTime {
				return errors.New("It's not time to unfreeze.")
			}
		case corepb.ResourceCode_ENERGY:
			if dr.FrozenBalanceForEnergy <= 0 {
				return errors.New("no delegated frozen energy")
			}
			if dr.ExpireTimeForEnergy > ctx.PrevBlockTime {
				return errors.New("It's not time to unfreeze.")
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
	delegated := len(uc.ReceiverAddress) > 0 && ctx.DynProps.AllowDelegateResource()

	withdrawReward(ctx.DB, ctx.State, ctx.DynProps, ownerAddr)

	var removed int64
	oldWeight := v1FrozenResourceWeight(ctx.State, ownerAddr, uc.Resource)
	var receiverAddr common.Address
	if delegated {
		receiverAddr = common.BytesToAddress(uc.ReceiverAddress)
		oldWeight = v1AcquiredDelegatedWeight(ctx.State, receiverAddr, uc.Resource)
	}
	if !delegated {
		switch uc.Resource {
		case corepb.ResourceCode_BANDWIDTH:
			removed = ctx.State.UnfreezeV1Bandwidth(ownerAddr, ctx.PrevBlockTime)
		case corepb.ResourceCode_ENERGY:
			removed = ctx.State.UnfreezeV1Energy(ownerAddr, ctx.PrevBlockTime)
		case corepb.ResourceCode_TRON_POWER:
			removed = ctx.State.UnfreezeV1TronPower(ownerAddr, ctx.PrevBlockTime)
		}
		ctx.State.AddBalance(ownerAddr, removed)
	} else {
		dr := ctx.State.ReadDelegatedResourceLegacy(ownerAddr, receiverAddr)
		if dr == nil {
			return nil, errors.New("delegated Resource does not exist")
		}
		switch uc.Resource {
		case corepb.ResourceCode_BANDWIDTH:
			removed = dr.FrozenBalanceForBandwidth
			dr.FrozenBalanceForBandwidth = 0
			dr.ExpireTimeForBandwidth = 0
			ctx.State.UnfreezeV1DelegatedBandwidth(ownerAddr, receiverAddr, removed)
		case corepb.ResourceCode_ENERGY:
			removed = dr.FrozenBalanceForEnergy
			dr.FrozenBalanceForEnergy = 0
			dr.ExpireTimeForEnergy = 0
			ctx.State.UnfreezeV1DelegatedEnergy(ownerAddr, receiverAddr, removed)
		}
		if dr.FrozenBalanceForBandwidth == 0 && dr.FrozenBalanceForEnergy == 0 {
			if err := ctx.State.DeleteDelegatedResourceLegacy(ownerAddr, receiverAddr); err != nil {
				return nil, err
			}
			if ctx.DynProps.AllowDelegateOptimization() {
				if err := ctx.State.ConvertDrAccountIndexLegacy(ownerAddr[:]); err != nil {
					return nil, err
				}
				if err := ctx.State.ConvertDrAccountIndexLegacy(receiverAddr[:]); err != nil {
					return nil, err
				}
				if err := ctx.State.WriteDrAccountIndexUnDelegate(false, ownerAddr[:], receiverAddr[:]); err != nil {
					return nil, err
				}
			} else if err := ctx.State.WriteDrAccountIndexLegacyUnDelegate(ownerAddr[:], receiverAddr[:]); err != nil {
				return nil, err
			}
		} else if err := ctx.State.WriteDelegatedResourceLegacy(ownerAddr, receiverAddr, dr); err != nil {
			return nil, err
		}
		ctx.State.AddBalance(ownerAddr, removed)
	}

	// Shrink global weight by the amount returned to liquid balance.
	// Intentionally NOT gated on allow_new_resource_model — historical V1
	// unfreezes must stay reachable post-fork.
	newWeight := v1FrozenResourceWeight(ctx.State, ownerAddr, uc.Resource)
	if delegated {
		newWeight = v1AcquiredDelegatedWeight(ctx.State, receiverAddr, uc.Resource)
	}
	traceWeightEvent(ctx.BlockNumber, ownerAddr, receiverAddr, delegated, uc.Resource, -removed)
	addV1ResourceWeight(ctx.DynProps, uc.Resource, -removed, oldWeight, newWeight)

	needToClearVote := true
	if ctx.DynProps.AllowNewResourceModel() {
		if account := ctx.State.GetAccount(ownerAddr); account != nil && account.OldTronPowerIsInvalid() {
			switch uc.Resource {
			case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_ENERGY:
				needToClearVote = false
			}
		}
	}
	if needToClearVote {
		if oldVotes := ctx.State.GetVotes(ownerAddr); len(oldVotes) > 0 {
			if err := recordPendingVotes(ctx, ownerAddr, oldVotes, nil); err != nil {
				return nil, err
			}
		}
		ctx.State.ClearVotes(ownerAddr)
	}
	if ctx.DynProps.AllowNewResourceModel() {
		if account := ctx.State.GetAccount(ownerAddr); account != nil && !account.OldTronPowerIsInvalid() {
			ctx.State.InvalidateOldTronPower(ownerAddr)
		}
	}

	return &Result{Fee: 0, UnfreezeAmount: removed, ContractRet: 1}, nil
}
