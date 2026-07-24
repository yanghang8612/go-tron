package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type UnfreezeBalanceActuator struct{}

func (a *UnfreezeBalanceActuator) getContract(ctx *Context) (*contractpb.UnfreezeBalanceContract, error) {
	return decodedContract[*contractpb.UnfreezeBalanceContract](ctx, "UnfreezeBalanceContract")
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
	if !delegated {
		switch uc.Resource {
		case corepb.ResourceCode_BANDWIDTH:
			if !ctx.State.HasExpiredFrozenV1Bandwidth(ownerAddr, ctx.PrevBlockTime) {
				return errors.New("no expired frozen bandwidth")
			}
		case corepb.ResourceCode_TRON_POWER:
			if !ctx.DynProps.AllowNewResourceModel() {
				return errors.New("invalid resource type")
			}
			amount, expireTime := ctx.State.FrozenV1ResourceInfo(ownerAddr, corepb.ResourceCode_TRON_POWER)
			if amount <= 0 {
				return errors.New("no frozenBalance(TronPower)")
			}
			if expireTime > ctx.PrevBlockTime {
				return errors.New("It's not time to unfreeze(TronPower).")
			}
		case corepb.ResourceCode_ENERGY:
			amount, expireTime := ctx.State.FrozenV1ResourceInfo(ownerAddr, corepb.ResourceCode_ENERGY)
			if amount == 0 {
				return errors.New("no frozen energy")
			}
			if expireTime > ctx.PrevBlockTime {
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
			// java-tron's DelegatedResourceCapsule historically returned the
			// bandwidth expiry for delegated energy until ALLOW_MULTI_SIGN was
			// enabled. Preserve that proposal-gated consensus behavior.
			expireTime := dr.ExpireTimeForEnergy
			if !ctx.DynProps.AllowMultiSign() {
				expireTime = dr.ExpireTimeForBandwidth
			}
			if expireTime > ctx.PrevBlockTime {
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
			ctx.State.UnfreezeV1DelegatedOwner(ownerAddr, removed, corepb.ResourceCode_BANDWIDTH)
		case corepb.ResourceCode_ENERGY:
			removed = dr.FrozenBalanceForEnergy
			dr.FrozenBalanceForEnergy = 0
			dr.ExpireTimeForEnergy = 0
			ctx.State.UnfreezeV1DelegatedOwner(ownerAddr, removed, corepb.ResourceCode_ENERGY)
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
	if delegated {
		// Mirror java-tron UnfreezeBalanceActuator's delegated branch. The
		// owner's delegated balance was already decremented above. The
		// receiver's acquired balance is touched ONLY when it is not a Contract
		// under allow_tvm_constantinople — otherwise java leaves the receiver's
		// acquired balance untouched (it accumulates). The weight delta is the
		// exact newWeight-oldWeight for a touched receiver, or -removed/TRX for
		// a skipped contract receiver. Under the floor model (pre allow_new_reward)
		// java always uses -removed/TRX.
		var decrease int64
		receiver := ctx.State.AccountReference(receiverAddr)
		// java UnfreezeBalanceActuator takes the floor branch
		// (decrease = -unfreezeBalance / TRX_PRECISION) whenever Constantinople is
		// active AND the receiver is either a Contract OR no longer exists (a
		// contract receiver that self-destructed). Only a live non-contract
		// receiver gets the carry-preserving acquired-weight delta. Routing a
		// missing receiver through DecrementReceiverAcquired (which returns 0)
		// under-releases the global weight and drifts total_energy_weight HIGH.
		// Completes a7fda66f (which only covered an existing Contract receiver);
		// effective only under allow_new_reward — below that the `weight =
		// -removed/TRX` floor override already matches java regardless.
		if ctx.DynProps.AllowTvmConstantinople() && (receiver == nil || receiver.Type() == corepb.AccountType_Contract) {
			decrease = -removed / trxPrecisionActuator
		} else {
			decrease = ctx.State.DecrementReceiverAcquired(receiverAddr, removed, uc.Resource, ctx.DynProps.AllowTvmSolidity059())
		}
		weight := decrease
		if !ctx.DynProps.AllowNewReward() {
			weight = -removed / trxPrecisionActuator
		}
		addResourceWeight(ctx.DynProps, uc.Resource, weight)
	} else {
		newWeight := v1FrozenResourceWeight(ctx.State, ownerAddr, uc.Resource)
		addV1ResourceWeight(ctx.DynProps, uc.Resource, -removed, oldWeight, newWeight)
	}

	needToClearVote := true
	if ctx.DynProps.AllowNewResourceModel() {
		if account := ctx.State.AccountReference(ownerAddr); account != nil && account.OldTronPowerIsInvalid() {
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
		if account := ctx.State.AccountReference(ownerAddr); account != nil && !account.OldTronPowerIsInvalid() {
			ctx.State.InvalidateOldTronPower(ownerAddr)
		}
	}

	return &Result{Fee: 0, UnfreezeAmount: removed, ContractRet: 1}, nil
}
