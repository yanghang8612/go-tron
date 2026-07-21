package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/core/delegation"
	"github.com/tronprotocol/go-tron/core/forks"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type UnDelegateResourceActuator struct{}

func (a *UnDelegateResourceActuator) getContract(ctx *Context) (*contractpb.UnDelegateResourceContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.UnDelegateResourceContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal UnDelegateResourceContract")
	}
	return c, nil
}

func (a *UnDelegateResourceActuator) Validate(ctx *Context) error {
	if !forks.IsActive(forks.AllowDelegateResource, ctx.BlockNumber, ctx.DynProps) {
		return errors.New("resource delegation not yet enabled")
	}
	if !ctx.DynProps.SupportUnfreezeDelay() {
		return errors.New("staking v2 not yet enabled")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr, err := checkedAddress(c.OwnerAddress, "address")
	if err != nil {
		return err
	}
	receiverAddr, err := checkedAddress(c.ReceiverAddress, "receiverAddress")
	if err != nil {
		return err
	}
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if ownerAddr == receiverAddr {
		return errors.New("receiverAddress must not be the same as ownerAddress")
	}
	if c.Balance <= 0 {
		return errors.New("undelegate balance must be positive")
	}
	unlockResource := ctx.State.ReadDelegatedResourceV2(ownerAddr, receiverAddr, false)
	lockResource := ctx.State.ReadDelegatedResourceV2(ownerAddr, receiverAddr, true)
	if unlockResource == nil && lockResource == nil {
		return errors.New("no delegation record found")
	}
	switch c.Resource {
	case corepb.ResourceCode_BANDWIDTH:
		delegateBalance := int64(0)
		if unlockResource != nil {
			delegateBalance += unlockResource.FrozenBalanceForBandwidth
		}
		if lockResource != nil && lockResource.ExpireTimeForBandwidth < ctx.PrevBlockTime {
			delegateBalance += lockResource.FrozenBalanceForBandwidth
		}
		if delegateBalance < c.Balance {
			return errors.New("insufficient delegated bandwidth balance")
		}
	case corepb.ResourceCode_ENERGY:
		delegateBalance := int64(0)
		if unlockResource != nil {
			delegateBalance += unlockResource.FrozenBalanceForEnergy
		}
		if lockResource != nil && lockResource.ExpireTimeForEnergy < ctx.PrevBlockTime {
			delegateBalance += lockResource.FrozenBalanceForEnergy
		}
		if delegateBalance < c.Balance {
			return errors.New("insufficient delegated energy balance")
		}
	default:
		return errors.New("invalid resource type")
	}
	return nil
}

func (a *UnDelegateResourceActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr, err := checkedAddress(c.OwnerAddress, "address")
	if err != nil {
		return nil, err
	}
	receiverAddr, err := checkedAddress(c.ReceiverAddress, "receiverAddress")
	if err != nil {
		return nil, err
	}

	// Mirrors java-tron UnDelegateResourceActuator.execute:
	// Before mutating balances, recover the receiver's current usage and
	// compute the portion of it attributable to the undelegated amount.
	// That transferUsage is deducted from the receiver and folded into the
	// owner's usage so neither party gets a free ride.
	resourceTime := ctx.ResourceTime()
	// tvmForm=false: this is the actuator path → java UnDelegateResourceActuator's
	// grouped (ub/TRX)*(limit/weight) unDelegateMaxUsage form.
	transferUsage, recvRawWindow, recvOptimized := delegation.TransferUsageFromReceiver(ctx.State, ctx.DynProps, receiverAddr, c.Resource, c.Balance, resourceTime, false)

	// Return to owner's frozen balance
	ctx.State.AddFreezeV2(ownerAddr, c.Resource, c.Balance)
	// Reduce outgoing delegation on owner
	ctx.State.SubDelegatedFrozenV2(ownerAddr, c.Resource, c.Balance)
	// Reduce incoming delegation on receiver
	ctx.State.SubAcquiredDelegatedFrozenV2(receiverAddr, c.Resource, c.Balance)

	// java gates the owner-side unDelegateIncrease on
	// `Objects.nonNull(receiverCapsule) && transferUsage > 0` — when the receiver
	// transferred no usage (it never spent the delegated resource, the proportional
	// share rounds to 0, the suicide/recreate guard fired, or the receiver account
	// is gone) the owner's usage/window/latest_consume_time are left untouched.
	// transferUsage is already 0 when the receiver account was nil, so the
	// receiver-nonNull half of java's guard is subsumed by transferUsage > 0.
	if transferUsage > 0 {
		delegation.FoldUsageIntoOwner(ctx.State, ctx.DynProps, ownerAddr, c.Resource, transferUsage, recvRawWindow, recvOptimized, resourceTime)
	}

	// Update delegation record
	if err := ctx.State.UnlockExpiredDelegatedResource(ownerAddr, receiverAddr, ctx.PrevBlockTime); err != nil {
		return nil, err
	}
	dr := ctx.State.ReadDelegatedResourceV2(ownerAddr, receiverAddr, false)
	if dr == nil {
		return nil, errors.New("unlocked delegation record not found")
	}
	if c.Resource == corepb.ResourceCode_BANDWIDTH {
		dr.FrozenBalanceForBandwidth -= c.Balance
	} else {
		dr.FrozenBalanceForEnergy -= c.Balance
	}

	if dr.FrozenBalanceForBandwidth <= 0 && dr.FrozenBalanceForEnergy <= 0 {
		if err := ctx.State.DeleteDelegatedResourceV2(ownerAddr, receiverAddr, false); err != nil {
			return nil, err
		}
	} else {
		if err := ctx.State.WriteDelegatedResourceV2(ownerAddr, receiverAddr, false, dr); err != nil {
			return nil, err
		}
	}

	if ctx.State.ReadDelegatedResourceV2(ownerAddr, receiverAddr, false) == nil &&
		ctx.State.ReadDelegatedResourceV2(ownerAddr, receiverAddr, true) == nil {
		if err := ctx.State.WriteDrAccountIndexUnDelegate(true, ownerAddr[:], receiverAddr[:]); err != nil {
			return nil, err
		}
	}

	return &Result{Fee: 0, ContractRet: 1}, nil
}
