package actuator

import (
	"errors"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type DelegateResourceActuator struct{}

func (a *DelegateResourceActuator) getContract(ctx *Context) (*contractpb.DelegateResourceContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.DelegateResourceContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal DelegateResourceContract")
	}
	return c, nil
}

func (a *DelegateResourceActuator) Validate(ctx *Context) error {
	if !forks.IsActive(forks.AllowDelegateResource, ctx.BlockNumber, ctx.DynProps) {
		return errors.New("resource delegation not yet enabled")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	receiverAddr := tcommon.BytesToAddress(c.ReceiverAddress)
	if ownerAddr == receiverAddr {
		return errors.New("cannot delegate to self")
	}
	if c.Balance <= 0 {
		return errors.New("delegation balance must be positive")
	}
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if !ctx.State.AccountExists(receiverAddr) {
		return errors.New("receiver account does not exist")
	}
	if c.Resource != corepb.ResourceCode_BANDWIDTH && c.Resource != corepb.ResourceCode_ENERGY {
		return errors.New("invalid resource type")
	}
	frozen := ctx.State.GetFrozenV2Amount(ownerAddr, c.Resource)
	alreadyDelegated := ctx.State.GetDelegatedFrozenV2(ownerAddr, c.Resource)
	available := frozen - alreadyDelegated
	if available < c.Balance {
		return errors.New("insufficient frozen balance to delegate")
	}
	return nil
}

func (a *DelegateResourceActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	receiverAddr := tcommon.BytesToAddress(c.ReceiverAddress)

	// Subtract from owner's frozen balance
	ctx.State.ReduceFreezeV2(ownerAddr, c.Resource, c.Balance)
	// Track outgoing delegation on owner
	ctx.State.AddDelegatedFrozenV2(ownerAddr, c.Resource, c.Balance)
	// Track incoming delegation on receiver
	ctx.State.AddAcquiredDelegatedFrozenV2(receiverAddr, c.Resource, c.Balance)

	// Update delegation record in rawdb
	if ctx.DB != nil {
		dr := rawdb.ReadDelegatedResource(ctx.DB, ownerAddr, receiverAddr)
		if dr == nil {
			dr = &rawdb.DelegatedResource{From: ownerAddr, To: receiverAddr}
		}
		if c.Resource == corepb.ResourceCode_BANDWIDTH {
			dr.FrozenBalanceForBandwidth += c.Balance
			if c.Lock {
				dr.ExpireTimeForBandwidth = ctx.BlockTime + c.LockPeriod
			}
		} else {
			dr.FrozenBalanceForEnergy += c.Balance
			if c.Lock {
				dr.ExpireTimeForEnergy = ctx.BlockTime + c.LockPeriod
			}
		}
		rawdb.WriteDelegatedResource(ctx.DB, ownerAddr, receiverAddr, dr)

		// Update delegation index
		receivers := rawdb.ReadDelegationIndex(ctx.DB, ownerAddr)
		if !containsAddress(receivers, receiverAddr) {
			receivers = append(receivers, receiverAddr)
			rawdb.WriteDelegationIndex(ctx.DB, ownerAddr, receivers)
		}
	}

	return &Result{Fee: 0, ContractRet: 1}, nil
}
