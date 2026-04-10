package actuator

import (
	"errors"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
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
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	receiverAddr := tcommon.BytesToAddress(c.ReceiverAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if c.Balance <= 0 {
		return errors.New("undelegate balance must be positive")
	}
	if ctx.DB == nil {
		return errors.New("database not available")
	}
	dr := rawdb.ReadDelegatedResource(ctx.DB, ownerAddr, receiverAddr)
	if dr == nil {
		return errors.New("no delegation record found")
	}
	if c.Resource == corepb.ResourceCode_BANDWIDTH {
		if dr.FrozenBalanceForBandwidth < c.Balance {
			return errors.New("insufficient delegated bandwidth balance")
		}
		if dr.ExpireTimeForBandwidth > ctx.BlockTime {
			return errors.New("delegation is still locked")
		}
	} else {
		if dr.FrozenBalanceForEnergy < c.Balance {
			return errors.New("insufficient delegated energy balance")
		}
		if dr.ExpireTimeForEnergy > ctx.BlockTime {
			return errors.New("delegation is still locked")
		}
	}
	return nil
}

func (a *UnDelegateResourceActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	receiverAddr := tcommon.BytesToAddress(c.ReceiverAddress)

	// Return to owner's frozen balance
	ctx.State.AddFreezeV2(ownerAddr, c.Resource, c.Balance)
	// Reduce outgoing delegation on owner
	ctx.State.SubDelegatedFrozenV2(ownerAddr, c.Resource, c.Balance)
	// Reduce incoming delegation on receiver
	ctx.State.SubAcquiredDelegatedFrozenV2(receiverAddr, c.Resource, c.Balance)

	// Update delegation record
	dr := rawdb.ReadDelegatedResource(ctx.DB, ownerAddr, receiverAddr)
	if c.Resource == corepb.ResourceCode_BANDWIDTH {
		dr.FrozenBalanceForBandwidth -= c.Balance
	} else {
		dr.FrozenBalanceForEnergy -= c.Balance
	}

	if dr.FrozenBalanceForBandwidth <= 0 && dr.FrozenBalanceForEnergy <= 0 {
		rawdb.DeleteDelegatedResource(ctx.DB, ownerAddr, receiverAddr)
		// Remove from delegation index
		receivers := rawdb.ReadDelegationIndex(ctx.DB, ownerAddr)
		receivers = removeAddress(receivers, receiverAddr)
		rawdb.WriteDelegationIndex(ctx.DB, ownerAddr, receivers)
	} else {
		rawdb.WriteDelegatedResource(ctx.DB, ownerAddr, receiverAddr, dr)
	}

	return &Result{Fee: 0, ContractRet: 1}, nil
}
