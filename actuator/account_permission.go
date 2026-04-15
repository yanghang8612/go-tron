package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type AccountPermissionUpdateActuator struct{}

func (a *AccountPermissionUpdateActuator) getContract(ctx *Context) (*contractpb.AccountPermissionUpdateContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.AccountPermissionUpdateContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal AccountPermissionUpdateContract")
	}
	return c, nil
}

func (a *AccountPermissionUpdateActuator) Validate(ctx *Context) error {
	if !forks.IsActive(forks.AllowMultiSign, ctx.BlockNumber, ctx.DynProps) {
		return errors.New("multi-sign not yet enabled")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if c.Owner == nil {
		return errors.New("owner permission is required")
	}
	if err := validatePermission(c.Owner); err != nil {
		return err
	}
	if c.Witness != nil {
		if ctx.State.GetWitness(ownerAddr) == nil {
			return errors.New("witness permission requires witness account")
		}
		if err := validatePermission(c.Witness); err != nil {
			return err
		}
		if len(c.Witness.Keys) != 1 {
			return errors.New("witness permission must have exactly 1 key")
		}
	}
	if len(c.Actives) > 8 {
		return errors.New("too many active permissions (max 8)")
	}
	totalKeys := len(c.Owner.Keys)
	if c.Witness != nil {
		totalKeys += len(c.Witness.Keys)
	}
	for _, active := range c.Actives {
		if err := validatePermission(active); err != nil {
			return err
		}
		if len(active.Operations) > 0 && len(active.Operations) != 32 {
			return errors.New("active permission operations must be exactly 32 bytes")
		}
		totalKeys += len(active.Keys)
	}
	maxKeys := int(ctx.DynProps.TotalSignNum())
	if totalKeys > maxKeys {
		return errors.New("too many keys across all permissions")
	}
	fee := ctx.DynProps.UpdateAccountPermissionFee()
	if ctx.State.GetBalance(ownerAddr) < fee {
		return errors.New("insufficient balance for account permission update fee")
	}
	return nil
}

func validatePermission(p *corepb.Permission) error {
	if len(p.Keys) == 0 {
		return errors.New("permission must have at least 1 key")
	}
	if p.Threshold <= 0 {
		return errors.New("permission threshold must be positive")
	}
	var totalWeight int64
	for _, k := range p.Keys {
		totalWeight += k.Weight
	}
	if p.Threshold > totalWeight {
		return errors.New("permission threshold exceeds total key weight")
	}
	return nil
}

func (a *AccountPermissionUpdateActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	fee := ctx.DynProps.UpdateAccountPermissionFee()
	if fee > 0 {
		if err := ctx.State.SubBalance(ownerAddr, fee); err != nil {
			return nil, err
		}
	}
	ctx.State.SetPermissions(ownerAddr, c.Owner, c.Witness, c.Actives)
	return &Result{Fee: fee, ContractRet: 1}, nil
}
