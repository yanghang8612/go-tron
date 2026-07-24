package actuator

import (
	"errors"
	"math"

	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// UnfreezeAssetActuator handles TRC10 frozen supply release (contract type 14).
// Token issuers call this to claim pre-frozen supply after lock-up periods expire.
type UnfreezeAssetActuator struct{}

func (a *UnfreezeAssetActuator) getContract(ctx *Context) (*contractpb.UnfreezeAssetContract, error) {
	return decodedContract[*contractpb.UnfreezeAssetContract](ctx, "UnfreezeAssetContract")
}

func (a *UnfreezeAssetActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	owner, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return err
	}
	acct := ctx.State.GetAccount(owner)
	if acct == nil {
		return errors.New("owner account does not exist")
	}
	if len(acct.Proto().GetFrozenSupply()) == 0 {
		return errors.New("no frozen supply balance")
	}
	if ctx.DynProps.AllowSameTokenName() {
		if len(acct.Proto().GetAssetIssued_ID()) == 0 {
			return errors.New("owner account has not issued any asset")
		}
	} else if len(acct.Proto().GetAssetIssuedName()) == 0 {
		return errors.New("owner account has not issued any asset")
	}
	now := ctx.DynProps.LatestBlockHeaderTimestamp()
	for _, frozen := range acct.Proto().GetFrozenSupply() {
		if frozen.GetExpireTime() <= now {
			return nil
		}
	}
	return errors.New("no frozen supply is currently available to unfreeze")
}

func (a *UnfreezeAssetActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	owner, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return nil, err
	}
	acct := ctx.State.GetAccount(owner)
	if acct == nil {
		return nil, errors.New("owner account does not exist")
	}
	issued, err := issuedAssetRef(ctx, acct)
	if err != nil {
		return nil, err
	}
	tokenID := issued.TokenID
	name := issued.Name
	if ctx.DynProps.AllowSameTokenName() {
		name = []byte(acct.Proto().GetAssetIssued_ID())
	}
	amount := ctx.State.RemoveExpiredFrozenSupply(owner, ctx.DynProps.LatestBlockHeaderTimestamp())
	if amount <= 0 {
		return nil, errors.New("no frozen supply is currently available to unfreeze")
	}
	if ctx.State.GetTRC10BalanceFinal(owner, name, tokenID, ctx.DynProps.AllowSameTokenName()) > math.MaxInt64-amount {
		return nil, errors.New("TRC10 balance overflows int64")
	}
	ctx.State.AddTRC10BalanceFinal(owner, name, tokenID, amount, ctx.DynProps.AllowSameTokenName())

	return &Result{Fee: 0, ContractRet: 1}, nil
}
