package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

const dayMs = int64(86_400_000) // milliseconds per day

// UnfreezeAssetActuator handles TRC10 frozen supply release (contract type 14).
// Token issuers call this to claim pre-frozen supply after lock-up periods expire.
type UnfreezeAssetActuator struct{}

func (a *UnfreezeAssetActuator) getContract(ctx *Context) (*contractpb.UnfreezeAssetContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.UnfreezeAssetContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal UnfreezeAssetContract")
	}
	return c, nil
}

// eligibleCount returns the number of frozen_supply entries that can be claimed.
func (a *UnfreezeAssetActuator) eligibleCount(ctx *Context, owner common.Address, tokenID int64, asset *contractpb.AssetIssueContract, issueTime int64) int {
	count := 0
	for i, f := range asset.FrozenSupply {
		if issueTime+f.FrozenDays*dayMs > ctx.BlockTime {
			continue
		}
		if ctx.State.IsFrozenClaimed(owner, tokenID, uint32(i)) {
			continue
		}
		count++
	}
	return count
}

func (a *UnfreezeAssetActuator) Validate(ctx *Context) error {
	if ctx.DB == nil {
		return errors.New("DB not available")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	owner := common.BytesToAddress(c.OwnerAddress)
	tokenID, ok := rawdb.ReadAssetOwnerIndex(ctx.DB, owner[:])
	if !ok {
		return errors.New("no token issued by this address")
	}
	asset := rawdb.ReadAssetIssue(ctx.DB, tokenID)
	if asset == nil {
		return errors.New("token not found")
	}
	if len(asset.FrozenSupply) == 0 {
		return errors.New("token has no frozen supply")
	}
	issueTime := rawdb.ReadAssetIssueTime(ctx.DB, tokenID)
	if a.eligibleCount(ctx, owner, tokenID, asset, issueTime) == 0 {
		return errors.New("no frozen supply is currently available to unfreeze")
	}
	return nil
}

func (a *UnfreezeAssetActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	owner := common.BytesToAddress(c.OwnerAddress)
	tokenID, ok := rawdb.ReadAssetOwnerIndex(ctx.DB, owner[:])
	if !ok {
		return nil, errors.New("no token issued by this address")
	}
	asset := rawdb.ReadAssetIssue(ctx.DB, tokenID)
	if asset == nil {
		return nil, errors.New("token not found")
	}
	issueTime := rawdb.ReadAssetIssueTime(ctx.DB, tokenID)

	for i, f := range asset.FrozenSupply {
		if issueTime+f.FrozenDays*dayMs > ctx.BlockTime {
			continue
		}
		if ctx.State.IsFrozenClaimed(owner, tokenID, uint32(i)) {
			continue
		}
		ctx.State.AddTRC10Balance(owner, tokenID, f.FrozenAmount)
		ctx.State.SetFrozenClaimed(owner, tokenID, uint32(i))
	}

	return &Result{Fee: 0, ContractRet: 1}, nil
}
