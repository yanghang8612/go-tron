package actuator

import (
	"errors"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type CancelAllUnfreezeV2Actuator struct{}

func (a *CancelAllUnfreezeV2Actuator) getContract(ctx *Context) (*contractpb.CancelAllUnfreezeV2Contract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.CancelAllUnfreezeV2Contract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal CancelAllUnfreezeV2Contract")
	}
	return c, nil
}

func (a *CancelAllUnfreezeV2Actuator) Validate(ctx *Context) error {
	if !forks.IsActive(forks.AllowStakingV2, ctx.BlockNumber, ctx.DynProps) {
		return errors.New("staking v2 not yet enabled")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if ctx.State.UnfreezeV2Count(ownerAddr) == 0 {
		return errors.New("no pending unfreeze entries")
	}
	return nil
}

func (a *CancelAllUnfreezeV2Actuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	acc := ctx.State.GetAccount(ownerAddr)

	// Sum pending unfreezes by resource type
	var bwTotal, energyTotal int64
	for _, u := range acc.UnfrozenV2() {
		if u.Type == corepb.ResourceCode_BANDWIDTH {
			bwTotal += u.UnfreezeAmount
		} else {
			energyTotal += u.UnfreezeAmount
		}
	}

	// Re-freeze
	if bwTotal > 0 {
		ctx.State.AddFreezeV2(ownerAddr, corepb.ResourceCode_BANDWIDTH, bwTotal)
	}
	if energyTotal > 0 {
		ctx.State.AddFreezeV2(ownerAddr, corepb.ResourceCode_ENERGY, energyTotal)
	}

	// Clear unfreeze queue
	ctx.State.ClearUnfrozenV2(ownerAddr)

	return &Result{Fee: 0, ContractRet: 1}, nil
}
