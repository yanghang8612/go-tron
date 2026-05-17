package actuator

import (
	"errors"

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
	if !ctx.DynProps.SupportCancelAllUnfreezeV2() {
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
	ownerAddr, err := checkedAddress(c.OwnerAddress, "address")
	if err != nil {
		return nil, err
	}
	acc := ctx.State.GetAccount(ownerAddr)

	cancelled := map[string]int64{
		"BANDWIDTH":  0,
		"ENERGY":     0,
		"TRON_POWER": 0,
	}
	var withdrawnExpired int64
	refreeze := func(resource corepb.ResourceCode, amount int64) {
		oldWeight := frozenV2WithDelegatedWeight(ctx.State, ownerAddr, resource)
		ctx.State.AddFreezeV2(ownerAddr, resource, amount)
		newWeight := frozenV2WithDelegatedWeight(ctx.State, ownerAddr, resource)
		addResourceWeight(ctx.DynProps, resource, newWeight-oldWeight)
	}
	for _, u := range acc.UnfrozenV2() {
		if u.UnfreezeExpireTime <= ctx.PrevBlockTime {
			withdrawnExpired += u.UnfreezeAmount
			continue
		}
		switch u.Type {
		case corepb.ResourceCode_BANDWIDTH:
			cancelled["BANDWIDTH"] += u.UnfreezeAmount
			refreeze(corepb.ResourceCode_BANDWIDTH, u.UnfreezeAmount)
		case corepb.ResourceCode_ENERGY:
			cancelled["ENERGY"] += u.UnfreezeAmount
			refreeze(corepb.ResourceCode_ENERGY, u.UnfreezeAmount)
		case corepb.ResourceCode_TRON_POWER:
			cancelled["TRON_POWER"] += u.UnfreezeAmount
			refreeze(corepb.ResourceCode_TRON_POWER, u.UnfreezeAmount)
		}
	}
	if withdrawnExpired > 0 {
		ctx.State.AddBalance(ownerAddr, withdrawnExpired)
	}

	// Clear unfreeze queue
	ctx.State.ClearUnfrozenV2(ownerAddr)

	return &Result{
		Fee:                    0,
		WithdrawExpireAmount:   withdrawnExpired,
		CancelUnfreezeV2Amount: cancelled,
		ContractRet:            1,
	}, nil
}
