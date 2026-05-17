package actuator

import (
	"errors"
	"math"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type WithdrawExpireUnfreezeActuator struct{}

func (a *WithdrawExpireUnfreezeActuator) getContract(ctx *Context) (*contractpb.WithdrawExpireUnfreezeContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	wc := &contractpb.WithdrawExpireUnfreezeContract{}
	if err := contract.Parameter.UnmarshalTo(wc); err != nil {
		return nil, errors.New("failed to unmarshal WithdrawExpireUnfreezeContract")
	}
	return wc, nil
}

func (a *WithdrawExpireUnfreezeActuator) Validate(ctx *Context) error {
	if !ctx.DynProps.SupportUnfreezeDelay() {
		return errors.New("staking v2 not yet enabled")
	}
	wc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr, err := checkedAddress(wc.OwnerAddress, "address")
	if err != nil {
		return err
	}
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	acc := ctx.State.GetAccount(ownerAddr)
	hasExpired := false
	for _, u := range acc.UnfrozenV2() {
		if u.UnfreezeExpireTime <= ctx.PrevBlockTime {
			hasExpired = true
			break
		}
	}
	if !hasExpired {
		return errors.New("no expired unfreeze entries")
	}
	if ctx.State.GetBalance(ownerAddr) > math.MaxInt64-expiredUnfreezeV2Amount(acc, ctx.PrevBlockTime) {
		return errors.New("integer overflow")
	}
	return nil
}

func (a *WithdrawExpireUnfreezeActuator) Execute(ctx *Context) (*Result, error) {
	wc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr, err := checkedAddress(wc.OwnerAddress, "address")
	if err != nil {
		return nil, err
	}
	withdrawn := ctx.State.RemoveExpiredUnfreezeV2(ownerAddr, ctx.PrevBlockTime)
	ctx.State.AddBalance(ownerAddr, withdrawn)
	return &Result{Fee: 0, WithdrawExpireAmount: withdrawn, ContractRet: 1}, nil
}

func expiredUnfreezeV2Amount(acc interface {
	UnfrozenV2() []*corepb.Account_UnFreezeV2
}, now int64) int64 {
	var total int64
	for _, u := range acc.UnfrozenV2() {
		if u.UnfreezeExpireTime <= now {
			total += u.UnfreezeAmount
		}
	}
	return total
}
