package actuator

import (
	"bytes"
	"errors"
	"math"

	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

const withdrawCooldown = 86_400_000 // 24 hours in ms

type WithdrawBalanceActuator struct{}

func (a *WithdrawBalanceActuator) getContract(ctx *Context) (*contractpb.WithdrawBalanceContract, error) {
	return decodedContract[*contractpb.WithdrawBalanceContract](ctx, "WithdrawBalanceContract")
}

func (a *WithdrawBalanceActuator) Validate(ctx *Context) error {
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
	if ctx.DB != nil && isGenesisWitness(ctx, ownerAddr[:]) {
		return errors.New("guard representative is not allowed to withdraw balance")
	}
	lastWithdraw := ctx.State.GetLatestWithdrawTime(ownerAddr)
	cooldown := int64(withdrawCooldown)
	if ctx.DynProps != nil {
		cooldown = ctx.DynProps.WitnessAllowanceFrozenTime() * withdrawCooldown
	}
	if ctx.PrevBlockTime-lastWithdraw < cooldown {
		return errors.New("withdraw too frequent")
	}
	// Must either have existing allowance OR a pending voter reward to
	// settle. This replaces the old IsWitness-only check so that voters
	// claiming vote rewards (M1.5 new reward path) can also withdraw.
	if ctx.State.GetAllowance(ownerAddr) <= 0 &&
		queryReward(ctx.DB, ctx.State, ctx.DynProps, ownerAddr) <= 0 {
		return errors.New("no allowance to withdraw")
	}
	if ctx.State.GetBalance(ownerAddr) > math.MaxInt64-ctx.State.GetAllowance(ownerAddr) {
		return errors.New("integer overflow")
	}
	return nil
}

func isGenesisWitness(ctx *Context, owner []byte) bool {
	for _, w := range rawdb.ReadGenesisWitnesses(ctx.DB) {
		if bytes.Equal(w.Address[:], owner) {
			return true
		}
	}
	return false
}

func (a *WithdrawBalanceActuator) Execute(ctx *Context) (*Result, error) {
	wc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr, err := checkedAddress(wc.OwnerAddress, "address")
	if err != nil {
		return nil, err
	}

	// Settle any pending voter reward into allowance first (no-op when
	// change_delegation is off — voter rewards only apply on the new path).
	withdrawReward(ctx.DB, ctx.State, ctx.DynProps, ownerAddr)

	allowance := ctx.State.GetAllowance(ownerAddr)
	ctx.State.AddBalance(ownerAddr, allowance)
	ctx.State.SetAllowance(ownerAddr, 0)
	ctx.State.SetLatestWithdrawTime(ownerAddr, ctx.PrevBlockTime)
	return &Result{Fee: 0, WithdrawAmount: allowance, ContractRet: 1}, nil
}
