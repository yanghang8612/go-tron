package actuator

import (
	"errors"

	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type WitnessCreateActuator struct{}

func (a *WitnessCreateActuator) getContract(ctx *Context) (*contractpb.WitnessCreateContract, error) {
	return decodedContract[*contractpb.WitnessCreateContract](ctx, "WitnessCreateContract")
}

func (a *WitnessCreateActuator) Validate(ctx *Context) error {
	wc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr, err := checkedAddress(wc.OwnerAddress, "ownerAddress")
	if err != nil {
		return err
	}
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if witnessExists(ctx, ownerAddr) {
		return errors.New("witness already exists")
	}
	if !validBytesLen(wc.Url, 256, false) {
		return errors.New("invalid witness URL")
	}
	fee := ctx.DynProps.AccountUpgradeCost()
	if ctx.State.GetBalance(ownerAddr) < fee {
		return errors.New("insufficient balance for witness creation fee")
	}
	return nil
}

func (a *WitnessCreateActuator) Execute(ctx *Context) (*Result, error) {
	wc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr, err := checkedAddress(wc.OwnerAddress, "ownerAddress")
	if err != nil {
		return nil, err
	}
	fee := ctx.DynProps.AccountUpgradeCost()
	if err := burnFee(ctx, ownerAddr, fee); err != nil {
		return nil, err
	}
	ctx.DynProps.AddTotalCreateWitnessCost(fee)
	ctx.State.PutWitness(ownerAddr, string(wc.Url))
	ctx.State.SetIsWitness(ownerAddr, true)

	// The witness index is rooted: append it through ctx.State — the same
	// *StateDB maintenance reads later this block — so the new SR is visible to
	// gatherWitnessVotes, is journaled, and rewinds with the root.
	if err := ctx.State.AppendWitnessIndex(ownerAddr); err != nil {
		return nil, err
	}
	// M11.5 slice 2a: java-tron AccountCapsule.setDefaultWitnessPermission,
	// gated on AllowMultiSign (WitnessCreateActuator.java:137).
	if ctx.DynProps.AllowMultiSign() {
		ctx.State.ApplyWitnessPermissions(ownerAddr, ctx.DynProps)
	}
	return &Result{Fee: fee, ContractRet: 1}, nil
}
