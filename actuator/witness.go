package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type WitnessCreateActuator struct{}

func (a *WitnessCreateActuator) getContract(ctx *Context) (*contractpb.WitnessCreateContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	wc := &contractpb.WitnessCreateContract{}
	if err := contract.Parameter.UnmarshalTo(wc); err != nil {
		return nil, errors.New("failed to unmarshal WitnessCreateContract")
	}
	return wc, nil
}

func (a *WitnessCreateActuator) Validate(ctx *Context) error {
	wc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(wc.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if ctx.State.GetWitness(ownerAddr) != nil {
		return errors.New("witness already exists")
	}
	if len(wc.Url) == 0 {
		return errors.New("witness URL is empty")
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
	ownerAddr := common.BytesToAddress(wc.OwnerAddress)
	fee := ctx.DynProps.AccountUpgradeCost()
	if err := burnFee(ctx, ownerAddr, fee); err != nil {
		return nil, err
	}
	ctx.DynProps.AddTotalCreateWitnessCost(fee)
	ctx.State.PutWitness(ownerAddr, string(wc.Url))
	ctx.State.SetIsWitness(ownerAddr, true)

	// Persist the new witness record + index entry through ctx.DB so it
	// survives the block commit. Without this the in-memory s.witnesses map
	// is discarded after applyBlock and the new SR is invisible to the next
	// block's pre-load. Same per-actuator pattern used by WitnessUpdate.
	rawdb.WriteWitness(ctx.DB, ownerAddr, types.NewWitness(ownerAddr, string(wc.Url)))
	rawdb.AppendWitnessIndex(ctx.DB, ownerAddr)
	// M11.5 slice 2a: java-tron AccountCapsule.setDefaultWitnessPermission,
	// gated on AllowMultiSign (WitnessCreateActuator.java:137).
	if ctx.DynProps.AllowMultiSign() {
		ctx.State.ApplyWitnessPermissions(ownerAddr, ctx.DynProps)
	}
	return &Result{Fee: fee, ContractRet: 1}, nil
}
