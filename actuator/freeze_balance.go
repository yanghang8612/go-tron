package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// trxPrecisionActuator is SUN per TRX — resource weights are in TRX.
const trxPrecisionActuator = 1_000_000

type FreezeBalanceActuator struct{}

func (a *FreezeBalanceActuator) getContract(ctx *Context) (*contractpb.FreezeBalanceContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	fc := &contractpb.FreezeBalanceContract{}
	if err := contract.Parameter.UnmarshalTo(fc); err != nil {
		return nil, errors.New("failed to unmarshal FreezeBalanceContract")
	}
	return fc, nil
}

func (a *FreezeBalanceActuator) Validate(ctx *Context) error {
	// Once the V2 resource model is active (proposal #62), V1 freeze is
	// closed. Mirror java-tron's FreezeBalanceActuator.validate.
	if forks.IsActive(forks.AllowNewResourceModel, ctx.BlockNumber, ctx.DynProps) {
		return errors.New("freeze v2 is open, old freeze is closed")
	}
	fc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(fc.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if fc.FrozenBalance < 1_000_000 {
		return errors.New("frozen balance must be at least 1 TRX")
	}
	if fc.FrozenDuration < 3 {
		return errors.New("frozen duration must be at least 3 days")
	}
	if ctx.State.GetBalance(ownerAddr) < fc.FrozenBalance {
		return errors.New("insufficient balance")
	}
	if fc.Resource != corepb.ResourceCode_BANDWIDTH &&
		fc.Resource != corepb.ResourceCode_ENERGY &&
		fc.Resource != corepb.ResourceCode_TRON_POWER {
		return errors.New("invalid resource type")
	}
	if len(fc.ReceiverAddress) > 0 {
		receiverAddr := common.BytesToAddress(fc.ReceiverAddress)
		if !ctx.State.AccountExists(receiverAddr) {
			return errors.New("receiver account does not exist")
		}
	}
	return nil
}

func (a *FreezeBalanceActuator) Execute(ctx *Context) (*Result, error) {
	fc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(fc.OwnerAddress)
	if err := ctx.State.SubBalance(ownerAddr, fc.FrozenBalance); err != nil {
		return nil, err
	}

	expireTimeMs := ctx.BlockTime + fc.FrozenDuration*86_400_000
	delegated := len(fc.ReceiverAddress) > 0

	if !delegated {
		switch fc.Resource {
		case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_TRON_POWER:
			ctx.State.FreezeV1Bandwidth(ownerAddr, fc.FrozenBalance, expireTimeMs)
		case corepb.ResourceCode_ENERGY:
			ctx.State.FreezeV1Energy(ownerAddr, fc.FrozenBalance, expireTimeMs)
		}
	} else {
		receiverAddr := common.BytesToAddress(fc.ReceiverAddress)
		switch fc.Resource {
		case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_TRON_POWER:
			ctx.State.FreezeV1DelegatedBandwidth(ownerAddr, receiverAddr, fc.FrozenBalance)
		case corepb.ResourceCode_ENERGY:
			ctx.State.FreezeV1DelegatedEnergy(ownerAddr, receiverAddr, fc.FrozenBalance)
		}
	}

	// Weight accounting mirrors java-tron's FreezeBalanceActuator.addTotalWeight:
	// under allow_new_reward the delta is net of this freeze, otherwise the
	// full frozen amount in TRX is added. Delegated flows accumulate the same
	// weight on the delegator's side (java tracks this via the delegator
	// account's frozen list).
	addV1ResourceWeight(ctx.DynProps, fc.Resource, fc.FrozenBalance)

	return &Result{Fee: 0, ContractRet: 1}, nil
}

// addV1ResourceWeight adjusts total_{net,energy,tron_power}_weight for a V1
// freeze of `frozenBalance` SUN. Handles both allow_new_reward paths. The
// delta is positive for freeze, negative for unfreeze (caller passes a
// negative frozenBalance).
func addV1ResourceWeight(dp *state.DynamicProperties, resource corepb.ResourceCode, frozenBalance int64) {
	weight := frozenBalance / trxPrecisionActuator
	// Under allow_new_reward java uses (newFrozenTotal/TRX - oldFrozenTotal/TRX)
	// as the increment, which differs from frozenBalance/TRX only when the
	// integer division truncates at the boundary. For the V1 path touched
	// here that happens only on sub-TRX dust — which Validate already rejects
	// (frozenBalance < 1_000_000). So the two paths are equivalent.
	switch resource {
	case corepb.ResourceCode_BANDWIDTH:
		dp.AddTotalNetWeight(weight)
	case corepb.ResourceCode_ENERGY:
		dp.AddTotalEnergyWeight(weight)
	case corepb.ResourceCode_TRON_POWER:
		dp.AddTotalTronPowerWeight(weight)
	}
}
