package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
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
	fc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr, err := checkedAddress(fc.OwnerAddress, "address")
	if err != nil {
		return err
	}
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if fc.FrozenBalance < 1_000_000 {
		return errors.New("frozen balance must be at least 1 TRX")
	}
	if fc.FrozenDuration < ctx.DynProps.MinFrozenTime() || fc.FrozenDuration > ctx.DynProps.MaxFrozenTime() {
		return errors.New("frozen duration is out of range")
	}
	if ctx.State.GetBalance(ownerAddr) < fc.FrozenBalance {
		return errors.New("insufficient balance")
	}
	if acct := ctx.State.GetAccount(ownerAddr); acct != nil {
		if len(acct.FrozenBandwidthList()) > 1 {
			return errors.New("frozenCount must be 0 or 1")
		}
	}
	switch fc.Resource {
	case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_ENERGY:
	case corepb.ResourceCode_TRON_POWER:
		if !ctx.DynProps.AllowNewResourceModel() {
			return errors.New("ResourceCode error, valid ResourceCode[BANDWIDTH、ENERGY]")
		}
		if len(fc.ReceiverAddress) > 0 {
			return errors.New("TRON_POWER is not allowed to delegate to other accounts.")
		}
	default:
		return errors.New("invalid resource type")
	}

	if len(fc.ReceiverAddress) > 0 && ctx.DynProps.AllowDelegateResource() {
		receiverAddr, err := checkedAddress(fc.ReceiverAddress, "receiverAddress")
		if err != nil {
			return err
		}
		if receiverAddr == ownerAddr {
			return errors.New("receiverAddress must not be the same as ownerAddress")
		}
		if !ctx.State.AccountExists(receiverAddr) {
			return errors.New("receiver account does not exist")
		}
		if ctx.DynProps.AllowTvmConstantinople() {
			receiver := ctx.State.GetAccount(receiverAddr)
			if receiver != nil && receiver.Type() == corepb.AccountType_Contract {
				return errors.New("Do not allow delegate resources to contract addresses")
			}
		}
	}
	if ctx.DynProps.SupportUnfreezeDelay() {
		return errors.New("freeze v2 is open, old freeze is closed")
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

	expireTimeMs := ctx.PrevBlockTime + fc.FrozenDuration*86_400_000
	if ctx.DynProps.AllowNewResourceModel() {
		ctx.State.InitializeOldTronPowerIfNeeded(ownerAddr)
	}
	delegated := len(fc.ReceiverAddress) > 0 && ctx.DynProps.AllowDelegateResource() && fc.Resource != corepb.ResourceCode_TRON_POWER
	oldWeight := v1FrozenResourceWeight(ctx.State, ownerAddr, fc.Resource)
	var receiverAddr common.Address
	if delegated {
		receiverAddr = common.BytesToAddress(fc.ReceiverAddress)
		oldWeight = v1AcquiredDelegatedWeight(ctx.State, receiverAddr, fc.Resource)
	}

	if !delegated {
		switch fc.Resource {
		case corepb.ResourceCode_BANDWIDTH:
			ctx.State.FreezeV1Bandwidth(ownerAddr, fc.FrozenBalance, expireTimeMs)
		case corepb.ResourceCode_ENERGY:
			ctx.State.FreezeV1Energy(ownerAddr, fc.FrozenBalance, expireTimeMs)
		case corepb.ResourceCode_TRON_POWER:
			ctx.State.FreezeV1TronPower(ownerAddr, fc.FrozenBalance, expireTimeMs)
		}
	} else {
		dr := ctx.State.ReadDelegatedResourceLegacy(ownerAddr, receiverAddr)
		if dr == nil {
			dr = &rawdb.DelegatedResource{From: ownerAddr, To: receiverAddr}
		}
		switch fc.Resource {
		case corepb.ResourceCode_BANDWIDTH:
			ctx.State.FreezeV1DelegatedBandwidth(ownerAddr, receiverAddr, fc.FrozenBalance)
			dr.FrozenBalanceForBandwidth += fc.FrozenBalance
			dr.ExpireTimeForBandwidth = expireTimeMs
		case corepb.ResourceCode_ENERGY:
			ctx.State.FreezeV1DelegatedEnergy(ownerAddr, receiverAddr, fc.FrozenBalance)
			dr.FrozenBalanceForEnergy += fc.FrozenBalance
			dr.ExpireTimeForEnergy = expireTimeMs
		}
		if err := ctx.State.WriteDelegatedResourceLegacy(ownerAddr, receiverAddr, dr); err != nil {
			return nil, err
		}
		if ctx.DynProps.AllowDelegateOptimization() {
			if err := ctx.State.ConvertDrAccountIndexLegacy(ownerAddr[:]); err != nil {
				return nil, err
			}
			if err := ctx.State.ConvertDrAccountIndexLegacy(receiverAddr[:]); err != nil {
				return nil, err
			}
			if err := ctx.State.WriteDrAccountIndexDelegate(false, ownerAddr[:], receiverAddr[:], ctx.PrevBlockTime); err != nil {
				return nil, err
			}
		} else if err := ctx.State.WriteDrAccountIndexLegacyDelegate(ownerAddr[:], receiverAddr[:]); err != nil {
			return nil, err
		}
	}

	newWeight := v1FrozenResourceWeight(ctx.State, ownerAddr, fc.Resource)
	if delegated {
		newWeight = v1AcquiredDelegatedWeight(ctx.State, receiverAddr, fc.Resource)
	}
	traceWeightEvent(ctx.BlockNumber, ownerAddr, receiverAddr, delegated, fc.Resource, fc.FrozenBalance)
	addV1ResourceWeight(ctx.DynProps, fc.Resource, fc.FrozenBalance, oldWeight, newWeight)

	return &Result{Fee: 0, ContractRet: 1}, nil
}

// addV1ResourceWeight mirrors java-tron's FreezeBalanceActuator.addTotalWeight:
// before allow_new_reward the delta is this operation's amount / TRX; after
// allow_new_reward it is the exact old/new total weight difference, preserving
// fractional-TRX carry from prior freezes.
func addV1ResourceWeight(dp *state.DynamicProperties, resource corepb.ResourceCode, frozenBalance, oldWeight, newWeight int64) {
	weight := frozenBalance / trxPrecisionActuator
	if dp.AllowNewReward() {
		weight = newWeight - oldWeight
	}
	addResourceWeight(dp, resource, weight)
}

// addResourceWeight applies weight to total_{net,energy,tron_power}_weight
// based on resource type. Shared between V1 and V2 freeze paths.
func addResourceWeight(dp *state.DynamicProperties, resource corepb.ResourceCode, weight int64) {
	switch resource {
	case corepb.ResourceCode_BANDWIDTH:
		dp.AddTotalNetWeight(weight)
	case corepb.ResourceCode_ENERGY:
		dp.AddTotalEnergyWeight(weight)
	case corepb.ResourceCode_TRON_POWER:
		dp.AddTotalTronPowerWeight(weight)
	}
}

// frozenV2WithDelegatedWeight computes the V2 stake weight (in TRX) for a
// resource, mirroring java-tron's accountCapsule.getFrozenV2BalanceWithDelegated
// for BANDWIDTH/ENERGY and getTronPowerFrozenV2Balance for TRON_POWER (which
// has no delegated leg). Used to compute (newWeight - oldWeight) when
// freezing/unfreezing V2 stake.
func frozenV2WithDelegatedWeight(s *state.StateDB, addr common.Address, resource corepb.ResourceCode) int64 {
	balance := s.GetFrozenV2Amount(addr, resource)
	if resource != corepb.ResourceCode_TRON_POWER {
		balance += s.GetDelegatedFrozenV2(addr, resource)
	}
	return balance / trxPrecisionActuator
}

func v1FrozenResourceWeight(s *state.StateDB, addr common.Address, resource corepb.ResourceCode) int64 {
	acct := s.GetAccount(addr)
	if acct == nil {
		return 0
	}
	switch resource {
	case corepb.ResourceCode_BANDWIDTH:
		return acct.TotalFrozenBandwidth() / trxPrecisionActuator
	case corepb.ResourceCode_ENERGY:
		return acct.FrozenEnergyAmount() / trxPrecisionActuator
	case corepb.ResourceCode_TRON_POWER:
		return acct.V1TronPowerFrozen() / trxPrecisionActuator
	default:
		return 0
	}
}

func v1AcquiredDelegatedWeight(s *state.StateDB, addr common.Address, resource corepb.ResourceCode) int64 {
	acct := s.GetAccount(addr)
	if acct == nil {
		return 0
	}
	switch resource {
	case corepb.ResourceCode_BANDWIDTH:
		return acct.AcquiredDelegatedFrozenBandwidth() / trxPrecisionActuator
	case corepb.ResourceCode_ENERGY:
		return acct.AcquiredDelegatedFrozenEnergy() / trxPrecisionActuator
	default:
		return 0
	}
}
