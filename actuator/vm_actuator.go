package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/vm"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// VMActuator handles CreateSmartContract (type 30) and TriggerSmartContract (type 31).
type VMActuator struct{}

// Validate checks basic smart contract transaction validity.
func (a *VMActuator) Validate(ctx *Context) error {
	ct := ctx.Tx.ContractType()

	switch {
	case ct == 30: // CreateSmartContract
		csc, err := a.getCreateContract(ctx)
		if err != nil {
			return err
		}
		owner := common.BytesToAddress(csc.OwnerAddress)
		if !ctx.State.AccountExists(owner) {
			return errors.New("owner account does not exist")
		}
		if csc.NewContract == nil {
			return errors.New("new_contract is nil")
		}
		if len(csc.NewContract.Bytecode) == 0 {
			return errors.New("bytecode is empty")
		}
		if ctx.Tx.FeeLimit() <= 0 {
			return errors.New("fee_limit must be positive")
		}
		return nil

	case ct == 31: // TriggerSmartContract
		tsc, err := a.getTriggerContract(ctx)
		if err != nil {
			return err
		}
		owner := common.BytesToAddress(tsc.OwnerAddress)
		if !ctx.State.AccountExists(owner) {
			return errors.New("owner account does not exist")
		}
		contractAddr := common.BytesToAddress(tsc.ContractAddress)
		if !ctx.State.IsContract(contractAddr) {
			return errors.New("contract does not exist")
		}
		if ctx.Tx.FeeLimit() <= 0 {
			return errors.New("fee_limit must be positive")
		}
		return nil

	default:
		return errors.New("unsupported contract type for VMActuator")
	}
}

// Execute runs the smart contract creation or call.
func (a *VMActuator) Execute(ctx *Context) (*Result, error) {
	ct := ctx.Tx.ContractType()

	switch {
	case ct == 30:
		return a.executeCreate(ctx)
	case ct == 31:
		return a.executeTrigger(ctx)
	default:
		return nil, errors.New("unsupported contract type for VMActuator")
	}
}

func contractRetFromError(err error) int32 {
	switch err {
	case vm.ErrExecutionReverted:
		return 2 // REVERT
	case vm.ErrInvalidJump:
		return 3 // BAD_JUMP_DESTINATION
	case vm.ErrOutOfEnergy:
		return 10 // OUT_OF_ENERGY
	case vm.ErrStackUnderflow:
		return 6 // STACK_TOO_SMALL
	case vm.ErrStackOverflow:
		return 7 // STACK_TOO_LARGE
	case vm.ErrWriteProtection:
		return 8 // ILLEGAL_OPERATION
	case vm.ErrDepthExceeded:
		return 9 // STACK_OVERFLOW
	case vm.ErrContractCodeTooLarge:
		return 15 // INVALID_CODE
	default:
		return 13 // UNKNOWN
	}
}

// executeCreate runs CreateSmartContract. It populates result.EnergyUsageTotal
// with the full VM energy consumed; the on-chain balance debit and the
// EnergyUsed/EnergyFee/Fee splits are deferred to PayEnergyBill, which the
// state processor calls after Execute returns. This mirrors java-tron's
// VMActuator -> TransactionTrace.pay -> ReceiptCapsule.payEnergyBill flow.
func (a *VMActuator) executeCreate(ctx *Context) (*Result, error) {
	csc, err := a.getCreateContract(ctx)
	if err != nil {
		return nil, err
	}

	owner := common.BytesToAddress(csc.OwnerAddress)
	callValue := csc.NewContract.CallValue
	bytecode := csc.NewContract.Bytecode

	energyFee := ctx.DynProps.EnergyFee()
	if energyFee <= 0 {
		energyFee = 100
	}
	energyLimit := uint64(ctx.Tx.FeeLimit()) / uint64(energyFee)

	cfg := vm.NewTVMConfig(ctx.BlockNumber, ctx.DynProps)
	evm := vm.NewTVM(ctx.State, ctx.DynProps, owner, ctx.BlockNumber, ctx.BlockTime, common.Address{}, 1, cfg)
	evm.SetDB(ctx.DB)

	ret, contractAddr, energyLeft, vmErr := evm.Create(owner, bytecode, energyLimit, callValue)

	energyUsed := energyLimit - energyLeft

	result := &Result{
		EnergyUsageTotal: int64(energyUsed),
		ContractResult:   ret,
		Logs:             evm.Logs,
	}

	if vmErr != nil {
		result.ContractRet = contractRetFromError(vmErr)
		return result, nil
	}

	result.ContractRet = 1 // SUCCESS
	result.ContractAddress = contractAddr[:]

	sc := csc.NewContract
	sc.ContractAddress = contractAddr[:]
	ctx.State.SetContract(contractAddr, sc)

	return result, nil
}

// executeTrigger runs TriggerSmartContract. See executeCreate for the
// energy-bill deferral note.
func (a *VMActuator) executeTrigger(ctx *Context) (*Result, error) {
	tsc, err := a.getTriggerContract(ctx)
	if err != nil {
		return nil, err
	}

	owner := common.BytesToAddress(tsc.OwnerAddress)
	contractAddr := common.BytesToAddress(tsc.ContractAddress)
	callValue := tsc.CallValue
	data := tsc.Data

	energyFee := ctx.DynProps.EnergyFee()
	if energyFee <= 0 {
		energyFee = 100
	}
	energyLimit := uint64(ctx.Tx.FeeLimit()) / uint64(energyFee)

	cfg := vm.NewTVMConfig(ctx.BlockNumber, ctx.DynProps)
	evm := vm.NewTVM(ctx.State, ctx.DynProps, owner, ctx.BlockNumber, ctx.BlockTime, common.Address{}, 1, cfg)
	evm.SetDB(ctx.DB)

	ret, energyLeft, vmErr := evm.Call(owner, contractAddr, data, energyLimit, callValue)

	energyUsed := energyLimit - energyLeft

	result := &Result{
		EnergyUsageTotal: int64(energyUsed),
		ContractResult:   ret,
		Logs:             evm.Logs,
	}

	if vmErr != nil {
		result.ContractRet = contractRetFromError(vmErr)
		return result, nil
	}

	result.ContractRet = 1 // SUCCESS
	return result, nil
}

func (a *VMActuator) getCreateContract(ctx *Context) (*contractpb.CreateSmartContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	csc := &contractpb.CreateSmartContract{}
	if err := contract.Parameter.UnmarshalTo(csc); err != nil {
		return nil, errors.New("failed to unmarshal CreateSmartContract")
	}
	return csc, nil
}

func (a *VMActuator) getTriggerContract(ctx *Context) (*contractpb.TriggerSmartContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	tsc := &contractpb.TriggerSmartContract{}
	if err := contract.Parameter.UnmarshalTo(tsc); err != nil {
		return nil, errors.New("failed to unmarshal TriggerSmartContract")
	}
	return tsc, nil
}
