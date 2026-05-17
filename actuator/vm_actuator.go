package actuator

import (
	"errors"
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/tronprotocol/go-tron/vm"
	"google.golang.org/protobuf/proto"
)

const contractNameMaxLen = 32
const vmMinTokenID = 1_000_000

// VMActuator handles CreateSmartContract (type 30) and TriggerSmartContract (type 31).
type VMActuator struct{}

// Validate checks basic smart contract transaction validity.
func (a *VMActuator) Validate(ctx *Context) error {
	ct := ctx.Tx.ContractType()
	if !ctx.DynProps.AllowCreationOfContracts() {
		return errors.New("vm work is off, need to be opened by the committee")
	}

	switch ct {
	case corepb.Transaction_Contract_CreateSmartContract:
		csc, err := a.getCreateContract(ctx)
		if err != nil {
			return err
		}
		owner, err := checkedAddress(csc.OwnerAddress, "ownerAddress")
		if err != nil {
			return err
		}
		if !ctx.State.AccountExists(owner) {
			return errors.New("owner account does not exist")
		}
		if csc.NewContract == nil {
			return errors.New("new_contract is nil")
		}
		origin, err := checkedAddress(csc.NewContract.OriginAddress, "originAddress")
		if err != nil {
			return err
		}
		if owner != origin {
			return errors.New("ownerAddress is not equals originAddress")
		}
		if len(csc.NewContract.Name) > contractNameMaxLen {
			return errors.New("contractName's length cannot be greater than 32")
		}
		percent := csc.NewContract.ConsumeUserResourcePercent
		if percent < 0 || percent > 100 {
			return errors.New("percent must be >= 0 and <= 100")
		}
		if err := validateVMFeeLimit(ctx); err != nil {
			return err
		}
		if energyLimitHardForkActive(ctx) {
			if csc.NewContract.CallValue < 0 {
				return errors.New("callValue must be >= 0")
			}
			if csc.CallTokenValue < 0 {
				return errors.New("tokenValue must be >= 0")
			}
			if csc.NewContract.OriginEnergyLimit <= 0 {
				return errors.New("The originEnergyLimit must be > 0")
			}
		}
		if err := validateVMTokenValueAndID(ctx, csc.CallTokenValue, csc.TokenId); err != nil {
			return err
		}
		if csc.NewContract.CallValue > ctx.State.GetBalance(owner) {
			return errors.New("balance is not sufficient")
		}
		if ctx.DynProps.AllowTvmTransferTrc10() && csc.CallTokenValue > 0 && ctx.State.GetTRC10Balance(owner, csc.TokenId) < csc.CallTokenValue {
			return errors.New("assetBalance is not sufficient")
		}
		contractAddr := generateContractAddress(ctx.Tx, owner)
		if ctx.State.AccountExists(contractAddr) {
			return fmt.Errorf("trying to create a contract with existing contract address: %s", contractAddr.Hex())
		}
		return nil

	case corepb.Transaction_Contract_TriggerSmartContract:
		tsc, err := a.getTriggerContract(ctx)
		if err != nil {
			return err
		}
		owner, err := checkedAddress(tsc.OwnerAddress, "ownerAddress")
		if err != nil {
			return err
		}
		if !ctx.State.AccountExists(owner) {
			return errors.New("owner account does not exist")
		}
		contractAddr, err := checkedAddress(tsc.ContractAddress, "contractAddress")
		if err != nil {
			return err
		}
		if ctx.State.GetContract(contractAddr) == nil {
			return errors.New("no contract or not a smart contract")
		}
		if err := validateVMFeeLimit(ctx); err != nil {
			return err
		}
		if energyLimitHardForkActive(ctx) {
			if tsc.CallValue < 0 {
				return errors.New("callValue must be >= 0")
			}
			if tsc.CallTokenValue < 0 {
				return errors.New("tokenValue must be >= 0")
			}
		}
		if err := validateVMTokenValueAndID(ctx, tsc.CallTokenValue, tsc.TokenId); err != nil {
			return err
		}
		if tsc.CallValue > ctx.State.GetBalance(owner) {
			return errors.New("balance is not sufficient")
		}
		if ctx.DynProps.AllowTvmTransferTrc10() && tsc.CallTokenValue > 0 && ctx.State.GetTRC10Balance(owner, tsc.TokenId) < tsc.CallTokenValue {
			return errors.New("assetBalance is not sufficient")
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

func configureTVMExecutionContext(evm *vm.TVM, ctx *Context) {
	evm.HeadSlot = ctx.HeadSlot
	evm.HasHeadSlot = ctx.HasHeadSlot
	evm.SetDB(ctx.DB)
	evm.SetRootTransactionID(ctx.Tx.Hash())
	if ctx.DB != nil {
		if blackhole := rawdb.ReadAccountNameIndex(ctx.DB, []byte("Blackhole")); len(blackhole) > 0 {
			evm.SetBlackholeAddress(common.BytesToAddress(blackhole))
		}
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

	owner, err := checkedAddress(csc.OwnerAddress, "ownerAddress")
	if err != nil {
		return nil, err
	}
	callValue := csc.NewContract.CallValue
	bytecode := csc.NewContract.Bytecode
	contractAddr := generateContractAddress(ctx.Tx, owner)

	energyFee := ctx.DynProps.EnergyFee()
	if energyFee <= 0 {
		energyFee = 100
	}
	energyLimit := uint64(ctx.Tx.FeeLimit()) / uint64(energyFee)

	cfg := vm.NewTVMConfig(ctx.BlockNumber, ctx.DynProps)
	tokenID := int64(0)
	tokenValue := int64(0)
	if cfg.TransferTrc10 {
		tokenID = csc.TokenId
		tokenValue = csc.CallTokenValue
	}
	evm := vm.NewTVM(ctx.State, ctx.DynProps, owner, ctx.BlockNumber, ctx.BlockTime, common.Address{}, 1, cfg)
	configureTVMExecutionContext(evm, ctx)

	ret, contractAddr, energyLeft, vmErr := evm.CreateAtWithToken(owner, contractAddr, bytecode, energyLimit, callValue, tokenID, tokenValue)

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

	sc := proto.Clone(csc.NewContract).(*contractpb.SmartContract)
	sc.ContractAddress = contractAddr[:]
	if ctx.DynProps.AllowTvmCompatibleEvm() {
		sc.Version = 1
	} else {
		sc.Version = 0
	}
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

	owner, err := checkedAddress(tsc.OwnerAddress, "ownerAddress")
	if err != nil {
		return nil, err
	}
	contractAddr, err := checkedAddress(tsc.ContractAddress, "contractAddress")
	if err != nil {
		return nil, err
	}
	callValue := tsc.CallValue
	data := tsc.Data

	energyFee := ctx.DynProps.EnergyFee()
	if energyFee <= 0 {
		energyFee = 100
	}
	energyLimit := uint64(ctx.Tx.FeeLimit()) / uint64(energyFee)

	cfg := vm.NewTVMConfig(ctx.BlockNumber, ctx.DynProps)
	tokenID := int64(0)
	tokenValue := int64(0)
	if cfg.TransferTrc10 {
		tokenID = tsc.TokenId
		tokenValue = tsc.CallTokenValue
	}
	evm := vm.NewTVM(ctx.State, ctx.DynProps, owner, ctx.BlockNumber, ctx.BlockTime, common.Address{}, 1, cfg)
	configureTVMExecutionContext(evm, ctx)

	var (
		ret        []byte
		energyLeft uint64
		vmErr      error
	)
	if cfg.TransferTrc10 {
		ret, energyLeft, vmErr = evm.CallToken(owner, contractAddr, data, energyLimit, callValue, tokenID, tokenValue)
	} else {
		ret, energyLeft, vmErr = evm.Call(owner, contractAddr, data, energyLimit, callValue)
	}

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

func validateVMFeeLimit(ctx *Context) error {
	feeLimit := ctx.Tx.FeeLimit()
	if feeLimit < 0 || feeLimit > ctx.DynProps.MaxFeeLimit() {
		return fmt.Errorf("feeLimit must be >= 0 and <= %d", ctx.DynProps.MaxFeeLimit())
	}
	return nil
}

func validateVMTokenValueAndID(ctx *Context, tokenValue, tokenID int64) error {
	if ctx == nil || ctx.DynProps == nil {
		return nil
	}
	if !ctx.DynProps.AllowTvmTransferTrc10() || !ctx.DynProps.AllowMultiSign() {
		return nil
	}
	if tokenID <= vmMinTokenID && tokenID != 0 {
		return fmt.Errorf("tokenId must be > %d", vmMinTokenID)
	}
	if tokenValue > 0 && tokenID == 0 {
		return fmt.Errorf("invalid arguments with tokenValue = %d, tokenId = %d", tokenValue, tokenID)
	}
	return nil
}

func energyLimitHardForkActive(ctx *Context) bool {
	return ctx.DynProps.LatestBlockHeaderNumber() >= blockNumForEnergyLimit
}

func generateContractAddress(tx *types.Transaction, owner common.Address) common.Address {
	txHash := tx.Hash()
	input := make([]byte, 0, len(txHash)+len(owner))
	input = append(input, txHash[:]...)
	input = append(input, owner[:]...)
	hash := common.Keccak256(input)

	var addr common.Address
	addr[0] = owner[0]
	copy(addr[1:], hash[12:])
	return addr
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
