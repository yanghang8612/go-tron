package actuator

import (
	"errors"
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/tronprotocol/go-tron/vm"
	"google.golang.org/protobuf/proto"
)

// multiSigCheckV2Pass evaluates VERSION_4_7_1 against the per-tx context.
// Returns false when DB or DynProps is missing (defensive — unit tests that
// stub a Context without DB will not have the fork stats anyway).
func multiSigCheckV2Pass(ctx *Context) bool {
	return ctx.PassVersion(27)
}

// cpuTimeGuardPass evaluates VERSION_4_8_1_1 (block version 35) against the per-tx
// context — java-tron's MUtil.checkCPUTimeForCreate2 / checkCPUTimeForModExp guard.
func cpuTimeGuardPass(ctx *Context) bool {
	return ctx.PassVersion(35)
}

const contractNameMaxLen = 32
const vmMinTokenID = 1_000_000
const creatorDefaultEnergyLimit = 1000 * 10_000

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
	switch {
	case errors.Is(err, vm.ErrExecutionReverted):
		return 2 // REVERT
	case errors.Is(err, vm.ErrOutOfMemory):
		return 4 // OUT_OF_MEMORY
	case errors.Is(err, vm.ErrPrecompiledContract):
		return 5 // PRECOMPILED_CONTRACT
	case errors.Is(err, vm.ErrAlreadyTimeOut):
		return 11 // OUT_OF_TIME
	case errors.Is(err, vm.ErrTransferFailed), errors.Is(err, vm.ErrTokenTransferFailed), errors.Is(err, vm.ErrEndowmentOutOfRange), errors.Is(err, vm.ErrInvalidTokenIDTransfer):
		return 14 // TRANSFER_FAILED
	case errors.Is(err, vm.ErrInvalidJump):
		return 3 // BAD_JUMP_DESTINATION
	case errors.Is(err, vm.ErrOutOfEnergy):
		return 10 // OUT_OF_ENERGY
	case errors.Is(err, vm.ErrStackUnderflow):
		return 6 // STACK_TOO_SMALL
	case errors.Is(err, vm.ErrStackOverflow):
		return 7 // STACK_TOO_LARGE
	case errors.Is(err, vm.ErrInvalidOpCode):
		return 8 // ILLEGAL_OPERATION
	case errors.Is(err, vm.ErrDepthExceeded):
		return 9 // STACK_OVERFLOW
	case errors.Is(err, vm.ErrJVMStackOverflow):
		return 12 // JVM_STACK_OVER_FLOW
	case errors.Is(err, vm.ErrContractCodeTooLarge), errors.Is(err, vm.ErrInvalidCode):
		return 15 // INVALID_CODE
	default:
		return 13 // UNKNOWN
	}
}

func runtimeMessageFromError(err error) []byte {
	if err == nil {
		return nil
	}
	if err == vm.ErrExecutionReverted {
		return []byte("REVERT opcode executed")
	}
	return []byte(err.Error())
}

func expectedContractRet(ctx *Context) (corepb.Transaction_ResultContractResult, bool) {
	if ctx == nil || !ctx.TrustTransactionRet || ctx.Tx == nil || ctx.Tx.Proto() == nil || len(ctx.Tx.Proto().Ret) == 0 {
		return corepb.Transaction_Result_DEFAULT, false
	}
	return ctx.Tx.Proto().Ret[0].GetContractRet(), true
}

func isReplayOutOfTime(ctx *Context) bool {
	ret, ok := expectedContractRet(ctx)
	return ok && ret == corepb.Transaction_Result_OUT_OF_TIME
}

func setReplayOutOfTimeResult(result *Result, energyLimit uint64) {
	result.EnergyUsageTotal = int64(energyLimit)
	result.ContractRet = int32(corepb.Transaction_Result_OUT_OF_TIME)
	result.ContractResult = []byte{}
	result.ContractResultPresent = true
	result.ResMessage = runtimeMessageFromError(vm.ErrAlreadyTimeOut)
}

func configureTVMExecutionContext(evm *vm.TVM, ctx *Context) {
	evm.HeadSlot = ctx.HeadSlot
	evm.HasHeadSlot = ctx.HasHeadSlot
	evm.GenesisHash = ctx.GenesisHash
	evm.TrustTransactionRet = ctx.TrustTransactionRet
	if ret, ok := expectedContractRet(ctx); ok {
		evm.ExpectedContractRet = ret
	}
	evm.SetDB(ctx.DB)
	evm.SetRootTransactionID(ctx.Tx.Hash())
	if ctx.State != nil {
		if blackhole := ctx.State.ReadAccountNameIndex([]byte("Blackhole")); len(blackhole) > 0 {
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

	result := &Result{ContractAddress: contractAddr[:]}
	energyLimit := uint64(accountEnergyLimit(ctx, owner, ctx.Tx.FeeLimit(), callValue, result))
	if isReplayOutOfTime(ctx) {
		setReplayOutOfTimeResult(result, energyLimit)
		return result, nil
	}

	cfg := vm.NewTVMConfig(ctx.BlockNumber, ctx.DynProps)
	cfg.MultiSigCheckV2 = multiSigCheckV2Pass(ctx)
	cfg.CpuTimeGuard = cpuTimeGuardPass(ctx)
	tokenID := int64(0)
	tokenValue := int64(0)
	if cfg.TransferTrc10 {
		tokenID = csc.TokenId
		tokenValue = csc.CallTokenValue
	}
	evm := vm.NewTVM(ctx.State, ctx.DynProps, owner, ctx.BlockNumber, ctx.BlockTime, ctx.Coinbase, 1, cfg)
	configureTVMExecutionContext(evm, ctx)

	sc := proto.Clone(csc.NewContract).(*contractpb.SmartContract)
	sc.ContractAddress = contractAddr[:]
	if ctx.DynProps.AllowTvmCompatibleEvm() {
		sc.Version = 1
	} else {
		sc.Version = 0
	}

	ret, createdAddr, energyLeft, vmErr := evm.CreateAtWithTokenAndContract(owner, contractAddr, bytecode, energyLimit, callValue, tokenID, tokenValue, sc)
	if !createdAddr.IsEmpty() {
		contractAddr = createdAddr
		sc.ContractAddress = contractAddr[:]
	}

	energyUsed := energyLimit - energyLeft

	result.EnergyUsageTotal = int64(energyUsed)
	result.ContractResult = ret
	result.ContractResultPresent = true
	result.Logs = evm.Logs
	result.InternalTransactions = evm.InternalTransactions

	if vmErr != nil {
		result.ContractRet = contractRetFromError(vmErr)
		result.ResMessage = runtimeMessageFromError(vmErr)
		return result, nil
	}

	result.ContractRet = 1 // SUCCESS

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

	result := &Result{}
	result.ContractAddress = contractAddr[:]
	energyLimit := uint64(triggerEnergyLimit(ctx, owner, contractAddr, ctx.Tx.FeeLimit(), callValue, result))
	if isReplayOutOfTime(ctx) {
		setReplayOutOfTimeResult(result, energyLimit)
		return result, nil
	}

	cfg := vm.NewTVMConfig(ctx.BlockNumber, ctx.DynProps)
	cfg.MultiSigCheckV2 = multiSigCheckV2Pass(ctx)
	cfg.CpuTimeGuard = cpuTimeGuardPass(ctx)
	tokenID := int64(0)
	tokenValue := int64(0)
	if cfg.TransferTrc10 {
		tokenID = tsc.TokenId
		tokenValue = tsc.CallTokenValue
	}
	evm := vm.NewTVM(ctx.State, ctx.DynProps, owner, ctx.BlockNumber, ctx.BlockTime, ctx.Coinbase, 1, cfg)
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

	result.EnergyUsageTotal = int64(energyUsed)
	result.ContractResult = ret
	result.ContractResultPresent = true
	result.Logs = evm.Logs
	result.InternalTransactions = evm.InternalTransactions

	if vmErr != nil {
		result.ContractRet = contractRetFromError(vmErr)
		result.ResMessage = runtimeMessageFromError(vmErr)
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

func vmEnergyFee(ctx *Context) int64 {
	if ctx == nil || ctx.DynProps == nil || ctx.DynProps.EnergyFee() <= 0 {
		return 100
	}
	return ctx.DynProps.EnergyFee()
}

func accountEnergyLimit(ctx *Context, account common.Address, feeLimit, callValue int64, result *Result) int64 {
	if energyLimitHardForkActive(ctx) {
		return accountEnergyLimitWithFixRatio(ctx, account, feeLimit, callValue, result)
	}
	return accountEnergyLimitWithFloatRatio(ctx, account, feeLimit, callValue)
}

func accountEnergyLimitWithFixRatio(ctx *Context, account common.Address, feeLimit, callValue int64, result *Result) int64 {
	acct := ctx.State.GetAccount(account)
	if acct == nil {
		return 0
	}
	sunPerEnergy := vmEnergyFee(ctx)
	leftFrozenEnergy := availableAccountEnergyForBill(ctx.State, ctx.DynProps, account, ctx.ResourceTime())
	// Diagnostic (cross-impl parity): record caller available energy at exec
	// start unconditionally (incl. pre-Stake-2.0). Billing still gates its reads
	// on vmReceiptEnergyLeftMode, so this only populates the receipt field.
	if result != nil {
		result.CallerEnergyLeft = leftFrozenEnergy
		result.HasCallerEnergyLeft = true
		// Diagnostic: caller energy recovery window (slots) at exec start. The
		// window is the per-account state that drifts in energy-window forks;
		// recovery reads but never persists it, so this is the pristine value.
		result.CallerEnergyWindow = acct.EnergyWindowSize()
	}
	if callValue < 0 {
		callValue = 0
	}
	energyFromBalance := maxInt64(ctx.State.GetBalance(account)-callValue, 0) / sunPerEnergy
	availableEnergy := leftFrozenEnergy + energyFromBalance
	energyFromFeeLimit := feeLimit / sunPerEnergy
	return minInt64(availableEnergy, energyFromFeeLimit)
}

func accountEnergyLimitWithFloatRatio(ctx *Context, account common.Address, feeLimit, callValue int64) int64 {
	acct := ctx.State.GetAccount(account)
	if acct == nil {
		return 0
	}
	sunPerEnergy := vmEnergyFee(ctx)
	leftFrozenEnergy := availableAccountEnergyForBill(ctx.State, ctx.DynProps, account, ctx.ResourceTime())
	if callValue < 0 {
		callValue = 0
	}
	energyFromBalance := maxInt64(ctx.State.GetBalance(account)-callValue, 0) / sunPerEnergy

	totalFrozen := allFrozenBalanceForEnergy(acct)
	var energyFromFeeLimit int64
	if totalFrozen == 0 {
		energyFromFeeLimit = feeLimit / sunPerEnergy
	} else {
		totalEnergyFromFreeze := calcAccountEnergyLimit(acct, ctx.DynProps)
		leftBalanceForEnergyFreeze := energyFeeForFrozenBalance(totalFrozen, leftFrozenEnergy, totalEnergyFromFreeze)
		if leftBalanceForEnergyFreeze >= feeLimit {
			energyFromFeeLimit = bigMulDivInt64(totalEnergyFromFreeze, feeLimit, totalFrozen)
		} else {
			energyFromFeeLimit = leftFrozenEnergy + (feeLimit-leftBalanceForEnergyFreeze)/sunPerEnergy
		}
	}
	return minInt64(leftFrozenEnergy+energyFromBalance, energyFromFeeLimit)
}

func triggerEnergyLimit(ctx *Context, caller, contractAddr common.Address, feeLimit, callValue int64, result *Result) int64 {
	contract := ctx.State.GetContract(contractAddr)
	if contract == nil {
		return accountEnergyLimit(ctx, caller, feeLimit, callValue, result)
	}
	origin := common.BytesToAddress(contract.OriginAddress)
	if origin == (common.Address{}) || origin == caller {
		return accountEnergyLimit(ctx, caller, feeLimit, callValue, result)
	}
	if !ctx.State.AccountExists(origin) && ctx.DynProps.AllowTvmConstantinople() {
		return accountEnergyLimit(ctx, caller, feeLimit, callValue, result)
	}
	if energyLimitHardForkActive(ctx) {
		return totalEnergyLimitWithFixRatio(ctx, origin, caller, contractAddr, feeLimit, callValue, result)
	}
	return totalEnergyLimitWithFloatRatio(ctx, origin, caller, contractAddr, feeLimit, callValue)
}

func totalEnergyLimitWithFixRatio(ctx *Context, origin, caller, contractAddr common.Address, feeLimit, callValue int64, result *Result) int64 {
	callerEnergyLimit := accountEnergyLimitWithFixRatio(ctx, caller, feeLimit, callValue, result)
	if origin == caller {
		return callerEnergyLimit
	}
	contract := ctx.State.GetContract(contractAddr)
	if contract == nil {
		return callerEnergyLimit
	}

	userPercent := clampPercent(contract.ConsumeUserResourcePercent)
	originPercent := 100 - userPercent
	if originPercent <= 0 {
		return callerEnergyLimit
	}

	originEnergyLeft := availableAccountEnergyForBill(ctx.State, ctx.DynProps, origin, ctx.ResourceTime())
	// Diagnostic (cross-impl parity): record origin available energy at exec
	// start unconditionally (incl. pre-Stake-2.0). Billing still gates its reads.
	if result != nil {
		result.OriginEnergyLeft = originEnergyLeft
		result.HasOriginEnergyLeft = true
		// Diagnostic: origin energy recovery window (slots) at exec start.
		if originAcct := ctx.State.GetAccount(origin); originAcct != nil {
			result.OriginEnergyWindow = originAcct.EnergyWindowSize()
		}
	}

	originLimit := contractOriginEnergyLimit(contract)
	var originEnergyLimit int64
	if userPercent <= 0 {
		originEnergyLimit = minInt64(originEnergyLeft, originLimit)
	} else {
		originEnergyLimit = minInt64(
			bigMulDivInt64(callerEnergyLimit, originPercent, userPercent),
			minInt64(originEnergyLeft, originLimit),
		)
	}
	return callerEnergyLimit + originEnergyLimit
}

func totalEnergyLimitWithFloatRatio(ctx *Context, origin, caller, contractAddr common.Address, feeLimit, callValue int64) int64 {
	callerEnergyLimit := accountEnergyLimitWithFloatRatio(ctx, caller, feeLimit, callValue)
	if origin == caller {
		return callerEnergyLimit
	}
	creatorEnergyLimit := availableAccountEnergyForBill(ctx.State, ctx.DynProps, origin, ctx.ResourceTime())
	contract := ctx.State.GetContract(contractAddr)
	if contract == nil {
		return callerEnergyLimit
	}
	userPercent := clampPercent(contract.ConsumeUserResourcePercent)
	originPercent := 100 - userPercent
	if userPercent > 0 && creatorEnergyLimit*userPercent > originPercent*callerEnergyLimit {
		return bigMulDivInt64(callerEnergyLimit, 100, userPercent)
	}
	return callerEnergyLimit + creatorEnergyLimit
}

func allFrozenBalanceForEnergy(acct *types.Account) int64 {
	if acct == nil {
		return 0
	}
	frozen := acct.FrozenEnergyAmount()
	frozen += acct.AcquiredDelegatedFrozenEnergy()
	frozen += acct.GetFrozenV2Amount(corepb.ResourceCode_ENERGY)
	frozen += acct.AcquiredDelegatedFrozenV2BalanceForEnergy()
	return frozen
}

func energyFeeForFrozenBalance(energyFrozen, energyUsage, energyTotal int64) int64 {
	if energyTotal <= 0 {
		return 0
	}
	return bigMulDivInt64(energyFrozen, energyUsage, energyTotal)
}

func contractOriginEnergyLimit(contract *contractpb.SmartContract) int64 {
	if contract == nil || contract.OriginEnergyLimit == 0 {
		return creatorDefaultEnergyLimit
	}
	return contract.OriginEnergyLimit
}

func clampPercent(percent int64) int64 {
	if percent < 0 {
		return 0
	}
	if percent > 100 {
		return 100
	}
	return percent
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func energyLimitHardForkActive(ctx *Context) bool {
	if ctx == nil || ctx.DynProps == nil {
		return false
	}
	forkBlock := blockNumForEnergyLimit
	if ctx.HasEnergyLimitForkBlockNum {
		forkBlock = ctx.EnergyLimitForkBlockNum
	}
	return ctx.DynProps.LatestBlockHeaderNumber() >= forkBlock
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
