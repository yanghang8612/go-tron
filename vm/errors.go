package vm

import (
	"errors"
	"fmt"
)

var (
	ErrOutOfEnergy           = errors.New("out of energy")
	ErrStackOverflow         = errors.New("stack overflow")
	ErrStackUnderflow        = errors.New("stack underflow")
	ErrInvalidJump           = errors.New("invalid jump destination")
	ErrWriteProtection       = errors.New("Attempt to call a state modifying opcode inside STATICCALL")
	ErrReturnDataOutOfBounds = errors.New("return data out of bounds")
	ErrDepthExceeded         = errors.New("max call depth exceeded")
	ErrInsufficientBalance   = errors.New("insufficient balance for transfer")
	ErrContractAlreadyExists = errors.New("contract already exists")
	ErrContractCodeTooLarge  = errors.New("max code size exceeded")
	ErrInvalidCode           = errors.New("invalid code: must not begin with 0xef")
	ErrInvalidOpCode         = errors.New("opcode not available in current fork")
	ErrExecutionReverted     = errors.New("execution reverted")
	ErrAlreadyTimeOut        = errors.New("Already Time Out")
	ErrOutOfMemory           = errors.New("out of memory")
	ErrJVMStackOverflow      = errors.New("StackOverflowError:  exceed default JVM stack size!")
	ErrPrecompiledContract   = errors.New("precompiled contract error")
	ErrEndowmentOutOfRange   = errors.New("endowment out of long range")
	// ErrLegacyEndowmentOutOfRange mirrors BigInteger.longValueExact before
	// Program.callToAddress began converting the ArithmeticException into a
	// TransferException under ALLOW_TVM_CONSTANTINOPLE. It also applies to
	// CREATE/CREATE2 and precompile calls, whose java paths never catch it.
	ErrLegacyEndowmentOutOfRange = errors.New("BigInteger out of long range")
	// ErrLegacyCreateEmptyCode mirrors java-tron's pre-ALLOW_MULTI_SIGN
	// internal-CREATE bug. A constructor that returned empty runtime code was
	// stored in DepositImpl as a Value whose type remained nil; commitCodeCache
	// then raised a message-less NullPointerException, which VM.play converted
	// to "Unknown Exception" and which consumed the whole transaction energy.
	ErrLegacyCreateEmptyCode  = errors.New("Unknown Exception")
	ErrTransferFailed         = errors.New("transfer trx failed: Cannot transfer TRX to yourself.")
	ErrTokenTransferFailed    = errors.New("transfer trc10 failed: Cannot transfer asset to yourself.")
	ErrInvalidTokenID         = errors.New("invalid token id")
	ErrInvalidTokenIDTransfer = errors.New("invalid token id")
	errPrecompileFailure      = errors.New("precompile returned failure")
	errExecutionFailed        = errors.New("execution failed")
	// ErrPrecompileUnknown models an uncaught RuntimeException escaping a
	// java-tron precompile's execute() (e.g. ValidateMultiSign's words[0..3]
	// access on a short input). java's RuntimeImpl.setResultCode maps such a raw
	// exception to contractResult.UNKNOWN(13) with spendAllEnergy + tx failure;
	// gtron mirrors that by surfacing this sentinel, which contractRetFromError
	// maps to its default UNKNOWN(13) and which propagates from sub-calls.
	ErrPrecompileUnknown = errors.New("precompiled contract: uncaught exception")
	// ErrSelfDestructTransferFailure mirrors java-tron's pre-Constantinople
	// Program.suicide BytecodeExecutionException when the beneficiary transfer
	// fails validation. VM.play spends all remaining energy and records UNKNOWN
	// with the byte-exact message "transfer failure".
	ErrSelfDestructTransferFailure = errors.New("transfer failure")
	// ErrPrecompileTransferFailure mirrors java-tron
	// Program.callToPrecompiledAddress's BytecodeExecutionException("transfer
	// failure"): a TRX endowment on a precompile-targeted CALL whose
	// MUtil.transfer -> validateForSmartContract rejects the credit (the
	// precompile address has no account -> "no ToAccount", or the credit
	// would overflow long). NOT a TransferException in java, so VM.play
	// spends ALL remaining energy and the receipt records UNKNOWN(13) —
	// Nile block 18,112,819. The message must stay byte-identical to java's
	// resMessage.
	ErrPrecompileTransferFailure = errors.New("transfer failure")
)

// ErrValidateForSmartContract models the pre-Constantinople exception thrown
// by java-tron Program.callToAddress when an internal TRX transfer fails
// validation. In particular, before allow_tvm_solidity059 a contract could not
// create a missing recipient account. The raw BytecodeExecutionException
// escapes the CALL frame, consumes all remaining energy, and RuntimeImpl maps
// it to contractResult.UNKNOWN.
var ErrValidateForSmartContract = errors.New("validateForSmartContract failure")

type invalidJumpError struct {
	pc uint64
}

type invalidOpCodeError struct {
	op OpCode
}

type stackUnderflowError struct {
	expected int
	actual   int
}

type stackOverflowError struct{}

type outOfMemoryError struct {
	op OpCode
}

// transferValidationError preserves java-tron's post-Constantinople runtime
// message while still classifying the failure as TRANSFER_FAILED.
type transferValidationError struct {
	reason string
}

func (e transferValidationError) Error() string {
	return "transfer trx failed: " + e.reason
}

func (e transferValidationError) Is(target error) bool {
	return target == ErrTransferFailed
}

type tokenTransferValidationError struct {
	reason string
}

func (e tokenTransferValidationError) Error() string {
	return "transfer trc10 failed: " + e.reason
}

func (e tokenTransferValidationError) Is(target error) bool {
	return target == ErrTokenTransferFailed
}

// selfDestructTransferValidationError mirrors java-tron's TransferException
// after ALLOW_TVM_CONSTANTINOPLE. Unlike the legacy bytecode exception, this
// classifies as TRANSFER_FAILED and preserves the remaining transaction energy.
type selfDestructTransferValidationError struct {
	reason string
}

func (e selfDestructTransferValidationError) Error() string {
	return "transfer all token or transfer all trx failed in suicide: " + e.reason
}

func (e selfDestructTransferValidationError) Is(target error) bool {
	return target == ErrTransferFailed
}

func newSelfDestructTransferError(constantinople bool, reason string) error {
	if constantinople {
		return selfDestructTransferValidationError{reason: reason}
	}
	return ErrSelfDestructTransferFailure
}

// callEndowmentOutOfRangeError selects the java exception class for CALL-like
// opcodes. Ordinary callToAddress converts the ArithmeticException only after
// ALLOW_TVM_CONSTANTINOPLE; callToPrecompiledAddress has no such catch.
func callEndowmentOutOfRangeError(constantinople, precompile bool) error {
	if constantinople && !precompile {
		return ErrEndowmentOutOfRange
	}
	return ErrLegacyEndowmentOutOfRange
}

type outOfEnergyError struct {
	op            OpCode
	invokeLimit   uint64
	opEnergy      uint64
	penaltyEnergy uint64
	usedEnergy    uint64
	hasPenalty    bool
}

func (e outOfEnergyError) Error() string {
	name := opNameForError(e.op)
	if e.hasPenalty {
		return fmt.Sprintf("Not enough energy for '%s' operation executing: curInvokeEnergyLimit[%d], curOpEnergy[%d], penaltyEnergy[%d], usedEnergy[%d]",
			name, e.invokeLimit, e.opEnergy, e.penaltyEnergy, e.usedEnergy)
	}
	return fmt.Sprintf("Not enough energy for '%s' operation executing: curInvokeEnergyLimit[%d], curOpEnergy[%d], usedEnergy[%d]",
		name, e.invokeLimit, e.opEnergy, e.usedEnergy)
}

func (e outOfEnergyError) Is(target error) bool {
	return target == ErrOutOfEnergy
}

func (e invalidJumpError) Error() string {
	return fmt.Sprintf("Operation with pc isn't 'JUMPDEST': PC[%d];", e.pc)
}

func (e invalidJumpError) Is(target error) bool {
	return target == ErrInvalidJump
}

func (e invalidOpCodeError) Error() string {
	return fmt.Sprintf("Invalid operation code: opCode[%x];", byte(e.op))
}

func (e invalidOpCodeError) Is(target error) bool {
	return target == ErrInvalidOpCode
}

func (e stackUnderflowError) Error() string {
	return fmt.Sprintf("Expected stack size %d but actual %d;", e.expected, e.actual)
}

func (e stackUnderflowError) Is(target error) bool {
	return target == ErrStackUnderflow
}

func (e stackOverflowError) Error() string {
	return "Expected: overflow 1024 elements stack limit"
}

func (e stackOverflowError) Is(target error) bool {
	return target == ErrStackOverflow
}

func (e outOfMemoryError) Error() string {
	name := opNameForError(e.op)
	return fmt.Sprintf("Out of Memory when '%s' operation executing", name)
}

func (e outOfMemoryError) Is(target error) bool {
	return target == ErrOutOfMemory
}

func newOutOfMemoryError(op OpCode) error {
	return outOfMemoryError{op: op}
}

func newInvalidJumpError(pc uint64) error {
	return invalidJumpError{pc: pc}
}

func newInvalidOpCodeError(op OpCode) error {
	return invalidOpCodeError{op: op}
}

func newStackUnderflowError(expected, actual int) error {
	return stackUnderflowError{expected: expected, actual: actual}
}

func newStackOverflowError() error {
	return stackOverflowError{}
}

func isFatalVMError(err error) bool {
	return errors.Is(err, ErrAlreadyTimeOut) || errors.Is(err, ErrJVMStackOverflow)
}

// shouldPropagateCreateError identifies exceptions raised by java's CREATE
// wrapper itself rather than by the constructor frame. The wrapper exception
// aborts the currently executing Program; ordinary constructor failures are
// still represented by CREATE pushing zero.
func shouldPropagateCreateError(err error) bool {
	return isFatalVMError(err) || errors.Is(err, ErrLegacyCreateEmptyCode)
}

func isTransferFailure(err error) bool {
	return errors.Is(err, ErrTransferFailed) ||
		errors.Is(err, ErrTokenTransferFailed) ||
		errors.Is(err, ErrEndowmentOutOfRange) ||
		errors.Is(err, ErrInvalidTokenIDTransfer)
}

func shouldPropagateCallError(err error) bool {
	return isFatalVMError(err) ||
		errors.Is(err, ErrPrecompiledContract) ||
		errors.Is(err, ErrPrecompileUnknown) ||
		errors.Is(err, ErrPrecompileTransferFailure) ||
		errors.Is(err, ErrValidateForSmartContract) ||
		isTransferFailure(err)
}

func childCallFailure(err error) error {
	if isFatalVMError(err) {
		return err
	}
	return errExecutionFailed
}

func newOutOfEnergyError(op OpCode, contract *Contract, opEnergy, penaltyEnergy uint64, hasPenalty bool) error {
	return outOfEnergyError{
		op:            op,
		invokeLimit:   contract.Energy + contract.EnergyUsed,
		opEnergy:      opEnergy,
		penaltyEnergy: penaltyEnergy,
		usedEnergy:    contract.EnergyUsed,
		hasPenalty:    hasPenalty,
	}
}

func opNameForError(op OpCode) string {
	name := opCodeNames[op]
	if name == "" {
		name = fmt.Sprintf("0x%02x", byte(op))
	}
	return name
}
