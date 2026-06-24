package vm

import (
	"errors"
	"fmt"
)

var (
	ErrOutOfEnergy            = errors.New("out of energy")
	ErrStackOverflow          = errors.New("stack overflow")
	ErrStackUnderflow         = errors.New("stack underflow")
	ErrInvalidJump            = errors.New("invalid jump destination")
	ErrWriteProtection        = errors.New("Attempt to call a state modifying opcode inside STATICCALL")
	ErrReturnDataOutOfBounds  = errors.New("return data out of bounds")
	ErrDepthExceeded          = errors.New("max call depth exceeded")
	ErrInsufficientBalance    = errors.New("insufficient balance for transfer")
	ErrContractAlreadyExists  = errors.New("contract already exists")
	ErrContractCodeTooLarge   = errors.New("max code size exceeded")
	ErrInvalidCode            = errors.New("invalid code: must not begin with 0xef")
	ErrInvalidOpCode          = errors.New("opcode not available in current fork")
	ErrExecutionReverted      = errors.New("execution reverted")
	ErrAlreadyTimeOut         = errors.New("Already Time Out")
	ErrOutOfMemory            = errors.New("out of memory")
	ErrJVMStackOverflow       = errors.New("StackOverflowError:  exceed default JVM stack size!")
	ErrPrecompiledContract    = errors.New("precompiled contract error")
	ErrEndowmentOutOfRange    = errors.New("endowment out of long range")
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
