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
	ErrWriteProtection       = errors.New("write protection")
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
	ErrEndowmentOutOfRange   = errors.New("endowment out of long range")
	ErrTransferFailed        = errors.New("transfer trx failed: Cannot transfer TRX to yourself.")
	ErrTokenTransferFailed   = errors.New("transfer trc10 failed: Cannot transfer asset to yourself.")
	errPrecompileFailure     = errors.New("precompile returned failure")
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
