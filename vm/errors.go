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

type outOfMemoryError struct {
	op OpCode
}

func (e outOfMemoryError) Error() string {
	name := opCodeNames[e.op]
	if name == "" {
		name = fmt.Sprintf("0x%02x", byte(e.op))
	}
	return fmt.Sprintf("Out of Memory when '%s' operation executing", name)
}

func (e outOfMemoryError) Is(target error) bool {
	return target == ErrOutOfMemory
}

func newOutOfMemoryError(op OpCode) error {
	return outOfMemoryError{op: op}
}
