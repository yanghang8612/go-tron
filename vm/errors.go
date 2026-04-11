package vm

import "errors"

var (
	ErrOutOfEnergy           = errors.New("out of energy")
	ErrStackOverflow         = errors.New("stack overflow")
	ErrStackUnderflow        = errors.New("stack underflow")
	ErrInvalidJump           = errors.New("invalid jump destination")
	ErrWriteProtection       = errors.New("write protection")
	ErrReturnDataOutOfBounds = errors.New("return data out of bounds")
	ErrDepthExceeded         = errors.New("max call depth exceeded")
	ErrInsufficientBalance   = errors.New("insufficient balance for transfer")
	ErrContractCodeTooLarge  = errors.New("max code size exceeded")
	ErrInvalidCode           = errors.New("invalid contract code")
	ErrInvalidOpCode         = errors.New("opcode not available in current fork")
	ErrExecutionReverted     = errors.New("execution reverted")
)
