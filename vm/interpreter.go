package vm

import (
	"github.com/holiman/uint256"
)

// Interpreter executes TVM bytecode.
type Interpreter struct {
	tvm        *TVM
	table      JumpTable
	readOnly   bool   // static call mode
	returnData []byte // return data from last CALL/CREATE
	tvmConfig  TVMConfig
	transient  map[uint256.Int]uint256.Int // transient storage for TLOAD/TSTORE (EIP-1153)
}

// NewInterpreter creates a new interpreter.
func NewInterpreter(tvm *TVM, cfg TVMConfig) *Interpreter {
	return &Interpreter{
		tvm:       tvm,
		table:     newJumpTable(),
		tvmConfig: cfg,
		transient: make(map[uint256.Int]uint256.Int),
	}
}

// Run executes the contract's bytecode. Returns the result data and any error.
func (in *Interpreter) Run(contract *Contract) ([]byte, error) {
	var (
		pc    uint64 = 0
		mem          = newMemory()
		stack        = newStack()
	)

	for {
		if pc >= uint64(len(contract.Code)) {
			break
		}

		op := contract.GetOp(pc)
		operation := in.table[op]
		if operation == nil {
			return nil, ErrInvalidCode
		}

		// Fork gate
		if operation.enabledFn != nil && !operation.enabledFn(in.tvmConfig) {
			return nil, ErrInvalidOpCode
		}

		// Stack validation
		if stack.len() < operation.minStack {
			return nil, ErrStackUnderflow
		}

		// Static mode check
		if in.readOnly && operation.writes {
			return nil, ErrWriteProtection
		}

		// Charge static energy cost
		if operation.energyCost > 0 {
			if !contract.UseEnergy(operation.energyCost) {
				return nil, ErrOutOfEnergy
			}
		}

		// Execute
		ret, err := operation.execute(&pc, in, contract, mem, stack)
		if err != nil {
			return nil, err
		}

		// Terminal opcodes
		if op == STOP || op == RETURN || op == REVERT || op == SELFDESTRUCT {
			if op == REVERT {
				return ret, ErrExecutionReverted
			}
			return ret, nil
		}

		pc++
	}

	return nil, nil
}

// makePush creates a PUSH instruction handler.
func makePush(size int) executionFunc {
	return func(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
		startMin := *pc + 1
		endMin := startMin + uint64(size)
		if endMin > uint64(len(contract.Code)) {
			endMin = uint64(len(contract.Code))
		}

		var v uint256.Int
		v.SetBytes(contract.Code[startMin:endMin])
		stack.push(&v)
		*pc += uint64(size)
		return nil, nil
	}
}

// makeDup creates a DUP instruction handler.
func makeDup(n int) executionFunc {
	return func(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
		stack.dup(n)
		return nil, nil
	}
}

// makeSwap creates a SWAP instruction handler.
func makeSwap(n int) executionFunc {
	return func(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
		stack.swap(n)
		return nil, nil
	}
}
