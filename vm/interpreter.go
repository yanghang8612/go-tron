package vm

import (
	"github.com/holiman/uint256"

	"github.com/tronprotocol/go-tron/core/types"
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

	// Fetch (and advance) the contract's dynamic-energy factor once at
	// the start of execution. factor is the effective multiplier in
	// units of DynamicEnergyFactorDecimal (factor==10_000 → 1.0×). We
	// only apply penalties when factor strictly exceeds the decimal;
	// otherwise opcodes charge their base cost unchanged.
	//
	// rawEnergyUsed tracks the unscaled opcode costs so we can commit
	// them to the contract's ContractState.energy_usage counter at the
	// end — that counter is the input to the next cycle's catchUp math.
	var (
		factor        int64 = types.DynamicEnergyFactorDecimal
		rawEnergyUsed uint64
	)
	if in.tvmConfig.DynamicEnergy {
		factor = updateContractEnergyFactor(in.tvm, contract.Address)
	}

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
			rawEnergyUsed += operation.energyCost
			cost := operation.energyCost
			if factor > types.DynamicEnergyFactorDecimal {
				cost += applyDynamicEnergyPenalty(operation.energyCost, factor)
			}
			if !contract.UseEnergy(cost) {
				return nil, ErrOutOfEnergy
			}
		}

		// Execute
		ret, err := operation.execute(&pc, in, contract, mem, stack)
		if err != nil {
			if in.tvmConfig.DynamicEnergy {
				recordContractEnergyUsage(in.tvm, contract.Address, int64(rawEnergyUsed))
			}
			return nil, err
		}

		// Terminal opcodes
		if op == STOP || op == RETURN || op == REVERT || op == SELFDESTRUCT {
			if in.tvmConfig.DynamicEnergy {
				recordContractEnergyUsage(in.tvm, contract.Address, int64(rawEnergyUsed))
			}
			if op == REVERT {
				return ret, ErrExecutionReverted
			}
			return ret, nil
		}

		pc++
	}

	if in.tvmConfig.DynamicEnergy {
		recordContractEnergyUsage(in.tvm, contract.Address, int64(rawEnergyUsed))
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
