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

	// Dynamic-energy state — reset at the top of each Run call.
	// factor is the effective multiplier (DynamicEnergyFactorDecimal == 1.0×).
	// rawEnergyUsed accumulates the unscaled (pre-penalty) cost of every op
	// so we can feed the correct value into the ContractState feedback counter
	// at the end of execution — mirroring java-tron VM.play's energyUsage.
	factor        int64
	rawEnergyUsed uint64
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
	//
	// Both are stored as Interpreter fields so that instruction functions
	// (MLOAD, CODECOPY, SHA3, LOG, etc.) can call in.useEnergy() and have
	// their costs participate in penalty and raw-usage accounting without
	// any extra parameters — mirroring java-tron VM.play's single
	// energy/energyUsage variables that getEnergyCost writes into.
	in.factor = types.DynamicEnergyFactorDecimal
	in.rawEnergyUsed = 0
	if in.tvmConfig.DynamicEnergy {
		in.factor = updateContractEnergyFactor(in.tvm, contract.Address)
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

		// Charge static energy cost via useEnergy so that the factor and
		// rawEnergyUsed accumulation are applied uniformly here and in all
		// instruction-function callsites.
		if operation.energyCost > 0 {
			if !in.useEnergy(contract, operation.energyCost) {
				return nil, ErrOutOfEnergy
			}
		}

		// Execute
		ret, err := operation.execute(&pc, in, contract, mem, stack)
		if err != nil {
			if in.tvmConfig.DynamicEnergy {
				recordContractEnergyUsage(in.tvm, contract.Address, int64(in.rawEnergyUsed))
			}
			return nil, err
		}

		// Terminal opcodes
		if op == STOP || op == RETURN || op == REVERT || op == SELFDESTRUCT {
			if in.tvmConfig.DynamicEnergy {
				recordContractEnergyUsage(in.tvm, contract.Address, int64(in.rawEnergyUsed))
			}
			if op == REVERT {
				return ret, ErrExecutionReverted
			}
			return ret, nil
		}

		pc++
	}

	if in.tvmConfig.DynamicEnergy {
		recordContractEnergyUsage(in.tvm, contract.Address, int64(in.rawEnergyUsed))
	}
	return nil, nil
}

// useEnergy is the single energy-spend point for all inline charges from
// instruction functions (memExpansion, per-word copy, SHA3 word cost, LOG
// data cost, etc.). It mirrors java-tron VM.play's pattern where
// op.getEnergyCost(program) returns the full cost before the single
// dynamic-energy penalty calculation.
//
// baseCost is the raw (pre-penalty) cost. It is added to rawEnergyUsed
// unconditionally (mirrors energyUsage accumulation in java-tron VM.play).
// If DynamicEnergy is active and factor > 1.0×, the penalty is applied
// and the total scaled cost is debited from the contract.
//
// Returns false (out-of-energy) if the contract cannot pay. Callers must
// propagate ErrOutOfEnergy on false return.
func (in *Interpreter) useEnergy(contract *Contract, baseCost uint64) bool {
	if baseCost == 0 {
		return true
	}
	in.rawEnergyUsed += baseCost
	cost := baseCost
	if in.tvmConfig.DynamicEnergy && in.factor > types.DynamicEnergyFactorDecimal {
		cost += applyDynamicEnergyPenalty(baseCost, in.factor)
	}
	return contract.UseEnergy(cost)
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
