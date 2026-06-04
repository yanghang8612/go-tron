package vm

import (
	"errors"

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
	currentOp  OpCode
	energyErr  error

	// Dynamic-energy state — reset at the top of each Run call.
	// factor is the effective multiplier (DynamicEnergyFactorDecimal == 1.0×).
	// rawEnergyUsed accumulates the unscaled (pre-penalty) cost of every op
	// so we can feed the correct value into the ContractState feedback counter
	// at the end of execution — mirroring java-tron VM.play's energyUsage.
	factor        int64
	rawEnergyUsed uint64

	// opBaseAccum accumulates the current opcode's pre-penalty base cost across
	// its (possibly multiple) useEnergy calls. The dynamic-energy penalty is
	// charged INCREMENTALLY so its running total equals a single floor over the
	// whole op cost — matching java VM.play, which computes one
	// `energy*factor/DECIMAL - energy` over the full op cost. Reset per op.
	opBaseAccum uint64
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
	parentFactor, parentRawEnergyUsed := in.factor, in.rawEnergyUsed
	defer func() {
		in.factor = parentFactor
		in.rawEnergyUsed = parentRawEnergyUsed
	}()

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
			return nil, newInvalidOpCodeError(op)
		}
		in.currentOp = op
		in.energyErr = nil
		// Per-op base accumulator for the single-floor dynamic-energy penalty.
		in.opBaseAccum = 0

		// Fork gate
		if operation.enabledFn != nil && !operation.enabledFn(in.tvmConfig) {
			return nil, newInvalidOpCodeError(op)
		}

		// Stack validation
		if stack.len() < operation.minStack {
			return nil, newStackUnderflowError(operation.minStack, stack.len())
		}
		if stack.len()-operation.minStack+operationStackReturns(op, operation) > stackLimit {
			return nil, newStackOverflowError()
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
				return nil, in.outOfEnergyError()
			}
		}

		// Execute
		ret, err := operation.execute(&pc, in, contract, mem, stack)
		if err != nil {
			if in.tvmConfig.DynamicEnergy {
				recordContractEnergyUsage(in.tvm, contract.Address, int64(in.rawEnergyUsed))
			}
			if errors.Is(err, ErrOutOfEnergy) {
				return nil, in.outOfEnergyError()
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

func operationStackReturns(op OpCode, operation *operation) int {
	if op >= PUSH0 && op <= PUSH32 {
		return 1
	}
	if op >= DUP1 && op <= DUP16 {
		return operation.minStack + 1
	}
	if op >= SWAP1 && op <= SWAP16 {
		return operation.minStack
	}
	switch op {
	case ADD, MUL, SUB, DIV, SDIV, MOD, SMOD, ADDMOD, MULMOD, EXP, SIGNEXTEND,
		LT, GT, SLT, SGT, EQ, ISZERO, AND, OR, XOR, NOT, BYTE, SHL, SHR, SAR, CLZ,
		SHA3, ADDRESS, BALANCE, ORIGIN, CALLER, CALLVALUE, CALLDATALOAD,
		CALLDATASIZE,
		CODESIZE, GASPRICE, EXTCODESIZE, RETURNDATASIZE, EXTCODEHASH, BLOCKHASH,
		COINBASE, TIMESTAMP, NUMBER, DIFFICULTY, GASLIMIT, CHAINID, SELFBALANCE,
		BASEFEE, BLOBHASH, BLOBBASEFEE, MLOAD, SLOAD, PC, MSIZE, GAS, TLOAD,
		CALLTOKEN, TOKENBALANCE, CALLTOKENVALUE, CALLTOKENID, ISCONTRACT, FREEZE,
		UNFREEZE, FREEZEEXPIRETIME, VOTEWITNESS, WITHDRAWREWARD, FREEZEBALANCEV2,
		UNFREEZEBALANCEV2, CANCELALLUNFREEZEV2, WITHDRAWEXPIREUNFREEZE,
		DELEGATERESOURCE, UNDELEGATERESOURCE, CREATE, CREATE2, CALL, CALLCODE,
		DELEGATECALL, STATICCALL:
		return 1
	default:
		return 0
	}
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
	penalty := uint64(0)
	hasPenalty := false
	if in.tvmConfig.DynamicEnergy && in.factor > types.DynamicEnergyFactorDecimal {
		hasPenalty = true
		// Charge the penalty INCREMENTALLY so the running total over this op is a
		// SINGLE floor of (totalBase*factor/DECIMAL - totalBase), matching java
		// VM.play (one penalty over the full op cost). Flooring each useEnergy
		// chunk independently undercharges by up to (chunks-1).
		prevPenalty := applyDynamicEnergyPenalty(in.opBaseAccum, in.factor)
		in.opBaseAccum += baseCost
		penalty = applyDynamicEnergyPenalty(in.opBaseAccum, in.factor) - prevPenalty
		cost += penalty
	}
	if contract.UseEnergy(cost) {
		return true
	}
	in.energyErr = newOutOfEnergyError(in.currentOp, contract, baseCost, penalty, hasPenalty)
	return false
}

func (in *Interpreter) outOfEnergyError() error {
	if in.energyErr != nil {
		return in.energyErr
	}
	return ErrOutOfEnergy
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
