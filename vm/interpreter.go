package vm

import (
	"errors"

	"github.com/holiman/uint256"

	"github.com/tronprotocol/go-tron/core/types"
)

// Interpreter executes TVM bytecode.
type Interpreter struct {
	tvm        *TVM
	table      *JumpTable
	readOnly   bool   // static call mode
	returnData []byte // return data from last CALL/CREATE
	tvmConfig  TVMConfig
	currentOp  OpCode
	energyErr  error

	// Dynamic-energy state — reset at the top of each Run call.
	// factor is the effective multiplier (DynamicEnergyFactorDecimal == 1.0×).
	// rawEnergyUsed accumulates the unscaled (pre-penalty) cost of every op
	// so we can feed the correct value into the ContractState feedback counter
	// at the end of execution — mirroring java-tron VM.play's energyUsage.
	factor        int64
	rawEnergyUsed uint64
	// rawEnergyUncharged is non-zero only when the current frame exits because
	// an energy charge failed. ContractState feedback intentionally records the
	// requested base cost; execution telemetry excludes that unexecuted charge.
	rawEnergyUncharged uint64

	// opBaseAccum accumulates the current opcode's pre-penalty base cost across
	// its (possibly multiple) useEnergy calls. The dynamic-energy penalty is
	// charged INCREMENTALLY so its running total equals a single floor over the
	// whole op cost — matching java VM.play, which computes one
	// `energy*factor/DECIMAL - energy` over the full op cost. Reset per op.
	opBaseAccum uint64
}

// NewInterpreter creates a new interpreter.
func NewInterpreter(tvm *TVM, cfg TVMConfig) *Interpreter {
	in := new(Interpreter)
	resetInterpreter(in, tvm, cfg)
	return in
}

func resetInterpreter(in *Interpreter, tvm *TVM, cfg TVMConfig) {
	*in = Interpreter{
		tvm:       tvm,
		table:     jumpTableForConfig(cfg),
		tvmConfig: cfg,
	}
}

// Run executes the contract's bytecode. Returns the result data and any error.
func (in *Interpreter) Run(contract *Contract) ([]byte, error) {
	tracer := in.tvmConfig.Tracer
	mem := acquireExecutionMemory()
	stack := acquireExecutionStack()
	pc := &stack.pc
	defer releaseExecutionMemory(mem)
	defer releaseExecutionStack(stack)
	parentFactor, parentRawEnergyUsed, parentRawEnergyUncharged := in.factor, in.rawEnergyUsed, in.rawEnergyUncharged
	// The return-data buffer is per-frame state: java-tron gives every
	// Program its own returnDataBuffer, so RETURNDATASIZE is 0 at frame
	// entry until the frame completes a call of its own (EIP-211). gtron
	// shares one Interpreter across frames, so without this reset a child
	// frame would observe the parent's last call result — which breaks
	// solc's "returndatasize as cheap PUSH0" idiom in proxy fallbacks
	// (calldatacopy(ptr, returndatasize(), calldatasize())) and shifted
	// the forwarded calldata of Nile tx 62420abd… (block 14,151,095).
	parentReturnData := in.returnData
	in.returnData = nil
	defer func() {
		if in.tvmConfig.DynamicEnergy {
			// Each nested Run owns an independent rawEnergyUsed accumulator. Fold
			// successfully charged base work into the top-level TVM before restoring
			// the parent frame, so every frame is counted exactly once without
			// treating a rejected OOE charge as executed work.
			chargedRawEnergy := in.rawEnergyUsed
			if in.rawEnergyUncharged >= chargedRawEnergy {
				chargedRawEnergy = 0
			} else {
				chargedRawEnergy -= in.rawEnergyUncharged
			}
			in.tvm.addRawEnergyUsage(chargedRawEnergy)
		}
		in.factor = parentFactor
		in.rawEnergyUsed = parentRawEnergyUsed
		in.rawEnergyUncharged = parentRawEnergyUncharged
		in.returnData = parentReturnData
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
	in.rawEnergyUncharged = 0
	dynamicEnergy := in.tvmConfig.DynamicEnergy
	if dynamicEnergy {
		in.factor = updateContractEnergyFactor(in.tvm, contract.Address)
	}
	// Code and the fork-resolved jump table are immutable for this execution
	// frame. Keep both locally so the per-op dispatch path does not reload them
	// through Contract/Interpreter or repeat Contract.GetOp's bounds check after
	// the loop has already established pc < len(code).
	code := contract.Code
	codeLen := uint64(len(code))
	table := in.table
	// Historical replay runs without tracing before DynamicEnergy activates.
	// Select that stable mode once per frame so its per-op loop does not keep
	// sampling tracer state, dynamic-energy bookkeeping and trace operands that
	// are provably unused for the whole frame.
	if tracer == nil && !dynamicEnergy {
		return in.runBaseEnergy(contract, mem, stack, code, codeLen, table)
	}
	for {
		if *pc >= codeLen {
			break
		}

		op := OpCode(code[*pc])
		operation := &table[op]
		if operation.execute == nil {
			// A known-but-disabled opcode is nil only in the config-resolved
			// table. Preserve the former runtime-gate bookkeeping/tracer order on
			// this cold error path; genuinely unknown opcodes retain the original
			// early return below.
			if isForkGatedOpcode(op) {
				in.currentOp = op
				in.energyErr = nil
				in.opBaseAccum = 0
				pcStart := *pc
				energyBefore := contract.Energy
				opErr := newInvalidOpCodeError(op)
				if tracer != nil {
					in.traceState(tracer, pcStart, op, energyBefore, 0, mem, stack, contract, opErr)
				}
				return nil, opErr
			}
			opErr := newInvalidOpCodeError(op)
			if tracer != nil {
				in.traceState(tracer, *pc, op, contract.Energy, 0, mem, stack, contract, opErr)
			}
			return nil, opErr
		}
		in.currentOp = op
		in.energyErr = nil
		// Per-op base accumulator for the single-floor dynamic-energy penalty.
		if dynamicEnergy {
			in.opBaseAccum = 0
		}

		// Remaining energy before this opcode's charges; the tracer reports it as
		// the step's "gas" and the energyBefore-energyAfter delta as the cost.
		pcStart := *pc
		energyBefore := contract.Energy

		// Stack validation
		stackLen := stack.len()
		if stackLen < operation.minStack {
			opErr := newStackUnderflowError(operation.minStack, stackLen)
			if tracer != nil {
				in.traceState(tracer, pcStart, op, energyBefore, 0, mem, stack, contract, opErr)
			}
			return nil, opErr
		}
		if stackLen > operation.maxInputStack {
			opErr := newStackOverflowError()
			if tracer != nil {
				in.traceState(tracer, pcStart, op, energyBefore, 0, mem, stack, contract, opErr)
			}
			return nil, opErr
		}

		// Static mode check
		if in.readOnly && operation.writes {
			if tracer != nil {
				in.traceState(tracer, pcStart, op, energyBefore, 0, mem, stack, contract, ErrWriteProtection)
			}
			return nil, ErrWriteProtection
		}

		// Charge static energy cost via useEnergy so that the factor and
		// rawEnergyUsed accumulation are applied uniformly here and in all
		// instruction-function callsites.
		if operation.energyCost > 0 {
			if !dynamicEnergy {
				if !contract.UseEnergy(operation.energyCost) {
					opErr := newOutOfEnergyError(op, contract, operation.energyCost, 0, false)
					if tracer != nil {
						in.traceState(tracer, pcStart, op, energyBefore, energyBefore-contract.Energy, mem, stack, contract, opErr)
					}
					return nil, opErr
				}
			} else if !in.useEnergy(contract, operation.energyCost) {
				opErr := in.outOfEnergyError()
				if tracer != nil {
					in.traceState(tracer, pcStart, op, energyBefore, energyBefore-contract.Energy, mem, stack, contract, opErr)
				}
				return nil, opErr
			}
		}

		// Snapshot the PRE-execute operand stack: CaptureState is emitted AFTER
		// the opcode runs (so the exact energy delta is known), but the trace must
		// still show the operands the opcode consumed — exactly java-tron's
		// pre-execute stack dump that makes a failing comparison's operands visible.
		var preStack *Stack
		if tracer != nil {
			preStack = &Stack{data: append([]uint256.Int(nil), stack.data...)}
		}

		// Execute. PUSH1..PUSH32 dominate deployed bytecode and all share the
		// same handler. Call that handler directly so the compiler can inline the
		// small immediate-decode path instead of paying an indirect function call
		// through operation.execute for every literal.
		var ret []byte
		var err error
		if op >= PUSH1 && op <= PUSH32 {
			ret, err = opPush(pc, in, contract, mem, stack)
		} else {
			ret, err = operation.execute(pc, in, contract, mem, stack)
		}

		if tracer != nil {
			in.traceState(tracer, pcStart, op, energyBefore, energyBefore-contract.Energy, mem, preStack, contract, err)
		}

		if err != nil {
			if dynamicEnergy {
				recordContractEnergyUsage(in.tvm, contract.Address, int64(in.rawEnergyUsed))
			}
			if errors.Is(err, ErrOutOfEnergy) {
				return nil, in.outOfEnergyError()
			}
			return nil, err
		}

		// Terminal opcodes
		if op == STOP || op == RETURN || op == REVERT || op == SELFDESTRUCT {
			if dynamicEnergy {
				recordContractEnergyUsage(in.tvm, contract.Address, int64(in.rawEnergyUsed))
			}
			if op == REVERT {
				return ret, ErrExecutionReverted
			}
			return ret, nil
		}

		(*pc)++
	}

	if dynamicEnergy {
		recordContractEnergyUsage(in.tvm, contract.Address, int64(in.rawEnergyUsed))
	}
	return nil, nil
}

// runBaseEnergy is Run's no-tracer, pre-DynamicEnergy execution loop. Keep its
// validation, charging, dispatch, terminal and error order byte-for-byte aligned
// with the general loop above; only tracing and dynamic-energy-only work is
// absent. Selecting it once at frame entry avoids adding another opcode-level
// dispatch branch.
func (in *Interpreter) runBaseEnergy(contract *Contract, mem *Memory, stack *Stack, code []byte, codeLen uint64, table *JumpTable) ([]byte, error) {
	pc := &stack.pc
	for {
		if *pc >= codeLen {
			break
		}

		op := OpCode(code[*pc])
		operation := &table[op]
		if operation.execute == nil {
			// Preserve the general loop's bookkeeping for a known opcode disabled
			// by the resolved fork table. Unknown opcodes return immediately.
			if isForkGatedOpcode(op) {
				in.currentOp = op
				in.energyErr = nil
				in.opBaseAccum = 0
			}
			return nil, newInvalidOpCodeError(op)
		}
		in.currentOp = op

		stackLen := stack.len()
		if stackLen < operation.minStack {
			return nil, newStackUnderflowError(operation.minStack, stackLen)
		}
		if stackLen > operation.maxInputStack {
			return nil, newStackOverflowError()
		}
		if in.readOnly && operation.writes {
			return nil, ErrWriteProtection
		}
		if operation.energyCost > 0 {
			if !contract.UseEnergy(operation.energyCost) {
				return nil, newOutOfEnergyError(op, contract, operation.energyCost, 0, false)
			}
		} else {
			// Dynamic-cost handlers populate energyErr through useEnergy. Clear a
			// prior opcode's detail only for those handlers; fixed-cost opcodes
			// never consult it.
			in.energyErr = nil
		}

		var ret []byte
		var err error
		if op >= PUSH1 && op <= PUSH32 {
			ret, err = opPush(pc, in, contract, mem, stack)
		} else {
			ret, err = operation.execute(pc, in, contract, mem, stack)
		}
		if err != nil {
			if errors.Is(err, ErrOutOfEnergy) {
				return nil, in.outOfEnergyError()
			}
			return nil, err
		}
		if op == STOP || op == RETURN || op == REVERT || op == SELFDESTRUCT {
			if op == REVERT {
				return ret, ErrExecutionReverted
			}
			return ret, nil
		}
		(*pc)++
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
// baseCost is the raw (pre-penalty) cost. When DynamicEnergy is active it is
// added to rawEnergyUsed (mirroring energyUsage accumulation in java-tron
// VM.play), and factor > 1.0× adds the incremental penalty. Before that fork,
// rawEnergyUsed is unobservable and the fast path debits baseCost directly.
//
// Returns false (out-of-energy) if the contract cannot pay. Callers must
// propagate ErrOutOfEnergy on false return.
func (in *Interpreter) useEnergy(contract *Contract, baseCost uint64) bool {
	if baseCost == 0 {
		return true
	}
	if !in.tvmConfig.DynamicEnergy {
		if contract.UseEnergy(baseCost) {
			return true
		}
		in.energyErr = newOutOfEnergyError(in.currentOp, contract, baseCost, 0, false)
		return false
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
	// The protocol's ContractState feedback counter includes the requested raw
	// cost even when the charge fails. Keep that behavior in rawEnergyUsed, but
	// exclude the unexecuted request from process-local execution telemetry.
	in.rawEnergyUncharged += baseCost
	in.energyErr = newOutOfEnergyError(in.currentOp, contract, baseCost, penalty, hasPenalty)
	return false
}

func (in *Interpreter) outOfEnergyError() error {
	if in.energyErr != nil {
		return in.energyErr
	}
	return ErrOutOfEnergy
}

// opPush handles PUSH1..PUSH32. The current opcode already encodes the operand
// width, so every jump-table entry can share one immutable handler instead of
// loading a closure-captured size on TVM's most frequent opcode family.
func opPush(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	size := uint64(interpreter.currentOp - PUSH0)
	startMin := *pc + 1
	endMin := startMin + size
	if endMin > uint64(len(contract.Code)) {
		endMin = uint64(len(contract.Code))
	}

	if size == 1 && startMin < uint64(len(contract.Code)) {
		stack.pushByte(contract.Code[startMin])
	} else {
		stack.pushBytes(contract.Code[startMin:endMin])
	}
	*pc += size
	return nil, nil
}

// opDup handles DUP1..DUP16 using the width encoded in the current opcode.
func opDup(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
	stack.dup(int(interpreter.currentOp-DUP1) + 1)
	return nil, nil
}

// makeSwap creates a SWAP instruction handler.
func makeSwap(n int) executionFunc {
	return func(pc *uint64, interpreter *Interpreter, contract *Contract, memory *Memory, stack *Stack) ([]byte, error) {
		stack.swap(n)
		return nil, nil
	}
}
