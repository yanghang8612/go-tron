package vm

import (
	tcommon "github.com/tronprotocol/go-tron/common"
)

// ScopeContext exposes the live execution scope to a Tracer's CaptureState
// hook: the operand stack (a pre-execute snapshot), the frame memory, and the
// current call frame. It mirrors go-ethereum's vm.ScopeContext. A storage
// reader is attached so a struct logger can record SLOAD/SSTORE values without
// coupling the tracers package to core/state.
type ScopeContext struct {
	Memory   *Memory
	Stack    *Stack
	Contract *Contract

	storage func(tcommon.Hash) tcommon.Hash
}

// StorageAt returns the current value of the executing contract's storage slot,
// or the zero hash when no storage reader is wired (e.g. a synthetic scope in a
// unit test).
func (s *ScopeContext) StorageAt(key tcommon.Hash) tcommon.Hash {
	if s == nil || s.storage == nil {
		return tcommon.Hash{}
	}
	return s.storage(key)
}

// Tracer mirrors go-ethereum's vm.EVMLogger (the EIP-3155 hook set), adapted to
// TVM semantics: every "energy"/"cost" parameter carries ENERGY (TRON's cost
// unit), which the trace consumers treat as the gas/cost field. A nil Tracer
// disables tracing at zero per-opcode overhead (one nil check per opcode).
//
//   - CaptureStart / CaptureEnd bracket the OUTERMOST call frame.
//   - CaptureEnter / CaptureExit bracket every nested CALL/CREATE frame; typ is
//     the entry opcode (CALL, DELEGATECALL, CREATE, CREATE2, …).
//   - CaptureState fires once per executed opcode with the PRE-execute operand
//     stack and the EXACT energy charged for that opcode (cost). err is set on
//     the opcode that aborts the frame.
//   - CaptureFault fires when an opcode aborts the frame; struct loggers treat
//     it as a no-op and rely on the error carried by the matching CaptureState.
type Tracer interface {
	CaptureStart(from, to tcommon.Address, create bool, input []byte, energy uint64, value int64)
	CaptureEnd(output []byte, energyUsed uint64, err error)
	CaptureEnter(typ OpCode, from, to tcommon.Address, input []byte, energy uint64, value int64)
	CaptureExit(output []byte, energyUsed uint64, err error)
	CaptureState(pc uint64, op OpCode, energy, cost uint64, scope *ScopeContext, rData []byte, depth int, err error)
	CaptureFault(pc uint64, op OpCode, energy, cost uint64, scope *ScopeContext, depth int, err error)
}

// depth returns the current 1-based call depth while a frame executes, mirroring
// the value the interpreter reports to CaptureState. Zero when no TVM is set.
func (in *Interpreter) traceDepth() int {
	if in.tvm == nil {
		return 0
	}
	return in.tvm.Depth
}

// newScope builds a ScopeContext for the tracer. The storage reader resolves
// the executing contract's slots through the live StateDB so a struct logger
// can record SLOAD/SSTORE values; it is only ever invoked on the trace path.
func (in *Interpreter) newScope(mem *Memory, stack *Stack, contract *Contract) *ScopeContext {
	return &ScopeContext{
		Memory:   mem,
		Stack:    stack,
		Contract: contract,
		storage: func(key tcommon.Hash) tcommon.Hash {
			if in.tvm == nil || in.tvm.StateDB == nil {
				return tcommon.Hash{}
			}
			return in.tvm.StateDB.GetState(contract.Address, key)
		},
	}
}

// traceState is the single CaptureState callsite used by the Run loop, both for
// the per-opcode step and for the pre-execute fault returns. It wires the live
// return-data buffer and the current depth into the hook.
func (in *Interpreter) traceState(tracer Tracer, pc uint64, op OpCode, gas, cost uint64, mem *Memory, stack *Stack, contract *Contract, err error) {
	tracer.CaptureState(pc, op, gas, cost, in.newScope(mem, stack, contract), in.returnData, in.traceDepth(), err)
}

// captureFrameStart dispatches the outer-frame CaptureStart vs the nested-frame
// CaptureEnter based on the parent call depth (tvm.Depth is the parent's depth
// at frame entry, before runContract increments it). create marks CREATE/CREATE2
// frames; typ is the entry opcode for CaptureEnter.
func (tvm *TVM) captureFrameStart(tracer Tracer, typ OpCode, from, to tcommon.Address, create bool, input []byte, energy uint64, value int64) {
	if tvm.Depth == 0 {
		tracer.CaptureStart(from, to, create, input, energy, value)
		return
	}
	tracer.CaptureEnter(typ, from, to, input, energy, value)
}

// captureFrameEnd is the symmetric exit hook. It runs from a deferred closure
// after runContract has restored tvm.Depth to the parent's value, so the
// Start/End vs Enter/Exit choice matches captureFrameStart.
func (tvm *TVM) captureFrameEnd(tracer Tracer, output []byte, energyUsed uint64, err error) {
	if tvm.Depth == 0 {
		tracer.CaptureEnd(output, energyUsed, err)
		return
	}
	tracer.CaptureExit(output, energyUsed, err)
}
