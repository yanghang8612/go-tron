package vm

import (
	"testing"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// recorderTracer is a minimal Tracer that records the hook calls so tests can
// assert the per-opcode CaptureState stream (with the pre-execute operand
// stack) and the Call/Create frame boundaries.
type recorderTracer struct {
	steps  []recordedStep
	starts int
	ends   int
	enters []OpCode
	exits  int
}

type recordedStep struct {
	pc    uint64
	op    OpCode
	gas   uint64
	cost  uint64
	depth int
	stack []uint256.Int // pre-execute snapshot, bottom..top
	err   error
}

func (r *recorderTracer) CaptureStart(from, to tcommon.Address, create bool, input []byte, energy uint64, value int64) {
	r.starts++
}
func (r *recorderTracer) CaptureEnd(output []byte, energyUsed uint64, err error) { r.ends++ }
func (r *recorderTracer) CaptureEnter(typ OpCode, from, to tcommon.Address, input []byte, energy uint64, value int64) {
	r.enters = append(r.enters, typ)
}
func (r *recorderTracer) CaptureExit(output []byte, energyUsed uint64, err error) { r.exits++ }
func (r *recorderTracer) CaptureState(pc uint64, op OpCode, energy, cost uint64, scope *ScopeContext, rData []byte, depth int, err error) {
	var stk []uint256.Int
	if scope != nil && scope.Stack != nil {
		stk = append(stk, scope.Stack.Data()...)
	}
	r.steps = append(r.steps, recordedStep{pc: pc, op: op, gas: energy, cost: cost, depth: depth, stack: stk, err: err})
}
func (r *recorderTracer) CaptureFault(pc uint64, op OpCode, energy, cost uint64, scope *ScopeContext, depth int, err error) {
}

func (r *recorderTracer) ops() []OpCode {
	out := make([]OpCode, len(r.steps))
	for i, s := range r.steps {
		out[i] = s.op
	}
	return out
}

func TestTracerCaptureStateRecordsOpsAndPreExecuteStack(t *testing.T) {
	rec := &recorderTracer{}
	evm := newTestEVMWithConfig(t, TVMConfig{Tracer: rec})

	// PUSH1 3 PUSH1 4 ADD PUSH1 0 MSTORE PUSH1 32 PUSH1 0 RETURN
	code := []byte{
		byte(PUSH1), 0x03,
		byte(PUSH1), 0x04,
		byte(ADD),
		byte(PUSH1), 0x00,
		byte(MSTORE),
		byte(PUSH1), 0x20,
		byte(PUSH1), 0x00,
		byte(RETURN),
	}
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, code)

	if _, err := evm.interpreter.Run(contract); err != nil {
		t.Fatalf("run: %v", err)
	}

	wantOps := []OpCode{PUSH1, PUSH1, ADD, PUSH1, MSTORE, PUSH1, PUSH1, RETURN}
	got := rec.ops()
	if len(got) != len(wantOps) {
		t.Fatalf("op count: got %d (%v), want %d (%v)", len(got), got, len(wantOps), wantOps)
	}
	for i := range wantOps {
		if got[i] != wantOps[i] {
			t.Fatalf("op[%d]: got %v, want %v (full %v)", i, got[i], wantOps[i], got)
		}
	}

	// The ADD step (index 2) must see the two operands on the PRE-execute
	// stack, top = 4, below = 3 (Data() is bottom..top).
	add := rec.steps[2]
	if add.op != ADD {
		t.Fatalf("step 2 op: got %v, want ADD", add.op)
	}
	if len(add.stack) != 2 {
		t.Fatalf("ADD pre-execute stack depth: got %d, want 2", len(add.stack))
	}
	if add.stack[len(add.stack)-1].Uint64() != 4 || add.stack[len(add.stack)-2].Uint64() != 3 {
		t.Fatalf("ADD operands: got top=%d below=%d, want 4/3",
			add.stack[len(add.stack)-1].Uint64(), add.stack[len(add.stack)-2].Uint64())
	}

	// pc of the first PUSH1 is 0; the ADD is at pc 4.
	if rec.steps[0].pc != 0 {
		t.Fatalf("first step pc: got %d, want 0", rec.steps[0].pc)
	}
	if add.pc != 4 {
		t.Fatalf("ADD pc: got %d, want 4", add.pc)
	}
	// ADD costs EnergyVeryLow (3); the delta cost must be exact.
	if add.cost != EnergyVeryLow {
		t.Fatalf("ADD cost: got %d, want %d", add.cost, EnergyVeryLow)
	}
}

func TestTracerCaptureStateRecordsRevertOp(t *testing.T) {
	rec := &recorderTracer{}
	evm := newTestEVMWithConfig(t, TVMConfig{Tracer: rec})

	code := []byte{byte(PUSH1), 0x00, byte(PUSH1), 0x00, byte(REVERT)}
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, 100000)
	contract.SetCode(tcommon.Address{0x41, 0x02}, code)

	if _, err := evm.interpreter.Run(contract); err != ErrExecutionReverted {
		t.Fatalf("run: got %v, want ErrExecutionReverted", err)
	}
	ops := rec.ops()
	if len(ops) == 0 || ops[len(ops)-1] != REVERT {
		t.Fatalf("trace must end at REVERT, got %v", ops)
	}
}

func TestTracerFrameHooksAroundCall(t *testing.T) {
	rec := &recorderTracer{}
	evm := newTestEVMWithConfig(t, TVMConfig{Tracer: rec})
	parent := tcommon.Address{0x41, 0x21}
	child := tcommon.Address{0x41, 0x22}
	evm.StateDB.CreateAccount(parent, corepb.AccountType_Contract)
	evm.StateDB.CreateAccount(child, corepb.AccountType_Contract)
	evm.StateDB.SetCode(child, []byte{byte(STOP)})

	// Parent CALLs child (zero value), then STOPs.
	code := []byte{
		byte(PUSH1), 0x00, // retSize
		byte(PUSH1), 0x00, // retOffset
		byte(PUSH1), 0x00, // argsSize
		byte(PUSH1), 0x00, // argsOffset
		byte(PUSH1), 0x00, // value
		byte(PUSH20),
	}
	code = append(code, child[1:]...)
	code = append(code,
		byte(PUSH2), 0x27, 0x10, // energy 10000
		byte(CALL),
		byte(STOP),
	)
	evm.StateDB.SetCode(parent, code)

	if _, _, err := evm.Call(tcommon.Address{0x41, 0x01}, parent, nil, 1_000_000, 0); err != nil {
		t.Fatalf("call: %v", err)
	}

	if rec.starts != 1 {
		t.Fatalf("CaptureStart count: got %d, want 1", rec.starts)
	}
	if rec.ends != 1 {
		t.Fatalf("CaptureEnd count: got %d, want 1", rec.ends)
	}
	if len(rec.enters) != 1 || rec.enters[0] != CALL {
		t.Fatalf("CaptureEnter: got %v, want [CALL]", rec.enters)
	}
	if rec.exits != 1 {
		t.Fatalf("CaptureExit count: got %d, want 1", rec.exits)
	}
	// The outermost frame executes at depth 1 (runContract increments before Run).
	if len(rec.steps) == 0 || rec.steps[0].depth != 1 {
		t.Fatalf("top-frame depth: got %v", rec.steps)
	}
}
