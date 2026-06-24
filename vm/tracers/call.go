package tracers

import (
	"encoding/hex"
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/vm"
)

// CallFrame is one node of the callTracer call tree. Energy fills the gas
// fields; addresses are TRON-format (21-byte, 0x41-prefixed) 0x-hex.
type CallFrame struct {
	Type    string       `json:"type"`
	From    string       `json:"from"`
	To      string       `json:"to,omitempty"`
	Value   string       `json:"value,omitempty"`
	Gas     string       `json:"gas"`
	GasUsed string       `json:"gasUsed"`
	Input   string       `json:"input"`
	Output  string       `json:"output,omitempty"`
	Error   string       `json:"error,omitempty"`
	Calls   []*CallFrame `json:"calls,omitempty"`
}

// processOutput fills the frame's output/error on exit, mirroring geth: a plain
// success records the output; a REVERT records both the error and the revert
// payload; any other failure records only the error.
func (f *CallFrame) processOutput(output []byte, err error) {
	if err == nil {
		if len(output) > 0 {
			f.Output = bytesHex(output)
		}
		return
	}
	f.Error = err.Error()
	if err == vm.ErrExecutionReverted && len(output) > 0 {
		f.Output = bytesHex(output)
	}
}

// callTracer builds the call tree from the frame hook stream
// (CaptureStart/Enter open frames, CaptureExit/End close them).
type callTracer struct {
	callstack []*CallFrame
}

func newCallTracer() *callTracer {
	return &callTracer{callstack: make([]*CallFrame, 0, 8)}
}

func (t *callTracer) CaptureStart(from, to tcommon.Address, create bool, input []byte, energy uint64, value int64) {
	typ := vm.CALL
	if create {
		typ = vm.CREATE
	}
	t.callstack = append(t.callstack, &CallFrame{
		Type:  typ.String(),
		From:  addrHex(from),
		To:    addrHex(to),
		Input: bytesHex(input),
		Gas:   uintHex(energy),
		Value: valueHex(value),
	})
}

func (t *callTracer) CaptureEnd(output []byte, energyUsed uint64, err error) {
	if len(t.callstack) == 0 {
		return
	}
	t.callstack[0].GasUsed = uintHex(energyUsed)
	t.callstack[0].processOutput(output, err)
}

func (t *callTracer) CaptureEnter(typ vm.OpCode, from, to tcommon.Address, input []byte, energy uint64, value int64) {
	t.callstack = append(t.callstack, &CallFrame{
		Type:  typ.String(),
		From:  addrHex(from),
		To:    addrHex(to),
		Input: bytesHex(input),
		Gas:   uintHex(energy),
		Value: valueHex(value),
	})
}

func (t *callTracer) CaptureExit(output []byte, energyUsed uint64, err error) {
	size := len(t.callstack)
	if size <= 1 {
		// Unbalanced exit (no matching enter); drop defensively.
		return
	}
	call := t.callstack[size-1]
	t.callstack = t.callstack[:size-1]
	call.GasUsed = uintHex(energyUsed)
	call.processOutput(output, err)
	t.callstack[size-2].Calls = append(t.callstack[size-2].Calls, call)
}

func (t *callTracer) CaptureState(pc uint64, op vm.OpCode, energy, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
}

func (t *callTracer) CaptureFault(pc uint64, op vm.OpCode, energy, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}

// GetResult returns the root call frame once the outermost frame has closed.
func (t *callTracer) GetResult() (interface{}, error) {
	if len(t.callstack) != 1 {
		return nil, fmt.Errorf("callTracer: %d frames still open", len(t.callstack))
	}
	return t.callstack[0], nil
}

func addrHex(a tcommon.Address) string { return "0x" + hex.EncodeToString(a[:]) }

func bytesHex(b []byte) string {
	if len(b) == 0 {
		return "0x"
	}
	return "0x" + hex.EncodeToString(b)
}

func uintHex(n uint64) string { return fmt.Sprintf("0x%x", n) }

// valueHex renders a non-zero TRX value as 0x-hex; a zero value yields the empty
// string so the omitempty `value` field is dropped (value-less frames such as
// STATICCALL/DELEGATECALL and zero-value CALLs omit it, as geth does).
func valueHex(v int64) string {
	if v == 0 {
		return ""
	}
	return fmt.Sprintf("0x%x", v)
}
