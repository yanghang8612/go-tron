package tracers

import (
	"fmt"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/vm"
)

// LogConfig are the struct-logger toggles, mirroring the struct-log fields of
// geth's debug TraceConfig (logger.Config). The zero value captures stack,
// memory and storage with return data off and no limit, matching geth's
// defaults for the disable* flags.
type LogConfig struct {
	DisableStack     bool
	DisableMemory    bool
	DisableStorage   bool
	EnableReturnData bool
	Limit            int
}

// StructLog is a single captured opcode step (pre-rendering form).
type StructLog struct {
	Pc         uint64
	Op         vm.OpCode
	Gas        uint64
	GasCost    uint64
	Memory     []byte
	Stack      []uint256.Int
	ReturnData []byte
	Storage    map[tcommon.Hash]tcommon.Hash
	Depth      int
	Err        error
}

// StructLogger is the default ("structLogger") tracer: it accumulates an
// EIP-3155 opcode stream and renders the geth debug_trace* default result
// {gas, failed, returnValue, structLogs}. Energy fills the gas/gasCost slots.
type StructLogger struct {
	cfg     LogConfig
	logs    []StructLog
	storage map[tcommon.Address]map[tcommon.Hash]tcommon.Hash
	output  []byte
	err     error
	usedGas uint64
}

// NewStructLogger builds a struct logger honouring the given toggles.
func NewStructLogger(cfg LogConfig) *StructLogger {
	return &StructLogger{
		cfg:     cfg,
		storage: make(map[tcommon.Address]map[tcommon.Hash]tcommon.Hash),
	}
}

// CaptureStart records the outermost frame entry. The struct logger needs no
// per-frame state; it derives the result from the opcode stream and CaptureEnd.
func (l *StructLogger) CaptureStart(from, to tcommon.Address, create bool, input []byte, energy uint64, value int64) {
}

// CaptureEnd records the outermost frame's output, energy used and error.
func (l *StructLogger) CaptureEnd(output []byte, energyUsed uint64, err error) {
	l.output = output
	l.usedGas = energyUsed
	if err != nil {
		l.err = err
	}
}

func (l *StructLogger) CaptureEnter(typ vm.OpCode, from, to tcommon.Address, input []byte, energy uint64, value int64) {
}

func (l *StructLogger) CaptureExit(output []byte, energyUsed uint64, err error) {}

// CaptureState appends one struct log honouring the toggles. SLOAD/SSTORE values
// are recorded into the per-contract storage view (SLOAD reads the live slot via
// the scope, SSTORE reads the value being written off the operand stack).
func (l *StructLogger) CaptureState(pc uint64, op vm.OpCode, energy, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	if l.cfg.Limit != 0 && len(l.logs) >= l.cfg.Limit {
		return
	}
	stack := scope.Stack
	contractAddr := scope.Contract.Address

	log := StructLog{Pc: pc, Op: op, Gas: energy, GasCost: cost, Depth: depth, Err: err}

	if !l.cfg.DisableStack && stack != nil {
		log.Stack = append([]uint256.Int(nil), stack.Data()...)
	}
	if !l.cfg.DisableMemory && scope.Memory != nil {
		log.Memory = append([]byte(nil), scope.Memory.Data()...)
	}
	if l.cfg.EnableReturnData {
		log.ReturnData = append([]byte(nil), rData...)
	}
	if !l.cfg.DisableStorage && stack != nil {
		if l.storage[contractAddr] == nil {
			l.storage[contractAddr] = make(map[tcommon.Hash]tcommon.Hash)
		}
		data := stack.Data()
		n := len(data)
		switch op {
		case vm.SLOAD:
			if n >= 1 {
				var key tcommon.Hash
				b := data[n-1].Bytes32()
				copy(key[:], b[:])
				l.storage[contractAddr][key] = scope.StorageAt(key)
			}
		case vm.SSTORE:
			if n >= 2 {
				var key, value tcommon.Hash
				kb := data[n-1].Bytes32()
				vb := data[n-2].Bytes32()
				copy(key[:], kb[:])
				copy(value[:], vb[:])
				l.storage[contractAddr][key] = value
			}
		}
		cp := make(map[tcommon.Hash]tcommon.Hash, len(l.storage[contractAddr]))
		for k, v := range l.storage[contractAddr] {
			cp[k] = v
		}
		log.Storage = cp
	}
	l.logs = append(l.logs, log)
}

// CaptureFault is a no-op: the faulting opcode is already recorded by the
// matching CaptureState (which carries the error), mirroring geth's StructLogger.
func (l *StructLogger) CaptureFault(pc uint64, op vm.OpCode, energy, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}

// ExecutionResult is the rendered struct-logger output, matching geth's
// debug_trace* default response shape.
type ExecutionResult struct {
	Gas         uint64         `json:"gas"`
	Failed      bool           `json:"failed"`
	ReturnValue string         `json:"returnValue"`
	StructLogs  []StructLogRes `json:"structLogs"`
}

// StructLogRes is one rendered opcode step (EIP-3155 JSON shape). TRON has no
// refund counter, so the geth "refund" field is omitted.
type StructLogRes struct {
	Pc         uint64             `json:"pc"`
	Op         string             `json:"op"`
	Gas        uint64             `json:"gas"`
	GasCost    uint64             `json:"gasCost"`
	Depth      int                `json:"depth"`
	Error      string             `json:"error,omitempty"`
	Stack      *[]string          `json:"stack,omitempty"`
	Memory     *[]string          `json:"memory,omitempty"`
	Storage    *map[string]string `json:"storage,omitempty"`
	ReturnData string             `json:"returnData,omitempty"`
}

// GetResult renders the accumulated trace into an *ExecutionResult.
func (l *StructLogger) GetResult() (interface{}, error) {
	return &ExecutionResult{
		Gas:         l.usedGas,
		Failed:      l.err != nil,
		ReturnValue: fmt.Sprintf("%x", l.output),
		StructLogs:  formatLogs(l.logs),
	}, nil
}

// formatLogs renders StructLogs into the JSON shape: stack values as minimal
// 0x-hex, memory as 32-byte hex chunks, storage as 32-byte hex key→value (all
// matching geth's eth/tracers/logger formatLogs).
func formatLogs(logs []StructLog) []StructLogRes {
	out := make([]StructLogRes, len(logs))
	for i, trace := range logs {
		out[i] = StructLogRes{
			Pc:      trace.Pc,
			Op:      trace.Op.String(),
			Gas:     trace.Gas,
			GasCost: trace.GasCost,
			Depth:   trace.Depth,
		}
		if trace.Err != nil {
			out[i].Error = trace.Err.Error()
		}
		if trace.Stack != nil {
			stack := make([]string, len(trace.Stack))
			for j := range trace.Stack {
				stack[j] = trace.Stack[j].Hex()
			}
			out[i].Stack = &stack
		}
		if trace.Memory != nil {
			mem := make([]string, 0, (len(trace.Memory)+31)/32)
			for j := 0; j+32 <= len(trace.Memory); j += 32 {
				mem = append(mem, fmt.Sprintf("%x", trace.Memory[j:j+32]))
			}
			out[i].Memory = &mem
		}
		if trace.Storage != nil {
			storage := make(map[string]string, len(trace.Storage))
			for k, v := range trace.Storage {
				key, val := k, v
				storage[fmt.Sprintf("%x", key[:])] = fmt.Sprintf("%x", val[:])
			}
			out[i].Storage = &storage
		}
		if len(trace.ReturnData) > 0 {
			out[i].ReturnData = fmt.Sprintf("0x%x", trace.ReturnData)
		}
	}
	return out
}
