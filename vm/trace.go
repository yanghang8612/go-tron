package vm

import (
	"fmt"
	"os"
	"strings"
	"sync"

	tcommon "github.com/tronprotocol/go-tron/common"
)

// Lightweight opt-in VM opcode tracer for sync-stall diagnosis. Completely off
// unless the GTRON_TVM_TRACE env var names a file path; when set, every executed
// opcode (across all call frames) is recorded with its contract, pc, opcode and
// the top stack words, and the whole trace is flushed to that file at the end of
// a TriggerConstantContract call. Zero cost on the production path (a single
// bool check per opcode). NOT for mainnet — diagnostics only.
var (
	traceEnabled bool
	tracePath    string
	traceOnce    sync.Once
	traceMu      sync.Mutex
	traceBuf     []string
)

func traceInit() {
	traceOnce.Do(func() {
		if p := os.Getenv("GTRON_TVM_TRACE"); p != "" {
			tracePath = p
			traceEnabled = true
		}
	})
}

// traceStep records one opcode step. Cheap no-op when tracing is disabled.
func (in *Interpreter) traceStep(addr tcommon.Address, pc uint64, op OpCode, stack *Stack) {
	if !traceEnabled {
		return
	}
	var sb strings.Builder
	depth := 0
	if in.tvm != nil {
		depth = in.tvm.Depth
	}
	fmt.Fprintf(&sb, "d%d %x pc=%d %-18s |", depth, addr[1:5], pc, op.String())
	n := len(stack.data)
	for i := 0; i < 6 && i < n; i++ {
		fmt.Fprintf(&sb, " %x", &stack.data[n-1-i])
	}
	traceMu.Lock()
	// Soft cap: keep the most recent steps (a stall's condition check is at the
	// end), dropping the oldest half if the buffer grows unreasonably large.
	if len(traceBuf) >= 2_000_000 {
		traceBuf = append(traceBuf[:0], traceBuf[1_000_000:]...)
	}
	traceBuf = append(traceBuf, sb.String())
	traceMu.Unlock()
}

// DumpTVMTrace flushes the accumulated opcode trace to the configured file
// (appending a reason banner) and clears the buffer. Called by the constant-call
// path after execution. No-op when tracing is disabled.
func DumpTVMTrace(reason string) {
	traceInit()
	if !traceEnabled {
		return
	}
	traceMu.Lock()
	lines := traceBuf
	traceBuf = nil
	traceMu.Unlock()
	f, err := os.OpenFile(tracePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "=== TVM trace: %s (%d steps) ===\n", reason, len(lines))
	for _, l := range lines {
		f.WriteString(l)
		f.WriteString("\n")
	}
}
