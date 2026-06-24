package tracers

import (
	"fmt"
	"os"
	"strings"
	"sync"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/vm"
)

// fileLoggerSoftCap bounds the in-memory line buffer; once exceeded the oldest
// half is dropped (a stall's failing condition check is near the end of the
// trace), mirroring the old vm.traceStep soft cap.
const fileLoggerSoftCap = 2_000_000

// FileLogger is the GTRON_TVM_TRACE diagnostic tracer: it records a compact
// one-line-per-opcode trace across all call frames and flushes it (with a reason
// banner) to a file. It supersedes the old bespoke vm.traceStep / DumpTVMTrace
// mechanism by riding the unified vm.Tracer hook stream — diagnostics only, not
// for mainnet. Each line is "d<depth> <addr[1:5]> pc=<pc> <OP> | <top stack words>".
type FileLogger struct {
	path  string
	mu    sync.Mutex
	lines []string
}

// NewFileLogger builds a FileLogger that flushes to path.
func NewFileLogger(path string) *FileLogger {
	return &FileLogger{path: path}
}

// FileLoggerFromEnv returns a FileLogger writing to the path named by the
// GTRON_TVM_TRACE env var, or nil when the var is unset/empty (the common,
// zero-overhead production case).
func FileLoggerFromEnv() *FileLogger {
	if p := os.Getenv("GTRON_TVM_TRACE"); p != "" {
		return NewFileLogger(p)
	}
	return nil
}

func (f *FileLogger) CaptureStart(from, to tcommon.Address, create bool, input []byte, energy uint64, value int64) {
}

func (f *FileLogger) CaptureEnd(output []byte, energyUsed uint64, err error) {}

func (f *FileLogger) CaptureEnter(typ vm.OpCode, from, to tcommon.Address, input []byte, energy uint64, value int64) {
}

func (f *FileLogger) CaptureExit(output []byte, energyUsed uint64, err error) {}

// CaptureState records the pre-execute step. The scope's stack is the
// pre-execute operand snapshot, so a failing comparison's operands are visible.
func (f *FileLogger) CaptureState(pc uint64, op vm.OpCode, energy, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
	var sb strings.Builder
	addr := scope.Contract.Address
	fmt.Fprintf(&sb, "d%d %x pc=%d %-18s |", depth, addr[1:5], pc, op.String())
	if scope.Stack != nil {
		data := scope.Stack.Data()
		n := len(data)
		for i := 0; i < 6 && i < n; i++ {
			fmt.Fprintf(&sb, " %x", &data[n-1-i])
		}
	}

	f.mu.Lock()
	if len(f.lines) >= fileLoggerSoftCap {
		f.lines = append(f.lines[:0], f.lines[fileLoggerSoftCap/2:]...)
	}
	f.lines = append(f.lines, sb.String())
	f.mu.Unlock()
}

func (f *FileLogger) CaptureFault(pc uint64, op vm.OpCode, energy, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}

// Flush writes the accumulated trace (with a reason banner) to the file,
// truncating it, and clears the buffer. No-op-safe to call once per traced call.
func (f *FileLogger) Flush(reason string) error {
	f.mu.Lock()
	lines := f.lines
	f.lines = nil
	f.mu.Unlock()

	file, err := os.OpenFile(f.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := fmt.Fprintf(file, "=== TVM trace: %s (%d steps) ===\n", reason, len(lines)); err != nil {
		return err
	}
	for _, l := range lines {
		if _, err := file.WriteString(l + "\n"); err != nil {
			return err
		}
	}
	return nil
}
