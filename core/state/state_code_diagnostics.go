package state

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	tcommon "github.com/tronprotocol/go-tron/common"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
)

const (
	stateCodeDiagnosticBurst    = 8
	stateCodeDiagnosticWindow   = time.Minute
	stateCodeDiagnosticFrames   = 16
	stateCodeDiagnosticFrameLen = 180
)

var (
	stateCodeDiagnosticLog        = gtronlog.NewModule("core/state/code")
	stateCodeDiagnosticLogged     = metrics.NewRegisteredCounter("state/code_hash/diagnostic/logged", nil)
	stateCodeDiagnosticSuppressed = metrics.NewRegisteredCounter("state/code_hash/diagnostic/suppressed", nil)
	stateCodeRejectionDiagnostics = new(stateCodeDiagnosticLimiter)
)

// StateDB views are execution-confined. This value carries only already-known
// context; constructing it performs no state reads, hashing, or stack capture.
// A content-hash-only caller leaves addressKnown false instead of inventing a
// current contract address for historical code.
type stateCodeDiagnosticContext struct {
	state        *StateDB
	address      tcommon.Address
	addressKnown bool
	source       string
}

// The limiter is process-wide and fixed-size, including when different StateDB
// views or attacker-controlled hashes reject concurrently. Suppressed events
// still increment the existing rejection counters and a diagnostic counter.
type stateCodeDiagnosticLimiter struct {
	mu         sync.Mutex
	window     time.Time
	emitted    int
	suppressed uint64
}

func (l *stateCodeDiagnosticLimiter) allow(now time.Time) (bool, uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.window.IsZero() || now.Sub(l.window) >= stateCodeDiagnosticWindow {
		l.window, l.emitted = now, 0
	}
	if l.emitted >= stateCodeDiagnosticBurst {
		l.suppressed++
		return false, 0
	}
	l.emitted++
	suppressed := l.suppressed
	l.suppressed = 0
	return true, suppressed
}

// report receives the hash already computed by the rejecting branch. It never
// retains or logs bytecode, retries a read, or modifies StateDB/journal state.
// Stack capture and formatting occur only for admitted diagnostic events.
func (c stateCodeDiagnosticContext) report(expected, actual tcommon.Hash, codeBytes int) {
	allowed, suppressed := stateCodeRejectionDiagnostics.allow(time.Now())
	if !allowed {
		stateCodeDiagnosticSuppressed.Inc(1)
		return
	}
	fields := []any{
		"source", c.source,
		"expectedHash", expected.Hex(), "actualHash", actual.Hex(), "codeBytes", codeBytes,
		"addressKnown", c.addressKnown, "suppressedSinceLastLog", suppressed,
	}
	if c.addressKnown {
		fields = append(fields, "contract", c.address.Hex())
	}
	if s := c.state; s != nil {
		fields = append(fields, "codeStore", fmt.Sprintf("%T", s.getStateCodeStore()),
			"versionedReader", s.transactionVersionedReader != nil)
		if c.addressKnown {
			if obj := s.stateObjects[c.address]; obj != nil {
				fields = append(fields, "accountGeneration", obj.accountKVGeneration, "codeDirty", obj.codeDirty)
			}
		}
		if s.changeSet.enabled {
			fields = append(fields, "captureBlock", s.changeSet.blockNum, "captureBlockHash", s.changeSet.blockHash.Hex(),
				"captureTxNum", s.changeSet.txNum)
		}
		if s.codeColdHistory != nil {
			fields = append(fields, "coldTxNumCutoff", s.codeColdTxNum)
		}
		if s.transactionVersionedReader != nil {
			fields = append(fields, "versionedTxIndex", s.transactionVersionedTxIndex)
		}
		if trace := s.balanceTrace; trace != nil && trace.currentTx != nil {
			fields = append(fields, "traceBlock", trace.blockNum,
				"traceTxHash", tcommon.BytesToHash(trace.currentTx.TransactionIdentifier).Hex())
		}
	}
	var pcs [stateCodeDiagnosticFrames]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	var callers strings.Builder
	for i := 0; i < stateCodeDiagnosticFrames; i++ {
		frame, more := frames.Next()
		if callers.Len() > 0 {
			callers.WriteByte(';')
		}
		name := frame.Function
		if len(name) > stateCodeDiagnosticFrameLen {
			name = name[:stateCodeDiagnosticFrameLen]
		}
		callers.WriteString(name)
		if !more {
			break
		}
	}
	fields = append(fields, "callers", callers.String())
	stateCodeDiagnosticLogged.Inc(1)
	stateCodeDiagnosticLog.Warn("State code hash verification rejected", fields...)
}
