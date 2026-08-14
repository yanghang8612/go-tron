package rpc

import (
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRPCLogFieldsBoundRemoteControlledValues(t *testing.T) {
	rawID := json.RawMessage(`"` + strings.Repeat("x", 4_096) + `"`)
	id := idForLog{rawID}
	if !id.Truncated() || id.Len() != len(rawID) {
		t.Fatalf("request ID metadata = len %d truncated %t", id.Len(), id.Truncated())
	}
	if got := id.String(); len(got) > 96 || !strings.Contains(got, "sha256=") {
		t.Fatalf("bounded request ID = %q", got)
	}

	errorText := strings.Repeat("remote-error-", 100)
	bounded := boundedRPCLogText(errorText)
	if len(bounded) >= len(errorText) || !strings.Contains(bounded, "truncated bytes=") || !strings.Contains(bounded, "sha256=") {
		t.Fatalf("bounded error text = %q", bounded)
	}
	method := strings.Repeat("remote_method_", 100)
	if got := boundedRPCMethodLog(method); len(got) > 80 || !strings.Contains(got, "sha256=") {
		t.Fatalf("bounded method = %q", got)
	}
}

func TestRPCClientErrorsUseDiagnosticLevelClassification(t *testing.T) {
	for _, code := range []int{-32700, -32600, -32601, -32602} {
		if !isRPCClientErrorCode(code) {
			t.Errorf("code %d should be classified as a client error", code)
		}
	}
	for _, code := range []int{-32603, -32000, 3} {
		if isRPCClientErrorCode(code) {
			t.Errorf("code %d should retain server/application warning visibility", code)
		}
	}
}

func TestRPCServerErrorWarnLimiterAggregatesConcurrently(t *testing.T) {
	var limiter rpcWarnLimiter
	base := time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	if report, suppressed := limiter.Allow(base); !report || suppressed != 0 {
		t.Fatalf("initial event = report %t suppressed %d, want true/0", report, suppressed)
	}

	const concurrent = 64
	var wg sync.WaitGroup
	var unexpected atomic.Uint64
	for range concurrent {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if report, _ := limiter.Allow(base.Add(time.Second)); report {
				unexpected.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := unexpected.Load(); got != 0 {
		t.Fatalf("unexpected reports inside interval = %d", got)
	}
	if report, suppressed := limiter.Allow(base.Add(rpcErrorWarnInterval)); !report || suppressed != concurrent {
		t.Fatalf("summary event = report %t suppressed %d, want true/%d", report, suppressed, concurrent)
	}
}
