package debugapi

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestServer_PProfIndexServes binds on an ephemeral port, fetches the pprof
// index, and verifies the standard "/debug/pprof/" page renders. This catches
// regressions where a route gets dropped (e.g. someone replaces the mux
// without re-mounting pprof.Index).
func TestServer_PProfIndexServes(t *testing.T) {
	s := NewServer("127.0.0.1:0")
	t.Cleanup(func() { _ = s.Stop() })
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Start spawns Serve on a goroutine; give it a tick to bind.
	time.Sleep(20 * time.Millisecond)

	addr := s.ListenAddr()
	if !strings.Contains(addr, ":") {
		t.Fatalf("expected host:port addr, got %q", addr)
	}
	url := "http://" + addr + "/debug/pprof/"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "goroutine") {
		t.Fatalf("expected pprof index to mention 'goroutine', got:\n%s", body)
	}
}
