package debugapi

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
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

func TestServer_MetricsServesRegistry(t *testing.T) {
	name := "debugapi/test/metrics/" + strings.ReplaceAll(t.Name(), "/", "_")
	gauge := metrics.GetOrRegisterGauge(name, nil)
	gauge.Update(42)
	t.Cleanup(func() { metrics.DefaultRegistry.Unregister(name) })

	s := NewServer("127.0.0.1:0")
	t.Cleanup(func() { _ = s.Stop() })
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	url := "http://" + s.ListenAddr() + "/debug/metrics?prefix=" + name
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Prefix  string `json:"prefix"`
		Count   int    `json:"count"`
		Metrics []struct {
			Name   string                 `json:"name"`
			Values map[string]interface{} `json:"values"`
		} `json:"metrics"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode metrics response: %v", err)
	}
	if body.Prefix != name || body.Count != 1 || len(body.Metrics) != 1 {
		t.Fatalf("metrics response = %+v, want one row for prefix %q", body, name)
	}
	if body.Metrics[0].Name != name {
		t.Fatalf("metric name = %q, want %q", body.Metrics[0].Name, name)
	}
	if got := body.Metrics[0].Values["value"]; got != float64(42) {
		t.Fatalf("metric value = %v, want 42", got)
	}
}
