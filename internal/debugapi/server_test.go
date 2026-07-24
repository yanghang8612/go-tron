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

func TestServer_MetricsSupportsPrefixFilter(t *testing.T) {
	const (
		includedName = "debugapi-test/cache/hit"
		excludedName = "debugapi-test/compact/debt"
	)
	metrics.DefaultRegistry.Unregister(includedName)
	metrics.DefaultRegistry.Unregister(excludedName)
	t.Cleanup(func() {
		metrics.DefaultRegistry.Unregister(includedName)
		metrics.DefaultRegistry.Unregister(excludedName)
	})
	metrics.GetOrRegisterGauge(includedName, nil).Update(42)
	metrics.GetOrRegisterGauge(excludedName, nil).Update(99)

	s := NewServer("127.0.0.1:0")
	t.Cleanup(func() { _ = s.Stop() })
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	resp, err := http.Get("http://" + s.ListenAddr() + "/debug/pprof/metrics?prefix=debugapi-test/cache/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Prefix  string                            `json:"prefix"`
		Count   int                               `json:"count"`
		Metrics map[string]map[string]interface{} `json:"metrics"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Prefix != "debugapi-test/cache/" || body.Count != 1 {
		t.Fatalf("metrics response prefix/count = %q/%d, want debugapi-test/cache//1", body.Prefix, body.Count)
	}
	if got := body.Metrics[includedName]["value"]; got != float64(42) {
		t.Fatalf("included gauge value = %#v, want 42", got)
	}
	if _, ok := body.Metrics[excludedName]; ok {
		t.Fatal("prefix filter included unrelated metric")
	}
}
