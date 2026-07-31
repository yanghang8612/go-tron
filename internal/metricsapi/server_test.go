package metricsapi

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/metrics"
)

func TestServerExportsMetricsWithoutDebugRoutes(t *testing.T) {
	registry := metrics.NewRegistry()
	gauge := metrics.NewRegisteredGauge("gtron/test/value", registry)
	gauge.Update(42)

	server := newServer("127.0.0.1:0", registry)
	if err := server.Start(); err != nil {
		t.Fatalf("start metrics server: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Stop(); err != nil {
			t.Errorf("stop metrics server: %v", err)
		}
	})

	baseURL := "http://" + server.ListenAddr()
	for _, path := range []string{"/metrics", "/debug/metrics/prometheus"} {
		resp, err := http.Get(baseURL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			t.Fatalf("read %s: %v", path, readErr)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d", path, resp.StatusCode)
		}
		if !strings.Contains(string(body), "gtron_test_value 42") {
			t.Fatalf("GET %s body does not contain exported gauge: %q", path, body)
		}
	}

	resp, err := http.Get(baseURL + "/debug/pprof/")
	if err != nil {
		t.Fatalf("GET pprof route: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("pprof status = %d, want 404", resp.StatusCode)
	}
}
