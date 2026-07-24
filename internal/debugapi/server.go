// Package debugapi serves runtime profiling endpoints for live diagnosis of a
// running gtron process — CPU profiles, heap snapshots, goroutine dumps,
// block/mutex profiles, and the standard /debug/pprof/ index.
//
// Mirrors the layering of internal/tronapi: a thin Server type that owns an
// *http.Server, started/stopped as a node.Lifecycle. Routes are mounted on a
// dedicated mux so they never leak into the public HTTP API surface (port
// 8090).
//
// The server is opt-in: gtron registers it only when --pprof.port > 0. The
// default bind address is 127.0.0.1 so an operator must explicitly opt in to
// remote profiling — pprof exposes runtime internals and is not authenticated.
//
// Why not reuse the tronapi mux? Two reasons. (1) Security: pprof endpoints
// can be used to denial-of-service the process via repeated CPU profiles or
// to leak heap contents; keeping them on a separate, default-localhost port
// makes the exposure footprint a deliberate operator choice. (2) Future
// surface: /debug/metrics (go-ethereum-style Timer/Meter dump) will land
// here too; bundling them keeps the diagnostic surface in one place.
package debugapi

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	runtimepprof "runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

var log = gtronlog.NewModule("debugapi")

// Server hosts the pprof endpoints. Implements node.Lifecycle.
type Server struct {
	httpServer *http.Server
	addr       string
	listener   net.Listener // populated after Start; exposes the bound port when addr used :0
}

// ListenAddr returns the actual bind address after Start. Returns the empty
// string if Start has not been called yet. Useful for tests that bind to :0.
func (s *Server) ListenAddr() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// NewServer constructs the server bound to addr (e.g. "127.0.0.1:6060").
// The caller is expected to gate construction on a non-zero port.
func NewServer(addr string) *Server {
	mux := http.NewServeMux()

	// Standard pprof index + per-profile handlers. Mounting these explicitly
	// (rather than via DefaultServeMux side-effects on net/http/pprof import)
	// keeps the mux isolated from anything else that might want pprof on the
	// default mux in a test binary.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// Toggle block + mutex profilers via query: ?rate=N for block (nanoseconds
	// between samples, 0 disables), ?fraction=N for mutex (1/N events sampled,
	// 0 disables). Both default to off on a Go process; enabling them adds
	// per-event overhead so we expose the toggle here rather than turning them
	// on unconditionally at startup.
	mux.HandleFunc("/debug/block", func(w http.ResponseWriter, r *http.Request) {
		rate := parseIntDefault(r.URL.Query().Get("rate"), 0)
		runtime.SetBlockProfileRate(rate)
		fmt.Fprintf(w, "block profile rate set to %d ns\n", rate)
	})
	mux.HandleFunc("/debug/mutex", func(w http.ResponseWriter, r *http.Request) {
		frac := parseIntDefault(r.URL.Query().Get("fraction"), 0)
		runtime.SetMutexProfileFraction(frac)
		fmt.Fprintf(w, "mutex profile fraction set to %d\n", frac)
	})

	// Plain text dumps for ad-hoc inspection without a pprof toolchain.
	mux.HandleFunc("/debug/goroutines", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		runtimepprof.Lookup("goroutine").WriteTo(w, 2)
	})

	// /debug/state-hotspots: per-(domain,key) write activity since process
	// start. Supports query params:
	//   ?top=N            limit results (default 100; 0 = unlimited)
	//   ?sort=activity|puts|deletes|bytes  (default activity)
	//   ?domain=Name      filter to a single KVDomain (e.g. SystemDynamicProperty)
	//   ?enabled=true|false  toggle tracker recording (POST/GET both honored)
	//   ?reset=true       clear all tracked entries (no-op when false/absent)
	// Returns JSON: { enabled: bool, count: int, top: [ { domain, keyHex,
	// puts, deletes, putBytes } ] }
	mux.HandleFunc("/debug/state-hotspots", stateHotspotsHandler)

	// /debug/metrics exposes snapshots of the process metrics registry. Pebble
	// already updates cache/filter/compaction gauges every three seconds; making
	// them reachable here lets live sync sampling distinguish block-cache misses,
	// table-cache misses, Bloom-filter effectiveness, and compaction debt without
	// enabling any additional hot-path instrumentation. Use ?prefix=cache/ (or
	// another metric-name prefix) to keep the response focused.
	mux.HandleFunc("/debug/metrics", metricsHandler)
	// The deployment gateway already proxies /debug/pprof/ as one protected
	// diagnostic prefix. Keep an alias below it so operators can use metrics
	// immediately without adding another externally reachable Nginx location.
	mux.HandleFunc("/debug/pprof/metrics", metricsHandler)

	return &Server{
		httpServer: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
		addr: addr,
	}
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	all := metrics.DefaultRegistry.GetAll()
	selected := make(map[string]map[string]interface{}, len(all))
	for name, values := range all {
		if prefix == "" || strings.HasPrefix(name, prefix) {
			selected[name] = values
		}
	}
	out := struct {
		Prefix  string                            `json:"prefix,omitempty"`
		Count   int                               `json:"count"`
		Metrics map[string]map[string]interface{} `json:"metrics"`
	}{
		Prefix:  prefix,
		Count:   len(selected),
		Metrics: selected,
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
}

// Start begins listening. Implements node.Lifecycle.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("debug listen: %w", err)
	}
	s.listener = ln
	log.Info("Debug API listening", "addr", ln.Addr().String())
	go s.httpServer.Serve(ln)
	return nil
}

// Stop shuts down the server with a short grace period.
func (s *Server) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}

func parseIntDefault(s string, def int) int {
	if s == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return def
	}
	return n
}

// stateHotspotsHandler serves /debug/state-hotspots. Uses the process
// singleton state.DefaultKVHotspotTracker.
func stateHotspotsHandler(w http.ResponseWriter, r *http.Request) {
	tracker := state.DefaultKVHotspotTracker()
	q := r.URL.Query()

	if v := q.Get("enabled"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			tracker.SetEnabled(b)
		}
	}
	if v := q.Get("reset"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil && b {
			tracker.Reset()
		}
	}

	top := parseIntDefault(q.Get("top"), 100)
	sortKey := state.HotspotSortByActivity
	switch q.Get("sort") {
	case "puts":
		sortKey = state.HotspotSortByPuts
	case "deletes":
		sortKey = state.HotspotSortByDeletes
	case "bytes":
		sortKey = state.HotspotSortByBytes
	}
	domainFilter := q.Get("domain")

	entries := tracker.Top(top, sortKey, domainFilter)

	type row struct {
		Domain   string `json:"domain"`
		KeyHex   string `json:"keyHex"`
		Puts     uint64 `json:"puts"`
		Deletes  uint64 `json:"deletes"`
		PutBytes uint64 `json:"putBytes"`
	}
	out := struct {
		Enabled bool  `json:"enabled"`
		Count   int   `json:"count"`
		Top     []row `json:"top"`
	}{
		Enabled: tracker.Enabled(),
		Count:   len(entries),
		Top:     make([]row, 0, len(entries)),
	}
	for _, e := range entries {
		out.Top = append(out.Top, row{
			Domain:   kvdomains.Name(e.Domain),
			KeyHex:   hex.EncodeToString(e.Key),
			Puts:     e.Puts,
			Deletes:  e.Deletes,
			PutBytes: e.PutBytes,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
}
