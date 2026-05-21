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
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	runtimepprof "runtime/pprof"
	"time"

	gtronlog "github.com/tronprotocol/go-tron/common/log"
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

	return &Server{
		httpServer: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
		addr: addr,
	}
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
