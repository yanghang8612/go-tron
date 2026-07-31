// Package metricsapi exposes the process-wide metrics registry in Prometheus
// format on a dedicated, opt-in HTTP listener.
//
// The listener intentionally does not serve pprof or any other debug route.
// This lets operators scrape metrics without making runtime profiles remotely
// reachable.
package metricsapi

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	metricsprometheus "github.com/ethereum/go-ethereum/metrics/prometheus"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
)

var log = gtronlog.NewModule("metricsapi")

// Server hosts Prometheus metrics. It implements node.Lifecycle.
type Server struct {
	httpServer *http.Server
	addr       string
	listener   net.Listener
}

// NewServer constructs a server backed by the process-wide metrics registry.
func NewServer(addr string) *Server {
	return newServer(addr, metrics.DefaultRegistry)
}

func newServer(addr string, registry metrics.Registry) *Server {
	mux := http.NewServeMux()
	handler := metricsprometheus.Handler(registry)
	mux.Handle("/metrics", handler)
	// Keep an Erigon-compatible alias for existing dashboards and scrape jobs.
	mux.Handle("/debug/metrics/prometheus", handler)

	return &Server{
		httpServer: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		addr: addr,
	}
}

// ListenAddr returns the actual bound address after Start.
func (s *Server) ListenAddr() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Start begins listening.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("metrics listen: %w", err)
	}
	s.listener = ln
	log.Info("Prometheus metrics listening", "addr", ln.Addr().String())
	go func() {
		if err := s.httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("Prometheus metrics server stopped", "err", err)
		}
	}()
	return nil
}

// Stop gracefully shuts down the listener.
func (s *Server) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}
