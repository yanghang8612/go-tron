// Package metricsapi exposes the process-wide metrics registry in Prometheus
// format on a dedicated, opt-in HTTP listener.
//
// The listener intentionally does not serve pprof or any other debug route.
// This lets operators scrape metrics without making runtime profiles remotely
// reachable.
package metricsapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	metricsprometheus "github.com/ethereum/go-ethereum/metrics/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
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

// NewServerWithChainMetrics adds the node's isolated TRON metric families to
// the existing geth-compatible /metrics endpoint.
func NewServerWithChainMetrics(addr string, chain *ChainMetrics) *Server {
	return newServer(addr, metrics.DefaultRegistry, chain.registry)
}

func newServer(addr string, registry metrics.Registry, chainRegistry ...*prometheus.Registry) *Server {
	mux := http.NewServeMux()
	handler := metricsprometheus.Handler(registry)
	if len(chainRegistry) != 0 {
		handler = appendChainMetrics(handler, chainRegistry[0])
	}
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

type combinedMetricsWriter struct {
	http.ResponseWriter
}

func (w combinedMetricsWriter) Write(data []byte) (int, error) {
	// The geth handler's length describes only its own contribution.
	w.Header().Del("Content-Length")
	return w.ResponseWriter.Write(data)
}

func appendChainMetrics(base http.Handler, registry *prometheus.Registry) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		families, err := registry.Gather()
		if err != nil {
			http.Error(w, "collect TRON metrics", http.StatusInternalServerError)
			return
		}
		var chain bytes.Buffer
		for _, family := range families {
			if _, err := expfmt.MetricFamilyToText(&chain, family); err != nil {
				http.Error(w, "encode TRON metrics", http.StatusInternalServerError)
				return
			}
		}
		base.ServeHTTP(combinedMetricsWriter{w}, r)
		_, _ = w.Write(chain.Bytes())
	})
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
