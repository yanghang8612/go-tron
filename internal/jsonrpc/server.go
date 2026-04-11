package jsonrpc

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"
)

// Server runs the Ethereum-compatible JSON-RPC HTTP server.
type Server struct {
	httpServer *http.Server
	api        *API
}

// NewServer creates a JSON-RPC server on the given port.
func NewServer(backend Backend, port int) *Server {
	api := &API{backend: backend}
	return &Server{
		httpServer: &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: api,
		},
		api: api,
	}
}

// Handler returns the HTTP handler for use in tests.
func (s *Server) Handler() http.Handler {
	return s.httpServer.Handler
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("jsonrpc listen: %w", err)
	}
	go s.httpServer.Serve(ln)
	return nil
}

func (s *Server) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}
