package tronapi

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"
)

type Server struct {
	httpServer *http.Server
	api        *API
	listener   net.Listener
}

func NewServer(backend Backend, port int) *Server {
	api := NewAPI(backend)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	return &Server{
		httpServer: &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: mux,
		},
		api: api,
	}
}

// ListenAddr returns the actual bind address after Start. It is useful for
// tests that start the server on port 0.
func (s *Server) ListenAddr() string {
	if s == nil || s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("http listen: %w", err)
	}
	s.listener = ln
	go s.httpServer.Serve(ln)
	return nil
}

func (s *Server) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}
