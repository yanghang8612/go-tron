package tronapi

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

type Server struct {
	httpServer *http.Server
	api        *API
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

func (s *Server) Start() error {
	go s.httpServer.ListenAndServe()
	return nil
}

func (s *Server) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}
