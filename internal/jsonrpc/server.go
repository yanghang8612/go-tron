package jsonrpc

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tronprotocol/go-tron/internal/rpc"
)

// Server runs the Ethereum-compatible JSON-RPC server. HTTP JSON-RPC requests
// are dispatched by the reflection-based internal/rpc framework over the
// Web3API/NetAPI/EthAPI services; WebSocket upgrades are handed to the TRON
// subscription manager (newHeads/logs), which the framework does not own.
type Server struct {
	httpServer *http.Server
	filters    *FilterManager
}

// NewServer creates a JSON-RPC server on the given port.
func NewServer(backend Backend, port int) *Server {
	sm := newSubscriptionManager()
	fm := NewFilterManager(backend)
	fm.subMgr = sm
	fm.Start()

	rpcSrv := rpc.NewServer()
	for _, reg := range []struct {
		name string
		svc  interface{}
	}{
		{"web3", new(Web3API)},
		{"net", NewNetAPI(backend)},
		{"eth", NewEthAPI(backend, fm)},
	} {
		// RegisterName only fails if a service exposes no eligible methods,
		// which is a static programming error for these fixed structs.
		if err := rpcSrv.RegisterName(reg.name, reg.svc); err != nil {
			panic(fmt.Sprintf("jsonrpc: register %q service: %v", reg.name, err))
		}
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if websocket.IsWebSocketUpgrade(r) {
			sm.ServeWS(w, r)
			return
		}
		rpcSrv.ServeHTTP(w, r)
	})

	return &Server{
		httpServer: &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: handler,
		},
		filters: fm,
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
	if s.filters != nil {
		s.filters.Stop()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}
