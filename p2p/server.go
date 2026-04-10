package p2p

import (
	"log"
	"net"
	"sync"
)

// ServerConfig holds P2P server configuration.
type ServerConfig struct {
	ListenAddr string   // "host:port" to listen on
	MaxPeers   int      // max peers allowed
	SeedNodes  []string // initial peers to dial
}

// Server manages TCP connections to peers.
type Server struct {
	config   ServerConfig
	handler  Handler
	listener net.Listener
	peers    map[string]*Peer
	mu       sync.RWMutex
	quit     chan struct{}
	wg       sync.WaitGroup
}

// NewServer creates a new P2P server.
func NewServer(config ServerConfig, handler Handler) *Server {
	if config.MaxPeers <= 0 {
		config.MaxPeers = 30
	}
	return &Server{
		config:  config,
		handler: handler,
		peers:   make(map[string]*Peer),
		quit:    make(chan struct{}),
	}
}

// Start begins listening and dials seed nodes.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return err
	}
	s.listener = ln
	log.Printf("P2P listening on %s", ln.Addr().String())

	s.wg.Add(1)
	go s.acceptLoop()

	// Dial seed nodes in background
	for _, addr := range s.config.SeedNodes {
		go func(addr string) {
			if err := s.AddPeer(addr); err != nil {
				log.Printf("Failed to connect to seed %s: %v", addr, err)
			}
		}(addr)
	}

	return nil
}

// Stop shuts down the server and disconnects all peers.
func (s *Server) Stop() error {
	close(s.quit)
	s.listener.Close()

	// Snapshot peers and clear map before stopping to avoid deadlock:
	// p.Stop() waits for readLoop which calls removePeer which needs the lock.
	s.mu.Lock()
	peers := make([]*Peer, 0, len(s.peers))
	for _, p := range s.peers {
		peers = append(peers, p)
	}
	s.peers = make(map[string]*Peer)
	s.mu.Unlock()

	for _, p := range peers {
		p.Stop()
	}

	s.wg.Wait()
	log.Println("P2P server stopped")
	return nil
}

// ListenAddr returns the actual listen address (useful when port is 0).
func (s *Server) ListenAddr() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// PeerCount returns the number of connected peers.
func (s *Server) PeerCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.peers)
}

// Peers returns a snapshot of all connected peers.
func (s *Server) Peers() []*Peer {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*Peer, 0, len(s.peers))
	for _, p := range s.peers {
		result = append(result, p)
	}
	return result
}

// AddPeer dials a remote address and adds the peer.
func (s *Server) AddPeer(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	return s.addPeerConn(conn, addr, false)
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
				log.Printf("P2P accept error: %v", err)
				continue
			}
		}
		addr := conn.RemoteAddr().String()
		if err := s.addPeerConn(conn, addr, true); err != nil {
			log.Printf("Reject peer %s: %v", addr, err)
			conn.Close()
		}
	}
}

func (s *Server) addPeerConn(conn net.Conn, id string, inbound bool) error {
	s.mu.Lock()
	if len(s.peers) >= s.config.MaxPeers {
		s.mu.Unlock()
		conn.Close()
		return net.ErrClosed
	}
	if _, exists := s.peers[id]; exists {
		s.mu.Unlock()
		conn.Close()
		return nil // already connected
	}
	p := NewPeer(conn, id, inbound, s)
	s.peers[id] = p
	s.mu.Unlock()

	p.Start()
	s.handler.OnPeerConnected(p)
	return nil
}

// removePeer removes a peer from the map (called on disconnect).
func (s *Server) removePeer(id string) {
	s.mu.Lock()
	delete(s.peers, id)
	s.mu.Unlock()
}

// --- Server implements Handler to intercept disconnect events ---

func (s *Server) OnPeerConnected(p *Peer) {
	s.handler.OnPeerConnected(p)
}

func (s *Server) OnPeerDisconnected(p *Peer) {
	s.removePeer(p.ID())
	s.handler.OnPeerDisconnected(p)
}

func (s *Server) OnMessage(p *Peer, code byte, payload []byte) {
	s.handler.OnMessage(p, code, payload)
}
