package p2p

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/tronprotocol/go-tron/p2p/discover"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	p2ppb "github.com/tronprotocol/go-tron/proto/p2p"
)

// errDuplicatePeer is returned by addPeerConn when the peer ID is already
// connected. Callers must close the connection.
var errDuplicatePeer = errors.New("duplicate peer")

// ServerConfig holds P2P server configuration.
type ServerConfig struct {
	ListenAddr string            // "host:port" to listen on
	MaxPeers   int               // max peers allowed
	SeedNodes  []string          // initial peers to dial
	Discovery  *discover.Service // optional; nil = no discovery

	// Libp2p handshake parameters. NodeID must be 64 bytes. If NetworkID or
	// Version is 0, defaults are applied (NetworkID=1, Version=Libp2pProtocolVersion).
	NodeID     []byte // 64-byte node identity
	NetworkID  int32  // network discriminator; 1 = mainnet
	Version    int32  // protocol version; defaults to Libp2pProtocolVersion
	ExternalIP string // IPv4 ASCII string used in HelloMessage.from.address
	Port       int32  // our TCP port, echoed in HelloMessage.from.port
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
	// Apply libp2p handshake defaults.
	if len(config.NodeID) != 64 {
		config.NodeID = discover.GenerateNodeID()
	}
	if config.NetworkID == 0 {
		config.NetworkID = 1
	}
	if config.Version == 0 {
		config.Version = Libp2pProtocolVersion
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

	// Start discovery service if configured
	if s.config.Discovery != nil {
		s.config.Discovery.Start()
		s.config.Discovery.AddBootstrap(s.config.SeedNodes)
	} else {
		// Dial seed nodes directly when discovery is disabled
		for _, addr := range s.config.SeedNodes {
			go func(addr string) {
				if err := s.AddPeer(addr); err != nil {
					log.Printf("Failed to connect to seed %s: %v", addr, err)
				}
			}(addr)
		}
	}

	return nil
}

// Stop shuts down the server and disconnects all peers.
func (s *Server) Stop() error {
	close(s.quit)
	s.listener.Close()

	// Stop discovery service if running
	if s.config.Discovery != nil {
		s.config.Discovery.Stop()
	}

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
	if err := s.addPeerConn(conn, addr, false); err != nil {
		conn.Close()
		return err
	}
	return nil
}

// performLibp2pHandshake runs the libp2p HANDSHAKE_HELLO exchange on a raw
// connection. On success, the conn is ready for application-layer messages.
// On failure, it sends a DISCONNECT and returns an error — caller must close.
func (s *Server) performLibp2pHandshake(conn net.Conn) error {
	// Deadline so a hung peer can't stall us forever.
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetDeadline(time.Time{}) //nolint:errcheck

	localEP := &corepb.Endpoint{
		Address: []byte(s.config.ExternalIP),
		Port:    s.config.Port,
		NodeId:  s.config.NodeID,
	}

	// Send our HANDSHAKE_HELLO.
	hello := BuildHelloMessage(localEP, s.config.NetworkID, 0)
	helloPayload, err := EncodeHello(hello)
	if err != nil {
		return fmt.Errorf("encode hello: %w", err)
	}
	if err := WriteMsg(conn, MsgLibp2pHello, helloPayload); err != nil {
		return fmt.Errorf("send hello: %w", err)
	}

	// Read peer's response — must be either HANDSHAKE_HELLO or DISCONNECT.
	code, payload, err := ReadMsg(conn)
	if err != nil {
		return fmt.Errorf("read hello: %w", err)
	}
	if code == MsgLibp2pDisconnect {
		dm, _ := ParseDisconnect(payload)
		reason := p2ppb.DisconnectReason_UNKNOWN
		if dm != nil {
			reason = dm.Reason
		}
		return fmt.Errorf("peer sent disconnect: %v", reason)
	}
	if code != MsgLibp2pHello {
		return fmt.Errorf("expected HELLO, got code %#x", code)
	}
	peerHello, err := ParseHello(payload)
	if err != nil {
		return fmt.Errorf("parse hello: %w", err)
	}
	reason := ValidateHello(peerHello, s.config.NetworkID, s.config.Version, time.Now())
	if reason != p2ppb.DisconnectReason_PEER_QUITING { // 0 = accept
		// Send disconnect back so peer knows why.
		dm := BuildDisconnect(reason)
		body, _ := EncodeDisconnect(dm)
		_ = WriteMsg(conn, MsgLibp2pDisconnect, body)
		return fmt.Errorf("reject peer: %v", reason)
	}
	return nil
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
	// Capacity + dedup check BEFORE expensive handshake.
	s.mu.Lock()
	if len(s.peers) >= s.config.MaxPeers {
		s.mu.Unlock()
		return net.ErrClosed
	}
	if _, exists := s.peers[id]; exists {
		s.mu.Unlock()
		return errDuplicatePeer
	}
	s.mu.Unlock()

	// Libp2p handshake on the raw conn — before peer is registered.
	if err := s.performLibp2pHandshake(conn); err != nil {
		return err
	}

	// Re-check capacity/dedup under lock (another peer may have joined meanwhile).
	s.mu.Lock()
	if len(s.peers) >= s.config.MaxPeers {
		s.mu.Unlock()
		return net.ErrClosed
	}
	if _, exists := s.peers[id]; exists {
		s.mu.Unlock()
		return errDuplicatePeer
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
