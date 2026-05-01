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

const (
	// defaultMaxConnectionsWithSameIP mirrors java-tron libp2p ChannelManager
	// (maxConnectionsWithSameIp = 2 from config.conf default).
	defaultMaxConnectionsWithSameIP = 2
	// maintainInterval is how often we check whether seed nodes need reconnection.
	maintainInterval = 10 * time.Second
)

// errDuplicatePeer is returned by addPeerConn when the peer ID is already
// connected. Callers must close the connection.
var errDuplicatePeer = errors.New("duplicate peer")

// errTooManyFromSameIP is returned when an inbound peer exceeds the per-IP cap.
var errTooManyFromSameIP = errors.New("too many connections from same IP")

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

	// MaxConnectionsWithSameIP caps inbound connections from a single remote IP.
	// 0 → default (2), matching java-tron ChannelManager.processPeer().
	MaxConnectionsWithSameIP int
}

// Server manages TCP connections to peers.
type Server struct {
	config     ServerConfig
	handler    Handler
	listener   net.Listener
	peers      map[string]*Peer
	mu         sync.RWMutex
	quit       chan struct{}
	stopOnce   sync.Once
	wg         sync.WaitGroup
	maintainCh chan struct{} // signals the maintain loop to reconnect now
}

// NewServer creates a new P2P server.
func NewServer(config ServerConfig, handler Handler) *Server {
	if config.MaxPeers <= 0 {
		config.MaxPeers = 30
	}
	// Apply libp2p handshake defaults.
	// NetworkID is NOT defaulted: 0 is a valid value (java-tron's libp2p
	// Parameter.nodeP2pVersion defaults to 0 when the config omits p2p.version).
	// Callers must set NetworkID explicitly to match their target chain.
	if len(config.NodeID) != 64 {
		config.NodeID = discover.GenerateNodeID()
	}
	if config.Version == 0 {
		config.Version = Libp2pProtocolVersion
	}
	if config.MaxConnectionsWithSameIP <= 0 {
		config.MaxConnectionsWithSameIP = defaultMaxConnectionsWithSameIP
	}
	return &Server{
		config:     config,
		handler:    handler,
		peers:      make(map[string]*Peer),
		quit:       make(chan struct{}),
		maintainCh: make(chan struct{}, 1),
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

	s.wg.Add(2)
	go s.acceptLoop()
	go s.maintainLoop()

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

// Stop shuts down the server and disconnects all peers. Safe to call multiple times.
func (s *Server) Stop() error {
	s.stopOnce.Do(func() { close(s.quit) })
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

// NetworkID returns the libp2p networkId this server advertises in its
// Hello (= java-tron's `p2p.version` config field).
func (s *Server) NetworkID() int32 {
	return s.config.NetworkID
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
	// Capacity + dedup + per-IP cap check BEFORE expensive handshake.
	s.mu.Lock()
	if len(s.peers) >= s.config.MaxPeers {
		s.mu.Unlock()
		return net.ErrClosed
	}
	if _, exists := s.peers[id]; exists {
		s.mu.Unlock()
		return errDuplicatePeer
	}
	if inbound {
		remoteHost, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
		if s.countInboundFromIPLocked(remoteHost) >= s.config.MaxConnectionsWithSameIP {
			s.mu.Unlock()
			return errTooManyFromSameIP
		}
	}
	s.mu.Unlock()

	// Libp2p handshake on the raw conn — before peer is registered.
	if err := s.performLibp2pHandshake(conn); err != nil {
		return err
	}

	// Re-check capacity/dedup/per-IP under lock (another peer may have joined meanwhile).
	s.mu.Lock()
	if len(s.peers) >= s.config.MaxPeers {
		s.mu.Unlock()
		return net.ErrClosed
	}
	if _, exists := s.peers[id]; exists {
		s.mu.Unlock()
		return errDuplicatePeer
	}
	if inbound {
		remoteHost, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
		if s.countInboundFromIPLocked(remoteHost) >= s.config.MaxConnectionsWithSameIP {
			s.mu.Unlock()
			return errTooManyFromSameIP
		}
	}
	p := NewPeer(conn, id, inbound, s)
	s.peers[id] = p
	s.mu.Unlock()

	p.Start()
	s.handler.OnPeerConnected(p)
	return nil
}

// removePeer removes a peer from the map (called on disconnect) and nudges
// the maintain loop to reconnect to seeds if needed.
func (s *Server) removePeer(id string) {
	s.mu.Lock()
	delete(s.peers, id)
	s.mu.Unlock()
	// Non-blocking send: if a signal is already pending, skip.
	select {
	case s.maintainCh <- struct{}{}:
	default:
	}
}

// countInboundFromIPLocked counts active inbound peers whose remote host equals
// remoteHost. Must be called with s.mu held at least for reading.
func (s *Server) countInboundFromIPLocked(remoteHost string) int {
	count := 0
	for id, p := range s.peers {
		if !p.Inbound() {
			continue
		}
		host, _, _ := net.SplitHostPort(id)
		if host == remoteHost {
			count++
		}
	}
	return count
}

// maintainLoop periodically reconnects to configured seed nodes when we are
// below capacity. Mirrors java-tron ConnPoolService.connect() + triggerConnect().
func (s *Server) maintainLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(maintainInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.quit:
			return
		case <-ticker.C:
			s.maintainPeers()
		case <-s.maintainCh:
			s.maintainPeers()
		}
	}
}

// maintainPeers dials seed nodes that are not yet connected, up to MaxPeers.
func (s *Server) maintainPeers() {
	if len(s.config.SeedNodes) == 0 {
		return
	}
	s.mu.RLock()
	current := len(s.peers)
	connected := make(map[string]bool, len(s.peers))
	for id := range s.peers {
		connected[id] = true
	}
	s.mu.RUnlock()

	if current >= s.config.MaxPeers {
		return
	}
	for _, addr := range s.config.SeedNodes {
		if connected[addr] {
			continue
		}
		go func(a string) {
			if err := s.AddPeer(a); err != nil {
				log.Printf("P2P: reconnect to seed %s: %v", a, err)
			}
		}(addr)
	}
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
