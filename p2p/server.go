package p2p

import (
	"encoding/hex"
	"errors"
	"fmt"
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
	// defaultDialTimeout bounds the TCP-connect half of an outbound attempt.
	// The libp2p handshake has its own 10-second deadline after connect.
	defaultDialTimeout = 10 * time.Second
)

// errDuplicatePeer is returned by addPeerConn when the peer ID is already
// connected. Callers must close the connection.
var errDuplicatePeer = errors.New("duplicate peer")

// errAlreadyConnected is the cheap pre-dial result for an endpoint already in
// the peer map. Unlike errDuplicatePeer, it does not indicate that a remote
// node ID collided during handshake and is therefore routine discovery noise.
var errAlreadyConnected = errors.New("peer endpoint already connected")

// errTooManyFromSameIP is returned when an inbound peer exceeds the per-IP cap.
var errTooManyFromSameIP = errors.New("too many connections from same IP")

// errPeerCapacity is returned when MaxPeers is already occupied. It is kept
// distinct from net.ErrClosed so callers can treat normal discovery saturation
// as a non-error without hiding actual socket closure failures.
var errPeerCapacity = errors.New("peer capacity reached")

// errDialThrottled is returned by AddPeer when the per-address dial throttle
// is still in cooldown. Callers (maintainPeers, discovery's onNewPeer
// callback) should silently swallow it instead of logging.
var errDialThrottled = errors.New("dial throttled")

// defaultDialThrottleInterval is the minimum gap between outbound dial
// attempts to the same address. Picked to dampen the maintainCh thundering
// herd without making transient drops take minutes to recover.
const defaultDialThrottleInterval = 30 * time.Second

// Discovery is the surface the Server uses from the Kademlia discovery service.
// Real implementation: *discover.Service. Tests can substitute a fake.
type Discovery interface {
	Start()
	Stop()
	AddBootstrap(addrs []string)
}

// ServerConfig holds P2P server configuration.
type ServerConfig struct {
	ListenAddr     string    // "host:port" to listen on
	MaxPeers       int       // max peers allowed
	SeedNodes      []string  // explicit peers to dial (CLI --seednode)
	BootstrapNodes []string  // built-in fallback peers fed into Discovery.AddBootstrap (e.g. params.MainnetBootstrapNodes)
	Discovery      Discovery // optional; nil = no discovery

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

	// DialThrottleInterval is the minimum gap between outbound dial attempts
	// to the same address. Zero ⇒ default (30s); negative ⇒ disabled.
	DialThrottleInterval time.Duration

	// DialTimeout bounds outbound TCP connection establishment. Zero or a
	// negative value uses the 10-second default.
	DialTimeout time.Duration
}

// Server manages TCP connections to peers.
type Server struct {
	config      ServerConfig
	handler     Handler
	listener    net.Listener
	peers       map[string]*Peer
	peerNodeIDs map[string]string // remote libp2p node ID hex -> peer ID
	mu          sync.RWMutex
	quit        chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup
	maintainCh  chan struct{} // signals the maintain loop to reconnect now
	dialLimiter *dialLimiter  // per-addr outbound dial throttle; nil ⇒ disabled
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
	if config.DialTimeout <= 0 {
		config.DialTimeout = defaultDialTimeout
	}
	throttle := config.DialThrottleInterval
	if throttle == 0 {
		throttle = defaultDialThrottleInterval
	}
	var limiter *dialLimiter
	if throttle > 0 {
		limiter = newDialLimiter(throttle)
	}

	return &Server{
		config:      config,
		handler:     handler,
		peers:       make(map[string]*Peer),
		peerNodeIDs: make(map[string]string),
		quit:        make(chan struct{}),
		maintainCh:  make(chan struct{}, 1),
		dialLimiter: limiter,
	}
}

// Start begins listening and dials seed nodes.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return err
	}
	s.listener = ln
	log.Info("P2P listening", "addr", ln.Addr().String())

	s.wg.Add(2)
	go s.acceptLoop()
	go s.maintainLoop()

	// Start discovery service if configured
	if s.config.Discovery != nil {
		s.config.Discovery.Start()
		bootstrap := make([]string, 0, len(s.config.SeedNodes)+len(s.config.BootstrapNodes))
		seen := make(map[string]struct{}, len(s.config.SeedNodes)+len(s.config.BootstrapNodes))
		for _, addr := range s.config.SeedNodes {
			if _, ok := seen[addr]; ok {
				continue
			}
			seen[addr] = struct{}{}
			bootstrap = append(bootstrap, addr)
		}
		for _, addr := range s.config.BootstrapNodes {
			if _, ok := seen[addr]; ok {
				continue
			}
			seen[addr] = struct{}{}
			bootstrap = append(bootstrap, addr)
		}
		s.config.Discovery.AddBootstrap(bootstrap)
	} else {
		// Dial seed nodes directly when discovery is disabled
		for _, addr := range s.config.SeedNodes {
			go func(addr string) {
				if err := s.AddPeer(addr); err != nil {
					log.Debug("Seed dial failed", "addr", addr, "err", err)
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
	s.peerNodeIDs = make(map[string]string)
	s.mu.Unlock()

	for _, p := range peers {
		p.Stop()
	}

	s.wg.Wait()
	log.Info("P2P server stopped")
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

// LocalEndpoint returns the endpoint this node advertises in libp2p and TRON
// hello messages. Callers receive a copy so they cannot mutate server config.
func (s *Server) LocalEndpoint() *corepb.Endpoint {
	return &corepb.Endpoint{
		Address: append([]byte(nil), []byte(s.config.ExternalIP)...),
		Port:    s.config.Port,
		NodeId:  append([]byte(nil), s.config.NodeID...),
	}
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

// AddPeer dials a remote address and adds the peer. Subject to the per-addr
// dial throttle: returns errDialThrottled (a sentinel callers may swallow) if
// a dial to addr was started in the past DialThrottleInterval.
func (s *Server) AddPeer(addr string) error {
	// Discovery reports the same reachable nodes repeatedly. Avoid a TCP dial
	// and libp2p handshake when the exact endpoint is already connected or the
	// peer set is full; addPeerConn repeats both checks to close the race with a
	// concurrent inbound/outbound handshake.
	s.mu.RLock()
	full := len(s.peers) >= s.config.MaxPeers
	_, connected := s.peers[addr]
	s.mu.RUnlock()
	if full {
		return errPeerCapacity
	}
	if connected {
		return errAlreadyConnected
	}
	if s.dialLimiter != nil && !s.dialLimiter.Allow(addr) {
		return errDialThrottled
	}
	conn, err := net.DialTimeout("tcp", addr, s.config.DialTimeout)
	if err != nil {
		return err
	}
	if err := s.addPeerConn(conn, addr, false); err != nil {
		conn.Close()
		return err
	}
	return nil
}

// AddDiscoveredPeer dials a candidate reported by UDP discovery. Repeated
// pongs commonly rediscover an address inside the dial cooldown, so throttle
// errors stay silent; other failures remain visible at debug level for peer
// population diagnostics.
func (s *Server) AddDiscoveredPeer(addr string) {
	if err := s.AddPeer(addr); err != nil &&
		!errors.Is(err, errDialThrottled) &&
		!errors.Is(err, errAlreadyConnected) &&
		!errors.Is(err, errPeerCapacity) {
		log.Debug("Discovery peer dial failed", "addr", addr, "err", err)
	}
}

// performLibp2pHandshake runs the libp2p HANDSHAKE_HELLO exchange on a raw
// connection. On success, the conn is ready for application-layer messages.
// On failure, it sends a DISCONNECT and returns an error — caller must close.
func (s *Server) performLibp2pHandshake(conn net.Conn) (*p2ppb.HelloMessage, error) {
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
		return nil, fmt.Errorf("encode hello: %w", err)
	}
	if err := WriteMsg(conn, MsgLibp2pHello, helloPayload); err != nil {
		return nil, fmt.Errorf("send hello: %w", err)
	}

	// Read peer's response — must be either HANDSHAKE_HELLO or DISCONNECT.
	code, payload, err := ReadMsg(conn)
	if err != nil {
		return nil, fmt.Errorf("read hello: %w", err)
	}
	if code == MsgLibp2pDisconnect {
		dm, _ := ParseDisconnect(payload)
		reason := p2ppb.DisconnectReason_UNKNOWN
		if dm != nil {
			reason = dm.Reason
		}
		return nil, fmt.Errorf("peer sent disconnect: %v", reason)
	}
	if code != MsgLibp2pHello {
		return nil, fmt.Errorf("expected HELLO, got code %#x", code)
	}
	peerHello, err := ParseHello(payload)
	if err != nil {
		return nil, fmt.Errorf("parse hello: %w", err)
	}
	reason := ValidateHello(peerHello, s.config.NetworkID, s.config.Version, time.Now())
	if reason != p2ppb.DisconnectReason_PEER_QUITING { // 0 = accept
		// Send disconnect back so peer knows why.
		dm := BuildDisconnect(reason)
		body, _ := EncodeDisconnect(dm)
		_ = WriteMsg(conn, MsgLibp2pDisconnect, body)
		return nil, fmt.Errorf("reject peer: %v", reason)
	}
	return peerHello, nil
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
				log.Warn("P2P accept error", "err", err)
				continue
			}
		}
		addr := conn.RemoteAddr().String()
		if err := s.addPeerConn(conn, addr, true); err != nil {
			log.Debug("Peer rejected", "addr", addr, "err", err)
			conn.Close()
		}
	}
}

func (s *Server) addPeerConn(conn net.Conn, id string, inbound bool) error {
	// Capacity + dedup + per-IP cap check BEFORE expensive handshake.
	s.mu.Lock()
	if len(s.peers) >= s.config.MaxPeers {
		s.mu.Unlock()
		return errPeerCapacity
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
	peerHello, err := s.performLibp2pHandshake(conn)
	if err != nil {
		return err
	}
	remoteNodeID := helloNodeIDKey(peerHello)
	if remoteNodeID != "" && remoteNodeID == nodeIDKey(s.config.NodeID) {
		_ = writePostHandshakeDisconnect(conn, p2ppb.DisconnectReason_DUPLICATE_PEER)
		return errDuplicatePeer
	}

	// Re-check capacity/dedup/per-IP under lock (another peer may have joined meanwhile).
	s.mu.Lock()
	if len(s.peers) >= s.config.MaxPeers {
		s.mu.Unlock()
		_ = writePostHandshakeDisconnect(conn, p2ppb.DisconnectReason_TOO_MANY_PEERS)
		return errPeerCapacity
	}
	if _, exists := s.peers[id]; exists {
		s.mu.Unlock()
		_ = writePostHandshakeDisconnect(conn, p2ppb.DisconnectReason_DUPLICATE_PEER)
		return errDuplicatePeer
	}
	if existing, exists := s.peerNodeIDs[remoteNodeID]; remoteNodeID != "" && exists {
		s.mu.Unlock()
		_ = writePostHandshakeDisconnect(conn, p2ppb.DisconnectReason_DUPLICATE_PEER)
		log.Info("Peer rejected duplicate node ID", "addr", id, "existing", existing)
		return errDuplicatePeer
	}
	if inbound {
		remoteHost, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
		if s.countInboundFromIPLocked(remoteHost) >= s.config.MaxConnectionsWithSameIP {
			s.mu.Unlock()
			_ = writePostHandshakeDisconnect(conn, p2ppb.DisconnectReason_TOO_MANY_PEERS_WITH_SAME_IP)
			return errTooManyFromSameIP
		}
	}
	p := NewPeer(conn, id, inbound, s)
	p.remoteNodeID = remoteNodeID
	s.peers[id] = p
	if remoteNodeID != "" {
		s.peerNodeIDs[remoteNodeID] = id
	}
	s.mu.Unlock()

	p.Start()
	s.handler.OnPeerConnected(p)
	return nil
}

// removePeer removes a peer from the map (called on disconnect) and nudges
// the maintain loop to reconnect to seeds if needed.
func (s *Server) removePeer(id string) {
	s.mu.Lock()
	if p := s.peers[id]; p != nil && p.remoteNodeID != "" {
		delete(s.peerNodeIDs, p.remoteNodeID)
	}
	delete(s.peers, id)
	s.mu.Unlock()
	// Non-blocking send: if a signal is already pending, skip.
	select {
	case s.maintainCh <- struct{}{}:
	default:
	}
}

func nodeIDKey(id []byte) string {
	if len(id) == 0 {
		return ""
	}
	return hex.EncodeToString(id)
}

func helloNodeIDKey(hello *p2ppb.HelloMessage) string {
	if hello == nil || hello.From == nil {
		return ""
	}
	return nodeIDKey(hello.From.NodeId)
}

func writePostHandshakeDisconnect(conn net.Conn, reason p2ppb.DisconnectReason) error {
	dm := BuildDisconnect(reason)
	payload, err := EncodeDisconnect(dm)
	if err != nil {
		return err
	}
	body, err := WrapPostHandshake(MsgLibp2pDisconnect, payload)
	if err != nil {
		return err
	}
	return WriteFrameBody(conn, body)
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

// maintainLoop periodically reconnects to configured seed/bootstrap nodes when
// we are below capacity. Mirrors java-tron ConnPoolService.connect() +
// triggerConnect().
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

// maintainPeers dials configured peers that are not yet connected, up to the
// currently available peer slots. BootstrapNodes are also direct TCP fallback
// candidates: relying on their UDP discovery replies alone can leave a freshly
// restarted node at zero peers indefinitely when UDP is filtered, even though
// the same bootstrap endpoints accept the TRON TCP handshake.
func (s *Server) maintainPeers() {
	if len(s.config.SeedNodes) == 0 && len(s.config.BootstrapNodes) == 0 {
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

	remaining := s.config.MaxPeers - current
	candidates := make([]string, 0, len(s.config.SeedNodes)+len(s.config.BootstrapNodes))
	seen := make(map[string]struct{}, cap(candidates))
	for _, group := range [][]string{s.config.SeedNodes, s.config.BootstrapNodes} {
		for _, addr := range group {
			if _, ok := seen[addr]; ok {
				continue
			}
			seen[addr] = struct{}{}
			candidates = append(candidates, addr)
		}
	}
	for _, addr := range candidates {
		if remaining == 0 {
			break
		}
		if connected[addr] {
			continue
		}
		remaining--
		go func(a string) {
			if err := s.AddPeer(a); err != nil &&
				!errors.Is(err, errDialThrottled) &&
				!errors.Is(err, errAlreadyConnected) &&
				!errors.Is(err, errPeerCapacity) {
				log.Debug("Configured peer reconnect failed", "addr", a, "err", err)
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
