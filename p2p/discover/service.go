package discover

import (
	"crypto/ecdsa"
	"crypto/rand"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	discoverpb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const (
	pingInterval    = 30 * time.Second
	refreshInterval = 60 * time.Second
	maxUDPPacket    = 1280
)

// Service is the discovery service that maintains the routing table and discovers
// new peers via the TRON UDP discovery protocol.
type Service struct {
	conn      *Conn
	table     *Table
	privKey   *ecdsa.PrivateKey
	localEP   *discoverpb.Endpoint
	onNewPeer func(addr string) // called when a new reachable peer is discovered
	quit      chan struct{}
	wg        sync.WaitGroup
}

// NewService creates a discovery service.
//
// listenAddr: "host:port" for UDP discovery socket (typically same port as TCP).
// privKey: node's secp256k1 identity key.
// onNewPeer: callback invoked when a new peer is ready to dial (addr = "host:port").
// Pass nil for onNewPeer and set it later via SetOnNewPeer before Start().
func NewService(listenAddr string, privKey *ecdsa.PrivateKey, onNewPeer func(addr string)) (*Service, error) {
	conn, err := NewConn(listenAddr, privKey)
	if err != nil {
		return nil, err
	}

	localID := PubKeyToNodeID(privKey.PublicKey)

	host, portStr, _ := net.SplitHostPort(listenAddr)
	port, _ := strconv.Atoi(portStr)

	ip := net.ParseIP(host)
	if ip == nil || ip.IsUnspecified() {
		ip = net.ParseIP("0.0.0.0")
	}

	localEP := &discoverpb.Endpoint{
		NodeId: localID,
		Port:   int32(port),
	}
	if ip4 := ip.To4(); ip4 != nil {
		localEP.Address = ip4
	} else {
		localEP.AddressIpv6 = ip.To16()
	}

	return &Service{
		conn:      conn,
		table:     NewTable(localID),
		privKey:   privKey,
		localEP:   localEP,
		onNewPeer: onNewPeer,
		quit:      make(chan struct{}),
	}, nil
}

// SetOnNewPeer sets the callback invoked when a newly discovered peer is available to dial.
// Must be called before Start().
func (s *Service) SetOnNewPeer(fn func(addr string)) {
	s.onNewPeer = fn
}

// AddBootstrap adds seed nodes to the routing table and sends them initial pings.
func (s *Service) AddBootstrap(addrs []string) {
	for _, addr := range addrs {
		host, portStr, err := net.SplitHostPort(addr)
		if err != nil {
			continue
		}
		port, _ := strconv.Atoi(portStr)
		n := &Node{
			IP:   net.ParseIP(host),
			Port: port,
			ID:   make([]byte, 64), // placeholder until we receive their pong
		}
		s.table.Add(n)
		udpAddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			continue
		}
		go func(ua *net.UDPAddr, node *Node) {
			if err := s.conn.SendPing(ua, s.localEP, node.Endpoint()); err != nil {
				log.Printf("discover: ping seed %s failed: %v", ua, err)
			}
		}(udpAddr, n)
	}
}

// Start begins the discovery service background goroutines.
func (s *Service) Start() {
	s.wg.Add(2)
	go s.readLoop()
	go s.maintainLoop()
}

// Stop shuts down the service and waits for goroutines to finish.
func (s *Service) Stop() {
	close(s.quit)
	s.conn.Close()
	s.wg.Wait()
}

// readLoop reads incoming UDP datagrams and handles them.
func (s *Service) readLoop() {
	defer s.wg.Done()
	buf := make([]byte, maxUDPPacket)
	for {
		n, from, err := s.conn.ReadFrom(buf)
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
				log.Printf("discover: read error: %v", err)
				continue
			}
		}
		// Copy slice before handing off to goroutine
		pkt := make([]byte, n)
		copy(pkt, buf[:n])
		go s.handlePacket(pkt, from)
	}
}

// handlePacket processes a single incoming UDP datagram.
func (s *Service) handlePacket(data []byte, from *net.UDPAddr) {
	msgType, payload, senderID, err := DecodeMessage(data)
	if err != nil {
		return
	}

	sender := &Node{
		ID:   senderID,
		IP:   from.IP,
		Port: from.Port,
	}

	switch msgType {
	case MsgPing:
		var ping discoverpb.PingMessage
		if err := proto.Unmarshal(payload, &ping); err != nil {
			return
		}
		// Reply with pong; echo the Version field as the echo value
		if err := s.conn.SendPong(from, s.localEP, ping.Version); err != nil {
			log.Printf("discover: pong failed: %v", err)
		}
		s.table.Add(sender)

	case MsgPong:
		// Add or refresh sender in table
		s.table.Add(sender)
		// Notify p2p server of new peer candidate
		if s.onNewPeer != nil {
			s.onNewPeer(from.String())
		}

	case MsgFindNode:
		var fn discoverpb.FindNeighbours
		if err := proto.Unmarshal(payload, &fn); err != nil {
			return
		}
		closest := s.table.Closest(fn.TargetId, BucketSize)
		eps := make([]*discoverpb.Endpoint, len(closest))
		for i, n := range closest {
			eps[i] = n.Endpoint()
		}
		s.conn.SendNeighbours(from, s.localEP, eps) //nolint:errcheck

	case MsgNeighbours:
		var nb discoverpb.Neighbours
		if err := proto.Unmarshal(payload, &nb); err != nil {
			return
		}
		for _, ep := range nb.Neighbours {
			n := EndpointToNode(ep)
			s.table.Add(n)
			// Ping each new neighbour to confirm liveness
			udpAddr := &net.UDPAddr{IP: n.IP, Port: n.Port}
			go func(ua *net.UDPAddr, node *Node) {
				s.conn.SendPing(ua, s.localEP, node.Endpoint()) //nolint:errcheck
			}(udpAddr, n)
		}
	}
}

// maintainLoop periodically pings known nodes and performs random lookups.
func (s *Service) maintainLoop() {
	defer s.wg.Done()
	pingTicker := time.NewTicker(pingInterval)
	refreshTicker := time.NewTicker(refreshInterval)
	defer pingTicker.Stop()
	defer refreshTicker.Stop()

	for {
		select {
		case <-s.quit:
			return
		case <-pingTicker.C:
			s.pingAll()
		case <-refreshTicker.C:
			s.lookupRandom()
		}
	}
}

// pingAll sends pings to all known nodes to refresh liveness.
func (s *Service) pingAll() {
	nodes := s.table.Closest(s.table.localID, 256)
	for _, n := range nodes {
		if n.IP == nil {
			continue
		}
		udpAddr := &net.UDPAddr{IP: n.IP, Port: n.Port}
		go s.conn.SendPing(udpAddr, s.localEP, n.Endpoint()) //nolint:errcheck
	}
}

// lookupRandom picks a random target and asks known nodes for their neighbours.
func (s *Service) lookupRandom() {
	var targetID [64]byte
	rand.Read(targetID[:]) //nolint:errcheck

	nodes := s.table.Closest(targetID[:], 3)
	for _, n := range nodes {
		if n.IP == nil {
			continue
		}
		udpAddr := &net.UDPAddr{IP: n.IP, Port: n.Port}
		go s.conn.SendFindNode(udpAddr, s.localEP, targetID[:]) //nolint:errcheck
	}
}

// GenerateKey generates a new secp256k1 private key for node identity.
func GenerateKey() (*ecdsa.PrivateKey, error) {
	return ethcrypto.GenerateKey()
}

// KeyToBytes serializes an ECDSA private key to 32 bytes.
func KeyToBytes(key *ecdsa.PrivateKey) []byte {
	return ethcrypto.FromECDSA(key)
}

// KeyFromBytes deserializes an ECDSA private key from 32 bytes.
func KeyFromBytes(b []byte) (*ecdsa.PrivateKey, error) {
	return ethcrypto.ToECDSA(b)
}
