package discover

import (
	"crypto/rand"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	discoverpb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const (
	discoverCycle   = 7200 * time.Millisecond // libp2p KademliaOptions.DISCOVER_CYCLE
	pingInterval    = 60 * time.Second        // how often we re-ping table nodes to refresh liveness
	pingTimeout     = 15 * time.Second        // libp2p KadService.pingTimeout
	kademliaAlpha   = 3                       // libp2p KademliaOptions.ALPHA
	kademliaMaxLoop = 5                       // libp2p KademliaOptions.MAX_LOOP_NUM
	kademliaWait    = 100 * time.Millisecond  // libp2p KademliaOptions.WAIT_TIME
	maxUDPPacket    = 2048                    // libp2p P2pPacketDecoder.MAXSIZE
)

// Service is the discovery service that maintains the routing table and discovers
// new peers via the TRON UDP discovery protocol.
type Service struct {
	conn      *Conn
	table     *Table
	localID   []byte
	networkID int32
	localEP   *discoverpb.Endpoint
	onNewPeer func(addr string) // called when a new reachable peer is discovered
	quit      chan struct{}
	wg        sync.WaitGroup
	seeds     []*net.UDPAddr // bootstrap addresses we keep re-pinging until their pong installs real nodeIDs
	seedsMu   sync.Mutex

	lookupCycle    int
	pendingPings   map[string]time.Time
	pendingPingsMu sync.Mutex
}

// NewService creates a discovery service.
//
// listenAddr: "host:port" for UDP discovery socket (typically same port as TCP).
// nodeID: 64-byte random node identity (use GenerateNodeID()).
// networkID: network identifier sent in PingMessage.Version (matches java-tron networkId).
// onNewPeer: callback invoked when a new peer is ready to dial (addr = "host:port").
// Pass nil for onNewPeer and set it later via SetOnNewPeer before Start().
func NewService(listenAddr string, nodeID []byte, networkID int32, onNewPeer func(addr string)) (*Service, error) {
	conn, err := NewConn(listenAddr)
	if err != nil {
		return nil, err
	}

	host, portStr, _ := net.SplitHostPort(listenAddr)
	port, _ := strconv.Atoi(portStr)

	ip := net.ParseIP(host)
	if ip == nil || ip.IsUnspecified() {
		ip = net.ParseIP("0.0.0.0")
	}

	localEP := &discoverpb.Endpoint{
		NodeId: nodeID,
		Port:   int32(port),
	}
	if ip4 := ip.To4(); ip4 != nil {
		localEP.Address = []byte(ip4.String())
	} else {
		localEP.AddressIpv6 = []byte(ip.String())
	}

	return &Service{
		conn:         conn,
		table:        NewTable(nodeID),
		localID:      nodeID,
		networkID:    networkID,
		localEP:      localEP,
		onNewPeer:    onNewPeer,
		quit:         make(chan struct{}),
		pendingPings: make(map[string]time.Time),
	}, nil
}

// SetOnNewPeer sets the callback invoked when a newly discovered peer is available to dial.
// Must be called before Start().
func (s *Service) SetOnNewPeer(fn func(addr string)) {
	s.onNewPeer = fn
}

// AddBootstrap records seed node addresses and sends them initial pings.
// Seeds are kept separately from the routing table; they are re-pinged on each
// maintenance cycle until a pong arrives and the real nodeID is installed by
// the normal MsgPong handling path.
func (s *Service) AddBootstrap(addrs []string) {
	for _, addr := range addrs {
		udpAddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			continue
		}
		s.seedsMu.Lock()
		s.seeds = append(s.seeds, udpAddr)
		s.seedsMu.Unlock()

		// Send initial ping. The remote endpoint node_id is unknown at this point;
		// receivers ignore the remote endpoint's nodeId so a placeholder is fine.
		remoteEP := &discoverpb.Endpoint{
			Port: int32(udpAddr.Port),
		}
		if ip4 := udpAddr.IP.To4(); ip4 != nil {
			remoteEP.Address = []byte(ip4.String())
		} else {
			remoteEP.AddressIpv6 = []byte(udpAddr.IP.String())
		}
		go func(ua *net.UDPAddr, ep *discoverpb.Endpoint) {
			if err := s.sendPingAndTrack(ua, ep); err != nil {
				log.Printf("discover: ping seed %s failed: %v", ua, err)
			}
		}(udpAddr, remoteEP)
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
	msgType, payload, err := DecodeMessage(data)
	if err != nil {
		return
	}

	switch msgType {
	case MsgPing:
		var ping discoverpb.PingMessage
		if err := proto.Unmarshal(payload, &ping); err != nil {
			return
		}
		// Sender identity comes from proto; use UDP source IP as canonical IP.
		sender := EndpointToNode(ping.From)
		sender.IP = from.IP
		sender.Port = from.Port

		// Reply with pong; echo the Version field (networkId) from the ping
		if err := s.conn.SendPong(from, s.localEP, ping.Version); err != nil {
			log.Printf("discover: pong failed: %v", err)
		}
		s.table.Add(sender)

	case MsgPong:
		var pong discoverpb.PongMessage
		if err := proto.Unmarshal(payload, &pong); err != nil {
			return
		}
		// Use UDP source IP as canonical IP; ID comes from proto.
		sender := EndpointToNode(pong.From)
		sender.IP = from.IP
		sender.Port = from.Port

		// Clear pending ping for this peer — it's alive.
		key := fmt.Sprintf("%s:%d", from.IP, from.Port)
		s.pendingPingsMu.Lock()
		delete(s.pendingPings, key)
		s.pendingPingsMu.Unlock()

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
				s.sendPingAndTrack(ua, node.Endpoint()) //nolint:errcheck
			}(udpAddr, n)
		}
	}
}

// maintainLoop periodically pings known nodes, performs target-rotating lookups,
// and evicts timed-out pending pings.
func (s *Service) maintainLoop() {
	defer s.wg.Done()
	pingTicker := time.NewTicker(pingInterval)
	discoverTicker := time.NewTicker(discoverCycle)
	cleanupTicker := time.NewTicker(5 * time.Second)
	defer pingTicker.Stop()
	defer discoverTicker.Stop()
	defer cleanupTicker.Stop()

	for {
		select {
		case <-s.quit:
			return
		case <-pingTicker.C:
			s.pingAll()
		case <-discoverTicker.C:
			s.lookup(s.nextLookupTarget())
		case <-cleanupTicker.C:
			s.evictTimedOutPings()
		}
	}
}

// pingAll sends pings to all known nodes to refresh liveness, and also re-pings
// stored bootstrap seeds so the bootstrap path isn't lost if the first attempt
// failed before the pong installed the real nodeID.
func (s *Service) pingAll() {
	nodes := s.table.Closest(s.localID, 256)
	for _, n := range nodes {
		if n.IP == nil {
			continue
		}
		udpAddr := &net.UDPAddr{IP: n.IP, Port: n.Port}
		go s.sendPingAndTrack(udpAddr, n.Endpoint()) //nolint:errcheck
	}

	s.seedsMu.Lock()
	seeds := append([]*net.UDPAddr(nil), s.seeds...)
	s.seedsMu.Unlock()
	for _, ua := range seeds {
		remoteEP := &discoverpb.Endpoint{
			Port: int32(ua.Port),
		}
		if ip4 := ua.IP.To4(); ip4 != nil {
			remoteEP.Address = []byte(ip4.String())
		} else {
			remoteEP.AddressIpv6 = []byte(ua.IP.String())
		}
		go s.sendPingAndTrack(ua, remoteEP) //nolint:errcheck
	}
}

// nextLookupTarget returns the lookup target for the current cycle.
// Every kademliaMaxLoop-th cycle the local node ID is used (table self-health);
// all other cycles use a random 64-byte target.
func (s *Service) nextLookupTarget() []byte {
	s.lookupCycle++
	if s.lookupCycle%kademliaMaxLoop == 0 {
		return s.table.localID
	}
	return randomBytes(NodeIDLen)
}

// lookup asks the kademliaAlpha closest known nodes for neighbours of target.
func (s *Service) lookup(target []byte) {
	nodes := s.table.Closest(target, kademliaAlpha)
	for _, n := range nodes {
		if n.IP == nil {
			continue
		}
		udpAddr := &net.UDPAddr{IP: n.IP, Port: n.Port}
		go s.conn.SendFindNode(udpAddr, s.localEP, target) //nolint:errcheck
	}
}

// sendPingAndTrack sends a Ping to target and records it in pendingPings for
// liveness tracking. Stale entries are evicted by evictTimedOutPings.
func (s *Service) sendPingAndTrack(target *net.UDPAddr, remoteEP *discoverpb.Endpoint) error {
	key := fmt.Sprintf("%s:%d", target.IP, target.Port)
	s.pendingPingsMu.Lock()
	s.pendingPings[key] = time.Now()
	s.pendingPingsMu.Unlock()
	return s.conn.SendPing(target, s.localEP, remoteEP, s.networkID)
}

// evictTimedOutPings removes pendingPings entries that have not received a pong
// within pingTimeout. Stale entries no longer consume memory; the routing table
// will naturally replace stale nodes when fresh neighbours are discovered —
// bucket eviction policy overwrites the oldest entry on bucket-full insertions.
func (s *Service) evictTimedOutPings() {
	cutoff := time.Now().Add(-pingTimeout)
	s.pendingPingsMu.Lock()
	var expired []string
	for k, t := range s.pendingPings {
		if t.Before(cutoff) {
			expired = append(expired, k)
		}
	}
	for _, k := range expired {
		delete(s.pendingPings, k)
	}
	s.pendingPingsMu.Unlock()
}

// randomBytes returns n cryptographically random bytes.
func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}
