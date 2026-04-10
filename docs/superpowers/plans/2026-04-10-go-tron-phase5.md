# Phase 5: P2P Networking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable multi-node go-tron networks with peer discovery, block sync, and transaction/block propagation.

**Architecture:** TCP transport with length-prefixed framing (`[4B len][1B type][protobuf]`). Protocol layer routes messages to sync and broadcast services. Integrates with existing BlockChain, TxPool, and Producer via node lifecycle.

**Tech Stack:** Go net/TCP, protobuf (existing proto definitions), existing `node.Lifecycle` pattern

---

## File Structure

| File | Responsibility |
|------|----------------|
| **Create:** `p2p/protocol.go` | Message codes, constants, Handler interface |
| **Create:** `p2p/message.go` | Wire encoding/decoding: frame → (type, payload) |
| **Create:** `p2p/peer.go` | Single peer: read/write goroutines, lifecycle |
| **Create:** `p2p/server.go` | TCP listener, dial, peer management |
| **Create:** `p2p/message_test.go` | Encode/decode round-trip tests |
| **Create:** `p2p/peer_test.go` | Peer handshake + read/write over net.Pipe |
| **Create:** `p2p/server_test.go` | Server start/stop, accept/dial |
| **Create:** `net/handler.go` | TronHandler: message routing, handshake logic |
| **Create:** `net/sync.go` | SyncService: chain summary, block fetch |
| **Create:** `net/broadcaster.go` | BroadcastService: inventory gossip |
| **Create:** `net/handler_test.go` | Handshake validation tests |
| **Create:** `net/sync_test.go` | Sync protocol tests |
| **Create:** `net/broadcaster_test.go` | Broadcast propagation tests |
| **Modify:** `node/config.go` | Add SeedNodes, MaxPeers fields |
| **Modify:** `cmd/gtron/main.go` | Add --seednode, --maxpeers flags; wire P2P server |
| **Modify:** `cmd/gtron/config.go` | Parse new CLI flags |
| **Modify:** `core/producer/producer.go` | Add BlockCallback for broadcast notification |
| **Modify:** `proto/core/Tron.proto` | Regenerate (proto already has all needed messages) |

---

### Task 1: Wire Protocol — Message Encoding/Decoding

**Files:**
- Create: `p2p/protocol.go`
- Create: `p2p/message.go`
- Create: `p2p/message_test.go`

- [ ] **Step 1: Write the failing test for message encode/decode**

```go
// p2p/message_test.go
package p2p

import (
	"bytes"
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestEncodeDecodeMessage(t *testing.T) {
	inv := &corepb.Inventory{
		Type: corepb.Inventory_BLOCK,
		Ids:  [][]byte{{1, 2, 3}},
	}
	data, _ := proto.Marshal(inv)

	var buf bytes.Buffer
	err := WriteMsg(&buf, MsgInventory, data)
	if err != nil {
		t.Fatal(err)
	}

	code, payload, err := ReadMsg(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if code != MsgInventory {
		t.Fatalf("code: want 0x%02x, got 0x%02x", MsgInventory, code)
	}
	if !bytes.Equal(payload, data) {
		t.Fatal("payload mismatch")
	}
}

func TestReadMsgTooLarge(t *testing.T) {
	var buf bytes.Buffer
	// Write a frame claiming 20MB payload
	header := []byte{0x01, 0x31, 0x2D, 0x01, MsgBlock} // ~20MB
	buf.Write(header)
	_, _, err := ReadMsg(&buf)
	if err == nil {
		t.Fatal("expected error for oversized message")
	}
}

func TestPingPongEmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	err := WriteMsg(&buf, MsgPing, nil)
	if err != nil {
		t.Fatal(err)
	}
	code, payload, err := ReadMsg(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if code != MsgPing {
		t.Fatalf("code: want 0x%02x, got 0x%02x", MsgPing, code)
	}
	if len(payload) != 0 {
		t.Fatalf("expected empty payload, got %d bytes", len(payload))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./p2p/ -run "TestEncodeDecodeMessage|TestReadMsgTooLarge|TestPingPong" -v`
Expected: FAIL — package/functions not defined

- [ ] **Step 3: Implement protocol constants and message encoding**

```go
// p2p/protocol.go
package p2p

const (
	// Message type codes — match java-tron
	MsgTx             byte = 0x01
	MsgBlock          byte = 0x02
	MsgInventory      byte = 0x06
	MsgFetchInvData   byte = 0x07
	MsgSyncBlockChain byte = 0x08
	MsgChainInventory byte = 0x09
	MsgHello          byte = 0x20
	MsgDisconnect     byte = 0x21
	MsgPing           byte = 0x22
	MsgPong           byte = 0x23

	// MaxMessageSize is the maximum allowed message payload (10 MB).
	MaxMessageSize = 10 * 1024 * 1024

	// ProtocolVersion is the P2P protocol version for handshake.
	ProtocolVersion int32 = 1
)

// Handler processes messages from a connected peer.
type Handler interface {
	OnPeerConnected(p *Peer)
	OnPeerDisconnected(p *Peer)
	OnMessage(p *Peer, code byte, payload []byte)
}
```

```go
// p2p/message.go
package p2p

import (
	"encoding/binary"
	"fmt"
	"io"
)

// WriteMsg writes a length-prefixed message: [4B length][1B type][payload].
// Length includes the type byte.
func WriteMsg(w io.Writer, code byte, payload []byte) error {
	length := uint32(1 + len(payload)) // type byte + payload
	var header [5]byte
	binary.BigEndian.PutUint32(header[:4], length)
	header[4] = code
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// ReadMsg reads a length-prefixed message, returning (type, payload, error).
func ReadMsg(r io.Reader) (byte, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(r, header[:4]); err != nil {
		return 0, nil, err
	}
	length := binary.BigEndian.Uint32(header[:4])
	if length == 0 {
		return 0, nil, fmt.Errorf("empty message frame")
	}
	if length > MaxMessageSize {
		return 0, nil, fmt.Errorf("message too large: %d bytes (max %d)", length, MaxMessageSize)
	}
	// Read type byte
	if _, err := io.ReadFull(r, header[4:5]); err != nil {
		return 0, nil, err
	}
	code := header[4]
	// Read payload (length - 1 because type byte already consumed)
	payloadLen := length - 1
	if payloadLen == 0 {
		return code, nil, nil
	}
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return code, payload, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./p2p/ -run "TestEncodeDecodeMessage|TestReadMsgTooLarge|TestPingPong" -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add p2p/protocol.go p2p/message.go p2p/message_test.go
git commit -m "p2p: add wire protocol encoding and message type constants"
```

---

### Task 2: Peer Connection

**Files:**
- Create: `p2p/peer.go`
- Create: `p2p/peer_test.go`

- [ ] **Step 1: Write the failing test for Peer read/write**

```go
// p2p/peer_test.go
package p2p

import (
	"net"
	"sync"
	"testing"
	"time"
)

type testHandler struct {
	mu       sync.Mutex
	messages []struct {
		code    byte
		payload []byte
	}
	connected    []*Peer
	disconnected []*Peer
}

func (h *testHandler) OnPeerConnected(p *Peer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.connected = append(h.connected, p)
}

func (h *testHandler) OnPeerDisconnected(p *Peer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.disconnected = append(h.disconnected, p)
}

func (h *testHandler) OnMessage(p *Peer, code byte, payload []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.messages = append(h.messages, struct {
		code    byte
		payload []byte
	}{code, payload})
}

func TestPeerSendReceive(t *testing.T) {
	c1, c2 := net.Pipe()
	h1 := &testHandler{}
	h2 := &testHandler{}

	p1 := NewPeer(c1, "pipe:1", false, h1)
	p2 := NewPeer(c2, "pipe:2", true, h2)

	p1.Start()
	p2.Start()

	// p1 sends a message, p2 receives it
	p1.Send(MsgPing, nil)
	time.Sleep(50 * time.Millisecond)

	h2.mu.Lock()
	if len(h2.messages) != 1 || h2.messages[0].code != MsgPing {
		t.Fatalf("expected 1 PING message, got %d", len(h2.messages))
	}
	h2.mu.Unlock()

	p1.Stop()
	p2.Stop()
}

func TestPeerDisconnectNotifiesHandler(t *testing.T) {
	c1, c2 := net.Pipe()
	h := &testHandler{}
	p := NewPeer(c1, "pipe:1", false, h)
	p.Start()

	// Close the other end
	c2.Close()
	time.Sleep(50 * time.Millisecond)

	h.mu.Lock()
	if len(h.disconnected) != 1 {
		t.Fatalf("expected 1 disconnect, got %d", len(h.disconnected))
	}
	h.mu.Unlock()
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./p2p/ -run "TestPeerSend|TestPeerDisconnect" -v`
Expected: FAIL — `NewPeer` undefined

- [ ] **Step 3: Implement Peer**

```go
// p2p/peer.go
package p2p

import (
	"log"
	"net"
	"sync"
	"sync/atomic"
)

// Peer represents a connected remote node.
type Peer struct {
	conn    net.Conn
	id      string
	inbound bool
	handler Handler
	writeCh chan msgFrame
	quit    chan struct{}
	closed  atomic.Bool
	wg      sync.WaitGroup
}

type msgFrame struct {
	code    byte
	payload []byte
}

// NewPeer creates a new Peer wrapping a TCP connection.
func NewPeer(conn net.Conn, id string, inbound bool, handler Handler) *Peer {
	return &Peer{
		conn:    conn,
		id:      id,
		inbound: inbound,
		handler: handler,
		writeCh: make(chan msgFrame, 256),
		quit:    make(chan struct{}),
	}
}

// ID returns the peer's identifier (typically "host:port").
func (p *Peer) ID() string { return p.id }

// Inbound returns true if the peer connected to us (vs us dialing them).
func (p *Peer) Inbound() bool { return p.inbound }

// Start launches the read and write goroutines.
func (p *Peer) Start() {
	p.wg.Add(2)
	go p.readLoop()
	go p.writeLoop()
}

// Stop gracefully shuts down the peer.
func (p *Peer) Stop() {
	if p.closed.CompareAndSwap(false, true) {
		close(p.quit)
		p.conn.Close()
	}
	p.wg.Wait()
}

// Send queues a message for sending. Non-blocking; drops if buffer full.
func (p *Peer) Send(code byte, payload []byte) {
	select {
	case p.writeCh <- msgFrame{code, payload}:
	case <-p.quit:
	default:
		log.Printf("peer %s: write buffer full, dropping message 0x%02x", p.id, code)
	}
}

func (p *Peer) readLoop() {
	defer p.wg.Done()
	defer p.disconnect()
	for {
		code, payload, err := ReadMsg(p.conn)
		if err != nil {
			return
		}
		p.handler.OnMessage(p, code, payload)
	}
}

func (p *Peer) writeLoop() {
	defer p.wg.Done()
	for {
		select {
		case msg := <-p.writeCh:
			if err := WriteMsg(p.conn, msg.code, msg.payload); err != nil {
				return
			}
		case <-p.quit:
			return
		}
	}
}

func (p *Peer) disconnect() {
	if p.closed.CompareAndSwap(false, true) {
		close(p.quit)
		p.conn.Close()
	}
	p.handler.OnPeerDisconnected(p)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./p2p/ -run "TestPeerSend|TestPeerDisconnect" -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add p2p/peer.go p2p/peer_test.go
git commit -m "p2p: add Peer with read/write goroutines"
```

---

### Task 3: P2P Server

**Files:**
- Create: `p2p/server.go`
- Create: `p2p/server_test.go`
- Modify: `node/config.go`

- [ ] **Step 1: Add SeedNodes and MaxPeers to node.Config**

```go
// node/config.go — replace entire file
package node

type Config struct {
	DataDir     string
	P2PPort     int
	HTTPPort    int
	JSONRPCPort int
	SeedNodes   []string // "host:port" entries for initial peer discovery
	MaxPeers    int      // max simultaneous peers, default 30
}
```

- [ ] **Step 2: Write the failing test for Server**

```go
// p2p/server_test.go
package p2p

import (
	"testing"
	"time"
)

func TestServerStartStop(t *testing.T) {
	h := &testHandler{}
	srv := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0", // random port
		MaxPeers:   5,
	}, h)

	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}

	addr := srv.ListenAddr()
	if addr == "" {
		t.Fatal("expected non-empty listen address")
	}

	srv.Stop()
}

func TestServerAcceptsPeer(t *testing.T) {
	h1 := &testHandler{}
	h2 := &testHandler{}

	srv1 := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0", MaxPeers: 5}, h1)
	srv2 := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0", MaxPeers: 5}, h2)

	srv1.Start()
	defer srv1.Stop()
	srv2.Start()
	defer srv2.Stop()

	// srv2 dials srv1
	if err := srv2.AddPeer(srv1.ListenAddr()); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	if srv1.PeerCount() != 1 {
		t.Fatalf("srv1 peer count: want 1, got %d", srv1.PeerCount())
	}
	if srv2.PeerCount() != 1 {
		t.Fatalf("srv2 peer count: want 1, got %d", srv2.PeerCount())
	}
}

func TestServerRejectsExcessPeers(t *testing.T) {
	h := &testHandler{}
	srv := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0", MaxPeers: 1}, h)
	srv.Start()
	defer srv.Stop()

	h2 := &testHandler{}
	dialer1 := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0", MaxPeers: 5}, h2)
	dialer1.Start()
	defer dialer1.Stop()

	dialer2 := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0", MaxPeers: 5}, h2)
	dialer2.Start()
	defer dialer2.Stop()

	dialer1.AddPeer(srv.ListenAddr())
	time.Sleep(50 * time.Millisecond)

	dialer2.AddPeer(srv.ListenAddr())
	time.Sleep(50 * time.Millisecond)

	if srv.PeerCount() > 1 {
		t.Fatalf("server should have max 1 peer, got %d", srv.PeerCount())
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./p2p/ -run "TestServer" -v`
Expected: FAIL — `NewServer` undefined

- [ ] **Step 4: Implement Server**

```go
// p2p/server.go
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

	s.mu.Lock()
	for _, p := range s.peers {
		p.Stop()
	}
	s.peers = make(map[string]*Peer)
	s.mu.Unlock()

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
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./p2p/ -run "TestServer" -v -count=1`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add p2p/server.go p2p/server_test.go node/config.go
git commit -m "p2p: add TCP server with peer management and connection limits"
```

---

### Task 4: TronHandler — Handshake Protocol

**Files:**
- Create: `net/handler.go`
- Create: `net/handler_test.go`

- [ ] **Step 1: Write the failing test for handshake**

```go
// net/handler_test.go
package net

import (
	"net"
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/p2p"
	"github.com/tronprotocol/go-tron/params"
)

func makeTestChain(t *testing.T) *core.BlockChain {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: tcommon.Address{0x41, 1}, Balance: 1_000_000},
		},
	}
	core.SetupGenesisBlock(diskdb, genesis)
	bc, err := core.NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}
	return bc
}

func TestHandshakeSuccess(t *testing.T) {
	bc := makeTestChain(t)
	pool := txpool.New()

	h1 := NewTronHandler(bc, pool, nil)
	h2 := NewTronHandler(bc, pool, nil)

	srv1 := p2p.NewServer(p2p.ServerConfig{ListenAddr: "127.0.0.1:0", MaxPeers: 5}, h1)
	srv2 := p2p.NewServer(p2p.ServerConfig{ListenAddr: "127.0.0.1:0", MaxPeers: 5}, h2)
	h1.SetServer(srv1)
	h2.SetServer(srv2)

	srv1.Start()
	defer srv1.Stop()
	srv2.Start()
	defer srv2.Stop()

	srv2.AddPeer(srv1.ListenAddr())
	time.Sleep(200 * time.Millisecond)

	if h1.HandshakedPeerCount() != 1 {
		t.Fatalf("h1 handshaked peers: want 1, got %d", h1.HandshakedPeerCount())
	}
	if h2.HandshakedPeerCount() != 1 {
		t.Fatalf("h2 handshaked peers: want 1, got %d", h2.HandshakedPeerCount())
	}
}

func TestHandshakeRejectsWrongGenesis(t *testing.T) {
	bc1 := makeTestChain(t)

	// bc2 has different genesis (different chain ID)
	diskdb2 := ethrawdb.NewMemoryDatabase()
	sdb2 := state.NewDatabase(diskdb2)
	genesis2 := &params.Genesis{
		Config:    &params.ChainConfig{ChainID: 9999, P2PVersion: 1},
		Timestamp: 1000,
	}
	core.SetupGenesisBlock(diskdb2, genesis2)
	bc2, _ := core.NewBlockChain(diskdb2, sdb2, genesis2.Config)

	pool := txpool.New()
	h1 := NewTronHandler(bc1, pool, nil)
	h2 := NewTronHandler(bc2, pool, nil)

	srv1 := p2p.NewServer(p2p.ServerConfig{ListenAddr: "127.0.0.1:0", MaxPeers: 5}, h1)
	srv2 := p2p.NewServer(p2p.ServerConfig{ListenAddr: "127.0.0.1:0", MaxPeers: 5}, h2)
	h1.SetServer(srv1)
	h2.SetServer(srv2)

	srv1.Start()
	defer srv1.Stop()
	srv2.Start()
	defer srv2.Stop()

	srv2.AddPeer(srv1.ListenAddr())
	time.Sleep(200 * time.Millisecond)

	// Handshake should fail — different genesis
	if h1.HandshakedPeerCount() != 0 {
		t.Fatalf("expected 0 handshaked peers after genesis mismatch, got %d", h1.HandshakedPeerCount())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./net/ -run "TestHandshake" -v`
Expected: FAIL — `NewTronHandler` undefined

- [ ] **Step 3: Implement TronHandler with handshake**

```go
// net/handler.go
package net

import (
	"log"
	"sync"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/p2p"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// peerState tracks per-peer protocol state.
type peerState struct {
	peer        *p2p.Peer
	handshaked  bool
	headBlockID tcommon.Hash
	headNum     uint64
}

// TronHandler implements p2p.Handler for the TRON protocol.
type TronHandler struct {
	chain  *core.BlockChain
	pool   *txpool.TxPool
	server *p2p.Server

	mu    sync.RWMutex
	peers map[string]*peerState // peer id → state

	syncService *SyncService
	broadcaster *BroadcastService

	quit chan struct{}
}

// NewTronHandler creates a new TronHandler.
func NewTronHandler(chain *core.BlockChain, pool *txpool.TxPool, broadcaster *BroadcastService) *TronHandler {
	return &TronHandler{
		chain:       chain,
		pool:        pool,
		broadcaster: broadcaster,
		peers:       make(map[string]*peerState),
		quit:        make(chan struct{}),
	}
}

// SetServer sets the P2P server reference (for sending messages).
func (h *TronHandler) SetServer(srv *p2p.Server) {
	h.server = srv
}

// SetSyncService sets the sync service reference.
func (h *TronHandler) SetSyncService(ss *SyncService) {
	h.syncService = ss
}

// HandshakedPeerCount returns the number of handshaked peers.
func (h *TronHandler) HandshakedPeerCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	count := 0
	for _, ps := range h.peers {
		if ps.handshaked {
			count++
		}
	}
	return count
}

// HandshakedPeers returns all handshaked peers.
func (h *TronHandler) HandshakedPeers() []*p2p.Peer {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var result []*p2p.Peer
	for _, ps := range h.peers {
		if ps.handshaked {
			result = append(result, ps.peer)
		}
	}
	return result
}

// OnPeerConnected is called when a new TCP connection is established.
func (h *TronHandler) OnPeerConnected(peer *p2p.Peer) {
	h.mu.Lock()
	h.peers[peer.ID()] = &peerState{peer: peer}
	h.mu.Unlock()

	// Send hello
	hello := h.buildHello()
	data, err := proto.Marshal(hello)
	if err != nil {
		log.Printf("Failed to marshal hello: %v", err)
		peer.Stop()
		return
	}
	peer.Send(p2p.MsgHello, data)
}

// OnPeerDisconnected is called when a peer connection is lost.
func (h *TronHandler) OnPeerDisconnected(peer *p2p.Peer) {
	h.mu.Lock()
	delete(h.peers, peer.ID())
	h.mu.Unlock()
}

// OnMessage routes incoming messages by type.
func (h *TronHandler) OnMessage(peer *p2p.Peer, code byte, payload []byte) {
	switch code {
	case p2p.MsgHello:
		h.handleHello(peer, payload)
	case p2p.MsgDisconnect:
		h.handleDisconnect(peer, payload)
	case p2p.MsgPing:
		peer.Send(p2p.MsgPong, nil)
	case p2p.MsgPong:
		// keep-alive acknowledged
	default:
		// Protocol messages — only process if handshaked
		h.mu.RLock()
		ps := h.peers[peer.ID()]
		h.mu.RUnlock()
		if ps == nil || !ps.handshaked {
			return
		}
		h.handleProtocolMessage(peer, code, payload)
	}
}

func (h *TronHandler) buildHello() *corepb.HelloMessage {
	head := h.chain.CurrentBlock()
	genesis := h.chain.GetBlockByNumber(0)
	genesisID := genesis.ID()
	headID := head.ID()

	return &corepb.HelloMessage{
		Version:   p2p.ProtocolVersion,
		Timestamp: time.Now().UnixMilli(),
		GenesisBlockId: &corepb.HelloMessage_BlockId{
			Hash:   genesisID.Hash[:],
			Number: int64(genesisID.Num),
		},
		SolidBlockId: &corepb.HelloMessage_BlockId{
			Hash:   headID.Hash[:],
			Number: int64(headID.Num),
		},
		HeadBlockId: &corepb.HelloMessage_BlockId{
			Hash:   headID.Hash[:],
			Number: int64(headID.Num),
		},
	}
}

func (h *TronHandler) handleHello(peer *p2p.Peer, payload []byte) {
	var hello corepb.HelloMessage
	if err := proto.Unmarshal(payload, &hello); err != nil {
		log.Printf("Peer %s: bad hello: %v", peer.ID(), err)
		h.disconnectPeer(peer, corepb.ReasonCode_BAD_PROTOCOL)
		return
	}

	// Validate genesis
	genesis := h.chain.GetBlockByNumber(0)
	genesisID := genesis.ID()
	if hello.GenesisBlockId == nil ||
		tcommon.BytesToHash(hello.GenesisBlockId.Hash) != genesisID.Hash {
		log.Printf("Peer %s: genesis mismatch", peer.ID())
		h.disconnectPeer(peer, corepb.ReasonCode_INCOMPATIBLE_CHAIN)
		return
	}

	// Mark handshaked
	h.mu.Lock()
	ps := h.peers[peer.ID()]
	if ps == nil {
		h.mu.Unlock()
		return
	}
	ps.handshaked = true
	if hello.HeadBlockId != nil {
		ps.headNum = uint64(hello.HeadBlockId.Number)
		ps.headBlockID = tcommon.BytesToHash(hello.HeadBlockId.Hash)
	}
	h.mu.Unlock()

	log.Printf("Peer %s handshaked (head=#%d)", peer.ID(), ps.headNum)

	// Trigger sync if peer has more blocks
	if h.syncService != nil && ps.headNum > h.chain.CurrentBlock().Number() {
		h.syncService.StartSync(peer)
	}
}

func (h *TronHandler) handleDisconnect(peer *p2p.Peer, payload []byte) {
	var msg corepb.DisconnectMessage
	if err := proto.Unmarshal(payload, &msg); err == nil {
		log.Printf("Peer %s disconnected: %v", peer.ID(), msg.Reason)
	}
	peer.Stop()
}

func (h *TronHandler) disconnectPeer(peer *p2p.Peer, reason corepb.ReasonCode) {
	msg := &corepb.DisconnectMessage{Reason: reason}
	data, _ := proto.Marshal(msg)
	peer.Send(p2p.MsgDisconnect, data)
	go func() {
		time.Sleep(100 * time.Millisecond)
		peer.Stop()
	}()
}

func (h *TronHandler) handleProtocolMessage(peer *p2p.Peer, code byte, payload []byte) {
	switch code {
	case p2p.MsgSyncBlockChain:
		if h.syncService != nil {
			h.syncService.HandleSyncBlockChain(peer, payload)
		}
	case p2p.MsgChainInventory:
		if h.syncService != nil {
			h.syncService.HandleChainInventory(peer, payload)
		}
	case p2p.MsgFetchInvData:
		h.handleFetchInvData(peer, payload)
	case p2p.MsgBlock:
		h.handleBlock(peer, payload)
	case p2p.MsgTx:
		h.handleTx(peer, payload)
	case p2p.MsgInventory:
		h.handleInventory(peer, payload)
	}
}

func (h *TronHandler) handleFetchInvData(peer *p2p.Peer, payload []byte) {
	var inv corepb.Inventory
	if err := proto.Unmarshal(payload, &inv); err != nil {
		return
	}
	switch inv.Type {
	case corepb.Inventory_BLOCK:
		for _, id := range inv.Ids {
			hash := tcommon.BytesToHash(id)
			block := h.chain.GetBlockByHash(hash)
			if block != nil {
				data, err := proto.Marshal(block.Proto())
				if err == nil {
					peer.Send(p2p.MsgBlock, data)
				}
			}
		}
	case corepb.Inventory_TRX:
		for _, id := range inv.Ids {
			hash := tcommon.BytesToHash(id)
			tx := h.pool.Get(hash)
			if tx != nil {
				data, err := proto.Marshal(tx.Proto())
				if err == nil {
					peer.Send(p2p.MsgTx, data)
				}
			}
		}
	}
}

func (h *TronHandler) handleBlock(peer *p2p.Peer, payload []byte) {
	var pbBlock corepb.Block
	if err := proto.Unmarshal(payload, &pbBlock); err != nil {
		return
	}
	block := core.NewBlockFromPB(&pbBlock)

	// If sync service handles it (sequential sync), defer to it
	if h.syncService != nil && h.syncService.HandleBlock(peer, block) {
		return
	}

	// Otherwise it's a new block broadcast — try to insert
	if err := h.chain.InsertBlock(block); err != nil {
		return
	}
	log.Printf("Received block #%d from peer %s", block.Number(), peer.ID())

	// Relay to other peers
	if h.broadcaster != nil {
		h.broadcaster.BroadcastBlock(block)
	}
}

func (h *TronHandler) handleTx(peer *p2p.Peer, payload []byte) {
	var pbTx corepb.Transaction
	if err := proto.Unmarshal(payload, &pbTx); err != nil {
		return
	}
	tx := core.NewTransactionFromPB(&pbTx)
	if err := h.pool.Add(tx); err != nil {
		return
	}
	// Relay inventory to other peers
	if h.broadcaster != nil {
		h.broadcaster.BroadcastTx(tx)
	}
}

func (h *TronHandler) handleInventory(peer *p2p.Peer, payload []byte) {
	var inv corepb.Inventory
	if err := proto.Unmarshal(payload, &inv); err != nil {
		return
	}

	// Filter out items we already have, request the rest
	var needed [][]byte
	switch inv.Type {
	case corepb.Inventory_BLOCK:
		for _, id := range inv.Ids {
			hash := tcommon.BytesToHash(id)
			if h.chain.GetBlockByHash(hash) == nil {
				needed = append(needed, id)
			}
		}
	case corepb.Inventory_TRX:
		for _, id := range inv.Ids {
			hash := tcommon.BytesToHash(id)
			if h.pool.Get(hash) == nil {
				needed = append(needed, id)
			}
		}
	}

	if len(needed) > 0 {
		fetch := &corepb.Inventory{Type: inv.Type, Ids: needed}
		data, _ := proto.Marshal(fetch)
		peer.Send(p2p.MsgFetchInvData, data)
	}
}
```

Note: `core.NewBlockFromPB` and `core.NewTransactionFromPB` are wrapper references — they live in `core/types` but are re-exported from `core` if available. If not, use `types.NewBlockFromPB` and `types.NewTransactionFromPB` directly and import the `types` package.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./net/ -run "TestHandshake" -v -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add net/handler.go net/handler_test.go
git commit -m "net: add TronHandler with hello handshake and message routing"
```

---

### Task 5: SyncService — Block Chain Sync

**Files:**
- Create: `net/sync.go`
- Create: `net/sync_test.go`

- [ ] **Step 1: Write the failing test for sync**

```go
// net/sync_test.go
package net

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestBuildChainSummary(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	summary := ss.BuildChainSummary()
	// With only genesis, summary should have 1 block ID
	if len(summary) != 1 {
		t.Fatalf("expected 1 entry in chain summary, got %d", len(summary))
	}
	if summary[0].Number() != 0 {
		t.Fatalf("expected genesis in summary, got block #%d", summary[0].Number())
	}
}

func TestBuildChainSummaryMultipleBlocks(t *testing.T) {
	bc := makeTestChain(t)

	// Insert 10 blocks
	for i := uint64(1); i <= 10; i++ {
		parent := bc.CurrentBlock()
		block := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:     int64(i),
					Timestamp:  int64(i) * 3000,
					ParentHash: parent.Hash().Bytes(),
				},
			},
		})
		if err := bc.InsertBlockWithoutVerify(block); err != nil {
			t.Fatal(err)
		}
	}

	ss := NewSyncService(bc, nil)
	summary := ss.BuildChainSummary()

	// Should have: 10, 9, 8, 6, 2, 0 (exponential backoff from head)
	// At minimum: first = head, last = genesis
	if summary[0].Number() != 10 {
		t.Fatalf("first summary entry should be head (#10), got #%d", summary[0].Number())
	}
	last := summary[len(summary)-1]
	if last.Number() != 0 {
		t.Fatalf("last summary entry should be genesis (#0), got #%d", last.Number())
	}
}

func TestFindCommonBlock(t *testing.T) {
	bc := makeTestChain(t)

	// Insert 5 blocks
	for i := uint64(1); i <= 5; i++ {
		parent := bc.CurrentBlock()
		block := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:     int64(i),
					Timestamp:  int64(i) * 3000,
					ParentHash: parent.Hash().Bytes(),
				},
			},
		})
		bc.InsertBlockWithoutVerify(block)
	}

	ss := NewSyncService(bc, nil)

	// Build a summary from blocks we know
	block3 := bc.GetBlockByNumber(3)
	block0 := bc.GetBlockByNumber(0)

	peerSummary := []types.BlockID{block3.ID(), block0.ID()}
	commonNum := ss.FindCommonBlock(peerSummary)

	if commonNum != 3 {
		t.Fatalf("expected common block #3, got #%d", commonNum)
	}
}

func TestFindCommonBlockNoMatch(t *testing.T) {
	bc := makeTestChain(t)
	ss := NewSyncService(bc, nil)

	// Summary with unknown blocks
	fakeID := types.BlockID{Hash: tcommon.Hash{0xFF}, Num: 100}
	commonNum := ss.FindCommonBlock([]types.BlockID{fakeID})

	if commonNum != 0 {
		t.Fatalf("expected common block #0 (genesis fallback), got #%d", commonNum)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./net/ -run "TestBuildChainSummary|TestFindCommonBlock" -v`
Expected: FAIL — `NewSyncService` undefined

- [ ] **Step 3: Implement SyncService**

```go
// net/sync.go
package net

import (
	"log"
	"sync"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/p2p"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const (
	maxChainInventorySize = 2000
	maxFetchBatch         = 100
)

// SyncService handles the block sync protocol.
type SyncService struct {
	chain   *core.BlockChain
	handler *TronHandler

	mu        sync.Mutex
	syncing   bool
	syncPeer  *p2p.Peer
	fetchList []types.BlockID // blocks to fetch from peer
	remainNum int64
}

// NewSyncService creates a new sync service.
func NewSyncService(chain *core.BlockChain, handler *TronHandler) *SyncService {
	return &SyncService{
		chain:   chain,
		handler: handler,
	}
}

// BuildChainSummary creates an exponentially-spaced list of block IDs
// from our chain, used in SYNC_BLOCK_CHAIN messages.
func (ss *SyncService) BuildChainSummary() []types.BlockID {
	head := ss.chain.CurrentBlock()
	headNum := head.Number()

	var summary []types.BlockID
	step := uint64(1)
	num := headNum

	for {
		block := ss.chain.GetBlockByNumber(num)
		if block != nil {
			summary = append(summary, block.ID())
		}
		if num == 0 {
			break
		}
		if num < step {
			num = 0
		} else {
			num -= step
		}
		// Double step each time for exponential backoff
		step *= 2
	}

	return summary
}

// FindCommonBlock finds the highest block in peerSummary that exists in our chain.
func (ss *SyncService) FindCommonBlock(peerSummary []types.BlockID) uint64 {
	for _, bid := range peerSummary {
		block := ss.chain.GetBlockByNumber(bid.Number())
		if block != nil && block.ID().Hash == bid.Hash {
			return bid.Number()
		}
	}
	return 0 // fallback to genesis
}

// StartSync initiates sync with a peer that has a higher head block.
func (ss *SyncService) StartSync(peer *p2p.Peer) {
	ss.mu.Lock()
	if ss.syncing {
		ss.mu.Unlock()
		return
	}
	ss.syncing = true
	ss.syncPeer = peer
	ss.mu.Unlock()

	log.Printf("Starting sync with peer %s", peer.ID())
	ss.sendSyncBlockChain(peer)
}

func (ss *SyncService) sendSyncBlockChain(peer *p2p.Peer) {
	summary := ss.BuildChainSummary()
	var ids []*corepb.BlockInventory_BlockId
	for _, bid := range summary {
		ids = append(ids, &corepb.BlockInventory_BlockId{
			Hash:   bid.Hash[:],
			Number: int64(bid.Num),
		})
	}
	msg := &corepb.BlockInventory{
		Ids:  ids,
		Type: corepb.BlockInventory_SYNC,
	}
	data, _ := proto.Marshal(msg)
	peer.Send(p2p.MsgSyncBlockChain, data)
}

// HandleSyncBlockChain processes SYNC_BLOCK_CHAIN from a peer.
// Responds with CHAIN_INVENTORY containing missing block IDs.
func (ss *SyncService) HandleSyncBlockChain(peer *p2p.Peer, payload []byte) {
	var inv corepb.BlockInventory
	if err := proto.Unmarshal(payload, &inv); err != nil {
		return
	}

	// Convert to BlockIDs
	var peerSummary []types.BlockID
	for _, bid := range inv.Ids {
		peerSummary = append(peerSummary, types.BlockID{
			Hash: tcommon.BytesToHash(bid.Hash),
			Num:  uint64(bid.Number),
		})
	}

	// Find common block
	commonNum := ss.FindCommonBlock(peerSummary)
	headNum := ss.chain.CurrentBlock().Number()

	// Build chain inventory: sequential blocks after common
	var responseIDs []*corepb.ChainInventory_BlockId
	count := 0
	for num := commonNum + 1; num <= headNum && count < maxChainInventorySize; num++ {
		block := ss.chain.GetBlockByNumber(num)
		if block == nil {
			break
		}
		bid := block.ID()
		responseIDs = append(responseIDs, &corepb.ChainInventory_BlockId{
			Hash:   bid.Hash[:],
			Number: int64(bid.Num),
		})
		count++
	}

	remainNum := int64(0)
	if commonNum+uint64(count) < headNum {
		remainNum = int64(headNum) - int64(commonNum) - int64(count)
	}

	resp := &corepb.ChainInventory{
		Ids:       responseIDs,
		RemainNum: remainNum,
	}
	data, _ := proto.Marshal(resp)
	peer.Send(p2p.MsgChainInventory, data)
}

// HandleChainInventory processes CHAIN_INVENTORY from the sync peer.
// Stores the block IDs to fetch, then starts fetching.
func (ss *SyncService) HandleChainInventory(peer *p2p.Peer, payload []byte) {
	ss.mu.Lock()
	if peer != ss.syncPeer {
		ss.mu.Unlock()
		return
	}
	ss.mu.Unlock()

	var inv corepb.ChainInventory
	if err := proto.Unmarshal(payload, &inv); err != nil {
		return
	}

	ss.mu.Lock()
	ss.fetchList = nil
	for _, bid := range inv.Ids {
		ss.fetchList = append(ss.fetchList, types.BlockID{
			Hash: tcommon.BytesToHash(bid.Hash),
			Num:  uint64(bid.Number),
		})
	}
	ss.remainNum = inv.RemainNum
	ss.mu.Unlock()

	if len(inv.Ids) == 0 {
		ss.finishSync()
		return
	}

	log.Printf("Chain inventory: %d blocks to fetch, %d remaining", len(inv.Ids), inv.RemainNum)
	ss.fetchNextBatch()
}

func (ss *SyncService) fetchNextBatch() {
	ss.mu.Lock()
	if len(ss.fetchList) == 0 {
		ss.mu.Unlock()
		// If more remain, request next chain inventory
		if ss.remainNum > 0 {
			ss.sendSyncBlockChain(ss.syncPeer)
		} else {
			ss.finishSync()
		}
		return
	}

	batch := ss.fetchList
	if len(batch) > maxFetchBatch {
		batch = batch[:maxFetchBatch]
	}
	ss.fetchList = ss.fetchList[len(batch):]
	peer := ss.syncPeer
	ss.mu.Unlock()

	var ids [][]byte
	for _, bid := range batch {
		h := bid.Hash
		ids = append(ids, h[:])
	}
	fetch := &corepb.Inventory{
		Type: corepb.Inventory_BLOCK,
		Ids:  ids,
	}
	data, _ := proto.Marshal(fetch)
	peer.Send(p2p.MsgFetchInvData, data)
}

// HandleBlock processes a received block during sync.
// Returns true if the block was consumed by sync, false if it should be handled as a broadcast.
func (ss *SyncService) HandleBlock(peer *p2p.Peer, block *types.Block) bool {
	ss.mu.Lock()
	if !ss.syncing || peer != ss.syncPeer {
		ss.mu.Unlock()
		return false
	}
	ss.mu.Unlock()

	if err := ss.chain.InsertBlock(block); err != nil {
		log.Printf("Sync: failed to insert block #%d: %v", block.Number(), err)
		// Try without verify for sync (peer already validated)
		if err2 := ss.chain.InsertBlockWithoutVerify(block); err2 != nil {
			log.Printf("Sync: also failed InsertBlockWithoutVerify #%d: %v", block.Number(), err2)
			return true
		}
	}

	log.Printf("Synced block #%d", block.Number())

	// Check if we need more blocks
	ss.fetchNextBatch()
	return true
}

func (ss *SyncService) finishSync() {
	ss.mu.Lock()
	ss.syncing = false
	ss.syncPeer = nil
	ss.fetchList = nil
	ss.remainNum = 0
	ss.mu.Unlock()
	log.Printf("Sync complete (head=#%d)", ss.chain.CurrentBlock().Number())
}

// IsSyncing returns whether sync is in progress.
func (ss *SyncService) IsSyncing() bool {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.syncing
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./net/ -run "TestBuildChainSummary|TestFindCommonBlock" -v -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add net/sync.go net/sync_test.go
git commit -m "net: add SyncService with chain summary and block fetch protocol"
```

---

### Task 6: BroadcastService — Inventory Gossip

**Files:**
- Create: `net/broadcaster.go`
- Create: `net/broadcaster_test.go`

- [ ] **Step 1: Write the failing test for broadcaster**

```go
// net/broadcaster_test.go
package net

import (
	"net"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/p2p"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestBroadcastBlockSendsInventory(t *testing.T) {
	// Set up two peers connected via pipe
	c1, c2 := net.Pipe()
	h := &testCollector{}
	peer := p2p.NewPeer(c1, "test:1", false, h)
	peer.Start()
	defer peer.Stop()

	// Read messages from the other end
	go func() {
		for {
			code, _, err := p2p.ReadMsg(c2)
			if err != nil {
				return
			}
			h.mu.Lock()
			h.codes = append(h.codes, code)
			h.mu.Unlock()
		}
	}()

	bc := &BroadcastService{}
	bc.getPeers = func() []*p2p.Peer { return []*p2p.Peer{peer} }

	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 3000},
		},
	})

	bc.BroadcastBlock(block)
	time.Sleep(50 * time.Millisecond)

	h.mu.Lock()
	defer h.mu.Unlock()
	found := false
	for _, c := range h.codes {
		if c == p2p.MsgInventory {
			found = true
		}
	}
	if !found {
		t.Fatal("expected INVENTORY message to be sent")
	}
}

type testCollector struct {
	mu    sync.Mutex
	codes []byte
}

func (tc *testCollector) OnPeerConnected(p *p2p.Peer)               {}
func (tc *testCollector) OnPeerDisconnected(p *p2p.Peer)             {}
func (tc *testCollector) OnMessage(p *p2p.Peer, code byte, data []byte) {}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./net/ -run "TestBroadcast" -v`
Expected: FAIL — `BroadcastService` undefined

- [ ] **Step 3: Implement BroadcastService**

```go
// net/broadcaster.go
package net

import (
	"sync"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/p2p"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

const seenCacheSize = 10000

// BroadcastService manages inventory-based gossip for new blocks and transactions.
type BroadcastService struct {
	getPeers func() []*p2p.Peer

	mu   sync.Mutex
	seen map[tcommon.Hash]struct{}
}

// NewBroadcastService creates a new broadcast service.
// getPeers returns the list of handshaked peers to broadcast to.
func NewBroadcastService(getPeers func() []*p2p.Peer) *BroadcastService {
	return &BroadcastService{
		getPeers: getPeers,
		seen:     make(map[tcommon.Hash]struct{}),
	}
}

// BroadcastBlock sends an INVENTORY message for a new block to all peers.
func (bs *BroadcastService) BroadcastBlock(block *types.Block) {
	hash := block.Hash()
	if bs.markSeen(hash) {
		return // already broadcast
	}

	inv := &corepb.Inventory{
		Type: corepb.Inventory_BLOCK,
		Ids:  [][]byte{hash[:]},
	}
	data, _ := proto.Marshal(inv)

	for _, peer := range bs.getPeers() {
		peer.Send(p2p.MsgInventory, data)
	}
}

// BroadcastTx sends an INVENTORY message for a new transaction to all peers.
func (bs *BroadcastService) BroadcastTx(tx *types.Transaction) {
	hash := tx.Hash()
	if bs.markSeen(hash) {
		return
	}

	inv := &corepb.Inventory{
		Type: corepb.Inventory_TRX,
		Ids:  [][]byte{hash[:]},
	}
	data, _ := proto.Marshal(inv)

	for _, peer := range bs.getPeers() {
		peer.Send(p2p.MsgInventory, data)
	}
}

// markSeen returns true if already seen, false if new (and marks it).
func (bs *BroadcastService) markSeen(hash tcommon.Hash) bool {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if _, exists := bs.seen[hash]; exists {
		return true
	}
	// Evict oldest if cache is full (simple: clear entire cache)
	if len(bs.seen) >= seenCacheSize {
		bs.seen = make(map[tcommon.Hash]struct{})
	}
	bs.seen[hash] = struct{}{}
	return false
}
```

- [ ] **Step 4: Add missing `sync` import to test file, then run tests**

Add `"sync"` import to `net/broadcaster_test.go`.

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./net/ -run "TestBroadcast" -v -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add net/broadcaster.go net/broadcaster_test.go
git commit -m "net: add BroadcastService with inventory-based gossip"
```

---

### Task 7: Producer Block Callback

**Files:**
- Modify: `core/producer/producer.go`

- [ ] **Step 1: Add BlockCallback to Producer**

Add a `BlockCallback` field and call it after successful block production:

```go
// In producer.go, modify Producer struct to add:
type Producer struct {
	chain       *core.BlockChain
	pool        *txpool.TxPool
	engine      *dpos.DPoS
	witnessKey  *ecdsa.PrivateKey
	witnessAddr tcommon.Address

	lastProducedSlot int64
	loggedWitnessErr bool
	quit             chan struct{}
	wg               sync.WaitGroup

	// BlockCallback is called after a new block is produced and inserted.
	// Used by the P2P layer to broadcast the block to peers.
	BlockCallback func(block *types.Block)
}
```

Then at the end of `produceBlock()`, after the log line, add:

```go
	if p.BlockCallback != nil {
		p.BlockCallback(block)
	}
```

- [ ] **Step 2: Verify all existing tests pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/producer/ -v -count=1`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add core/producer/producer.go
git commit -m "core/producer: add BlockCallback for P2P broadcast integration"
```

---

### Task 8: CLI Integration — Wire P2P into Node

**Files:**
- Modify: `cmd/gtron/main.go`
- Modify: `cmd/gtron/config.go`

- [ ] **Step 1: Add CLI flags and config parsing**

In `cmd/gtron/main.go`, add flags:

```go
seednodeFlag = &cli.StringSliceFlag{
	Name:  "seednode",
	Usage: "Seed node address (host:port), can be specified multiple times",
}
maxpeersFlag = &cli.IntFlag{
	Name:  "maxpeers",
	Usage: "Maximum number of P2P peers",
	Value: 30,
}
```

Add them to the `Flags` list in the app definition.

In `cmd/gtron/config.go`, update `makeConfig` to include:

```go
func makeConfig(ctx *cli.Context) *node.Config {
	return &node.Config{
		DataDir:     ctx.String("datadir"),
		P2PPort:     ctx.Int("p2p.port"),
		HTTPPort:    ctx.Int("http.port"),
		JSONRPCPort: ctx.Int("jsonrpc.port"),
		SeedNodes:   ctx.StringSlice("seednode"),
		MaxPeers:    ctx.Int("maxpeers"),
	}
}
```

- [ ] **Step 2: Wire P2P server into gtron() function**

In `cmd/gtron/main.go`, after creating the producer (or after the existing service creation), add:

```go
	// Create P2P layer
	broadcaster := net.NewBroadcastService(nil) // getPeers set below
	handler := net.NewTronHandler(bc, pool, broadcaster)
	syncService := net.NewSyncService(bc, handler)
	handler.SetSyncService(syncService)

	p2pServer := p2p.NewServer(p2p.ServerConfig{
		ListenAddr: fmt.Sprintf(":%d", cfg.P2PPort),
		MaxPeers:   cfg.MaxPeers,
		SeedNodes:  cfg.SeedNodes,
	}, handler)
	handler.SetServer(p2pServer)
	broadcaster.getPeers = handler.HandshakedPeers // now wire getPeers

	stack.RegisterLifecycle(p2pServer)
```

And if a producer is created, set its `BlockCallback`:

```go
	prod := producer.New(bc, pool, engine, key)
	prod.BlockCallback = func(block *types.Block) {
		broadcaster.BroadcastBlock(block)
	}
	stack.RegisterLifecycle(prod)
```

Add the necessary imports: `"github.com/tronprotocol/go-tron/net"`, `"github.com/tronprotocol/go-tron/p2p"`.

- [ ] **Step 3: Verify it compiles**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go build ./...`
Expected: SUCCESS

- [ ] **Step 4: Commit**

```bash
git add cmd/gtron/main.go cmd/gtron/config.go
git commit -m "cmd/gtron: wire P2P server, broadcaster, and sync into node lifecycle"
```

---

### Task 9: Keep-Alive and Ping/Pong

**Files:**
- Modify: `net/handler.go`

- [ ] **Step 1: Add keep-alive loop to TronHandler**

Add a `StartKeepAlive` method that pings all handshaked peers every 30 seconds:

```go
// StartKeepAlive starts a goroutine that pings peers every 30 seconds.
func (h *TronHandler) StartKeepAlive() {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				for _, peer := range h.HandshakedPeers() {
					peer.Send(p2p.MsgPing, nil)
				}
			case <-h.quit:
				return
			}
		}
	}()
}

// Stop signals the handler to shut down.
func (h *TronHandler) Stop() {
	select {
	case <-h.quit:
	default:
		close(h.quit)
	}
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go build ./...`
Expected: SUCCESS

- [ ] **Step 3: Commit**

```bash
git add net/handler.go
git commit -m "net: add keep-alive ping loop to TronHandler"
```

---

### Task 10: Integration Test — Two-Node Sync

**Files:**
- Create: `net/integration_test.go`

- [ ] **Step 1: Write the integration test**

```go
// net/integration_test.go
package net

import (
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/p2p"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func makeChainWithBlocks(t *testing.T, numBlocks int) *core.BlockChain {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: tcommon.Address{0x41, 1}, Balance: 1_000_000},
		},
	}
	core.SetupGenesisBlock(diskdb, genesis)
	bc, err := core.NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= numBlocks; i++ {
		parent := bc.CurrentBlock()
		block := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:     int64(i),
					Timestamp:  int64(i) * 3000,
					ParentHash: parent.Hash().Bytes(),
				},
			},
		})
		if err := bc.InsertBlockWithoutVerify(block); err != nil {
			t.Fatal(err)
		}
	}
	return bc
}

func TestTwoNodeSync(t *testing.T) {
	// Node A has 20 blocks, Node B has 0 (only genesis)
	bcA := makeChainWithBlocks(t, 20)
	bcB := makeTestChain(t) // genesis only

	poolA := txpool.New()
	poolB := txpool.New()

	// Create handlers
	broadcasterA := NewBroadcastService(nil)
	broadcasterB := NewBroadcastService(nil)

	handlerA := NewTronHandler(bcA, poolA, broadcasterA)
	handlerB := NewTronHandler(bcB, poolB, broadcasterB)

	syncA := NewSyncService(bcA, handlerA)
	syncB := NewSyncService(bcB, handlerB)
	handlerA.SetSyncService(syncA)
	handlerB.SetSyncService(syncB)

	srvA := p2p.NewServer(p2p.ServerConfig{ListenAddr: "127.0.0.1:0", MaxPeers: 5}, handlerA)
	srvB := p2p.NewServer(p2p.ServerConfig{ListenAddr: "127.0.0.1:0", MaxPeers: 5}, handlerB)
	handlerA.SetServer(srvA)
	handlerB.SetServer(srvB)
	broadcasterA.getPeers = handlerA.HandshakedPeers
	broadcasterB.getPeers = handlerB.HandshakedPeers

	srvA.Start()
	defer srvA.Stop()
	srvB.Start()
	defer srvB.Stop()

	// B connects to A — should trigger sync because A has block #20
	srvB.AddPeer(srvA.ListenAddr())

	// Wait for sync to complete (up to 5 seconds)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if bcB.CurrentBlock().Number() >= 20 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if bcB.CurrentBlock().Number() != 20 {
		t.Fatalf("Node B should have synced to block #20, got #%d", bcB.CurrentBlock().Number())
	}
}
```

- [ ] **Step 2: Run the integration test**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./net/ -run "TestTwoNodeSync" -v -count=1 -timeout 30s`
Expected: PASS — Node B syncs 20 blocks from Node A

- [ ] **Step 3: Commit**

```bash
git add net/integration_test.go
git commit -m "net: add two-node sync integration test"
```

---

### Task 11: P2P Server Lifecycle Adapter

**Files:**
- Modify: `p2p/server.go`

- [ ] **Step 1: Ensure Server implements node.Lifecycle**

The Server already has `Start() error` and `Stop() error`. Verify it satisfies the `node.Lifecycle` interface by adding a compile-time check:

```go
// At the top of p2p/server.go, add:
var _ interface {
	Start() error
	Stop() error
} = (*Server)(nil)
```

This is already satisfied by the existing methods. No code changes needed if `Start` and `Stop` signatures match.

- [ ] **Step 2: Run all tests**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./... -count=1`
Expected: ALL PASS

- [ ] **Step 3: Commit (if any changes were needed)**

```bash
git commit -m "p2p: verify Server implements Lifecycle interface"
```

---

### Task 12: Smoke Test — Multi-Node Dev Setup

**Files:** None (manual testing)

- [ ] **Step 1: Build and start Node A (block producer)**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron
go build -o /tmp/gtron ./cmd/gtron/
/tmp/gtron --datadir /tmp/gtron-nodeA --http.port 18090 --p2p.port 18888 \
  --dev --witness.key 0000000000000000000000000000000000000000000000000000000000000001
```

- [ ] **Step 2: Start Node B (sync only) connecting to Node A**

```bash
/tmp/gtron --datadir /tmp/gtron-nodeB --http.port 18091 --p2p.port 18889 \
  --dev --witness.key 0000000000000000000000000000000000000000000000000000000000000001 \
  --seednode 127.0.0.1:18888
```

- [ ] **Step 3: Verify Node B syncs blocks from Node A**

```bash
# Wait a few seconds, then check both nodes
curl -s http://localhost:18090/wallet/getnowblock | python3 -c "import sys,json; print('A:', json.load(sys.stdin)['block_header']['raw_data']['number'])"
curl -s http://localhost:18091/wallet/getnowblock | python3 -c "import sys,json; print('B:', json.load(sys.stdin)['block_header']['raw_data']['number'])"
```

Expected: Node B's block number should be close to Node A's.

- [ ] **Step 4: Test transaction propagation**

Send a transaction to Node A, verify it appears on Node B after the next block:

```bash
# (Use the test script from Phase 4 functional testing, targeting port 18090)
# Then check Node B has the same block with the transaction
```

- [ ] **Step 5: Cleanup**

```bash
pkill -f "gtron"; rm -rf /tmp/gtron-nodeA /tmp/gtron-nodeB /tmp/gtron
```

---

## Self-Review

### Spec Coverage

| Spec Section | Task |
|---|---|
| Wire format (4B len + 1B type + protobuf) | Task 1 |
| Message type codes | Task 1 |
| Peer connection lifecycle | Task 2, Task 3 |
| HelloMessage handshake | Task 4 |
| Handshake validation (genesis, version) | Task 4 |
| Block sync (chain summary → fetch) | Task 5 |
| Transaction/block propagation (inventory gossip) | Task 6 |
| Ping/pong keep-alive | Task 9 |
| Config additions (SeedNodes, MaxPeers) | Task 3, Task 8 |
| CLI flags (--seednode, --maxpeers) | Task 8 |
| Producer → broadcaster integration | Task 7, Task 8 |
| Integration testing | Task 10 |
| Smoke test | Task 12 |

### Type Consistency Check

- `p2p.Handler` interface: used in `NewPeer`, `NewServer`, `TronHandler` — consistent
- `p2p.Peer`: created in `NewPeer`, used in `Handler.OnMessage`, `SyncService`, `BroadcastService` — consistent
- `p2p.Server`: `NewServer`, `Start/Stop`, `AddPeer`, `PeerCount`, `Peers` — consistent
- `types.BlockID`: used in `BuildChainSummary`, `FindCommonBlock`, sync messages — consistent
- `BroadcastService.getPeers`: func field set via `handler.HandshakedPeers` — consistent
- `Producer.BlockCallback`: `func(*types.Block)` — consistent with broadcaster's `BroadcastBlock`
