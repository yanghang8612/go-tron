package p2p

import (
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	p2ppb "github.com/tronprotocol/go-tron/proto/p2p"
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

	// lastPongNanos holds the UnixNano timestamp of the most recent keepalive
	// pong received (or the handshake completion time if no pong yet). It is
	// written and read atomically so no mutex is needed.
	// Note: UnixNano does not overflow for any plausible clock value in the
	// range [1678, 2262] CE, which covers all expected production use.
	lastPongNanos atomic.Int64
}

type msgFrame struct {
	code    byte
	payload []byte
}

// NewPeer creates a new Peer wrapping a TCP connection.
func NewPeer(conn net.Conn, id string, inbound bool, handler Handler) *Peer {
	p := &Peer{
		conn:    conn,
		id:      id,
		inbound: inbound,
		handler: handler,
		writeCh: make(chan msgFrame, 256),
		quit:    make(chan struct{}),
	}
	// Treat handshake completion as the "last live" event so the keepalive
	// timer doesn't immediately expire on a freshly connected peer.
	p.lastPongNanos.Store(time.Now().UnixNano())
	return p
}

// ID returns the peer's identifier (typically "host:port").
func (p *Peer) ID() string { return p.id }

// Inbound returns true if the peer connected to us (vs us dialing them).
func (p *Peer) Inbound() bool { return p.inbound }

// Start launches the read, write, and keepalive goroutines.
func (p *Peer) Start() {
	p.wg.Add(3)
	go p.readLoop()
	go p.writeLoop()
	go p.keepaliveLoop()
}

// Stop gracefully shuts down the peer and waits for goroutines to exit.
func (p *Peer) Stop() {
	if p.closed.CompareAndSwap(false, true) {
		close(p.quit)
		p.conn.Close()
	}
	p.wg.Wait()
}

// Close closes the connection without waiting for goroutines.
// Safe to call from within readLoop or keepaliveLoop (unlike Stop which would
// deadlock).
func (p *Peer) Close() {
	if p.closed.CompareAndSwap(false, true) {
		close(p.quit)
		p.conn.Close()
	}
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

// GoodbyeAndClose sends a DISCONNECT message with the given reason and then
// closes the peer. The write is best-effort (sent directly on the conn to
// bypass the write buffer which may be full or draining).
func (p *Peer) GoodbyeAndClose(reason p2ppb.DisconnectReason) {
	dm := BuildDisconnect(reason)
	if body, err := EncodeDisconnect(dm); err == nil {
		_ = WriteMsg(p.conn, MsgLibp2pDisconnect, body)
	}
	p.Close()
}

func (p *Peer) readLoop() {
	defer p.wg.Done()
	defer p.disconnect()
	for {
		code, payload, err := ReadMsg(p.conn)
		if err != nil {
			return
		}
		switch code {
		case MsgLibp2pKeepAlivePing:
			// Echo back as pong — include the caller's timestamp payload so the
			// remote can measure RTT if desired.
			p.Send(MsgLibp2pKeepAlivePong, payload)
		case MsgLibp2pKeepAlivePong:
			p.lastPongNanos.Store(time.Now().UnixNano())
		case MsgLibp2pDisconnect:
			// Peer told us to go away; close gracefully.
			return
		case MsgLibp2pStatus:
			// Accept but ignore — application layer doesn't consume this yet.
		case MsgLibp2pHello:
			// A second HELLO after the handshake is unexpected; drop silently.
		default:
			p.handler.OnMessage(p, code, payload)
		}
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

// keepaliveLoop sends KEEP_ALIVE_PING every KeepAliveTimeout/2 and closes the
// peer if no pong has been received within 2*KeepAliveTimeout.
func (p *Peer) keepaliveLoop() {
	defer p.wg.Done()
	// Fire pings at roughly half the timeout so we get a pong before the peer
	// would consider us dead.
	tick := time.NewTicker(KeepAliveTimeout / 2)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			// Check pong freshness — if we've gone too long without one, drop.
			lastPong := time.Unix(0, p.lastPongNanos.Load())
			if time.Since(lastPong) > 2*KeepAliveTimeout {
				log.Printf("peer %s: keepalive timeout, closing", p.id)
				p.Close()
				return
			}
			ka := BuildKeepAlive()
			payload, err := EncodeKeepAlive(ka)
			if err == nil {
				p.Send(MsgLibp2pKeepAlivePing, payload)
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
