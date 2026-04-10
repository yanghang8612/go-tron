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
