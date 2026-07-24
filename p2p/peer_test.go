package p2p

import (
	"net"
	"sync"
	"testing"
	"time"

	p2ppb "github.com/tronprotocol/go-tron/proto/p2p"
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

func TestPeerGoodbyeAndCloseWritesDisconnectReason(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c2.Close()
	p := NewPeer(c1, "pipe:goodbye", false, &testHandler{})
	done := make(chan struct{})
	go func() {
		p.GoodbyeAndClose(p2ppb.DisconnectReason_PEER_QUITING)
		close(done)
	}()

	_ = c2.SetReadDeadline(time.Now().Add(time.Second))
	body, err := ReadFrameBody(c2)
	if err != nil {
		t.Fatalf("read goodbye frame: %v", err)
	}
	code, payload, err := UnwrapPostHandshake(body)
	if err != nil {
		t.Fatalf("unwrap goodbye frame: %v", err)
	}
	if code != MsgLibp2pDisconnect {
		t.Fatalf("goodbye code = %#x, want %#x", code, MsgLibp2pDisconnect)
	}
	msg, err := ParseDisconnect(payload)
	if err != nil {
		t.Fatalf("parse goodbye: %v", err)
	}
	if msg.Reason != p2ppb.DisconnectReason_PEER_QUITING {
		t.Fatalf("goodbye reason = %v, want PEER_QUITING", msg.Reason)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("GoodbyeAndClose did not return")
	}
	p.Stop()
}

func TestPeerRefreshesLastSeenOnInboundFrame(t *testing.T) {
	c1, c2 := net.Pipe()
	h := &testHandler{}
	p := NewPeer(c1, "pipe:seen", false, h)
	p.lastSeenNanos.Store(time.Now().Add(-time.Hour).UnixNano())
	before := p.lastSeenNanos.Load()
	p.Start()
	defer p.Stop()
	defer c2.Close()

	body, err := WrapPostHandshake(MsgLibp2pKeepAlivePing, []byte{0x01})
	if err != nil {
		t.Fatalf("wrap ping: %v", err)
	}
	if err := WriteFrameBody(c2, body); err != nil {
		t.Fatalf("write ping: %v", err)
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := p.lastSeenNanos.Load(); got > before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("lastSeenNanos was not refreshed by inbound keepalive ping")
}
