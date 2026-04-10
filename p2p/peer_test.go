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
