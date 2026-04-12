package p2p

import (
	"io"
	"net"
	"testing"
	"time"

	p2ppb "github.com/tronprotocol/go-tron/proto/p2p"
)

// newTestServer builds a minimal Server suitable for handshake testing.
// Both sides have matching NetworkID/Version by default.
func newTestServer(nodeIDByte byte, networkID int32, port int32) *Server {
	id := make([]byte, 64)
	id[0] = nodeIDByte
	return &Server{config: ServerConfig{
		NodeID:     id,
		NetworkID:  networkID,
		Version:    Libp2pProtocolVersion,
		ExternalIP: "127.0.0.1",
		Port:       port,
	}}
}

// dialPair returns two connected TCP net.Conn using a loopback listener.
// net.Pipe() is synchronous (no kernel buffer) and deadlocks when both sides
// write before reading, which is exactly what performLibp2pHandshake does.
// Real TCP connections have kernel-buffered sends, avoiding the deadlock.
func dialPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	type result struct {
		conn net.Conn
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		c, err := ln.Accept()
		ch <- result{c, err}
	}()

	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	r := <-ch
	if r.err != nil {
		client.Close()
		t.Fatalf("accept: %v", r.err)
	}
	t.Cleanup(func() {
		client.Close()
		r.conn.Close()
	})
	return client, r.conn
}

// TestPerformLibp2pHandshakeSuccess checks that two compatible peers can
// complete the libp2p HANDSHAKE_HELLO exchange without error.
func TestPerformLibp2pHandshakeSuccess(t *testing.T) {
	a, b := dialPair(t)

	srvA := newTestServer(0xAA, 1, 18888)
	srvB := newTestServer(0xBB, 1, 18889)

	errA := make(chan error, 1)
	errB := make(chan error, 1)
	go func() { errA <- srvA.performLibp2pHandshake(a) }()
	go func() { errB <- srvB.performLibp2pHandshake(b) }()

	select {
	case err := <-errA:
		if err != nil {
			t.Fatalf("A handshake: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("A timed out")
	}
	select {
	case err := <-errB:
		if err != nil {
			t.Fatalf("B handshake: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("B timed out")
	}
}

// TestPerformLibp2pHandshakeNetworkMismatch ensures that a NetworkID mismatch
// causes at least one side to reject the handshake.
func TestPerformLibp2pHandshakeNetworkMismatch(t *testing.T) {
	a, b := dialPair(t)

	srvA := newTestServer(0xAA, 1, 18888)
	srvB := newTestServer(0xBB, 999, 18889) // different network

	errA := make(chan error, 1)
	errB := make(chan error, 1)
	go func() { errA <- srvA.performLibp2pHandshake(a) }()
	go func() { errB <- srvB.performLibp2pHandshake(b) }()

	var aErr, bErr error
	select {
	case aErr = <-errA:
	case <-time.After(5 * time.Second):
		t.Fatal("A timed out")
	}
	select {
	case bErr = <-errB:
	case <-time.After(5 * time.Second):
		t.Fatal("B timed out")
	}
	if aErr == nil && bErr == nil {
		t.Fatal("expected at least one side to reject; both accepted")
	}
}

// TestPerformLibp2pHandshakeVersionMismatch ensures that a Version mismatch
// causes at least one side to reject the handshake.
func TestPerformLibp2pHandshakeVersionMismatch(t *testing.T) {
	a, b := dialPair(t)

	srvA := newTestServer(0xAA, 1, 18888)
	srvB := newTestServer(0xBB, 1, 18889)
	srvB.config.Version = 99 // incompatible version

	errA := make(chan error, 1)
	errB := make(chan error, 1)
	go func() { errA <- srvA.performLibp2pHandshake(a) }()
	go func() { errB <- srvB.performLibp2pHandshake(b) }()

	var aErr, bErr error
	select {
	case aErr = <-errA:
	case <-time.After(5 * time.Second):
		t.Fatal("A timed out")
	}
	select {
	case bErr = <-errB:
	case <-time.After(5 * time.Second):
		t.Fatal("B timed out")
	}
	if aErr == nil && bErr == nil {
		t.Fatal("expected at least one side to reject; both accepted")
	}
}

// writeWrappedFrame wraps [code][payload] in a CompressMessage and writes the
// complete frame (varint32-prefixed). Used by tests that simulate a remote
// peer's post-handshake traffic into our Peer's conn.
func writeWrappedFrame(w io.Writer, code byte, payload []byte) error {
	body, err := WrapPostHandshake(code, payload)
	if err != nil {
		return err
	}
	return WriteFrameBody(w, body)
}

// readWrappedFrame reads one varint32-prefixed frame, unwraps the
// CompressMessage, and returns the real (code, payload).
func readWrappedFrame(r io.Reader) (byte, []byte, error) {
	body, err := ReadFrameBody(r)
	if err != nil {
		return 0, nil, err
	}
	return UnwrapPostHandshake(body)
}

// TestPeerReadLoopInterceptsLibp2pCodes verifies that the readLoop intercepts
// libp2p control codes and never forwards them to the application handler.
func TestPeerReadLoopInterceptsLibp2pCodes(t *testing.T) {
	c1, c2 := net.Pipe()
	h := &testHandler{}
	p := NewPeer(c1, "test", false, h)
	p.Start()
	defer p.Stop()

	// Drain any outgoing messages from c2 in a background goroutine so that
	// net.Pipe()'s synchronous writes don't deadlock. (A KEEP_ALIVE_PING causes
	// the writeLoop to respond with a PONG, which blocks until c2 is read.)
	go func() {
		for {
			if _, _, err := readWrappedFrame(c2); err != nil {
				return
			}
		}
	}()

	// Send each control code from the other end (CompressMessage-wrapped).
	controlCodes := []byte{
		MsgLibp2pKeepAlivePing,
		MsgLibp2pKeepAlivePong,
		MsgLibp2pStatus,
		MsgLibp2pHello,
	}
	for _, code := range controlCodes {
		if err := writeWrappedFrame(c2, code, nil); err != nil {
			t.Fatalf("write code %#x: %v", code, err)
		}
	}

	// Also send an application message to verify routing is still working.
	if err := writeWrappedFrame(c2, MsgPing, nil); err != nil {
		t.Fatalf("write MsgPing: %v", err)
	}

	// Wait for the application message to arrive.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		n := len(h.messages)
		h.mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.messages) != 1 {
		t.Fatalf("expected 1 application message, got %d", len(h.messages))
	}
	if h.messages[0].code != MsgPing {
		t.Fatalf("expected MsgPing, got %#x", h.messages[0].code)
	}
}

// TestPeerKeepalivePingPong verifies that when a PING arrives the peer echoes
// a PONG back on the same connection.
func TestPeerKeepalivePingPong(t *testing.T) {
	c1, c2 := net.Pipe()
	h := &testHandler{}
	p := NewPeer(c1, "test-ping", false, h)
	p.Start()
	defer p.Stop()

	// Send a KEEP_ALIVE_PING with a small payload, CompressMessage-wrapped.
	pingPayload := []byte{0x01, 0x02}
	if err := writeWrappedFrame(c2, MsgLibp2pKeepAlivePing, pingPayload); err != nil {
		t.Fatalf("write ping: %v", err)
	}

	c2.SetDeadline(time.Now().Add(2 * time.Second))
	defer c2.SetDeadline(time.Time{})

	code, payload, err := readWrappedFrame(c2)
	if err != nil {
		t.Fatalf("read pong: %v", err)
	}
	if code != MsgLibp2pKeepAlivePong {
		t.Fatalf("expected PONG (%#x), got %#x", MsgLibp2pKeepAlivePong, code)
	}
	if len(payload) != len(pingPayload) {
		t.Fatalf("pong payload length: want %d, got %d", len(pingPayload), len(payload))
	}
}

// TestPeerDisconnectMsgCloses verifies that receiving a DISCONNECT message
// causes the peer to close (handler.OnPeerDisconnected fires).
func TestPeerDisconnectMsgCloses(t *testing.T) {
	c1, c2 := net.Pipe()
	h := &testHandler{}
	p := NewPeer(c1, "test-disc", false, h)
	p.Start()

	// Build and send a DISCONNECT message.
	dm := BuildDisconnect(p2ppb.DisconnectReason_DIFFERENT_VERSION)
	body, err := EncodeDisconnect(dm)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeWrappedFrame(c2, MsgLibp2pDisconnect, body); err != nil {
		t.Fatalf("write disconnect: %v", err)
	}

	// Wait for the handler to be notified.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		n := len(h.disconnected)
		h.mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.disconnected) == 0 {
		t.Fatal("expected OnPeerDisconnected to be called after DISCONNECT message")
	}
}
