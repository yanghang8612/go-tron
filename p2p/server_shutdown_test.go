package p2p

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

type shutdownBlockingHandler struct {
	testHandler
	entered chan struct{}
	release chan struct{}
}

func (h *shutdownBlockingHandler) OnPeerConnected(p *Peer) {
	close(h.entered)
	<-h.release
	h.testHandler.OnPeerConnected(p)
}

func awaitShutdownSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for shutdown boundary")
	}
}

// An outbound callback can still be using chain storage after the maintenance
// loop exits. Stop must join it before the caller closes that storage.
func TestServerStopJoinsOutboundCallback(t *testing.T) {
	remote := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0"}, &testHandler{})
	if err := remote.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = remote.Stop() }()
	h := &shutdownBlockingHandler{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(h.release) }) }
	defer release()
	client := NewServer(ServerConfig{}, h)
	added := make(chan error, 1)
	go func() { added <- client.AddPeer(remote.ListenAddr()) }()
	awaitShutdownSignal(t, h.entered)
	stopped := make(chan struct{})
	go func() { _ = client.Stop(); close(stopped) }()
	awaitShutdownSignal(t, client.quit)
	select {
	case <-stopped:
		t.Fatal("Stop returned while OnPeerConnected still owned chain access")
	case <-time.After(50 * time.Millisecond):
	}
	release()
	awaitShutdownSignal(t, stopped)
	if err := <-added; err != nil {
		t.Fatal(err)
	}
	if client.PeerCount() != 0 {
		t.Fatal("peer remained after Stop")
	}
	if err := client.AddPeer(remote.ListenAddr()); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("late AddPeer = %v, want closed", err)
	}
	if err := client.addPeerConn(nil, "late", false); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("late addPeerConn = %v, want closed", err)
	}
}

// Complete an admitted handshake only after Stop closes admission. It must
// never publish a peer or invoke the application callback.
func TestServerStopRejectsFinishingHandshake(t *testing.T) {
	h := &testHandler{}
	srv := NewServer(ServerConfig{}, h)
	local, remote := net.Pipe()
	defer func() { _ = local.Close() }()
	defer func() { _ = remote.Close() }()
	_ = remote.SetDeadline(time.Now().Add(3 * time.Second))
	added := make(chan error, 1)
	go func() { added <- srv.addPeerConn(local, "pending", false) }()
	code, _, err := ReadMsg(remote)
	if err != nil || code != MsgLibp2pHello {
		t.Fatalf("initial handshake: code=%v err=%v", code, err)
	}
	stopped := make(chan struct{})
	go func() { _ = srv.Stop(); close(stopped) }()
	awaitShutdownSignal(t, srv.quit)
	other := NewServer(ServerConfig{}, &testHandler{})
	hello, err := EncodeHello(BuildHelloMessage(other.LocalEndpoint(), srv.config.NetworkID, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteMsg(remote, MsgLibp2pHello, hello); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-added:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("handshake after stop = %v, want closed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handshake did not exit")
	}
	awaitShutdownSignal(t, stopped)
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.connected) != 0 || srv.PeerCount() != 0 {
		t.Fatal("handshake published a peer after stop")
	}
}
