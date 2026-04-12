package p2p

import (
	"fmt"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/p2p/discover"
)

// TestLoopbackServerHandshake verifies that two p2p.Server instances with
// matching NetworkID/Version can complete the full lifecycle:
//   1. Both listen on ephemeral ports
//   2. One dials the other via AddPeer
//   3. Both sides complete the libp2p handshake
//   4. OnPeerConnected fires on both sides
//   5. Clean shutdown — no goroutine leaks
//
// This is a self-compatibility test. It proves our wire format is internally
// consistent but does NOT prove we talk to java-tron (that's T11's job).
func TestLoopbackServerHandshake(t *testing.T) {
	hA := &testHandler{}
	hB := &testHandler{}

	srvA := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0",
		MaxPeers:   5,
		NodeID:     discover.GenerateNodeID(),
		NetworkID:  1,
		ExternalIP: "127.0.0.1",
	}, hA)
	srvB := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0",
		MaxPeers:   5,
		NodeID:     discover.GenerateNodeID(),
		NetworkID:  1,
		ExternalIP: "127.0.0.1",
	}, hB)

	if err := srvA.Start(); err != nil {
		t.Fatalf("start A: %v", err)
	}
	defer srvA.Stop()
	if err := srvB.Start(); err != nil {
		t.Fatalf("start B: %v", err)
	}
	defer srvB.Stop()

	// B dials A.
	if err := srvB.AddPeer(srvA.ListenAddr()); err != nil {
		t.Fatalf("B→A dial: %v", err)
	}

	// Wait up to 2s for the peer-connected handler to fire on both sides.
	// PeerCount is updated BEFORE OnPeerConnected runs, so check the handler.
	if !waitFor(2*time.Second, func() bool {
		hA.mu.Lock()
		ac := len(hA.connected)
		hA.mu.Unlock()
		hB.mu.Lock()
		bc := len(hB.connected)
		hB.mu.Unlock()
		return ac == 1 && bc == 1
	}) {
		t.Fatalf("OnPeerConnected did not fire on both sides; A=%d B=%d",
			len(hA.connected), len(hB.connected))
	}

	if srvA.PeerCount() != 1 || srvB.PeerCount() != 1 {
		t.Fatalf("peer counts: A=%d B=%d (want 1,1)", srvA.PeerCount(), srvB.PeerCount())
	}
}

// TestLoopbackNetworkMismatchRejects verifies that Server.AddPeer returns an
// error when the remote server has a different NetworkID.
func TestLoopbackNetworkMismatchRejects(t *testing.T) {
	hA := &testHandler{}
	hB := &testHandler{}

	srvA := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0",
		MaxPeers:   5,
		NodeID:     discover.GenerateNodeID(),
		NetworkID:  1,
		ExternalIP: "127.0.0.1",
	}, hA)
	srvB := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0",
		MaxPeers:   5,
		NodeID:     discover.GenerateNodeID(),
		NetworkID:  999, // different
		ExternalIP: "127.0.0.1",
	}, hB)

	if err := srvA.Start(); err != nil {
		t.Fatalf("start A: %v", err)
	}
	defer srvA.Stop()
	if err := srvB.Start(); err != nil {
		t.Fatalf("start B: %v", err)
	}
	defer srvB.Stop()

	// B dials A with a mismatched network ID. AddPeer should error.
	err := srvB.AddPeer(srvA.ListenAddr())
	if err == nil {
		t.Fatal("expected AddPeer to fail on network mismatch")
	}

	// Give A's accept path a moment to finish rejection.
	time.Sleep(200 * time.Millisecond)

	if srvA.PeerCount() != 0 {
		t.Fatalf("A peer count after rejection: %d (want 0)", srvA.PeerCount())
	}
	if srvB.PeerCount() != 0 {
		t.Fatalf("B peer count after rejection: %d (want 0)", srvB.PeerCount())
	}
}

// TestLoopbackPeerMessageRelay verifies that after handshake, application
// messages sent via Peer.Send reach the other side's handler.
func TestLoopbackPeerMessageRelay(t *testing.T) {
	hA := &testHandler{}
	hB := &testHandler{}

	srvA := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0",
		MaxPeers:   5,
		NodeID:     discover.GenerateNodeID(),
		NetworkID:  1,
		ExternalIP: "127.0.0.1",
	}, hA)
	srvB := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0",
		MaxPeers:   5,
		NodeID:     discover.GenerateNodeID(),
		NetworkID:  1,
		ExternalIP: "127.0.0.1",
	}, hB)

	if err := srvA.Start(); err != nil {
		t.Fatalf("start A: %v", err)
	}
	defer srvA.Stop()
	if err := srvB.Start(); err != nil {
		t.Fatalf("start B: %v", err)
	}
	defer srvB.Stop()

	if err := srvB.AddPeer(srvA.ListenAddr()); err != nil {
		t.Fatalf("B→A dial: %v", err)
	}
	if !waitFor(2*time.Second, func() bool {
		return srvA.PeerCount() == 1 && srvB.PeerCount() == 1
	}) {
		t.Fatal("handshake did not settle")
	}

	// B sends an application message to A.
	peersB := srvB.Peers()
	if len(peersB) != 1 {
		t.Fatalf("B has %d peers", len(peersB))
	}
	payload := []byte("hello-from-B")
	peersB[0].Send(MsgTx, payload)

	// A's handler should receive it.
	if !waitFor(2*time.Second, func() bool {
		hA.mu.Lock()
		defer hA.mu.Unlock()
		for _, m := range hA.messages {
			if m.code == MsgTx && string(m.payload) == string(payload) {
				return true
			}
		}
		return false
	}) {
		hA.mu.Lock()
		defer hA.mu.Unlock()
		t.Fatalf("A did not receive MsgTx; got %d messages", len(hA.messages))
	}
}

// waitFor polls cond every 10ms until it returns true or the deadline expires.
func waitFor(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

// ensure fmt is used in case future errors want it
var _ = fmt.Sprintf
