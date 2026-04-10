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
