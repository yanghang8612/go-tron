package net

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/p2p"
	"github.com/tronprotocol/go-tron/params"
	"google.golang.org/protobuf/proto"
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

func TestHandleBlockDropsBroadcastWhileSyncPaused(t *testing.T) {
	bc := makeTestChain(t)
	handler := NewTronHandler(bc, txpool.New(), nil)
	syncSvc := NewSyncService(bc, handler)
	handler.SetSyncService(syncSvc)

	syncSvc.pause.Enter(0, fmt.Errorf("test pause"))

	block := stubBlock(1, bc.CurrentBlock().Hash())
	payload, err := proto.Marshal(block.Proto())
	if err != nil {
		t.Fatalf("marshal block: %v", err)
	}

	handler.handleBlock(p2p.NewPeer(nil, "paused-peer", false, nil), payload)

	if got := bc.CurrentBlock().Number(); got != 0 {
		t.Fatalf("paused sync should drop block broadcasts; head=%d, want 0", got)
	}
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

func TestBuildHelloIncludesFromEndpoint(t *testing.T) {
	bc := makeTestChain(t)
	handler := NewTronHandler(bc, txpool.New(), nil)
	nodeID := bytes.Repeat([]byte{0x42}, 64)
	srv := p2p.NewServer(p2p.ServerConfig{
		ListenAddr: "127.0.0.1:0",
		MaxPeers:   5,
		NodeID:     nodeID,
		NetworkID:  params.NileNetworkID,
		ExternalIP: "203.0.113.9",
		Port:       18888,
	}, handler)
	handler.SetServer(srv)

	hello := handler.buildHello()
	if hello.From == nil {
		t.Fatal("hello.from is nil")
	}
	if !bytes.Equal(hello.From.NodeId, nodeID) {
		t.Fatalf("hello.from.nodeId mismatch")
	}
	if got := string(hello.From.Address); got != "203.0.113.9" {
		t.Fatalf("hello.from.address = %q", got)
	}
	if hello.From.Port != 18888 {
		t.Fatalf("hello.from.port = %d", hello.From.Port)
	}
	if hello.Version != params.NileNetworkID {
		t.Fatalf("hello.version = %d, want %d", hello.Version, params.NileNetworkID)
	}
}

func TestPbftMsgDispatch(t *testing.T) {
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

	if h2.HandshakedPeerCount() != 1 {
		t.Fatalf("expected 1 handshaked peer, got %d", h2.HandshakedPeerCount())
	}

	// Send PBFT messages from h2 to h1 — stubs must dispatch without panic.
	peers2 := h2.HandshakedPeers()
	peers2[0].Send(p2p.MsgPbftMsg, []byte{})
	peers2[0].Send(p2p.MsgPbftCommitMsg, []byte{})

	time.Sleep(50 * time.Millisecond)
	// success = no panic, no disconnect
	if h1.HandshakedPeerCount() != 1 {
		t.Fatalf("peer disconnected after PBFT messages")
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
