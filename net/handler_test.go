package net

import (
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
