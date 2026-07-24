package net

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/p2p"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func makeTestChain(t testing.TB) *core.BlockChain {
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

func TestBuildHelloUsesSolidifiedBlockInsteadOfHead(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	witnesses := []tcommon.Address{
		{0x41, 1},
		{0x41, 2},
		{0x41, 3},
		{0x41, 4},
	}
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 1<<62 - 1,
		},
	}
	for _, addr := range witnesses {
		genesis.Accounts = append(genesis.Accounts, params.GenesisAccount{
			Address: addr,
			Balance: 1_000_000,
		})
		genesis.Witnesses = append(genesis.Witnesses, params.GenesisWitness{
			Address:   addr,
			VoteCount: 1,
			URL:       "test",
		})
	}
	if _, _, err := core.SetupGenesisBlock(diskdb, genesis); err != nil {
		t.Fatal(err)
	}
	bc, err := core.NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()

	for i := 1; i <= 3; i++ {
		parent := bc.CurrentBlock()
		block := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:         int64(i),
					Timestamp:      int64(i) * 3000,
					ParentHash:     parent.Hash().Bytes(),
					WitnessAddress: witnesses[i-1].Bytes(),
				},
			},
		})
		if err := bc.InsertBlock(block); err != nil {
			t.Fatalf("insert block %d: %v", i, err)
		}
	}

	if got := bc.DynProps().LatestSolidifiedBlockNum(); got != 1 {
		t.Fatalf("test setup solidified block = %d, want 1", got)
	}

	handler := NewTronHandler(bc, txpool.New(), nil)
	hello := handler.buildHello()
	solidID, ok := bc.BlockIDByNumber(1)
	if !ok {
		t.Fatal("solidified block #1 is unavailable")
	}
	if got := hello.GetSolidBlockId().GetNumber(); got != int64(solidID.Num) {
		t.Fatalf("hello solid block number = %d, want %d", got, solidID.Num)
	}
	if got := hello.GetSolidBlockId().GetHash(); !bytes.Equal(got, solidID.Hash[:]) {
		t.Fatalf("hello solid block hash = %x, want %x", got, solidID.Hash)
	}
	if got := hello.GetHeadBlockId().GetNumber(); got != 3 {
		t.Fatalf("hello head block number = %d, want 3", got)
	}
	if bytes.Equal(hello.GetSolidBlockId().GetHash(), hello.GetHeadBlockId().GetHash()) {
		t.Fatal("hello solid block must not be the unsolidified head")
	}
}

func TestHandleHelloCachesOnlyPeerCoveringNextBlock(t *testing.T) {
	bc := makeTestChain(t)
	cachePath := filepath.Join(t.TempDir(), "p2p-peers")
	handler := NewTronHandler(bc, txpool.New(), nil)
	srv := p2p.NewServer(p2p.ServerConfig{
		ListenAddr:    "127.0.0.1:0",
		MaxPeers:      5,
		PeerCachePath: cachePath,
	}, handler)
	handler.SetServer(srv)

	peer := p2p.NewPeer(nil, "198.51.100.7:18888", false, nil)
	handler.mu.Lock()
	handler.peers[peer.ID()] = &peerState{peer: peer, rl: p2p.NewRateLimiter()}
	handler.mu.Unlock()

	hello := handler.buildHello()
	hello.HeadBlockId.Number = 10
	hello.LowestBlockNum = 2 // local next block is 1, so this peer cannot serve it
	payload, err := proto.Marshal(hello)
	if err != nil {
		t.Fatal(err)
	}
	handler.handleHello(peer, payload)
	if _, err := os.Stat(cachePath); !os.IsNotExist(err) {
		t.Fatalf("unusable peer must not populate cache: %v", err)
	}

	hello.LowestBlockNum = 1
	payload, err = proto.Marshal(hello)
	if err != nil {
		t.Fatal(err)
	}
	handler.handleHello(peer, payload)
	data, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(data), peer.ID()+"\n"; got != want {
		t.Fatalf("cached peer = %q, want %q", got, want)
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
