package net

import (
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
)

func makeChainWithBlocks(t *testing.T, numBlocks int) *core.BlockChain {
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

	for i := 1; i <= numBlocks; i++ {
		parent := bc.CurrentBlock()
		block := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:     int64(i),
					Timestamp:  int64(i) * 3000,
					ParentHash: parent.Hash().Bytes(),
				},
			},
		})
		if err := bc.InsertBlockWithoutVerify(block); err != nil {
			t.Fatal(err)
		}
	}
	return bc
}

func TestTwoNodeSync(t *testing.T) {
	// Node A has 20 blocks, Node B has 0 (only genesis)
	bcA := makeChainWithBlocks(t, 20)
	bcB := makeTestChain(t) // genesis only

	poolA := txpool.New()
	poolB := txpool.New()

	// Create handlers
	broadcasterA := NewBroadcastService(nil)
	broadcasterB := NewBroadcastService(nil)

	handlerA := NewTronHandler(bcA, poolA, broadcasterA)
	handlerB := NewTronHandler(bcB, poolB, broadcasterB)

	syncA := NewSyncService(bcA, handlerA)
	syncB := NewSyncService(bcB, handlerB)
	handlerA.SetSyncService(syncA)
	handlerB.SetSyncService(syncB)

	srvA := p2p.NewServer(p2p.ServerConfig{ListenAddr: "127.0.0.1:0", MaxPeers: 5}, handlerA)
	srvB := p2p.NewServer(p2p.ServerConfig{ListenAddr: "127.0.0.1:0", MaxPeers: 5}, handlerB)
	handlerA.SetServer(srvA)
	handlerB.SetServer(srvB)
	broadcasterA.SetPeersFunc(handlerA.HandshakedPeers)
	broadcasterB.SetPeersFunc(handlerB.HandshakedPeers)

	srvA.Start()
	defer srvA.Stop()
	srvB.Start()
	defer srvB.Stop()

	// B connects to A — should trigger sync because A has block #20
	srvB.AddPeer(srvA.ListenAddr())

	// Wait for sync to complete (up to 5 seconds)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if bcB.CurrentBlock().Number() >= 20 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if bcB.CurrentBlock().Number() != 20 {
		t.Fatalf("Node B should have synced to block #20, got #%d", bcB.CurrentBlock().Number())
	}
}
