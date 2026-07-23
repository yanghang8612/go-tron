package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	"github.com/tronprotocol/go-tron/params"
)

func TestTronBackendNodeInfoProvider(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	genesis := &params.Genesis{Config: params.MainnetChainConfig}
	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}
	backend := NewTronBackend(bc, txpool.New())
	backend.SetSyncInfoProvider(func() tronapi.SyncInfo {
		return tronapi.SyncInfo{Paused: true, PeerCount: 16, PauseBlock: 1, PauseError: "test failure"}
	})

	info := backend.GetNodeInfo()
	if info.CurrentBlock != 0 || info.LastInsertTime <= 0 {
		t.Fatalf("base node info = %+v", info)
	}
	if info.Sync == nil || !info.Sync.Paused || info.Sync.PeerCount != 16 || info.Sync.PauseBlock != 1 || info.Sync.PauseError != "test failure" {
		t.Fatalf("sync node info = %+v", info.Sync)
	}
}
