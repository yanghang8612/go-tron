package core

import (
	"context"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/params"
)

func TestHistoryStagingCanonicalReceiptSharesPebbleBlockCommit(t *testing.T) {
	for _, async := range []bool{false, true} {
		name := "sync"
		if async {
			name = "async"
		}
		t.Run(name, func(t *testing.T) {
			hot, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			stage, err := rawdb.NewHistoryStagingPebbleDB(t.TempDir(), 16, 16, false)
			if err != nil {
				_ = hot.Close()
				t.Fatal(err)
			}
			cfg := cloneMainnetChainConfig()
			cfg.HistoryEnabled = true
			genesis := &params.Genesis{
				Config:            cfg,
				Accounts:          []params.GenesisAccount{{Address: testInsertAddr(1), Balance: 99_000_000_000_000_000}},
				DynamicProperties: map[string]int64{"next_maintenance_time": 1<<62 - 1},
			}
			_, genesisHash, err := SetupGenesisBlock(hot, genesis)
			if err != nil {
				_ = stage.Close()
				_ = hot.Close()
				t.Fatal(err)
			}
			bc, err := NewBlockChain(hot, state.NewDatabase(rawdb.WrapKeyValueStore(hot)), cfg)
			if err != nil {
				_ = stage.Close()
				_ = hot.Close()
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = bc.Close(); _ = stage.Close(); _ = hot.Close() })
			identity := rawdb.HistoryStagingIdentity{
				Version: rawdb.HistoryStagingFormatVersion, GenesisHash: genesisHash,
				NetworkID: 1, SourceID: [32]byte{1}, TargetID: [32]byte{2},
			}
			manager, err := rawdb.NewHistoryStagingManager(hot, stage, identity)
			if err != nil {
				t.Fatal(err)
			}
			if err := manager.Initialize(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := bc.SetHistoryStagingManager(manager); err != nil {
				t.Fatal(err)
			}
			if async {
				bc.SetAsyncCommit(true)
			}
			block := buildTransferBlock(t, 1, 3000, genesisHash, testInsertAddr(1), 5_000_000)
			if err := bc.InsertBlock(block); err != nil {
				t.Fatal(err)
			}
			bc.WaitForCommitSettled()
			bc.WaitForFlushSettled()
			if err := bc.buffer.Flush(bc.db); err != nil {
				t.Fatal(err)
			}
			complete, present, err := rawdb.ReadHistoryStagingBlockComplete(hot, 1, 1)
			if err != nil || !present || complete.BlockHash != block.Hash() || complete.PhysicalRows == 0 {
				t.Fatalf("canonical receipt = %+v, present=%t, err=%v", complete, present, err)
			}
			view, release, err := rawdb.AcquireStateHistoryReadView(hot)
			if err != nil {
				t.Fatal(err)
			}
			computed, buildErr := rawdb.BuildHistoryStagingBlockComplete(context.Background(), view, 1, 1, block.Hash())
			closeErr := release()
			if buildErr != nil || closeErr != nil || computed != complete {
				t.Fatalf("receipt differs from persisted exact rows: got %+v, want %+v, buildErr=%v, closeErr=%v", computed, complete, buildErr, closeErr)
			}
		})
	}
}

func TestHistoryStagingRestartSyncPublishesNewEpochOnlyAfterReplay(t *testing.T) {
	hot, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	stage, err := rawdb.NewHistoryStagingPebbleDB(t.TempDir(), 16, 16, false)
	if err != nil {
		_ = hot.Close()
		t.Fatal(err)
	}
	defer hot.Close()
	defer stage.Close()
	cfg := cloneMainnetChainConfig()
	cfg.HistoryEnabled = true
	genesis := &params.Genesis{Config: cfg, Accounts: []params.GenesisAccount{{Address: testInsertAddr(1), Balance: 99_000_000_000_000_000}}, DynamicProperties: map[string]int64{"next_maintenance_time": 1<<62 - 1}}
	_, genesisHash, err := SetupGenesisBlock(hot, genesis)
	if err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(hot, state.NewDatabase(rawdb.WrapKeyValueStore(hot)), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()
	manager, err := rawdb.NewHistoryStagingManager(hot, stage, rawdb.HistoryStagingIdentity{Version: rawdb.HistoryStagingFormatVersion, GenesisHash: genesisHash, NetworkID: 1, SourceID: [32]byte{1}, TargetID: [32]byte{2}})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := bc.SetHistoryStagingManager(manager); err != nil {
		t.Fatal(err)
	}
	parent := genesisHash
	for height := uint64(1); height <= 3; height++ {
		block := buildTransferBlock(t, int64(height), int64(height)*3000, parent, testInsertAddr(1), 5_000_000)
		if err := bc.InsertBlock(block); err != nil {
			t.Fatal(err)
		}
		parent = block.Hash()
	}
	bc.WaitForFlushSettled()
	if err := bc.buffer.Flush(bc.db); err != nil {
		t.Fatal(err)
	}
	// A missing completeness receipt forces the conservative RESETTING path.
	if err := rawdb.DeleteHistoryStagingBlockComplete(hot, 1, 3); err != nil {
		t.Fatal(err)
	}
	if err := bc.RestartSyncFromHeight(2, genesis, nil, nil); err != nil {
		t.Fatal(err)
	}
	intent, present, err := manager.ReadResetIntent()
	if err != nil || !present || !intent.Complete || intent.NewEpoch != 2 || intent.ReadyThrough != 2 {
		t.Fatalf("reset intent = %+v, present=%t, err=%v", intent, present, err)
	}
	if epoch, err := manager.CurrentEpoch(); err != nil || epoch != 2 {
		t.Fatalf("current epoch = %d, err=%v", epoch, err)
	}
	for height := uint64(1); height <= 2; height++ {
		if _, present, err := rawdb.ReadHistoryStagingBlockComplete(hot, 2, height); err != nil || !present {
			t.Fatalf("replay receipt %d present=%t err=%v", height, present, err)
		}
	}
	// Model a crash after RESETTING has been synced and the materialized head
	// has been erased. Startup recovery must accept a target above that head.
	digest, err := preflightRestartSyncBlocks(bc.chaindb, 2, bc.CurrentBlock().Hash())
	if err != nil {
		t.Fatal(err)
	}
	pending := rawdb.HistoryStagingResetIntent{Version: rawdb.HistoryStagingFormatVersion, OldEpoch: 2, NewEpoch: 3, TargetHeight: 2, TargetHash: bc.CurrentBlock().Hash(), BlockDigest: digest}
	if err := manager.BeginResetIntent(context.Background(), pending); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.ResetMutableState(hot); err != nil {
		t.Fatal(err)
	}
	if err := bc.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := PrepareHistoryStagingResetStartup(context.Background(), hot, nil, nil, manager, genesis)
	if err != nil || recovered == nil || recovered.NewEpoch != 3 {
		t.Fatalf("pre-BC recovery = %+v, err=%v", recovered, err)
	}
	bc2, err := NewBlockChain(hot, state.NewDatabase(rawdb.WrapKeyValueStore(hot)), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer bc2.Close()
	if err := bc2.SetHistoryStagingManager(manager); err != nil {
		t.Fatal(err)
	}
	if err := bc2.RestartSyncFromHeight(recovered.TargetHeight, genesis, nil, nil); err != nil {
		t.Fatal(err)
	}
	if epoch, err := manager.CurrentEpoch(); err != nil || epoch != 3 {
		t.Fatalf("recovered epoch = %d, err=%v", epoch, err)
	}
}
