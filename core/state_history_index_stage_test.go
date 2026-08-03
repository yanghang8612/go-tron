package core

import (
	"errors"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
)

func TestBlockChainSyncInsertDefersStateHistoryIndexUntilSolidifiedStage(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	cfg := cloneMainnetChainConfig()
	cfg.HistoryEnabled = true
	genesis := &params.Genesis{
		Config: cfg,
		Accounts: []params.GenesisAccount{{
			Address: testInsertAddr(1),
			Balance: 99_000_000_000_000_000,
		}},
		DynamicProperties: map[string]int64{"next_maintenance_time": 1<<62 - 1},
	}
	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(diskdb, state.NewDatabase(diskdb), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer bc.Close()
	bc.SetStateHistoryIndexETLOptions(etl.Options{TempDir: t.TempDir(), BufferLimit: 1, BatchSize: 1})

	block := buildTransferBlock(t, 1, 3000, genesisHash, testInsertAddr(1), 5_000_000)
	if err := bc.InsertSyncBlocksWithStageHook([]*types.Block{block}, nil); err != nil {
		t.Fatalf("InsertSyncBlocksWithStageHook: %v", err)
	}
	var changes uint64
	if err := rawdb.IterateStateDomainChanges(bc.buffer, block.Number(), func(*rawdb.StateDomainChange) (bool, error) {
		changes++
		return true, nil
	}); err != nil || changes == 0 {
		t.Fatalf("authoritative changes after sync insert = %d err=%v, want non-zero", changes, err)
	}
	var indexed []uint64
	if err := rawdb.IterateStateAccountLatestChangeBlocks(bc.buffer, testInsertAddr(1), func(blockNum uint64) (bool, error) {
		indexed = append(indexed, blockNum)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(indexed) != 0 {
		t.Fatalf("inverse index before stage = %v, want empty", indexed)
	}

	// Archive reads must remain complete while the solidified tail has only
	// authoritative changesets and no inverse row.
	reader := state.NewPersistentHistoryReader(bc.buffer, nil, block.Number())
	reader.SetHotHistoryBlockRange(0, block.Number())
	pre, err := reader.AccountAt(testInsertAddr(1), 0)
	if err != nil {
		t.Fatalf("AccountAt before history index stage: %v", err)
	}
	if pre == nil || pre.Balance() != 99_000_000_000_000_000 {
		if pre == nil {
			t.Fatal("AccountAt before stage = nil, want genesis sender")
		}
		t.Fatalf("AccountAt before stage balance = %d, want genesis sender balance", pre.Balance())
	}
	block2 := buildTransferBlock(t, 2, 6000, block.Hash(), testInsertAddr(1), 1)
	if err := bc.InsertBlock(block2); err != nil {
		t.Fatalf("ordinary InsertBlock after deferred gap: %v", err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.buffer, rawdb.StageStateHistoryIndex); err != nil || !ok || row.BlockNum != 0 || row.BlockHash != genesisHash {
		t.Fatalf("ordinary inline import jumped deferred history-index gap: row=%+v ok=%v err=%v", row, ok, err)
	}
	if result, err := bc.AdvanceStateHistoryIndexStage(1); err != nil || result.Advanced {
		t.Fatalf("history index advanced past unsolidified boundary: result=%+v err=%v", result, err)
	}
	dynProps := bc.cachedDynProps()
	dynProps.SetLatestSolidifiedBlockNum(int64(block.Number()))
	bc.storeDynPropsCache(dynProps)
	if result, err := bc.AdvanceStateHistoryIndexStageBatchedInterruptible(2, 10, nil); err != nil || result.Advanced {
		t.Fatalf("history index ignored minimum batch: result=%+v err=%v", result, err)
	}

	if result, err := bc.AdvanceStateHistoryIndexStageInterruptible(1, func() bool { return true }); !errors.Is(err, rawdb.ErrStateHistoryIndexRebuildInterrupted) || result.Advanced {
		t.Fatalf("interrupted history index result=%+v err=%v", result, err)
	}
	result, err := bc.AdvanceStateHistoryIndexStage(1)
	if err != nil {
		t.Fatalf("AdvanceStateHistoryIndexStage: %v", err)
	}
	if !result.Advanced || result.Rebuilt == nil || result.Rebuilt.BlocksScanned != 1 || result.Rebuilt.ChangesScanned != changes {
		t.Fatalf("history index result = %+v, want one block/%d changes", result, changes)
	}
	if result.Rebuilt.ETL.SpilledRuns == 0 || result.Rebuilt.ETL.BatchWrites == 0 {
		t.Fatalf("history index ETL stats = %+v, want spill and batch writes", result.Rebuilt.ETL)
	}
	indexed = nil
	if err := rawdb.IterateStateAccountLatestChangeBlocks(bc.buffer, testInsertAddr(1), func(blockNum uint64) (bool, error) {
		indexed = append(indexed, blockNum)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(indexed) != 2 || indexed[0] != block.Number() || indexed[1] != block2.Number() {
		t.Fatalf("inverse index after stage = %v, want [%d %d]", indexed, block.Number(), block2.Number())
	}
	if row, ok, err := rawdb.ReadStageProgressRow(bc.DB(), rawdb.StageStateHistoryIndex); err != nil || !ok || row.BlockNum != block.Number() || !row.HasBlockHash || row.BlockHash != block.Hash() {
		t.Fatalf("StateHistoryIndex progress = %+v ok=%v err=%v", row, ok, err)
	}
}
