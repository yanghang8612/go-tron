package core

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
)

func newDirectHistoryIntegrationChain(t *testing.T, async, rewindable bool) (*BlockChain, ethdb.KeyValueStore, ethdb.KeyValueStore) {
	t.Helper()
	dir := t.TempDir()
	open := func(name string) ethdb.KeyValueStore {
		options := rawdb.DefaultPebbleOptions()
		options.MemTableSizeBytes = 1 << 20
		db, err := rawdb.NewPebbleDBWithOptions(filepath.Join(dir, name), 16, 16, options)
		if err != nil {
			t.Fatal(err)
		}
		return db
	}
	hot, history := open("hot"), open("history")
	store := rawdb.NewDirectHistoryStore(hot, history)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := rawdb.EnsureDirectHistoryPair(hot, history, true); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.MarkDirectHistoryLayout(hot); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.MarkDirectHistoryLayout(history); err != nil {
		t.Fatal(err)
	}
	cfg := cloneMainnetChainConfig()
	cfg.HistoryEnabled = true
	witness := testInsertAddr(1)
	witnesses := []params.GenesisWitness{{Address: witness, VoteCount: 1, URL: "test"}}
	if rewindable {
		witnesses = append(witnesses, params.GenesisWitness{Address: testInsertAddr(20), VoteCount: 1, URL: "sr2"}, params.GenesisWitness{Address: testInsertAddr(21), VoteCount: 1, URL: "sr3"})
	}
	genesis := &params.Genesis{Config: cfg, Accounts: []params.GenesisAccount{{Address: witness, Balance: 99_000_000_000_000_000}}, Witnesses: witnesses, DynamicProperties: map[string]int64{"next_maintenance_time": 1<<62 - 1}}
	if _, _, err := SetupGenesisBlock(store, genesis); err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(store, state.NewDatabase(rawdb.WrapKeyValueStore(store)), cfg)
	if err != nil {
		t.Fatal(err)
	}
	bc.SetAsyncCommit(async)
	t.Cleanup(func() { _ = bc.Close() })
	return bc, hot, history
}

func directIntegrationBlocks(t *testing.T, parent *types.Block, n int, offset, unit int64) []*types.Block {
	t.Helper()
	blocks := make([]*types.Block, 0, n)
	for i := int64(1); i <= int64(n); i++ {
		block := buildTransferBlock(t, i, i*3000+offset, parent.Hash(), testInsertAddr(1), i*unit)
		blocks = append(blocks, block)
		parent = block
	}
	return blocks
}
func checkDirectIntegrationHistory(t *testing.T, bc *BlockChain, hot, history ethdb.KeyValueStore, blocks []*types.Block, unit int64) {
	t.Helper()
	bc.WaitForCommitSettled()
	bc.WaitForFlushSettled()
	if err := bc.buffer.Flush(bc.db); err != nil {
		t.Fatal(err)
	}
	if manager := bc.historyStaging.Load(); manager != nil {
		t.Fatal("direct history installed legacy staging manager")
	}
	hotIterator := hot.NewIterator(nil, nil)
	for hotIterator.Next() {
		if rawdb.IsDirectHistoryKey(hotIterator.Key()) {
			t.Errorf("history key remains in hot: %q", hotIterator.Key())
		}
	}
	if err := hotIterator.Error(); err != nil {
		t.Fatal(err)
	}
	hotIterator.Release()
	backend := &TronBackend{chain: bc}
	for _, block := range blocks {
		row, present, err := rawdb.ReadStateTxRange(history, block.Number())
		if err != nil || !present || row.BlockHash != block.Hash() {
			t.Fatalf("physical history tx range block %d: %+v,%v,%v", block.Number(), row, present, err)
		}
		var changes int
		if err := rawdb.IterateStateDomainChanges(bc.db, block.Number(), func(*rawdb.StateDomainChange) (bool, error) { changes++; return true, nil }); err != nil || changes == 0 {
			t.Fatalf("history changes block %d: count=%d err=%v", block.Number(), changes, err)
		}
		balance, err := backend.GetBalanceAt(testInsertAddr(2), block.Number())
		want := unit * int64(block.Number()) * int64(block.Number()+1) / 2
		if err != nil || balance != want {
			t.Fatalf("archive balance block %d=%d,%v want %d", block.Number(), balance, err, want)
		}
	}
	// The stage singleton moves with its postings, while ordinary execution
	// metadata remains accessible in hot.
	if err := bc.EnsureStateHistoryIndexStage(); err != nil {
		t.Fatal(err)
	}
	if _, present, err := rawdb.ReadStageProgressRow(history, rawdb.StageStateHistoryIndex); err != nil || !present {
		t.Fatalf("history index progress present=%v err=%v", present, err)
	}
	if _, present, err := rawdb.ReadStageProgressRow(hot, rawdb.StageFinish); err != nil || !present {
		t.Fatalf("hot finish progress present=%v err=%v", present, err)
	}
}

func TestDirectHistoryInsertBlocksAndArchiveAfterFlush(t *testing.T) {
	for _, async := range []bool{false, true} {
		name := "sync"
		if async {
			name = "async"
		}
		t.Run(name, func(t *testing.T) {
			bc, hot, history := newDirectHistoryIntegrationChain(t, async, false)
			blocks := directIntegrationBlocks(t, bc.genesisBlock, 6, 0, 1000)
			if err := bc.InsertBlocks(blocks); err != nil {
				t.Fatal(err)
			}
			checkDirectIntegrationHistory(t, bc, hot, history, blocks, 1000)
			if bc.CurrentBlock().Hash() != blocks[len(blocks)-1].Hash() {
				t.Fatal("wrong imported head")
			}
		})
	}
}

func TestDirectHistoryAsyncReorgRetainsCanonicalArchive(t *testing.T) {
	bc, hot, history := newDirectHistoryIntegrationChain(t, true, true)
	orphan := directIntegrationBlocks(t, bc.genesisBlock, 4, 0, 999)
	canonical := directIntegrationBlocks(t, bc.genesisBlock, 5, 1, 1000)
	if err := bc.InsertBlocks(orphan); err != nil {
		t.Fatal(err)
	}
	if err := bc.InsertBlocks(canonical); err != nil {
		t.Fatal(err)
	}
	if bc.CurrentBlock().Hash() != canonical[len(canonical)-1].Hash() {
		t.Fatal("fork did not switch to canonical branch")
	}
	checkDirectIntegrationHistory(t, bc, hot, history, canonical, 1000)
}

func TestDirectHistoryAsyncFoldFailureDoesNotPersistSpeculativeHistory(t *testing.T) {
	bc, _, history := newDirectHistoryIntegrationChain(t, true, true)
	blocks := directIntegrationBlocks(t, bc.genesisBlock, 6, 0, 1000)
	const failAt = uint64(4)
	sentinel := errors.New("direct history fold failure")
	SetCommitFoldHookForTest(func(number uint64) error {
		if number == failAt {
			return sentinel
		}
		return nil
	})
	defer SetCommitFoldHookForTest(nil)
	if err := bc.InsertBlocks(blocks); !errors.Is(err, sentinel) {
		t.Fatalf("fold error=%v", err)
	}
	bc.WaitForCommitSettled()
	bc.WaitForFlushSettled()
	if bc.CurrentBlock().Number() != failAt-1 {
		t.Fatalf("head=%d want %d", bc.CurrentBlock().Number(), failAt-1)
	}
	if err := bc.buffer.Flush(bc.db); err != nil {
		t.Fatal(err)
	}
	for number := uint64(1); number <= 6; number++ {
		row, present, err := rawdb.ReadStateTxRange(history, number)
		if err != nil {
			t.Fatal(err)
		}
		if number < failAt {
			if !present || row.BlockHash != blocks[number-1].Hash() {
				t.Fatalf("committed history missing at block %d", number)
			}
		} else if present {
			t.Fatalf("speculative history persisted for block %d", number)
		}
	}
	if rawdb.ReadBlockStateRoot(bc.chaindb, blocks[failAt-1].Hash()) != (tcommon.Hash{}) {
		t.Fatal("failed block state root persisted")
	}
}
