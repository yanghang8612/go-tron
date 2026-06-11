package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

// cloneMainnetChainConfig returns a shallow copy of MainnetChainConfig so
// tests can flip per-chain flags (e.g. HistoryEnabled) without mutating the
// shared package-level pointer.
func cloneMainnetChainConfig() *params.ChainConfig {
	cfg := *params.MainnetChainConfig
	return &cfg
}

// buildTransferBlock constructs a single-tx block transferring `amount`
// from genesis-funded testInsertAddr(1) to testInsertAddr(2).
func buildTransferBlock(t *testing.T, number int64, ts int64, parentHash tcommon.Hash, witness tcommon.Address, amount int64) *types.Block {
	t.Helper()
	tc := &contractpb.TransferContract{
		OwnerAddress: testInsertAddr(1).Bytes(),
		ToAddress:    testInsertAddr(2).Bytes(),
		Amount:       amount,
	}
	param, _ := anypb.New(tc)
	txPB := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TransferContract,
				Parameter: param,
			}},
			RefBlockBytes: []byte{0, 0},
			Expiration:    ts + 1000,
		},
	}
	tx := types.NewTransactionFromPB(txPB)
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         number,
				Timestamp:      ts,
				ParentHash:     parentHash.Bytes(),
				WitnessAddress: witness.Bytes(),
			},
		},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})
}

// TestApplyBlock_HistoryEnabledRoutesToBuffer asserts that with
// HistoryEnabled=true a block import lands Erigon-style temporal domain rows
// in the chain's KV store (via bc.buffer flush).
func TestApplyBlock_HistoryEnabledRoutesToBuffer(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	cfg := cloneMainnetChainConfig()
	cfg.HistoryEnabled = true

	genesis := &params.Genesis{
		Config:    cfg,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testInsertAddr(1), Balance: 99_000_000_000_000_000},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 1<<62 - 1, // far future — no maintenance noise
		},
	}
	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}
	sdb := state.NewDatabase(diskdb)
	bc, err := NewBlockChain(diskdb, sdb, cfg)
	if err != nil {
		t.Fatal(err)
	}

	block1 := buildTransferBlock(t, 1, 3000, genesisHash, testInsertAddr(1), 5_000_000)
	if err := bc.InsertBlock(block1); err != nil {
		t.Fatalf("InsertBlock: %v", err)
	}

	// Read via BufferedDB so we see rows still pending in higher buffer
	// layers (single-witness test: solidified == head, so flush happens,
	// but reading through the buffer is correct either way).
	bdb := bc.BufferedDB()

	txRange, ok, err := rawdb.ReadStateTxRange(bdb, 1)
	if err != nil || !ok {
		t.Fatalf("StateTxRange missing: ok=%v err=%v", ok, err)
	}
	parentEnd, err := rawdb.StateTxNumAtBlockEnd(bdb, 0)
	if err != nil {
		t.Fatal(err)
	}
	wantBegin, wantEnd, err := rawdb.NextStateTxRange(parentEnd, uint64(len(block1.Transactions())))
	if err != nil {
		t.Fatal(err)
	}
	if txRange.BeginTxNum != wantBegin || txRange.EndTxNum != wantEnd {
		t.Fatalf("StateTxRange = %+v", txRange)
	}
	if txRange.BlockHash != block1.Hash() {
		t.Fatalf("StateTxRange block hash = %x, want %x", txRange.BlockHash, block1.Hash())
	}
	var sawDynProp bool
	touchedAccounts := make(map[tcommon.Address]bool)
	if err := rawdb.IterateStateDomainChanges(bc.buffer, 1, func(change *rawdb.StateDomainChange) (bool, error) {
		if change.BlockHash != block1.Hash() {
			t.Fatalf("StateDomainChange block hash = %x, want %x", change.BlockHash, block1.Hash())
		}
		if change.FlatDomain == rawdb.StateFlatDomainAccountLatest {
			touchedAccounts[change.Owner] = true
		}
		if change.Owner == tcommon.SystemAccountAddress && change.Domain == kvdomains.SystemDynamicProperty {
			sawDynProp = true
		}
		return true, nil
	}); err != nil {
		t.Fatalf("iterate state domain changes: %v", err)
	}
	if !touchedAccounts[testInsertAddr(1)] {
		t.Fatal("state domain change set did not capture sender account latest")
	}
	if !touchedAccounts[testInsertAddr(2)] {
		t.Fatal("state domain change set did not capture receiver account latest")
	}
	if !sawDynProp {
		t.Fatal("state domain change set did not capture rooted dynamic properties")
	}

	reader := state.NewPersistentHistoryReader(bc.buffer, nil, bc.CurrentBlock().Number())
	senderPre, err := reader.AccountAt(testInsertAddr(1), 0)
	if err != nil {
		t.Fatalf("AccountAt(sender, 0): %v", err)
	}
	if senderPre == nil || senderPre.Balance() != 99_000_000_000_000_000 {
		t.Fatalf("AccountAt(sender, 0).Balance = %v, want genesis balance", senderPre)
	}
	receiverPre, err := reader.AccountAt(testInsertAddr(2), 0)
	if err != nil {
		t.Fatalf("AccountAt(receiver, 0): %v", err)
	}
	if receiverPre != nil {
		t.Fatalf("AccountAt(receiver, 0) = %v, want nil", receiverPre)
	}
	receiverPost, err := reader.AccountAt(testInsertAddr(2), 1)
	if err != nil {
		t.Fatalf("AccountAt(receiver, 1): %v", err)
	}
	if receiverPost == nil || receiverPost.Balance() != 5_000_000 {
		t.Fatalf("AccountAt(receiver, 1).Balance = %v, want 5000000", receiverPost)
	}
}

// TestApplyBlock_HistoryDisabledNoRows asserts that with the default config
// (HistoryEnabled=false) no temporal domain rows are written for an inserted
// block.
func TestApplyBlock_HistoryDisabledNoRows(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	// Default config: HistoryEnabled defaults to false (not set explicitly).
	cfg := cloneMainnetChainConfig()
	if cfg.HistoryEnabled {
		t.Fatal("test precondition: default HistoryEnabled must be false")
	}

	genesis := &params.Genesis{
		Config:    cfg,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testInsertAddr(1), Balance: 99_000_000_000_000_000},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 1<<62 - 1,
		},
	}
	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}
	sdb := state.NewDatabase(diskdb)
	bc, err := NewBlockChain(diskdb, sdb, cfg)
	if err != nil {
		t.Fatal(err)
	}

	block1 := buildTransferBlock(t, 1, 3000, genesisHash, testInsertAddr(1), 1_000_000)
	if err := bc.InsertBlock(block1); err != nil {
		t.Fatalf("InsertBlock: %v", err)
	}

	if _, ok, err := rawdb.ReadStateTxRange(bc.buffer, 1); err != nil || ok {
		t.Errorf("StateTxRange leaked despite HistoryEnabled=false: ok=%v err=%v", ok, err)
	}
	var domainChanges int
	if err := rawdb.IterateStateDomainChanges(bc.buffer, 1, func(change *rawdb.StateDomainChange) (bool, error) {
		domainChanges++
		return true, nil
	}); err != nil {
		t.Fatalf("iterate state domain changes: %v", err)
	}
	if domainChanges != 0 {
		t.Errorf("StateDomainChange rows leaked despite HistoryEnabled=false: %d", domainChanges)
	}
}

// TestApplyBlock_HistoryReorgDropsOrphan inserts a canonical chain A then a
// longer competing chain B that triggers switchFork; asserts that canonical B
// temporal-domain rows are present after the rewind while the orphan A1 layer
// is no longer pending.
func TestApplyBlock_HistoryReorgDropsOrphan(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	cfg := cloneMainnetChainConfig()
	cfg.HistoryEnabled = true
	witnessAddr := testInsertAddr(1)

	genesis := &params.Genesis{
		Config:    cfg,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: witnessAddr, Balance: 99_000_000_000_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1, URL: "test"},
			{Address: testInsertAddr(20), VoteCount: 1, URL: "sr2"},
			{Address: testInsertAddr(21), VoteCount: 1, URL: "sr3"},
		},
		DynamicProperties: map[string]int64{
			// Three witnesses → solidified stays at 0 (nums[floor(0.3*N)] = nums[0]),
			// so all buffer layers stay in memory and switchFork can rewind them.
			"next_maintenance_time": 1<<62 - 1,
		},
	}
	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}
	sdb := state.NewDatabase(diskdb)
	bc, err := NewBlockChain(diskdb, sdb, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Chain A: 1 block on the canonical tip.
	blockA1 := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         1,
				Timestamp:      3000,
				ParentHash:     genesisHash.Bytes(),
				WitnessAddress: witnessAddr.Bytes(),
			},
		},
	})
	if err := bc.InsertBlock(blockA1); err != nil {
		t.Fatalf("insert A1: %v", err)
	}
	// Temporal rows present for the canonical A1 block.
	bdb := bc.BufferedDB()
	txRangeA1, ok, err := rawdb.ReadStateTxRange(bdb, 1)
	if err != nil || !ok {
		t.Fatalf("expected StateTxRange for A1: ok=%v err=%v", ok, err)
	}
	if txRangeA1.BlockHash != blockA1.Hash() {
		t.Fatalf("StateTxRange block 1 hash = %x, want A1 %x", txRangeA1.BlockHash, blockA1.Hash())
	}

	// Chain B: 2 blocks with different timestamps → distinct hashes,
	// longer than chain A → triggers switchFork.
	blockB1 := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         1,
				Timestamp:      3001, // distinct from A1
				ParentHash:     genesisHash.Bytes(),
				WitnessAddress: witnessAddr.Bytes(),
			},
		},
	})
	if err := bc.InsertBlock(blockB1); err != nil {
		t.Fatalf("insert B1: %v", err)
	}
	// At this point chain B is equal length, no switchFork yet.
	if bc.CurrentBlock().Hash() != blockA1.Hash() {
		t.Fatal("chain A should still be canonical")
	}

	blockB2 := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         2,
				Timestamp:      6001,
				ParentHash:     blockB1.Hash().Bytes(),
				WitnessAddress: witnessAddr.Bytes(),
			},
		},
	})
	if err := bc.InsertBlock(blockB2); err != nil {
		t.Fatalf("insert B2 (switch trigger): %v", err)
	}
	if bc.CurrentBlock().Hash() != blockB2.Hash() {
		t.Fatalf("after switchFork: head hash = %x, want B2 %x", bc.CurrentBlock().Hash(), blockB2.Hash())
	}

	// After switchFork the orphaned A1 block's temporal rows must be gone
	// (its buffer layer was discarded). The B1 / B2 temporal rows must be
	// present.
	bdb = bc.BufferedDB()

	// B1 + B2 present.
	txRangeB1, ok, err := rawdb.ReadStateTxRange(bdb, 1)
	if err != nil || !ok {
		t.Fatalf("expected StateTxRange for block 1 (B1) after switchFork: ok=%v err=%v", ok, err)
	}
	txRangeB2, ok, err := rawdb.ReadStateTxRange(bdb, 2)
	if err != nil || !ok {
		t.Fatalf("expected StateTxRange for block 2 (B2) after switchFork: ok=%v err=%v", ok, err)
	}
	if txRangeB1.BlockHash != blockB1.Hash() {
		t.Errorf("StateTxRange block 1 hash = %x, want B1 hash %x", txRangeB1.BlockHash, blockB1.Hash())
	}
	if txRangeB2.BlockHash != blockB2.Hash() {
		t.Errorf("StateTxRange block 2 hash = %x, want B2 hash %x", txRangeB2.BlockHash, blockB2.Hash())
	}

	// The buffer's pending-blocks stack should reflect only the new branch
	// (B1, B2). Any A1 hash here would prove DiscardBlock didn't strip the
	// orphan layer and history rows are leaking through the overlay even
	// though the canonical key happens to win the merge.
	pending := bc.buffer.PendingBlocks()
	for _, h := range pending {
		if h == blockA1.Hash() {
			t.Fatalf("A1 layer still pending in buffer after switchFork: %x", h)
		}
	}
}

// TestSwitchForkResetsProposalScanCache proves the wiring half of the
// proposal-scan-cache reorg invariant: a rewind must drop every node-local
// terminal mark so the re-applied branch re-reads proposal state from scratch
// (a rewound proposal can revert terminal→PENDING on the new branch). The
// unit-level reset semantics live in proposal_scan_cache_test.go; here we only
// assert switchFork actually calls reset().
func TestSwitchForkResetsProposalScanCache(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	cfg := cloneMainnetChainConfig()
	witnessAddr := testInsertAddr(1)

	genesis := &params.Genesis{
		Config:    cfg,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: witnessAddr, Balance: 99_000_000_000_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1, URL: "test"},
			{Address: testInsertAddr(20), VoteCount: 1, URL: "sr2"},
			{Address: testInsertAddr(21), VoteCount: 1, URL: "sr3"},
		},
		// Three witnesses → solidified stays at 0, so every buffer layer stays
		// in memory and switchFork can rewind them. Maintenance never fires.
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

	mkBlock := func(num uint64, ts int64, parent tcommon.Hash) *types.Block {
		return types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number:         int64(num),
			Timestamp:      ts,
			ParentHash:     parent.Bytes(),
			WitnessAddress: witnessAddr.Bytes(),
		}}})
	}

	// Canonical chain A: one block on the tip.
	blockA1 := mkBlock(1, 3000, genesisHash)
	if err := bc.InsertBlock(blockA1); err != nil {
		t.Fatalf("insert A1: %v", err)
	}

	// Warm the proposal scan cache as a prior maintenance scan would have. These
	// non-maintenance inserts never touch it, so the marks survive to the reorg.
	bc.proposalCache.markTerminal(1)
	bc.proposalCache.markTerminal(7)
	if !bc.proposalCache.isTerminal(1) {
		t.Fatal("precondition: cache should be warm before reorg")
	}

	// Competing chain B (2 blocks) outgrows A → triggers switchFork on B2.
	blockB1 := mkBlock(1, 3001, genesisHash)
	if err := bc.InsertBlock(blockB1); err != nil {
		t.Fatalf("insert B1: %v", err)
	}
	blockB2 := mkBlock(2, 6001, blockB1.Hash())
	if err := bc.InsertBlock(blockB2); err != nil {
		t.Fatalf("insert B2 (switch trigger): %v", err)
	}
	if bc.CurrentBlock().Hash() != blockB2.Hash() {
		t.Fatalf("expected switchFork to B2; head = %x", bc.CurrentBlock().Hash())
	}

	if bc.proposalCache.isTerminal(1) || bc.proposalCache.isTerminal(7) {
		t.Fatal("switchFork must reset the proposal scan cache, but terminal marks survived the rewind")
	}
}
