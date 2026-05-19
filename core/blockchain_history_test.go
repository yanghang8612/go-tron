package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
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
// HistoryEnabled=true a block import lands AccountDelta + inverse rows in
// the chain's KV store (via bc.buffer flush). We touch the transfer's
// sender and receiver, both of which must show up.
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

	if !rawdb.HasAccountDelta(bdb, 1, testInsertAddr(1)) {
		t.Error("AccountDelta missing for sender")
	}
	if !rawdb.HasAccountDelta(bdb, 1, testInsertAddr(2)) {
		t.Error("AccountDelta missing for receiver")
	}
	if !rawdb.HasAddrInverse(bdb, testInsertAddr(1), 1) {
		t.Error("AddrInverse missing for sender")
	}
	if !rawdb.HasAddrInverse(bdb, testInsertAddr(2), 1) {
		t.Error("AddrInverse missing for receiver")
	}

	meta := rawdb.ReadHistoryMeta(bdb, 1)
	if meta == nil {
		t.Fatal("StateHistoryMeta missing")
	}
	if meta.BlockNum != 1 {
		t.Errorf("BlockNum = %d, want 1", meta.BlockNum)
	}
	if string(meta.BlockHash) != string(block1.Hash().Bytes()) {
		t.Errorf("BlockHash mismatch")
	}
	if meta.NumAddrs < 2 {
		t.Errorf("NumAddrs = %d, want >=2 (sender+receiver, witness too if active)", meta.NumAddrs)
	}
}

// TestApplyBlock_HistoryDisabledNoRows asserts that with the default config
// (HistoryEnabled=false) NO sh-* rows are written for an inserted block —
// the zero-overhead promise.
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

	bdb := bc.BufferedDB()
	if rawdb.HasAccountDelta(bdb, 1, testInsertAddr(1)) {
		t.Error("sh-a- row leaked despite HistoryEnabled=false")
	}
	if rawdb.HasAddrInverse(bdb, testInsertAddr(1), 1) {
		t.Error("sh-i-a- row leaked despite HistoryEnabled=false")
	}
	if rawdb.HasHistoryMeta(bdb, 1) {
		t.Error("sh-m- row leaked despite HistoryEnabled=false")
	}
}

// TestApplyBlock_HistoryReorgDropsOrphan inserts a canonical chain A then
// a longer competing chain B that triggers switchFork; asserts that the
// canonical B history rows are present after the rewind (the orphan A1
// layer was discarded and the buffer is now showing B1's rows). Deeper
// fork-rewind invariants (e.g. no leaked rows after a full-flush cycle)
// land in Slice 4's integration suite.
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
	// History rows present for the canonical A1 block.
	bdb := bc.BufferedDB()
	if !rawdb.HasAddrInverse(bdb, witnessAddr, 1) {
		t.Fatal("expected sh-i-a- for witness after A1")
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

	// After switchFork the orphaned A1 block's history rows must be gone
	// (its buffer layer was discarded). The B1 / B2 history rows must be
	// present.
	bdb = bc.BufferedDB()

	// B1 + B2 present.
	if !rawdb.HasHistoryMeta(bdb, 1) {
		t.Error("expected sh-m- for block 1 (B1) after switchFork")
	}
	if !rawdb.HasHistoryMeta(bdb, 2) {
		t.Error("expected sh-m- for block 2 (B2) after switchFork")
	}
	metaB1 := rawdb.ReadHistoryMeta(bdb, 1)
	if metaB1 == nil {
		t.Fatal("sh-m- at block 1 missing after switchFork")
	}
	if string(metaB1.BlockHash) != string(blockB1.Hash().Bytes()) {
		t.Errorf("sh-m- at block 1 hash = %x, want B1 hash %x (A1 row would have a different hash and indicates the orphan layer wasn't discarded)", metaB1.BlockHash, blockB1.Hash().Bytes())
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
