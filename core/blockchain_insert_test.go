package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func testInsertAddr(b byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

func TestBlockChain_InsertBlock_Transfer(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testInsertAddr(1), Balance: 99_000_000_000_000_000},
		},
		DynamicProperties: map[string]int64{},
	}

	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}

	sdb := state.NewDatabase(diskdb)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Build transfer tx: addr(1) -> addr(2) for 5M TRX
	tc := &contractpb.TransferContract{
		OwnerAddress: testInsertAddr(1).Bytes(),
		ToAddress:    testInsertAddr(2).Bytes(),
		Amount:       5_000_000,
	}
	param, _ := anypb.New(tc)
	txPB := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TransferContract,
				Parameter: param,
			}},
		},
	}

	block1 := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     1,
				Timestamp:  3000,
				ParentHash: genesisHash[:],
			},
		},
		Transactions: []*corepb.Transaction{txPB},
	})

	if err := bc.InsertBlock(block1); err != nil {
		t.Fatal(err)
	}

	if bc.CurrentBlock().Number() != 1 {
		t.Fatalf("current block: got %d, want 1", bc.CurrentBlock().Number())
	}

	// Verify DynProps updated
	dynProps := state.LoadDynamicProperties(diskdb)
	if got := dynProps.LatestBlockHeaderNumber(); got != 1 {
		t.Fatalf("dynprops block number: got %d, want 1", got)
	}
}

func TestBlockChain_InsertBlock_MultipleBlocks(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testInsertAddr(1), Balance: 100_000_000},
		},
		DynamicProperties: map[string]int64{},
	}
	SetupGenesisBlock(diskdb, genesis)
	sdb := state.NewDatabase(diskdb)
	bc, _ := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)

	// Insert 3 empty blocks
	for i := uint64(1); i <= 3; i++ {
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
		if err := bc.InsertBlock(block); err != nil {
			t.Fatalf("block %d: %v", i, err)
		}
	}

	if bc.CurrentBlock().Number() != 3 {
		t.Fatalf("current: got %d, want 3", bc.CurrentBlock().Number())
	}
}

// TestBlockChain_ForkSwitch_10Block verifies that InsertBlock detects and switches
// to a competing chain when it becomes longer than the current canonical tip.
// Architecture: 10-block canonical chain A, then an 11-block competing chain B
// branching from genesis. When block 11B arrives, switchFork rewinds to genesis
// and replays chain B on top. Mirrors java-tron Manager.switchFork behaviour.
func TestBlockChain_ForkSwitch_10Block(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	witnessAddr := testInsertAddr(1)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: witnessAddr, Balance: 99_000_000_000_000_000},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 1<<62 - 1, // far future — no maintenance in test
		},
	}
	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}
	sdb := state.NewDatabase(diskdb)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Build and insert chain A: blocks 1..10 with timestamps 3000, 6000, …
	chainA := make([]*types.Block, 11) // [0]=genesis placeholder, [1..10]=actual
	chainA[0] = types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{ParentHash: genesisHash[:]}},
	})
	chainA[0] = bc.CurrentBlock() // use real genesis
	for i := 1; i <= 10; i++ {
		parent := chainA[i-1]
		b := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:         int64(i),
					Timestamp:      int64(i) * 3000,
					ParentHash:     parent.Hash().Bytes(),
					WitnessAddress: witnessAddr.Bytes(),
				},
			},
		})
		if err := bc.InsertBlock(b); err != nil {
			t.Fatalf("chain A block %d: %v", i, err)
		}
		chainA[i] = b
	}
	if bc.CurrentBlock().Number() != 10 {
		t.Fatalf("after chain A: head = %d, want 10", bc.CurrentBlock().Number())
	}
	tipA := bc.CurrentBlock().Hash()

	// Build chain B: 11 blocks branching from genesis with distinct timestamps.
	// Timestamps use offset +1 to produce different block hashes from chain A.
	chainB := make([]*types.Block, 12) // [0]=genesis, [1..11]=B blocks
	chainB[0] = bc.genesisBlock
	for i := 1; i <= 11; i++ {
		parent := chainB[i-1]
		b := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:         int64(i),
					Timestamp:      int64(i)*3000 + 1, // +1 → distinct hash from chain A
					ParentHash:     parent.Hash().Bytes(),
					WitnessAddress: witnessAddr.Bytes(),
				},
			},
		})
		chainB[i] = b
	}

	// Insert chain B blocks 1..10 — no fork switch yet (not longer than A).
	for i := 1; i <= 10; i++ {
		if err := bc.InsertBlock(chainB[i]); err != nil {
			t.Fatalf("chain B block %d (pre-switch): %v", i, err)
		}
		if bc.CurrentBlock().Hash() != tipA {
			t.Fatalf("block %dB should not trigger switch (chain A still longer)", i)
		}
	}

	// Insert chain B block 11 — triggers fork switch.
	if err := bc.InsertBlock(chainB[11]); err != nil {
		t.Fatalf("chain B block 11 (switch trigger): %v", err)
	}

	// Verify canonical chain switched to B.
	if bc.CurrentBlock().Number() != 11 {
		t.Fatalf("after fork switch: head = %d, want 11", bc.CurrentBlock().Number())
	}
	if bc.CurrentBlock().Hash() != chainB[11].Hash() {
		t.Fatalf("head hash = %x, want chain B tip %x", bc.CurrentBlock().Hash(), chainB[11].Hash())
	}

	// KhaosDB must still contain both chain tips.
	if !bc.khaosDB.ContainsBlock(tipA) {
		t.Error("chain A tip should still be in KhaosDB")
	}
	if !bc.khaosDB.ContainsBlock(chainB[11].Hash()) {
		t.Error("chain B tip should be in KhaosDB")
	}

	// State correctness: open StateDB from the new canonical root and verify
	// that witness allowance equals exactly 11 × WitnessPayPerBlock.
	// If switchFork opened applyBlock from the wrong parent root, allowance
	// would carry chain-A's accumulated rewards too (21 blocks × rate).
	statedb, err := state.New(bc.CurrentBlock().AccountStateRoot(), sdb)
	if err != nil {
		t.Fatalf("open state after fork switch: %v", err)
	}
	dynProps := state.LoadDynamicProperties(diskdb)
	wantAllowance := dynProps.WitnessPayPerBlock() * 11
	if got := statedb.GetAllowance(witnessAddr); got != wantAllowance {
		t.Fatalf("witness allowance after fork switch: got %d, want %d (11 × WitnessPayPerBlock)", got, wantAllowance)
	}
}
