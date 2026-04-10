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
)

func TestNewBlockChain(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000000},
		},
	}

	_, _, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}

	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	if bc.CurrentBlock() == nil {
		t.Fatal("current block should not be nil")
	}
	if bc.CurrentBlock().Number() != 0 {
		t.Fatalf("current block number: want 0, got %d", bc.CurrentBlock().Number())
	}
}

func TestBlockChainInsertBlock(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 99_000_000_000_000_000},
		},
	}

	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}

	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	block1Header := &corepb.BlockHeaderRaw{
		Number:     1,
		Timestamp:  3000,
		ParentHash: genesisHash[:],
	}

	block1 := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: block1Header,
		},
	})

	err = bc.InsertBlockWithoutVerify(block1)
	if err != nil {
		t.Fatal(err)
	}

	if bc.CurrentBlock().Number() != 1 {
		t.Fatalf("current block number: want 1, got %d", bc.CurrentBlock().Number())
	}

	stored := rawdb.ReadBlock(diskdb, 1)
	if stored == nil {
		t.Fatal("block 1 not stored")
	}
}

func TestBlockChainGetBlockByNumber(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000},
		},
	}

	SetupGenesisBlock(diskdb, genesis)
	bc, _ := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)

	block := bc.GetBlockByNumber(0)
	if block == nil {
		t.Fatal("genesis block not found")
	}
}

func TestBlockChainGetBlockByHash(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000},
		},
	}

	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}

	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	block := bc.GetBlockByHash(genesisHash)
	if block == nil {
		t.Fatal("genesis block not found by hash")
	}
	if block.Number() != 0 {
		t.Fatalf("expected block number 0, got %d", block.Number())
	}
}

func TestBlockChainInsertInvalidNumber(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000},
		},
	}

	SetupGenesisBlock(diskdb, genesis)
	bc, _ := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)

	// Try to insert block with wrong number (2 instead of 1)
	badBlock := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number: 2,
			},
		},
	})

	err := bc.InsertBlockWithoutVerify(badBlock)
	if err != ErrInvalidNumber {
		t.Fatalf("expected ErrInvalidNumber, got %v", err)
	}
}

func TestBlockChainInsertInvalidParent(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000},
		},
	}

	SetupGenesisBlock(diskdb, genesis)
	bc, _ := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)

	// Insert block 1 with wrong parent hash
	wrongParent := tcommon.Hash{0xde, 0xad}
	badBlock := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     1,
				ParentHash: wrongParent[:],
			},
		},
	})

	err := bc.InsertBlockWithoutVerify(badBlock)
	if err != ErrInvalidParent {
		t.Fatalf("expected ErrInvalidParent, got %v", err)
	}
}

func TestBlockChainActiveWitnesses(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: testCoreAddr(10), VoteCount: 100, URL: "http://w1"},
			{Address: testCoreAddr(11), VoteCount: 200, URL: "http://w2"},
		},
	}

	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	witnesses := bc.ActiveWitnesses()
	if len(witnesses) == 0 {
		t.Fatal("expected non-empty active witnesses")
	}

	newList := []tcommon.Address{testCoreAddr(20), testCoreAddr(21)}
	bc.SetActiveWitnesses(newList)

	got := bc.ActiveWitnesses()
	if len(got) != 2 || got[0] != testCoreAddr(20) || got[1] != testCoreAddr(21) {
		t.Fatalf("unexpected witnesses after set: %v", got)
	}

	persisted := rawdb.ReadActiveWitnesses(diskdb)
	if len(persisted) != 2 {
		t.Fatalf("expected 2 persisted witnesses, got %d", len(persisted))
	}
}

func TestBlockChainNextMaintenanceTime(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 1000,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 100000,
		},
	}

	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	if bc.NextMaintenanceTime() != 100000 {
		t.Fatalf("expected 100000, got %d", bc.NextMaintenanceTime())
	}
}

func TestBlockChainInsertBlock_Maintenance(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	witnessAddr := testCoreAddr(10)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 100_000_000},
			{Address: witnessAddr, Balance: 1_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1000, URL: "http://w1"},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 6000,
		},
	}

	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Build block 1 at timestamp 3000 (before maintenance)
	block1 := buildTestBlock(bc, witnessAddr, 3000)
	if err := bc.InsertBlock(block1); err != nil {
		t.Fatal(err)
	}

	dynProps := state.LoadDynamicProperties(diskdb)
	if dynProps.NextMaintenanceTime() != 6000 {
		t.Fatalf("maintenance should not have run yet, got %d", dynProps.NextMaintenanceTime())
	}

	// Build block 2 at timestamp 6000 (at maintenance boundary)
	block2 := buildTestBlock(bc, witnessAddr, 6000)
	if err := bc.InsertBlock(block2); err != nil {
		t.Fatal(err)
	}

	dynProps = state.LoadDynamicProperties(diskdb)
	if dynProps.NextMaintenanceTime() <= 6000 {
		t.Fatalf("next_maintenance_time should have advanced past 6000, got %d", dynProps.NextMaintenanceTime())
	}
}

func buildTestBlock(bc *BlockChain, witnessAddr tcommon.Address, timestamp int64) *types.Block {
	parent := bc.CurrentBlock()
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         int64(parent.Number() + 1),
				Timestamp:      timestamp,
				ParentHash:     parent.Hash().Bytes(),
				WitnessAddress: witnessAddr.Bytes(),
			},
		},
	})
}
