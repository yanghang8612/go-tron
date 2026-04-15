package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestBuildBlock_EmptyPool(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testProcessorAddr(1), Balance: 10_000_000},
		},
	}
	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	pool := txpool.New()
	witnessAddr := testProcessorAddr(0xFF)

	result, err := BuildBlock(bc, pool, witnessAddr, 3000)
	if err != nil {
		t.Fatal(err)
	}
	block := result.Block

	if block.Number() != 1 {
		t.Fatalf("block number: want 1, got %d", block.Number())
	}
	if block.Timestamp() != 3000 {
		t.Fatalf("timestamp: want 3000, got %d", block.Timestamp())
	}
	if block.WitnessAddress() != witnessAddr {
		t.Fatalf("witness: want %x, got %x", witnessAddr, block.WitnessAddress())
	}
	if block.AccountStateRoot() == (tcommon.Hash{}) {
		t.Fatal("expected non-empty state root")
	}
	if len(block.Transactions()) != 0 {
		t.Fatalf("expected 0 transactions, got %d", len(block.Transactions()))
	}
	if got := block.Version(); got != params.BlockVersion {
		t.Fatalf("block version: want %d, got %d", params.BlockVersion, got)
	}
}

func TestBuildBlock_WithTransactions(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	sender := testProcessorAddr(1)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: sender, Balance: 100_000_000},
		},
	}
	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	pool := txpool.New()
	tx := makeTestTransferTx(1, 2, 1_000_000)
	pool.Add(tx)

	witnessAddr := testProcessorAddr(0xFF)
	result, err := BuildBlock(bc, pool, witnessAddr, 3000)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Block.Transactions()) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(result.Block.Transactions()))
	}
}

func TestBuildBlock_SkipsFailingTx(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testProcessorAddr(1), Balance: 100_000_000},
		},
	}
	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	pool := txpool.New()
	tx1 := makeTestTransferTx(1, 2, 1_000_000)
	pool.Add(tx1)
	tx2 := makeTestTransferTx(3, 4, 1_000_000) // sender 3 doesn't exist
	pool.Add(tx2)

	witnessAddr := testProcessorAddr(0xFF)
	result, err := BuildBlock(bc, pool, witnessAddr, 3000)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Block.Transactions()) != 1 {
		t.Fatalf("expected 1 transaction (skipped failing), got %d", len(result.Block.Transactions()))
	}
	if len(result.FailedTxIDs) != 1 {
		t.Fatalf("expected 1 failed tx, got %d", len(result.FailedTxIDs))
	}
}

func TestSignBlock(t *testing.T) {
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    1,
				Timestamp: 3000,
			},
		},
	})

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}

	err = SignBlock(block, key)
	if err != nil {
		t.Fatal(err)
	}

	sig := block.WitnessSignature()
	if len(sig) != 65 {
		t.Fatalf("signature length: want 65, got %d", len(sig))
	}
}
