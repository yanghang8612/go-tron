package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/params"
)

func TestGenesisToBlock(t *testing.T) {
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}), Balance: 1000000},
		},
		DynamicProperties: map[string]int64{
			"witness_pay_per_block": 16000000,
		},
	}

	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	block, err := GenesisToBlock(genesis, sdb)
	if err != nil {
		t.Fatal(err)
	}

	if block.Number() != 0 {
		t.Fatalf("genesis block number: want 0, got %d", block.Number())
	}
	if block.ParentHash() != (common.Hash{}) {
		t.Fatal("genesis parent hash should be zero")
	}
	if block.AccountStateRoot() == (common.Hash{}) {
		t.Fatal("genesis accountStateRoot should not be zero")
	}
}

func TestGenesisHashDeterministic(t *testing.T) {
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}), Balance: 500},
		},
	}

	diskdb1 := ethrawdb.NewMemoryDatabase()
	block1, _ := GenesisToBlock(genesis, state.NewDatabase(diskdb1))

	diskdb2 := ethrawdb.NewMemoryDatabase()
	block2, _ := GenesisToBlock(genesis, state.NewDatabase(diskdb2))

	if block1.Hash() != block2.Hash() {
		t.Fatalf("genesis hash not deterministic: %x vs %x", block1.Hash(), block2.Hash())
	}
}

func TestSetupGenesisBlock(t *testing.T) {
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}), Balance: 1000000},
		},
	}

	diskdb := ethrawdb.NewMemoryDatabase()

	config, hash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}
	if config == nil {
		t.Fatal("config should not be nil")
	}
	if hash == (common.Hash{}) {
		t.Fatal("genesis hash should not be zero")
	}

	// Verify genesis block is stored
	block := rawdb.ReadBlock(diskdb, 0)
	if block == nil {
		t.Fatal("genesis block not found in DB")
	}
	if block.Hash() != hash {
		t.Fatal("stored genesis hash mismatch")
	}

	// Second call should succeed with same genesis
	config2, hash2, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}
	if hash2 != hash {
		t.Fatal("second SetupGenesisBlock returned different hash")
	}
	if config2.ChainID != config.ChainID {
		t.Fatal("config mismatch")
	}
}
