package producer

import (
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
)

func testAddr(b byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

func setupTestChain(t *testing.T, witnessAddr tcommon.Address) (*core.BlockChain, *txpool.TxPool, *dpos.DPoS) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: witnessAddr, Balance: 100_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1000, URL: "http://test"},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 21600000,
		},
	}

	_, _, err := core.SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}
	bc, err := core.NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	pool := txpool.New()
	engine := dpos.New(bc)

	return bc, pool, engine
}

func TestProducer_New(t *testing.T) {
	key, _ := crypto.GenerateKey()
	witnessAddr := crypto.PubkeyToAddress(&key.PublicKey)
	bc, pool, engine := setupTestChain(t, witnessAddr)

	p := New(bc, pool, engine, key)
	if p == nil {
		t.Fatal("producer should not be nil")
	}
}

func TestProducer_ProduceBlock(t *testing.T) {
	key, _ := crypto.GenerateKey()
	witnessAddr := crypto.PubkeyToAddress(&key.PublicKey)
	bc, pool, engine := setupTestChain(t, witnessAddr)
	_ = engine

	p := New(bc, pool, engine, key)

	timestamp := int64(params.BlockProducedInterval)

	err := p.produceBlock(witnessAddr, timestamp)
	if err != nil {
		t.Fatal(err)
	}

	if bc.CurrentBlock().Number() != 1 {
		t.Fatalf("expected block 1, got %d", bc.CurrentBlock().Number())
	}
	if bc.CurrentBlock().Timestamp() != timestamp {
		t.Fatalf("expected timestamp %d, got %d", timestamp, bc.CurrentBlock().Timestamp())
	}

	sig := bc.CurrentBlock().WitnessSignature()
	if len(sig) != 65 {
		t.Fatalf("expected 65-byte signature, got %d", len(sig))
	}
}

func TestProducer_StartStop(t *testing.T) {
	key, _ := crypto.GenerateKey()
	witnessAddr := crypto.PubkeyToAddress(&key.PublicKey)
	bc, pool, engine := setupTestChain(t, witnessAddr)

	p := New(bc, pool, engine, key)
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	p.Stop()
}
