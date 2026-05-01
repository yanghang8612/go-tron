package producer

import (
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
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

// seedFilledSlots writes a 128-byte BLOCK_FILLED_SLOTS ring with `filled`
// ones at the front (rest zeros) so CalculateFilledSlotsCount returns
// 100*filled/128. Used to drive the LOW_PARTICIPATION gate.
func seedFilledSlots(t *testing.T, bc *core.BlockChain, filled int) {
	t.Helper()
	if filled < 0 || filled > 128 {
		t.Fatalf("filled out of range: %d", filled)
	}
	ring := make([]byte, 128)
	for i := 0; i < filled; i++ {
		ring[i] = 1
	}
	rawdb.WriteDynamicProperty(bc.DB(), "block_filled_slots", ring)
}

// TestProduceBlock_LowParticipation_Skips: with rate ~10% (13 ones / 128 →
// 10), gate must skip the slot. Mirrors java-tron StateManager LOW_PARTICIPATION.
func TestProduceBlock_LowParticipation_Skips(t *testing.T) {
	key, _ := crypto.GenerateKey()
	witnessAddr := crypto.PubkeyToAddress(&key.PublicKey)
	bc, _, _ := setupTestChain(t, witnessAddr)

	seedFilledSlots(t, bc, 13) // 100*13/128 = 10

	skip, rate := shouldSkipLowParticipation(bc)
	if !skip {
		t.Fatalf("expected skip at rate=%d (threshold=%d)", rate, params.MinParticipationRate)
	}
	if rate != 10 {
		t.Fatalf("expected rate=10, got %d", rate)
	}
}

// TestProduceBlock_NormalParticipation_Produces: with rate 50% (64 ones /
// 128 → 50), gate must NOT skip.
func TestProduceBlock_NormalParticipation_Produces(t *testing.T) {
	key, _ := crypto.GenerateKey()
	witnessAddr := crypto.PubkeyToAddress(&key.PublicKey)
	bc, _, _ := setupTestChain(t, witnessAddr)

	seedFilledSlots(t, bc, 64) // 100*64/128 = 50

	skip, rate := shouldSkipLowParticipation(bc)
	if skip {
		t.Fatalf("expected produce at rate=%d (threshold=%d)", rate, params.MinParticipationRate)
	}
	if rate != 50 {
		t.Fatalf("expected rate=50, got %d", rate)
	}
}

// TestProduceBlock_AtThreshold_Produces: java-tron uses strict <, so rate
// equal to the threshold must PRODUCE. With 20 ones / 128 → 15 (exactly the
// default MinParticipationRate). java-tron StateManager.java:56.
func TestProduceBlock_AtThreshold_Produces(t *testing.T) {
	key, _ := crypto.GenerateKey()
	witnessAddr := crypto.PubkeyToAddress(&key.PublicKey)
	bc, _, _ := setupTestChain(t, witnessAddr)

	seedFilledSlots(t, bc, 20) // 100*20/128 = 15 exactly

	skip, rate := shouldSkipLowParticipation(bc)
	if skip {
		t.Fatalf("expected produce at threshold rate=%d (threshold=%d, strict <)",
			rate, params.MinParticipationRate)
	}
	if rate != int64(params.MinParticipationRate) {
		t.Fatalf("expected rate=%d, got %d", params.MinParticipationRate, rate)
	}
}

// TestProduceBlock_JustBelowThreshold_Skips: 19 ones → 100*19/128 = 14, just
// below MinParticipationRate=15.
func TestProduceBlock_JustBelowThreshold_Skips(t *testing.T) {
	key, _ := crypto.GenerateKey()
	witnessAddr := crypto.PubkeyToAddress(&key.PublicKey)
	bc, _, _ := setupTestChain(t, witnessAddr)

	seedFilledSlots(t, bc, 19) // 100*19/128 = 14

	skip, rate := shouldSkipLowParticipation(bc)
	if !skip {
		t.Fatalf("expected skip at rate=%d (threshold=%d)", rate, params.MinParticipationRate)
	}
	if rate != 14 {
		t.Fatalf("expected rate=14, got %d", rate)
	}
}
