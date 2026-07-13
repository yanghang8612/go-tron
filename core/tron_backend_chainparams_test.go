package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/params"
)

// TestTronBackendGetChainParametersJavaKeySet pins that the backend emits
// java-tron's Wallet.getChainParameters key set in java's order — translated
// getXxx names, no raw snake_case DP keys, no internal counters.
func TestTronBackendGetChainParametersJavaKeySet(t *testing.T) {
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

	backend := NewTronBackend(bc, txpool.New())
	got := backend.GetChainParameters()
	want := state.ChainParameterKeys()
	if len(got) != len(want) {
		t.Fatalf("GetChainParameters returned %d entries, want %d", len(got), len(want))
	}
	for i, key := range want {
		if got[i].Key != key {
			t.Fatalf("entry %d: got key %q, want %q", i, got[i].Key, key)
		}
	}

	mti, ok := bc.DynProps().Get("maintenance_time_interval")
	if !ok {
		t.Fatal("maintenance_time_interval missing from DynProps")
	}
	if got[0].Value != mti {
		t.Fatalf("getMaintenanceTimeInterval value %d, DP store has %d", got[0].Value, mti)
	}
}
