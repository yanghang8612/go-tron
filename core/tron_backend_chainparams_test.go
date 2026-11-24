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
	got, err := backend.GetChainParameters()
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, key := range state.ChainParameterKeys() {
		if key != "getAllowFnDsa512" && key != "getAllowMlDsa44" {
			want = append(want, key)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("GetChainParameters returned %d entries, want %d", len(got), len(want))
	}
	for i, key := range want {
		if got[i].Key != key {
			t.Fatalf("entry %d: got key %q, want %q", i, got[i].Key, key)
		}
		if key == "getAdaptiveResourceLimitTargetRatio" {
			if raw := bc.DynProps().AdaptiveResourceLimitTargetRatio(); raw != 14400 || got[i].Value != 10 {
				t.Fatalf("adaptive ratio stored=%d API=%d, want 14400 and 10", raw, got[i].Value)
			}
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
