package core

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
)

func readWitnessAtHead(tb testing.TB, bc *BlockChain, addr tcommon.Address) *types.Witness {
	tb.Helper()
	statedb, err := state.New(bc.HeadStateRoot(), bc.StateDB())
	if err != nil {
		tb.Fatalf("open head state: %v", err)
	}
	w := statedb.GetWitness(addr)
	if w == nil {
		tb.Fatalf("witness %s missing at head", addr.Hex())
	}
	return w
}

func readWitnessLatestBlockAtHead(tb testing.TB, bc *BlockChain, addr tcommon.Address) int64 {
	tb.Helper()
	statedb, err := state.New(bc.HeadStateRoot(), bc.StateDB())
	if err != nil {
		tb.Fatalf("open head state: %v", err)
	}
	return statedb.ReadWitnessLatestBlock(addr)
}

func seedGenesisWitnessCapsule(tb testing.TB, bc *BlockChain, w *types.Witness) {
	tb.Helper()
	statedb, err := state.New(bc.HeadStateRoot(), bc.StateDB())
	if err != nil {
		tb.Fatalf("open genesis state: %v", err)
	}
	if err := statedb.SetWitnessCapsule(w); err != nil {
		tb.Fatalf("seed genesis witness capsule: %v", err)
	}
	newRoot, err := statedb.Commit()
	if err != nil {
		tb.Fatalf("commit seeded witness capsule: %v", err)
	}
	rawdb.WriteGenesisStateRoot(bc.db, newRoot)
	if bc.genesisBlock != nil {
		rawdb.WriteBlockStateRoot(bc.db, bc.genesisBlock.Hash(), newRoot)
	}
}
