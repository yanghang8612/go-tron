package core

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/state"
)

func TestForkControllerForStateReusesControllerAndRebindsStore(t *testing.T) {
	bc := new(BlockChain)
	firstState := new(state.StateDB)
	first := bc.forkControllerForState(firstState)
	if first == nil || bc.stateForkStatsStore.statedb != firstState {
		t.Fatal("first controller did not bind the supplied state")
	}

	secondState := new(state.StateDB)
	second := bc.forkControllerForState(secondState)
	if second != first {
		t.Fatal("fork controller was allocated again")
	}
	if bc.stateForkStatsStore.statedb != secondState {
		t.Fatal("reused controller did not rebind the state store")
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if bc.forkControllerForState(secondState) != first {
			panic("controller changed")
		}
	}); allocs != 0 {
		t.Fatalf("warm controller lookup allocated %.2f objects, want 0", allocs)
	}
}
