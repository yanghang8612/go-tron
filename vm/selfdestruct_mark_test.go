package vm

import (
	"testing"

	"github.com/tronprotocol/go-tron/proto/core"
)

func TestRestrictedSelfDestructMarkerFollowsFrame(t *testing.T) {
	tvm, statedb, _ := newStakeParityTVM(t)
	tvm.interpreter.tvmConfig.SelfdestructRestrict = true
	tvm.interpreter.tvmConfig.StakeV2TimeGuard = true
	owner := stakeAddr(0x41)
	statedb.CreateAccount(owner, core.AccountType_Contract)
	contract := NewContract(owner, owner, 0, 100_000)
	selfDestruct := func() {
		t.Helper()
		stack := newStack()
		word := addressWord(owner)
		stack.push(&word)
		if _, err := opSelfDestruct(nil, tvm.interpreter, contract, nil, stack); err != nil {
			t.Fatal(err)
		}
		if !statedb.HasTVMSelfDestructMark(owner) || statedb.HasSelfDestructed(owner) {
			t.Fatal("restricted SELFDESTRUCT must mark the contract and retain the account")
		}
	}
	snapshot := statedb.Snapshot()
	selfDestruct()
	statedb.RevertToSnapshot(snapshot)
	if statedb.HasTVMSelfDestructMark(owner) {
		t.Fatal("reverted frame left a SELFDESTRUCT marker")
	}
	selfDestruct()
	statedb.FinalizeTransaction()
	if statedb.HasTVMSelfDestructMark(owner) || !statedb.AccountExists(owner) {
		t.Fatal("transaction must discard the marker and retain the account")
	}
}
