package vm

import (
	"testing"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// A FREEZE opcode that runs inside a frame which later reverts must NOT leave
// its total_energy_weight delta behind. java's freeze opcode mutates a
// discardable Repository, so the delta is dropped with the reverted frame; gtron
// journals the delta (StateDB.AddResourceWeightJournaled) so RevertToSnapshot
// rolls it back. Regression for the Nile 27,405,576 stall — a small tw
// over-count that floored an origin's energy limit 1 lower, over-burning 1
// energy on a later USDT call.
func TestVMFreezeThenRevertDoesNotLeakEnergyWeight(t *testing.T) {
	const amount = int64(tvmTRXPrecision) * 5 // 5 TRX -> +5 total_energy_weight

	tvm, statedb, dp := newTestTVMForCreate(t, TVMConfig{}, func(dp *state.DynamicProperties) {
		dp.SetLatestBlockHeaderTimestamp(1_000_000)
	})
	tvm.Timestamp = 1_003_000

	caller := tcommon.Address{0x41, 0x07}
	statedb.CreateAccount(caller, corepb.AccountType_Normal)
	statedb.AddBalance(caller, amount)

	before := dp.TotalEnergyWeight()

	snap := statedb.Snapshot()
	receiver := addressToUint256(caller)
	stack := newStack()
	stack.push(&receiver)
	stack.push(uint256.NewInt(uint64(amount)))
	stack.push(uint256.NewInt(1)) // resource = ENERGY
	contract := NewContract(caller, caller, 0, 100000)
	if _, err := opFreeze(nil, tvm.interpreter, contract, nil, stack); err != nil {
		t.Fatalf("FREEZE opcode error: %v", err)
	}
	result := stack.pop()
	if got := result.Uint64(); got != 1 {
		t.Fatalf("FREEZE result: got %d, want 1", got)
	}
	if got := dp.TotalEnergyWeight(); got != before+5 {
		t.Fatalf("after FREEZE: total_energy_weight=%d, want %d", got, before+5)
	}

	// The contract frame reverts (a later REVERT / failed require).
	statedb.RevertToSnapshot(snap)
	if got := dp.TotalEnergyWeight(); got != before {
		t.Fatalf("after revert: total_energy_weight=%d, want %d (FREEZE weight leaked)", got, before)
	}
}
