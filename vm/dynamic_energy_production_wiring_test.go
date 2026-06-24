package vm

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
)

// newProductionWiredTVM builds a TVM exactly like the production block-execution
// path (vm_actuator.go / tron_backend.go): the DynamicProperties is passed as the
// NewTVM argument (-> tvm.DynProps) and StateDB.SetDynamicProperties is NOT
// called, so tvm.StateDB.DynamicProperties() is nil. Regression guard for the
// dynamic-energy factor never accruing on-chain (Nile stall at block 33,185,251:
// expected OUT_OF_ENERGY, actual SUCCESS — the missing SSTORE penalty).
func newProductionWiredTVM(t *testing.T) (*TVM, *state.StateDB, *state.DynamicProperties) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	stateDB, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), sdb)
	if err != nil {
		t.Fatal(err)
	}
	// Intentionally NOT calling stateDB.SetDynamicProperties — mirrors production.
	// state.New seeds stateDB.dynProps with a fresh empty DynamicProperties
	// (cycle 0), so it is a DISTINCT object from the wired ctx-equivalent dp.
	dp := state.NewDynamicProperties()
	tvm := NewTVM(stateDB, dp, tcommon.Address{}, 1, 1000, tcommon.Address{}, 1, TVMConfig{DynamicEnergy: true})
	tvm.SetDB(diskdb)
	if stateDB.DynamicProperties() == dp {
		t.Fatal("precondition: production StateDB dp must DIFFER from the wired tvm.DynProps")
	}
	return tvm, stateDB, dp
}

// TestUpdateContractEnergyFactor_ProductionDpWiring pins that the factor catch-up
// reads the wired tvm.DynProps, not the production-nil StateDB dp. Pre-fix this
// returned 1.0x (catch-up skipped) regardless of accumulated usage.
func TestUpdateContractEnergyFactor_ProductionDpWiring(t *testing.T) {
	tvm, stateDB, dp := newProductionWiredTVM(t)
	dp.SetCurrentCycleNumber(1001)
	dp.Set("dynamic_energy_threshold", 900_000)
	dp.SetDynamicEnergyIncreaseFactor(2000)
	dp.SetDynamicEnergyMaxFactor(10000)

	addr := tcommon.Address{0x41, 0x55}
	prior := types.NewContractState(1000)
	prior.AddEnergyUsage(1_000_000) // last cycle exceeded threshold
	if err := stateDB.WriteContractState(addr, prior); err != nil {
		t.Fatal(err)
	}

	if got := updateContractEnergyFactor(tvm, addr); got != types.DynamicEnergyFactorDecimal+2000 {
		t.Fatalf("factor = %d, want %d (0.2 accrued)", got, types.DynamicEnergyFactorDecimal+2000)
	}
}

// TestDynamicEnergyAccrual_ProductionWiringEndToEnd exercises both fixed funcs:
// record usage across a busy cycle (cs cycle must come from tvm.DynProps), cross
// a maintenance boundary, then catch up — the factor must accrue to 0.2x. Mirrors
// Nile proposal 17105 (threshold 2,000,000, increase 2000, max 25000).
func TestDynamicEnergyAccrual_ProductionWiringEndToEnd(t *testing.T) {
	tvm, _, dp := newProductionWiredTVM(t)
	dp.Set("dynamic_energy_threshold", 2_000_000)
	dp.SetDynamicEnergyIncreaseFactor(2000)
	dp.SetDynamicEnergyMaxFactor(25000)

	addr := tcommon.Address{0x41, 0x56}

	// Cycle 100: accumulate > threshold via the recording path.
	dp.SetCurrentCycleNumber(100)
	if f := updateContractEnergyFactor(tvm, addr); f != types.DynamicEnergyFactorDecimal {
		t.Fatalf("fresh factor = %d, want 1.0x", f)
	}
	for i := 0; i < 50; i++ { // 50 * 50_000 = 2_500_000 > 2_000_000
		recordContractEnergyUsage(tvm, addr, 50_000)
	}
	if cs := tvm.StateDB.ReadContractState(addr); cs == nil || cs.UpdateCycle() != 100 {
		t.Fatalf("cs cycle = %v, want 100 (cycle must come from tvm.DynProps, not nil StateDB dp)", cs)
	}

	// Maintenance boundary: advance the cycle, then the next exec catches up.
	dp.SetCurrentCycleNumber(101)
	if got := updateContractEnergyFactor(tvm, addr); got != types.DynamicEnergyFactorDecimal+2000 {
		t.Fatalf("factor after boundary = %d, want %d (0.2 accrued)", got, types.DynamicEnergyFactorDecimal+2000)
	}
}
