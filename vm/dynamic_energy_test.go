package vm

import (
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
)

func newTestTVMWithDB(t *testing.T) (*TVM, ethdb.KeyValueStore, *state.DynamicProperties) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	stateDB, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), sdb)
	if err != nil {
		t.Fatal(err)
	}
	dp := state.NewDynamicProperties()
	stateDB.SetDynamicProperties(dp)

	tvm := NewTVM(stateDB, tcommon.Address{}, 1, 1000, tcommon.Address{}, 1, TVMConfig{DynamicEnergy: true})
	tvm.SetDB(diskdb)
	return tvm, diskdb, dp
}

func TestUpdateContractEnergyFactor_BootstrapsFresh(t *testing.T) {
	tvm, db, dp := newTestTVMWithDB(t)
	dp.SetCurrentCycleNumber(42)
	addr := tcommon.Address{0x41, 0x01}

	got := updateContractEnergyFactor(tvm, addr)

	// Fresh contract → factor is exactly 1.0× (= decimal).
	if got != types.DynamicEnergyFactorDecimal {
		t.Fatalf("factor: got %d, want %d", got, types.DynamicEnergyFactorDecimal)
	}
	// State persisted at cycle 42 with zero factor/usage.
	cs := rawdb.ReadContractState(db, addr)
	if cs == nil {
		t.Fatal("state should have been written")
	}
	if cs.UpdateCycle() != 42 || cs.EnergyFactor() != 0 {
		t.Fatalf("state: cycle=%d factor=%d", cs.UpdateCycle(), cs.EnergyFactor())
	}
}

func TestUpdateContractEnergyFactor_AdvancesAcrossCycles(t *testing.T) {
	tvm, db, dp := newTestTVMWithDB(t)
	dp.SetCurrentCycleNumber(1001)
	dp.Set("dynamic_energy_threshold", 900_000)
	dp.SetDynamicEnergyIncreaseFactor(2000)
	dp.SetDynamicEnergyMaxFactor(10000)

	addr := tcommon.Address{0x41, 0x02}
	// Seed prior state: cycle 1000, factor 5000, usage 1M.
	prior := types.NewContractState(1000)
	prior.SetEnergyFactor(5000)
	prior.AddEnergyUsage(1_000_000)
	_ = rawdb.WriteContractState(db, addr, prior)

	got := updateContractEnergyFactor(tvm, addr)

	// Expected: catchUp bumps factor to 8000, returned multiplier = 18000.
	if got != 18_000 {
		t.Fatalf("factor: got %d, want 18000", got)
	}
	updated := rawdb.ReadContractState(db, addr)
	if updated.EnergyFactor() != 8000 {
		t.Fatalf("stored factor: got %d, want 8000", updated.EnergyFactor())
	}
	if updated.UpdateCycle() != 1001 {
		t.Fatalf("cycle: got %d, want 1001", updated.UpdateCycle())
	}
}

func TestApplyDynamicEnergyPenalty(t *testing.T) {
	decimal := types.DynamicEnergyFactorDecimal
	// Factor at 1.0× → no penalty.
	if p := applyDynamicEnergyPenalty(100, decimal); p != 0 {
		t.Fatalf("no-penalty: got %d", p)
	}
	// Factor 1.5×: 100 × 1.5 = 150, penalty = 50.
	if p := applyDynamicEnergyPenalty(100, decimal*3/2); p != 50 {
		t.Fatalf("1.5x: got %d, want 50", p)
	}
	// Factor 2.0×: 100 → 200, penalty 100.
	if p := applyDynamicEnergyPenalty(100, 2*decimal); p != 100 {
		t.Fatalf("2x: got %d, want 100", p)
	}
	// Zero-cost opcode → zero penalty.
	if p := applyDynamicEnergyPenalty(0, 10*decimal); p != 0 {
		t.Fatalf("zero-cost: got %d", p)
	}
}

func TestRecordContractEnergyUsage_Accumulates(t *testing.T) {
	tvm, db, dp := newTestTVMWithDB(t)
	dp.SetCurrentCycleNumber(7)
	addr := tcommon.Address{0x41, 0x03}

	recordContractEnergyUsage(tvm, addr, 500)
	recordContractEnergyUsage(tvm, addr, 300)

	cs := rawdb.ReadContractState(db, addr)
	if cs == nil {
		t.Fatal("state not written")
	}
	if cs.EnergyUsage() != 800 {
		t.Fatalf("usage: got %d, want 800", cs.EnergyUsage())
	}
	if cs.UpdateCycle() != 7 {
		t.Fatalf("cycle on bootstrap: got %d, want 7", cs.UpdateCycle())
	}
}

func TestInterpreter_DynamicEnergyPenaltyCharged(t *testing.T) {
	tvm, db, dp := newTestTVMWithDB(t)
	dp.SetCurrentCycleNumber(10)
	dp.Set("dynamic_energy_threshold", 0)
	dp.SetDynamicEnergyIncreaseFactor(2000)
	dp.SetDynamicEnergyMaxFactor(10000)

	// Seed contract state with a factor > 0 so penalty applies.
	addr := tcommon.Address{0x41, 0x04}
	cs := types.NewContractState(10)
	cs.SetEnergyFactor(5000) // 1.5× multiplier
	_ = rawdb.WriteContractState(db, addr, cs)

	// ADD (0x01) has energyCost = 3. With factor 1.5× the charge is 4
	// (3 base + 1 penalty after truncation): 3 × 15000 / 10000 = 4.
	code := []byte{
		0x60, 0x01, // PUSH1 1
		0x60, 0x02, // PUSH1 2
		0x01,       // ADD
		0x00,       // STOP
	}
	contract := NewContract(tcommon.Address{0x41, 0x00}, addr, 0, 100)
	contract.SetCode(addr, code)

	_, err := tvm.interpreter.Run(contract)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	// PUSH1 (3 each) × 2 + ADD (3) = 9 base. With factor 1.5× → 12.
	// But PUSH1 is base 3, penalty = 3*15000/10000 − 3 = 4 − 3 = 1.
	// ADD: same. Total opcodes: 3 ops × 4 = 12 charged.
	used := 100 - contract.Energy
	if used != 12 {
		t.Fatalf("energy charged: got %d, want 12 (9 base + 3 penalty)", used)
	}

	// ContractState.EnergyUsage should accumulate only the base 9.
	updated := rawdb.ReadContractState(db, addr)
	if updated.EnergyUsage() != 9 {
		t.Fatalf("recorded usage: got %d, want 9", updated.EnergyUsage())
	}
}

func TestInterpreter_DynamicEnergyOffNoPenalty(t *testing.T) {
	tvm, db, dp := newTestTVMWithDB(t)
	tvm.cfg.DynamicEnergy = false
	tvm.interpreter.tvmConfig.DynamicEnergy = false
	dp.SetCurrentCycleNumber(10)

	addr := tcommon.Address{0x41, 0x05}
	// Seed a factor, but since flag is off the interpreter must ignore it.
	cs := types.NewContractState(10)
	cs.SetEnergyFactor(10000) // would be 2.0× if applied
	_ = rawdb.WriteContractState(db, addr, cs)

	code := []byte{0x60, 0x01, 0x60, 0x02, 0x01, 0x00}
	contract := NewContract(tcommon.Address{0x41, 0x00}, addr, 0, 100)
	contract.SetCode(addr, code)

	if _, err := tvm.interpreter.Run(contract); err != nil {
		t.Fatal(err)
	}
	// Exactly 9 energy spent (base, no penalty, no usage recorded).
	if used := 100 - contract.Energy; used != 9 {
		t.Fatalf("energy: got %d, want 9", used)
	}
}
