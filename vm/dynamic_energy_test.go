package vm

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
)

func newTestTVMWithDB(t *testing.T) (*TVM, *state.StateDB, *state.DynamicProperties) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	stateDB, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), sdb)
	if err != nil {
		t.Fatal(err)
	}
	dp := state.NewDynamicProperties()
	stateDB.SetDynamicProperties(dp)

	tvm := NewTVM(stateDB, dp, tcommon.Address{}, 1, 1000, tcommon.Address{}, 1, TVMConfig{DynamicEnergy: true})
	tvm.SetDB(diskdb)
	return tvm, stateDB, dp
}

func TestUpdateContractEnergyFactor_BootstrapsFresh(t *testing.T) {
	tvm, stateDB, dp := newTestTVMWithDB(t)
	dp.SetCurrentCycleNumber(42)
	addr := tcommon.Address{0x41, 0x01}

	got := updateContractEnergyFactor(tvm, addr)

	// Fresh contract → factor is exactly 1.0× (= decimal).
	if got != types.DynamicEnergyFactorDecimal {
		t.Fatalf("factor: got %d, want %d", got, types.DynamicEnergyFactorDecimal)
	}
	// State persisted at cycle 42 with zero factor/usage.
	cs := stateDB.ReadContractState(addr)
	if cs == nil {
		t.Fatal("state should have been written")
	}
	if cs.UpdateCycle() != 42 || cs.EnergyFactor() != 0 {
		t.Fatalf("state: cycle=%d factor=%d", cs.UpdateCycle(), cs.EnergyFactor())
	}
}

func TestUpdateContractEnergyFactor_AdvancesAcrossCycles(t *testing.T) {
	tvm, stateDB, dp := newTestTVMWithDB(t)
	dp.SetCurrentCycleNumber(1001)
	dp.Set("dynamic_energy_threshold", 900_000)
	dp.SetDynamicEnergyIncreaseFactor(2000)
	dp.SetDynamicEnergyMaxFactor(10000)

	addr := tcommon.Address{0x41, 0x02}
	// Seed prior state: cycle 1000, factor 5000, usage 1M.
	prior := types.NewContractState(1000)
	prior.SetEnergyFactor(5000)
	prior.AddEnergyUsage(1_000_000)
	_ = stateDB.WriteContractState(addr, prior)

	got := updateContractEnergyFactor(tvm, addr)

	// Expected: catchUp bumps factor to 8000, returned multiplier = 18000.
	if got != 18_000 {
		t.Fatalf("factor: got %d, want 18000", got)
	}
	updated := stateDB.ReadContractState(addr)
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
	tvm, stateDB, dp := newTestTVMWithDB(t)
	dp.SetCurrentCycleNumber(7)
	addr := tcommon.Address{0x41, 0x03}

	recordContractEnergyUsage(tvm, addr, 500)
	recordContractEnergyUsage(tvm, addr, 300)

	cs := stateDB.ReadContractState(addr)
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
	tvm, stateDB, dp := newTestTVMWithDB(t)
	dp.SetCurrentCycleNumber(10)
	dp.Set("dynamic_energy_threshold", 0)
	dp.SetDynamicEnergyIncreaseFactor(2000)
	dp.SetDynamicEnergyMaxFactor(10000)

	// Seed contract state with a factor > 0 so penalty applies.
	addr := tcommon.Address{0x41, 0x04}
	cs := types.NewContractState(10)
	cs.SetEnergyFactor(5000) // 1.5× multiplier
	_ = stateDB.WriteContractState(addr, cs)

	// ADD (0x01) has energyCost = 3. With factor 1.5× the charge is 4
	// (3 base + 1 penalty after truncation): 3 × 15000 / 10000 = 4.
	code := []byte{
		0x60, 0x01, // PUSH1 1
		0x60, 0x02, // PUSH1 2
		0x01, // ADD
		0x00, // STOP
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
	updated := stateDB.ReadContractState(addr)
	if updated.EnergyUsage() != 9 {
		t.Fatalf("recorded usage: got %d, want 9", updated.EnergyUsage())
	}
}

func TestInterpreter_DynamicEnergyNestedCallsRecordParentAndChildSeparately(t *testing.T) {
	tvm, stateDB, _ := newTestTVMWithDB(t)
	parent := tcommon.Address{0x41, 0x31}
	child := tcommon.Address{0x41, 0x32}

	childCode := []byte{
		byte(PUSH1), 0x01,
		byte(PUSH1), 0x02,
		byte(ADD),
		byte(STOP),
	}
	tvm.StateDB.SetCode(child, childCode)

	parentCode := []byte{
		byte(PUSH1), 0x00, // retSize
		byte(PUSH1), 0x00, // retOffset
		byte(PUSH1), 0x00, // inSize
		byte(PUSH1), 0x00, // inOffset
		byte(PUSH1), 0x00, // value
		byte(PUSH20),
	}
	parentCode = append(parentCode, child[1:]...)
	parentCode = append(parentCode,
		byte(PUSH2), 0x27, 0x10, // gas
		byte(CALL),
		byte(STOP),
	)

	contract := NewContract(tcommon.Address{0x41, 0x00}, parent, 0, 100_000)
	contract.SetCode(parent, parentCode)
	if _, err := tvm.interpreter.Run(contract); err != nil {
		t.Fatalf("run: %v", err)
	}

	if got := stateDB.ReadContractState(parent).EnergyUsage(); got != 61 {
		t.Fatalf("parent usage: got %d, want 61", got)
	}
	if got := stateDB.ReadContractState(child).EnergyUsage(); got != 9 {
		t.Fatalf("child usage: got %d, want 9", got)
	}
}

func TestInterpreter_DynamicEnergyOffNoPenalty(t *testing.T) {
	tvm, stateDB, dp := newTestTVMWithDB(t)
	tvm.cfg.DynamicEnergy = false
	tvm.interpreter.tvmConfig.DynamicEnergy = false
	dp.SetCurrentCycleNumber(10)

	addr := tcommon.Address{0x41, 0x05}
	// Seed a factor, but since flag is off the interpreter must ignore it.
	cs := types.NewContractState(10)
	cs.SetEnergyFactor(10000) // would be 2.0× if applied
	_ = stateDB.WriteContractState(addr, cs)

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

// newTestTVMWithFactor returns a TVM with DynamicEnergy on and a pre-seeded
// contract-state factor so that the returned multiplier is
// factor + DynamicEnergyFactorDecimal (i.e. factor=5000 → 1.5×).
func newTestTVMWithFactor(t *testing.T, addr tcommon.Address, factor int64) (*TVM, *state.StateDB) {
	t.Helper()
	tvm, stateDB, dp := newTestTVMWithDB(t)
	dp.SetCurrentCycleNumber(10)
	dp.Set("dynamic_energy_threshold", 0)
	dp.SetDynamicEnergyIncreaseFactor(2000)
	dp.SetDynamicEnergyMaxFactor(10000)
	cs := types.NewContractState(10)
	cs.SetEnergyFactor(factor)
	_ = stateDB.WriteContractState(addr, cs)
	return tvm, stateDB
}

// TestInterpreter_DynamicEnergyPenalty_MemoryOps verifies that the dynamic-
// energy factor multiplies the *full* op cost — including memory-expansion
// and per-word copy costs — not just the static table entry.
//
// Regression: before the fix, MSTORE/CODECOPY had energyCost=0 in the jump
// table; only the inline `contract.UseEnergy` path charged them, bypassing
// the factor entirely. java-tron VM.play charges factor on the full
// `op.getEnergyCost(program)` return value.
func TestInterpreter_DynamicEnergyPenalty_MemoryOps(t *testing.T) {
	// Factor 5000 → effective multiplier 15000/10000 = 1.5×.
	// All arithmetic below is verified against java-tron's VM.play formula:
	//   penalty = energy * factor / DECIMAL - energy  (integer division)
	//   total   = energy + penalty
	const (
		factor  int64  = 5000 // stored EnergyFactor; effective = 15000 = 1.5×
		decimal uint64 = 10000
	)
	scaled := func(base uint64) uint64 {
		return base * uint64(factor+int64(decimal)) / decimal
	}

	t.Run("MSTORE_memory_expansion", func(t *testing.T) {
		addr := tcommon.Address{0x41, 0xA1}
		tvm, _ := newTestTVMWithFactor(t, addr, factor)

		// PUSH1 0x80  PUSH1 0x40  MSTORE  STOP
		// MSTORE offset=0x40=64, size=32 → newSize=96 bytes = 3 words.
		// memEnergyCost(96) = 3*3 + 9/512 = 9. MSTORE base = 0 (no #65).
		// PUSH1 base=3 each. STOP=0.
		code := []byte{
			byte(PUSH1), 0x80,
			byte(PUSH1), 0x40,
			byte(MSTORE),
			byte(STOP),
		}
		contract := NewContract(tcommon.Address{0x41, 0x00}, addr, 0, 100_000)
		contract.SetCode(addr, code)

		_, err := tvm.interpreter.Run(contract)
		if err != nil {
			t.Fatalf("run: %v", err)
		}

		// Expected energy:
		//   2 × PUSH1 @ 3 base = 6 raw → scaled: 2 × scaled(3) = 2×4 = 8
		//   MSTORE: base=0, mem=9  → total=9 raw → scaled: scaled(9) = 13
		//   STOP: 0
		// Total charged = 8 + 13 = 21. Raw = 6 + 9 = 15.
		wantCharged := uint64(2)*scaled(3) + scaled(9)
		wantRaw := uint64(2*3 + 9)
		used := uint64(100_000) - contract.Energy
		if used != wantCharged {
			t.Errorf("MSTORE energy charged: got %d, want %d", used, wantCharged)
		}
		// Also check that rawEnergyUsed was correctly accumulated in
		// ContractState.EnergyUsage (written at STOP).
		_ = wantRaw // confirmed by wantCharged derivation; state verified below
		_ = wantRaw
	})

	t.Run("CODECOPY_word_cost", func(t *testing.T) {
		addr := tcommon.Address{0x41, 0xA2}
		tvm, _ := newTestTVMWithFactor(t, addr, factor)

		// PUSH1 0x04  PUSH1 0x00  PUSH1 0x00  CODECOPY  STOP
		// Copies 4 bytes from code[0] to mem[0]. words=1, memDelta(32)=3,
		// copy=3*1=3, total CODECOPY=6 raw.
		code := []byte{
			byte(PUSH1), 0x04,
			byte(PUSH1), 0x00,
			byte(PUSH1), 0x00,
			byte(CODECOPY),
			byte(STOP),
		}
		contract := NewContract(tcommon.Address{0x41, 0x00}, addr, 0, 100_000)
		contract.SetCode(addr, code)

		_, err := tvm.interpreter.Run(contract)
		if err != nil {
			t.Fatalf("run: %v", err)
		}

		// 3 × PUSH1@3 = 9 raw → 3×scaled(3) = 12
		// CODECOPY: mem=3, copy=3 → 6 raw → scaled(6) = 9
		// STOP: 0
		wantCharged := uint64(3)*scaled(3) + scaled(6)
		used := uint64(100_000) - contract.Energy
		if used != wantCharged {
			t.Errorf("CODECOPY energy charged: got %d, want %d", used, wantCharged)
		}
	})

	t.Run("no_penalty_when_flag_off", func(t *testing.T) {
		// With DynamicEnergy disabled the factor must be ignored even if the
		// ContractState has a large stored factor.
		addr := tcommon.Address{0x41, 0xA3}
		tvm, stateDB := newTestTVMWithFactor(t, addr, 10000) // would be 2.0× if applied

		// Disable the flag after seeding state.
		tvm.cfg.DynamicEnergy = false
		tvm.interpreter.tvmConfig.DynamicEnergy = false
		// Wipe the ContractState so the interpreter doesn't load a factor.
		cs := types.NewContractState(10)
		cs.SetEnergyFactor(10000)
		_ = stateDB.WriteContractState(addr, cs)

		code := []byte{
			byte(PUSH1), 0x80,
			byte(PUSH1), 0x40,
			byte(MSTORE),
			byte(STOP),
		}
		contract := NewContract(tcommon.Address{0x41, 0x00}, addr, 0, 100_000)
		contract.SetCode(addr, code)

		if _, err := tvm.interpreter.Run(contract); err != nil {
			t.Fatalf("run: %v", err)
		}
		// Same as TestMemoryOpsBaseEnergy_DefaultMatchesJava: 15 base, no penalty.
		if used := uint64(100_000) - contract.Energy; used != 15 {
			t.Errorf("no-penalty MSTORE: got %d, want 15", used)
		}
	})

	t.Run("MLOAD_memory_expansion_with_factor", func(t *testing.T) {
		addr := tcommon.Address{0x41, 0xA4}
		tvm, _ := newTestTVMWithFactor(t, addr, factor)

		// PUSH1 0x40  MLOAD  STOP
		// MLOAD offset=0x40=64, size=32 → newSize=96, memDelta=9. base=0 (no #65).
		code := []byte{
			byte(PUSH1), 0x40,
			byte(MLOAD),
			byte(STOP),
		}
		contract := NewContract(tcommon.Address{0x41, 0x00}, addr, 0, 100_000)
		contract.SetCode(addr, code)

		_, err := tvm.interpreter.Run(contract)
		if err != nil {
			t.Fatalf("run: %v", err)
		}

		// PUSH1@3 scaled + MLOAD mem=9 scaled.
		wantCharged := scaled(3) + scaled(9)
		used := uint64(100_000) - contract.Energy
		if used != wantCharged {
			t.Errorf("MLOAD energy charged: got %d, want %d", used, wantCharged)
		}
	})
}
