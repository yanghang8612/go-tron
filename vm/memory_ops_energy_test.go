package vm

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
)

// runOps executes a bytecode program against a fresh TVM and returns the
// energy used by the interpreter (initial - remaining).
func runOps(t *testing.T, code []byte, cfg TVMConfig, energyLimit uint64) uint64 {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	sdb, err := state.New(tcommon.Hash{}, db)
	if err != nil {
		t.Fatal(err)
	}
	tvm := NewTVM(sdb, nil, tcommon.Address{}, 1, 1000, tcommon.Address{}, 1, cfg)
	contract := NewContract(tcommon.Address{0x41, 0x01}, tcommon.Address{0x41, 0x02}, 0, energyLimit)
	contract.SetCode(tcommon.Address{0x41, 0x02}, code)
	if _, err := tvm.interpreter.Run(contract); err != nil && err != ErrExecutionReverted {
		t.Fatalf("run error: %v", err)
	}
	return energyLimit - contract.Energy
}

// TestMemoryOpsBaseEnergy_DefaultMatchesJava locks in java-tron's default
// (proposal #65 OFF) accounting: MLOAD/MSTORE/MSTORE8 charge memDelta only,
// no per-op base tier. Regression covers the historical go-ethereum-style
// `EnergyVeryLow + memDelta` over-charge that broke balance parity by
// 600 sun per affected TVM contract deploy on the cross-impl chain.
func TestMemoryOpsBaseEnergy_DefaultMatchesJava(t *testing.T) {
	// PUSH1 0x80  PUSH1 0x40  MSTORE  STOP
	// MSTORE writes 32 bytes at offset 0x40=64, so memory grows from 0 to
	// 96 bytes = 3 words. memEnergyCost(96) - memEnergyCost(0) = 3*3 + 9/512
	// = 9. Total expected: PUSH1(3) + PUSH1(3) + MSTORE(0+9) + STOP(0) = 15.
	// (Mirrors the first instruction sequence of every Solidity init code.)
	code := []byte{
		byte(PUSH1), 0x80,
		byte(PUSH1), 0x40,
		byte(MSTORE),
		byte(STOP),
	}
	got := runOps(t, code, TVMConfig{}, 100_000)
	if got != 15 {
		t.Fatalf("MSTORE default base = 0 expected total=15, got %d", got)
	}
}

// TestMemoryOpsBaseEnergy_WithHigherLimitProposal asserts that activating
// proposal #65 (`allow_higher_limit_for_max_cpu_time_of_one_tx`) bumps
// MLOAD/MSTORE/MSTORE8 to `SPECIAL_TIER (1) + memDelta`, mirroring
// java-tron `OperationRegistry.adjustMemOperations`.
func TestMemoryOpsBaseEnergy_WithHigherLimitProposal(t *testing.T) {
	code := []byte{
		byte(PUSH1), 0x80,
		byte(PUSH1), 0x40,
		byte(MSTORE),
		byte(STOP),
	}
	cfg := TVMConfig{HigherLimitForMaxCpuTimeOfOneTx: true}
	got := runOps(t, code, cfg, 100_000)
	// Same program with +1 base tier on MSTORE.
	if got != 16 {
		t.Fatalf("MSTORE proposal-on base = 1 expected total=16, got %d", got)
	}
}

// TestCopyOpsBaseEnergy_NoBaseTier covers CODECOPY / CALLDATACOPY /
// RETURNDATACOPY: java-tron `getCodeCopyCost` etc. charge only
// `memDelta + 3*words` — no per-op base tier even with proposal #65 active.
//
// Program:   PUSH1 0x04  PUSH1 0x00  PUSH1 0x00  CODECOPY  STOP
// (copy 4 bytes of code into memory at offset 0)
//
// Expected:
//   3*PUSH1 + CODECOPY + STOP
//   = 9 + (memDelta(32 bytes = 1 word) + 3*1) + 0
//   = 9 + (3 + 3) = 15
func TestCopyOpsBaseEnergy_NoBaseTier(t *testing.T) {
	code := []byte{
		byte(PUSH1), 0x04,
		byte(PUSH1), 0x00,
		byte(PUSH1), 0x00,
		byte(CODECOPY),
		byte(STOP),
	}
	got := runOps(t, code, TVMConfig{}, 100_000)
	if got != 15 {
		t.Fatalf("CODECOPY default expected total=15, got %d", got)
	}

	// And proposal #65 must NOT change copy-op cost (adjustMemOperations
	// only touches MLOAD/MSTORE/MSTORE8 — copy ops are not rebased).
	gotProp := runOps(t, code, TVMConfig{HigherLimitForMaxCpuTimeOfOneTx: true}, 100_000)
	if gotProp != got {
		t.Fatalf("CODECOPY cost must not change under proposal #65: default=%d, proposal-on=%d",
			got, gotProp)
	}
}
