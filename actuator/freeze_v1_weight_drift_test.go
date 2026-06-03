package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// These tests probe the Nile-8,825,873 total_energy_weight drift (gtron ends
// 3,091 below canonical). Each replays a freeze/unfreeze sequence through the
// REAL actuators and asserts total_energy_weight matches java-tron v4.0.1's
// floor method: freeze adds floor(thisAmount/1e6), unfreeze subtracts
// floor(storedTotal/1e6). A mismatch localizes the runtime divergence.

func execFreeze(t *testing.T, sdb *state.StateDB, dp *state.DynamicProperties, owner byte, amount, dur int64, res corepb.ResourceCode, recv *byte, blockTime int64) {
	t.Helper()
	tx := makeFreezeBalanceTx(owner, amount, dur, res, recv)
	ctx := &Context{State: sdb, DynProps: dp, Tx: tx, BlockTime: blockTime, PrevBlockTime: blockTime, BlockNumber: 1, DB: nil}
	if _, err := (&FreezeBalanceActuator{}).Execute(ctx); err != nil {
		t.Fatalf("freeze(owner=%d,amt=%d,res=%v) execute: %v", owner, amount, res, err)
	}
}

func execUnfreeze(t *testing.T, sdb *state.StateDB, dp *state.DynamicProperties, owner byte, res corepb.ResourceCode, recv *byte, blockTime int64) {
	t.Helper()
	tx := makeUnfreezeBalanceTx(owner, res, recv)
	ctx := &Context{State: sdb, DynProps: dp, Tx: tx, BlockTime: blockTime, PrevBlockTime: blockTime, BlockNumber: 2}
	if _, err := (&UnfreezeBalanceActuator{}).Execute(ctx); err != nil {
		t.Fatalf("unfreeze(owner=%d,res=%v) execute: %v", owner, res, err)
	}
}

// Non-delegated energy top-up then full unfreeze. v4.0.1: 1 + 1 - floor(3.0) = -1.
func TestEnergyTopUpFloorAsymmetry(t *testing.T) {
	sdb := setupStateDB(t)
	dp := state.NewDynamicProperties()
	owner := makeTestAddr(10)
	seedAccount(sdb, owner, 100_000_000)

	execFreeze(t, sdb, dp, 10, 1_500_000, 3, corepb.ResourceCode_ENERGY, nil, 1_000_000)
	if got := dp.TotalEnergyWeight(); got != 1 {
		t.Fatalf("after freeze#1: tw=%d want 1", got)
	}
	execFreeze(t, sdb, dp, 10, 1_500_000, 3, corepb.ResourceCode_ENERGY, nil, 1_000_000)
	if got := dp.TotalEnergyWeight(); got != 2 {
		t.Fatalf("after freeze#2 (top-up): tw=%d want 2", got)
	}
	execUnfreeze(t, sdb, dp, 10, corepb.ResourceCode_ENERGY, nil, 1_000_000+4*86_400_000)
	if got := dp.TotalEnergyWeight(); got != -1 {
		t.Errorf("after unfreeze: tw=%d want -1 (1+1-floor(3.0)); a !=-1 result is the drift", got)
	}
}

// Many small fractional top-ups: 5 × 1.3 TRX, then unfreeze.
// v4.0.1: 5×floor(1.3)=5, unfreeze floor(6.5)=6 -> 5-6 = -1.
func TestEnergyManyTopUpsFloorAsymmetry(t *testing.T) {
	sdb := setupStateDB(t)
	dp := state.NewDynamicProperties()
	seedAccount(sdb, makeTestAddr(11), 100_000_000)
	for i := 0; i < 5; i++ {
		execFreeze(t, sdb, dp, 11, 1_300_000, 3, corepb.ResourceCode_ENERGY, nil, 1_000_000)
	}
	if got := dp.TotalEnergyWeight(); got != 5 {
		t.Fatalf("after 5 top-ups: tw=%d want 5", got)
	}
	execUnfreeze(t, sdb, dp, 11, corepb.ResourceCode_ENERGY, nil, 1_000_000+4*86_400_000)
	if got := dp.TotalEnergyWeight(); got != -1 {
		t.Errorf("after unfreeze: tw=%d want -1 (5 - floor(6.5)=5-6); drift if !=-1", got)
	}
}

// Delegated energy top-up to the same receiver, then undelegate.
// v4.0.1 floor: 1 + 1 - floor(3.0) = -1.
func TestDelegatedEnergyTopUpFloorAsymmetry(t *testing.T) {
	sdb := setupStateDB(t)
	dp := state.NewDynamicProperties()
	dp.SetAllowDelegateResource(true)
	owner := makeTestAddr(12)
	recv := byte(13)
	seedAccount(sdb, owner, 100_000_000)
	seedAccount(sdb, makeTestAddr(13), 1_000_000)

	execFreeze(t, sdb, dp, 12, 1_500_000, 3, corepb.ResourceCode_ENERGY, &recv, 1_000_000)
	execFreeze(t, sdb, dp, 12, 1_500_000, 3, corepb.ResourceCode_ENERGY, &recv, 1_000_000)
	if got := dp.TotalEnergyWeight(); got != 2 {
		t.Fatalf("after delegated top-up: tw=%d want 2", got)
	}
	execUnfreeze(t, sdb, dp, 12, corepb.ResourceCode_ENERGY, &recv, 1_000_000+4*86_400_000)
	if got := dp.TotalEnergyWeight(); got != -1 {
		t.Errorf("after undelegate: tw=%d want -1; drift if !=-1", got)
	}
}

// TestDelegatedEnergyUnfreezeContractReceiverLeak is the regression test for the
// Nile-8,825,873 total_energy_weight drift. Under allow_new_reward (exact weight
// method) + allow_tvm_constantinople, java-tron's UnfreezeBalanceActuator does
// NOT decrement a Contract receiver's acquired-delegated balance and uses
// decrease = -unfreezeBalance/TRX. gtron previously always decremented the
// receiver and computed newWeight-oldWeight, which is 1 lower whenever
// frac(acquired) < frac(removed) — accumulating ~3,091 low over Nile history and
// flipping the origin's OUT_OF_ENERGY to SUCCESS at block 8,825,873.
//
// Scenario chosen so frac(acquired)=0 < frac(removed)=0.5, where old gtron gave
// tw=1 but java gives tw=2:
//   A delegates 2.5 TRX energy to contract C  -> tw += floor(2.5)-0          = 2
//   B delegates 1.5 TRX energy to contract C  -> tw += floor(4.0)-floor(2.5) = 2  (tw=4)
//   constantinople ON; A unfreezes its 2.5    -> java decrease = -floor(2.5) = -2 (tw=2)
//                                                old gtron decremented C 4.0->1.5:
//                                                floor(1.5)-floor(4.0) = -3 (tw=1, the bug)
func TestDelegatedEnergyUnfreezeContractReceiverLeak(t *testing.T) {
	sdb := setupStateDB(t)
	dp := state.NewDynamicProperties()
	dp.SetAllowDelegateResource(true)
	dp.SetAllowNewReward(true)
	// constantinople OFF during the freezes so delegating to a contract is allowed.
	cSeed := byte(22)
	cAddr := makeTestAddr(22)
	seedAccount(sdb, makeTestAddr(20), 100_000_000)
	seedAccount(sdb, makeTestAddr(21), 100_000_000)
	sdb.CreateAccount(cAddr, corepb.AccountType_Contract)

	execFreeze(t, sdb, dp, 20, 2_500_000, 3, corepb.ResourceCode_ENERGY, &cSeed, 1_000_000)
	execFreeze(t, sdb, dp, 21, 1_500_000, 3, corepb.ResourceCode_ENERGY, &cSeed, 1_000_000)
	if got := dp.TotalEnergyWeight(); got != 4 {
		t.Fatalf("after 2 delegations to contract: tw=%d want 4", got)
	}

	// Now constantinople + solidity059 active (the regime at Nile block 8.8M).
	dp.SetAllowTvmConstantinople(true)
	dp.SetAllowTvmSolidity059(true)

	execUnfreeze(t, sdb, dp, 20, corepb.ResourceCode_ENERGY, &cSeed, 1_000_000+4*86_400_000)

	if got := dp.TotalEnergyWeight(); got != 2 {
		t.Errorf("after contract-receiver unfreeze: tw=%d want 2 (java leaves receiver acquired; the pre-fix gtron drifted to 1)", got)
	}
	// The contract receiver's acquired must NOT have been decremented (java leak).
	if acq := sdb.GetAccount(cAddr).AcquiredDelegatedFrozenEnergy(); acq != 4_000_000 {
		t.Errorf("contract receiver acquired: got %d want 4_000_000 (java does not touch a contract receiver)", acq)
	}
}

// TestDelegatedEnergyUnfreezeNonContractReceiver confirms a NORMAL receiver is
// still decremented and the exact weight delta is unchanged by the fix.
func TestDelegatedEnergyUnfreezeNonContractReceiver(t *testing.T) {
	sdb := setupStateDB(t)
	dp := state.NewDynamicProperties()
	dp.SetAllowDelegateResource(true)
	dp.SetAllowNewReward(true)
	dp.SetAllowTvmConstantinople(true)
	dp.SetAllowTvmSolidity059(true)
	rSeed := byte(24)
	rAddr := makeTestAddr(24)
	seedAccount(sdb, makeTestAddr(23), 100_000_000)
	seedAccount(sdb, rAddr, 1_000_000) // normal receiver

	execFreeze(t, sdb, dp, 23, 3_000_000, 3, corepb.ResourceCode_ENERGY, &rSeed, 1_000_000)
	if got := dp.TotalEnergyWeight(); got != 3 {
		t.Fatalf("after delegation to normal receiver: tw=%d want 3", got)
	}
	execUnfreeze(t, sdb, dp, 23, corepb.ResourceCode_ENERGY, &rSeed, 1_000_000+4*86_400_000)
	if got := dp.TotalEnergyWeight(); got != 0 {
		t.Errorf("after normal-receiver unfreeze: tw=%d want 0", got)
	}
	if acq := sdb.GetAccount(rAddr).AcquiredDelegatedFrozenEnergy(); acq != 0 {
		t.Errorf("normal receiver acquired: got %d want 0 (decremented)", acq)
	}
}

// Sanity: ensure a fresh DynProps reads zero and the address helper is stable.
var _ = tcommon.Address{}
var _ = (*types.Transaction)(nil)
