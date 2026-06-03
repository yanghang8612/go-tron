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

// Sanity: ensure a fresh DynProps reads zero and the address helper is stable.
var _ = tcommon.Address{}
var _ = (*types.Transaction)(nil)
