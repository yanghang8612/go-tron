package state

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func newCancelTestStateDB(t *testing.T) *StateDB {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := NewDatabase(diskdb)
	statedb, err := New(tcommon.Hash{}, sdb)
	if err != nil {
		t.Fatal(err)
	}
	statedb.SetDynamicProperties(NewDynamicProperties())
	return statedb
}

func cancelTestAddr(last byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = last
	return addr
}

// TestCancelAllUnfreezeV2SplitsExpiredAndRefreezes locks the F-2 fix for the
// StateDB layer: CancelAllUnfreezeV2 must take `now` and split the pending
// UnfreezeV2 queue exactly like java CancelAllUnfreezeV2Processor.execute and
// go actuator CancelAllUnfreezeV2Actuator.Execute:
//   - expireTime <= now  -> withdrawn (accumulated into the returned total, the
//     op handler adds it to balance); NOT refrozen.
//   - expireTime  > now  -> refrozen into FrozenV2 + global resource weight
//     updated by (newWeight - oldWeight).
//
// Before the fix the method refroze EVERY entry unconditionally, never updated
// total_*_weight, and returned the full sum (so expired stake was wrongly
// refrozen instead of withdrawn to balance and the weight ledger drifted).
func TestCancelAllUnfreezeV2SplitsExpiredAndRefreezes(t *testing.T) {
	sdb := newCancelTestStateDB(t)
	dp := sdb.DynamicProperties()
	owner := cancelTestAddr(0x01)
	sdb.CreateAccount(owner, corepb.AccountType_Normal)

	const now = int64(1_000_000)
	// U1 = 100 TRX ENERGY, expired (expireTime <= now): must be withdrawn.
	sdb.AddUnfreezeV2(owner, corepb.ResourceCode_ENERGY, 100*trxPrecisionState, now-1)
	// U2 = 200 TRX ENERGY, unexpired (expireTime > now): must be refrozen.
	sdb.AddUnfreezeV2(owner, corepb.ResourceCode_ENERGY, 200*trxPrecisionState, now+1)

	energyWeightBefore := dp.TotalEnergyWeight()

	expired, deltas := sdb.CancelAllUnfreezeV2(owner, now)

	if expired != 100*trxPrecisionState {
		t.Fatalf("expired total: got %d, want %d", expired, 100*trxPrecisionState)
	}
	// 200 TRX refrozen into ENERGY.
	if got := sdb.GetFrozenV2Amount(owner, corepb.ResourceCode_ENERGY); got != 200*trxPrecisionState {
		t.Fatalf("refrozen energy: got %d, want %d", got, 200*trxPrecisionState)
	}
	// The returned ENERGY weight delta is +200 (TRX units, = 200*1e6 / 1e6); the
	// method no longer mutates dp itself — the caller applies it to the live dp.
	if got := deltas[corepb.ResourceCode_ENERGY]; got != 200 {
		t.Fatalf("returned energy weight delta: got %d, want 200", got)
	}
	if dp.TotalEnergyWeight() != energyWeightBefore {
		t.Fatalf("CancelAllUnfreezeV2 must not mutate dp itself: %d -> %d", energyWeightBefore, dp.TotalEnergyWeight())
	}
	sdb.AddResourceWeightJournaled(dp, corepb.ResourceCode_ENERGY, deltas[corepb.ResourceCode_ENERGY])
	if got := dp.TotalEnergyWeight() - energyWeightBefore; got != 200 {
		t.Fatalf("total_energy_weight delta after apply: got %d, want 200", got)
	}
	// Queue cleared.
	if got := sdb.UnfreezeV2Count(owner); got != 0 {
		t.Fatalf("unfreeze queue not cleared: got %d entries", got)
	}
}

// TestCancelAllUnfreezeV2AllExpiredNoRefreeze: when every entry is expired,
// nothing is refrozen, weight is untouched, and the full amount is returned for
// withdrawal.
func TestCancelAllUnfreezeV2AllExpiredNoRefreeze(t *testing.T) {
	sdb := newCancelTestStateDB(t)
	dp := sdb.DynamicProperties()
	owner := cancelTestAddr(0x02)
	sdb.CreateAccount(owner, corepb.AccountType_Normal)

	const now = int64(2_000_000)
	sdb.AddUnfreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 50*trxPrecisionState, now-10)
	sdb.AddUnfreezeV2(owner, corepb.ResourceCode_ENERGY, 70*trxPrecisionState, now)

	netWeightBefore := dp.TotalNetWeight()
	energyWeightBefore := dp.TotalEnergyWeight()

	expired, deltas := sdb.CancelAllUnfreezeV2(owner, now)

	if expired != 120*trxPrecisionState {
		t.Fatalf("expired total: got %d, want %d", expired, 120*trxPrecisionState)
	}
	if got := sdb.GetFrozenV2Amount(owner, corepb.ResourceCode_BANDWIDTH); got != 0 {
		t.Fatalf("bandwidth must not be refrozen: got %d", got)
	}
	if got := sdb.GetFrozenV2Amount(owner, corepb.ResourceCode_ENERGY); got != 0 {
		t.Fatalf("energy must not be refrozen: got %d", got)
	}
	// All entries expired -> no refreeze -> no weight delta.
	if deltas[corepb.ResourceCode_BANDWIDTH] != 0 || deltas[corepb.ResourceCode_ENERGY] != 0 {
		t.Fatalf("weight deltas must be zero when all expired: %v", deltas)
	}
	if dp.TotalNetWeight() != netWeightBefore || dp.TotalEnergyWeight() != energyWeightBefore {
		t.Fatalf("weights must be untouched: net %d->%d, energy %d->%d",
			netWeightBefore, dp.TotalNetWeight(), energyWeightBefore, dp.TotalEnergyWeight())
	}
}
