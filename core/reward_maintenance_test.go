package core

import (
	"math/big"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/reward"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
)

var _ = ethrawdb.NewMemoryDatabase // pin import

func seedWitness(t *testing.T, db ethdb.KeyValueStore, statedb *state.StateDB, addr tcommon.Address, votes int64) {
	t.Helper()
	w := types.NewWitness(addr, "")
	w.SetVoteCount(votes)
	statedb.PutWitness(addr, w.URL())
	statedb.AddWitnessVoteCount(addr, votes)
	rawdb.WriteWitness(db, addr, w)
	if err := statedb.AppendWitnessIndex(addr); err != nil {
		t.Fatal(err)
	}
}

func TestApplyRewardMaintenance_VIAccumulation(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dp := state.NewDynamicProperties()
	dp.SetNewRewardAlgorithmEffectiveCycle(0)
	dp.SetCurrentCycleNumber(5)

	addr := tcommon.BytesToAddress([]byte{0x41, 0x01})
	seedWitness(t, db, statedb, addr, 400)
	// Seed cycle 5's voter pool to 800.
	rawdb.WriteCycleReward(db, 5, addr.Bytes(), 800)

	applyRewardMaintenance(db, statedb, dp)

	// VI[5] should now hold 800 × 10^18 / 400 = 2 × 10^18.
	got := rawdb.ReadWitnessVI(db, 5, addr.Bytes())
	want := new(big.Int).Mul(big.NewInt(2), reward.DecimalOfViReward)
	if got.Cmp(want) != 0 {
		t.Fatalf("vi: got %s, want %s", got.String(), want.String())
	}
}

func TestApplyRewardMaintenance_CycleRollover(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dp := state.NewDynamicProperties()
	dp.SetChangeDelegation(true)
	dp.SetCurrentCycleNumber(7)

	addr := tcommon.BytesToAddress([]byte{0x41, 0x01})
	seedWitness(t, db, statedb, addr, 1_500)
	// Current brokerage is rooted in the witness-owned KV domain.
	if err := statedb.WriteWitnessBrokerage(addr, 15); err != nil {
		t.Fatal(err)
	}

	applyRewardMaintenance(db, statedb, dp)

	if got := dp.CurrentCycleNumber(); got != 8 {
		t.Fatalf("cycle: got %d, want 8", got)
	}
	if got := rawdb.ReadCycleBrokerage(db, 8, addr.Bytes()); got != 15 {
		t.Fatalf("brokerage snapshot: got %d, want 15", got)
	}
	if got := rawdb.ReadCycleVote(db, 8, addr.Bytes()); got != 1500 {
		t.Fatalf("vote snapshot: got %d, want 1500", got)
	}
}

func TestApplyRewardMaintenance_LegacyPath_NoOp(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dp := state.NewDynamicProperties()
	// Both flags off — nothing should happen.
	dp.SetCurrentCycleNumber(5)

	addr := tcommon.BytesToAddress([]byte{0x41, 0x01})
	seedWitness(t, db, statedb, addr, 400)

	applyRewardMaintenance(db, statedb, dp)

	if got := dp.CurrentCycleNumber(); got != 5 {
		t.Fatalf("cycle should not advance: got %d", got)
	}
	if got := rawdb.ReadWitnessVI(db, 5, addr.Bytes()); got.Sign() != 0 {
		t.Fatalf("vi should not be written: got %s", got.String())
	}
	// No cycle-8 snapshot either.
	if got := rawdb.ReadCycleVote(db, 6, addr.Bytes()); got != rawdb.RewardRemark {
		t.Fatalf("no snapshot expected, got %d", got)
	}
}

func TestApplyRewardMaintenance_VIAndCycleTogether(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dp := state.NewDynamicProperties()
	dp.SetChangeDelegation(true)
	dp.SetNewRewardAlgorithmEffectiveCycle(0)
	dp.SetCurrentCycleNumber(3)

	addr := tcommon.BytesToAddress([]byte{0x41, 0x01})
	seedWitness(t, db, statedb, addr, 250)
	rawdb.WriteCycleReward(db, 3, addr.Bytes(), 500)

	applyRewardMaintenance(db, statedb, dp)

	// VI accumulated at cycle 3 (current cycle BEFORE rollover).
	got := rawdb.ReadWitnessVI(db, 3, addr.Bytes())
	want := new(big.Int).Mul(big.NewInt(2), reward.DecimalOfViReward)
	if got.Cmp(want) != 0 {
		t.Fatalf("vi: got %s, want %s", got.String(), want.String())
	}
	// Cycle advanced.
	if got := dp.CurrentCycleNumber(); got != 4 {
		t.Fatalf("cycle: got %d, want 4", got)
	}
	// Snapshots at new cycle (4).
	if got := rawdb.ReadCycleVote(db, 4, addr.Bytes()); got != 250 {
		t.Fatalf("cycle4 vote snapshot: got %d", got)
	}
}
