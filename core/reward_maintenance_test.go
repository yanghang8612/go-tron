package core

import (
	"math/big"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/reward"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
)

var _ = ethrawdb.NewMemoryDatabase // pin import

func seedWitness(t *testing.T, statedb *state.StateDB, addr tcommon.Address, votes int64) {
	t.Helper()
	w := types.NewWitness(addr, "")
	w.SetVoteCount(votes)
	if err := statedb.SetWitnessCapsule(w); err != nil {
		t.Fatal(err)
	}
	if err := statedb.AppendWitnessIndex(addr); err != nil {
		t.Fatal(err)
	}
}

// TestBuildStandbyWitnessPaySet_OptAwareTiebreakAndFilter pins the standby-pay
// set against java WitnessStore.getWitnessStandby(allowWitnessSortOptimization()):
// votes DESC then the opt-aware tiebreak (hex DESC when set), top-127, then
// removeIf(voteCount < 1). The previous hand-rolled bytes.Compare ASC tiebreak
// ignored the flag and was direction-reversed — a latent standby-allowance
// state-root fork at a rank-127 vote tie.
func TestBuildStandbyWitnessPaySet_OptAwareTiebreakAndFilter(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)

	a := tcommon.BytesToAddress([]byte{0x41, 0x01})
	b := tcommon.BytesToAddress([]byte{0x41, 0x02})
	z := tcommon.BytesToAddress([]byte{0x41, 0x03})
	seedWitness(t, statedb, a, 100)
	seedWitness(t, statedb, b, 100) // ties with a
	seedWitness(t, statedb, z, 0)   // java removeIf(voteCount < 1)

	// opt ON: tie broken by hex DESC → hex("…4102") > hex("…4101") → b before a.
	// (Old bytes.Compare ASC put a first; direction-reversed from java.)
	set := buildStandbyWitnessPaySet(db, statedb, 1, true)
	if set == nil || len(set.witnesses) != 2 {
		t.Fatalf("want 2 witnesses (0-vote filtered), got %+v", set)
	}
	if set.witnesses[0].addr != b || set.witnesses[1].addr != a {
		t.Fatalf("opt-on tiebreak: want [b,a] (hex DESC), got [%x,%x]",
			set.witnesses[0].addr, set.witnesses[1].addr)
	}
	if set.voteSum != 200 {
		t.Fatalf("voteSum: want 200 (0-vote excluded), got %d", set.voteSum)
	}
	for _, w := range set.witnesses {
		if w.addr == z {
			t.Fatal("0-vote witness must be filtered (java removeIf voteCount<1)")
		}
	}
}

func TestApplyRewardMaintenance_VIAccumulation(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dp := state.NewDynamicProperties()
	dp.SetNewRewardAlgorithmEffectiveCycle(0)
	dp.SetCurrentCycleNumber(5)

	addr := tcommon.BytesToAddress([]byte{0x41, 0x01})
	seedWitness(t, statedb, addr, 400)
	// Seed cycle 5's voter pool to 800.
	_ = statedb.WriteCycleReward(5, addr.Bytes(), 800)

	applyRewardMaintenance(db, statedb, dp)

	// VI[5] should now hold 800 × 10^18 / 400 = 2 × 10^18.
	got := statedb.ReadWitnessVI(5, addr.Bytes())
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
	seedWitness(t, statedb, addr, 1_500)
	// Current brokerage is rooted in the witness-owned KV domain.
	if err := statedb.WriteWitnessBrokerage(addr, 15); err != nil {
		t.Fatal(err)
	}

	applyRewardMaintenance(db, statedb, dp)

	if got := dp.CurrentCycleNumber(); got != 8 {
		t.Fatalf("cycle: got %d, want 8", got)
	}
	if got := statedb.ReadCycleBrokerage(8, addr.Bytes()); got != 15 {
		t.Fatalf("brokerage snapshot: got %d, want 15", got)
	}
	if got := statedb.ReadCycleVote(8, addr.Bytes()); got != 1500 {
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
	seedWitness(t, statedb, addr, 400)

	applyRewardMaintenance(db, statedb, dp)

	if got := dp.CurrentCycleNumber(); got != 5 {
		t.Fatalf("cycle should not advance: got %d", got)
	}
	if got := statedb.ReadWitnessVI(5, addr.Bytes()); got.Sign() != 0 {
		t.Fatalf("vi should not be written: got %s", got.String())
	}
	// No cycle-8 snapshot either.
	if got := statedb.ReadCycleVote(6, addr.Bytes()); got != rawdb.RewardRemark {
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
	seedWitness(t, statedb, addr, 250)
	_ = statedb.WriteCycleReward(3, addr.Bytes(), 500)

	applyRewardMaintenance(db, statedb, dp)

	// VI accumulated at cycle 3 (current cycle BEFORE rollover).
	got := statedb.ReadWitnessVI(3, addr.Bytes())
	want := new(big.Int).Mul(big.NewInt(2), reward.DecimalOfViReward)
	if got.Cmp(want) != 0 {
		t.Fatalf("vi: got %s, want %s", got.String(), want.String())
	}
	// Cycle advanced.
	if got := dp.CurrentCycleNumber(); got != 4 {
		t.Fatalf("cycle: got %d, want 4", got)
	}
	// Snapshots at new cycle (4).
	if got := statedb.ReadCycleVote(4, addr.Bytes()); got != 250 {
		t.Fatalf("cycle4 vote snapshot: got %d", got)
	}
}
