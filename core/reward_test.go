package core

import (
	"math/big"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/reward"
	"github.com/tronprotocol/go-tron/core/state"
)

func newTestStateDB(t *testing.T) *state.StateDB {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	sdb2, err := state.New(tcommon.Hash{}, sdb)
	if err != nil {
		t.Fatal(err)
	}
	return sdb2
}

func TestPayBlockReward_LegacyFlat(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dp := state.NewDynamicProperties()
	// ChangeDelegation is false → legacy flat path.
	addr := tcommon.BytesToAddress([]byte{0x41, 0x01})
	statedb.CreateAccount(addr, 0)

	payBlockReward(db, statedb, dp, addr, 1000)

	if got := statedb.GetAllowance(addr); got != 1000 {
		t.Fatalf("legacy allowance: got %d, want 1000", got)
	}
	// Voter pool should be zero — legacy path doesn't touch it.
	if got := rawdb.ReadCycleReward(db, 0, addr.Bytes()); got != 0 {
		t.Fatalf("cycle reward: got %d, want 0", got)
	}
}

func TestPayBlockReward_BrokerageSplit(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dp := state.NewDynamicProperties()
	dp.SetChangeDelegation(true)
	dp.SetCurrentCycleNumber(5)
	addr := tcommon.BytesToAddress([]byte{0x41, 0x01})
	statedb.CreateAccount(addr, 0)

	// Brokerage default = 20% → witness keeps 20, voters get 80.
	payBlockReward(db, statedb, dp, addr, 100)

	if got := statedb.GetAllowance(addr); got != 20 {
		t.Fatalf("witness allowance: got %d, want 20", got)
	}
	if got := rawdb.ReadCycleReward(db, 5, addr.Bytes()); got != 80 {
		t.Fatalf("voter pool: got %d, want 80", got)
	}
}

func TestPayBlockReward_CustomBrokerage(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dp := state.NewDynamicProperties()
	dp.SetChangeDelegation(true)
	dp.SetCurrentCycleNumber(3)
	addr := tcommon.BytesToAddress([]byte{0x41, 0x01})
	statedb.CreateAccount(addr, 0)

	// Write 50% brokerage snapshot for cycle 3.
	rawdb.WriteCycleBrokerage(db, 3, addr.Bytes(), 50)

	payBlockReward(db, statedb, dp, addr, 1000)

	if got := statedb.GetAllowance(addr); got != 500 {
		t.Fatalf("witness allowance: got %d, want 500", got)
	}
	if got := rawdb.ReadCycleReward(db, 3, addr.Bytes()); got != 500 {
		t.Fatalf("voter pool: got %d, want 500", got)
	}
}

func TestPayBlockReward_AccumulatesAcrossBlocks(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dp := state.NewDynamicProperties()
	dp.SetChangeDelegation(true)
	dp.SetCurrentCycleNumber(1)
	addr := tcommon.BytesToAddress([]byte{0x41, 0x01})
	statedb.CreateAccount(addr, 0)

	payBlockReward(db, statedb, dp, addr, 100)
	payBlockReward(db, statedb, dp, addr, 100)
	payBlockReward(db, statedb, dp, addr, 100)

	// Three 20-share witness commissions.
	if got := statedb.GetAllowance(addr); got != 60 {
		t.Fatalf("accumulated allowance: got %d, want 60", got)
	}
	if got := rawdb.ReadCycleReward(db, 1, addr.Bytes()); got != 240 {
		t.Fatalf("accumulated pool: got %d, want 240", got)
	}
}

func TestAccumulateWitnessVi_FirstReward(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	addr := []byte{0x41, 0x01}

	// Seed cycle 5 with reward 1000 and voteCount 200.
	rawdb.WriteCycleReward(db, 5, addr, 1000)
	accumulateWitnessVi(db, 5, addr, 200)

	got := rawdb.ReadWitnessVI(db, 5, addr)
	// delta = 1000 * 10^18 / 200 = 5 * 10^18
	want := new(big.Int).Mul(big.NewInt(5), reward.DecimalOfViReward)
	if got.Cmp(want) != 0 {
		t.Fatalf("vi: got %s, want %s", got.String(), want.String())
	}
}

func TestAccumulateWitnessVi_ForwardsPreviousWhenNoReward(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	addr := []byte{0x41, 0x01}

	// Cycle 4 has VI = 7 × 10^18 (from some prior cycle), cycle 5 has no reward.
	prevVi := new(big.Int).Mul(big.NewInt(7), reward.DecimalOfViReward)
	rawdb.WriteWitnessVI(db, 4, addr, prevVi)

	accumulateWitnessVi(db, 5, addr, 200) // no reward written for cycle 5

	got := rawdb.ReadWitnessVI(db, 5, addr)
	if got.Cmp(prevVi) != 0 {
		t.Fatalf("vi should forward prior value: got %s, want %s", got.String(), prevVi.String())
	}
}

func TestAccumulateWitnessVi_SkipsWhenVoteZero(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	addr := []byte{0x41, 0x01}

	rawdb.WriteCycleReward(db, 5, addr, 1000)
	// voteCount = 0 → should skip write (no prior VI to forward).
	accumulateWitnessVi(db, 5, addr, 0)

	got := rawdb.ReadWitnessVI(db, 5, addr)
	if got.Sign() != 0 {
		t.Fatalf("vi: expected zero (no write), got %s", got.String())
	}
}

func TestAccumulateWitnessVi_AddsToPrevious(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	addr := []byte{0x41, 0x01}

	prevVi := new(big.Int).Mul(big.NewInt(3), reward.DecimalOfViReward)
	rawdb.WriteWitnessVI(db, 9, addr, prevVi)

	// Cycle 10 reward = 500, voteCount = 100 → delta = 5 × 10^18.
	rawdb.WriteCycleReward(db, 10, addr, 500)
	accumulateWitnessVi(db, 10, addr, 100)

	got := rawdb.ReadWitnessVI(db, 10, addr)
	// Expected: prev (3e18) + delta (5e18) = 8e18.
	want := new(big.Int).Mul(big.NewInt(8), reward.DecimalOfViReward)
	if got.Cmp(want) != 0 {
		t.Fatalf("vi: got %s, want %s", got.String(), want.String())
	}
}
