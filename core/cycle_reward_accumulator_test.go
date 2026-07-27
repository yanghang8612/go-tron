package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestCycleRewardAccumulatorRollbackRestoresChangedEntries(t *testing.T) {
	addrA := tcommon.BytesToAddress([]byte{1})
	addrB := tcommon.BytesToAddress([]byte{2})
	addrC := tcommon.BytesToAddress([]byte{3})
	acc := &cycleRewardAccumulator{
		cycle: 7,
		rewards: map[tcommon.Address]int64{
			addrA: 10,
			addrB: 20,
		},
	}

	snap := acc.BeginRollback()
	if handled, err := acc.AddCycleReward(7, addrA, 5); err != nil || !handled {
		t.Fatalf("add existing: handled=%v err=%v", handled, err)
	}
	if handled, err := acc.AddCycleReward(7, addrA, 2); err != nil || !handled {
		t.Fatalf("add existing twice: handled=%v err=%v", handled, err)
	}
	if handled, err := acc.AddCycleReward(7, addrB, -20); err != nil || !handled {
		t.Fatalf("delete existing: handled=%v err=%v", handled, err)
	}
	if handled, err := acc.AddCycleReward(7, addrC, 30); err != nil || !handled {
		t.Fatalf("add new: handled=%v err=%v", handled, err)
	}
	acc.Restore(snap)

	if acc.cycle != 7 || len(acc.rewards) != 2 || acc.rewards[addrA] != 10 || acc.rewards[addrB] != 20 {
		t.Fatalf("restored accumulator = cycle %d rewards %v", acc.cycle, acc.rewards)
	}
	if _, ok := acc.rewards[addrC]; ok {
		t.Fatalf("new reward survived rollback: %v", acc.rewards)
	}
}

func TestCycleRewardAccumulatorSnapshotIsIndependentCompactHandoff(t *testing.T) {
	addrA := tcommon.BytesToAddress([]byte{1})
	addrB := tcommon.BytesToAddress([]byte{2})
	acc := &cycleRewardAccumulator{
		cycle: 7,
		rewards: map[tcommon.Address]int64{
			addrA: 10,
			addrB: 20,
		},
	}

	snap := acc.Snapshot()
	acc.rewards[addrA] = 99
	delete(acc.rewards, addrB)
	db := ethrawdb.NewMemoryDatabase()
	if err := snap.Write(db); err != nil {
		t.Fatal(err)
	}
	cycle, rewards, ok, err := rawdb.ReadCycleRewardPending(db)
	if err != nil || !ok || cycle != 7 {
		t.Fatalf("snapshot header: cycle=%d ok=%v err=%v", cycle, ok, err)
	}
	if rewards[addrA] != 10 || rewards[addrB] != 20 {
		t.Fatalf("snapshot rewards = %v, want original values", rewards)
	}
}

var benchmarkCycleRewardSnapshot cycleRewardAccumulatorSnapshot

func TestCycleRewardAccumulatorSnapshotAllocatesOneCompactSlice(t *testing.T) {
	acc := &cycleRewardAccumulator{cycle: 7, rewards: make(map[tcommon.Address]int64, 27)}
	for i := 0; i < 27; i++ {
		acc.rewards[tcommon.BytesToAddress([]byte{byte(i + 1)})] = int64(i + 1)
	}
	allocs := testing.AllocsPerRun(100, func() {
		benchmarkCycleRewardSnapshot = acc.Snapshot()
	})
	if allocs != 1 {
		t.Fatalf("compact snapshot allocations = %v, want 1 entry slice", allocs)
	}
}

func TestCycleRewardAccumulatorRollbackCommitKeepsChanges(t *testing.T) {
	addr := tcommon.BytesToAddress([]byte{1})
	acc := &cycleRewardAccumulator{cycle: 7, rewards: map[tcommon.Address]int64{addr: 10}}

	snap := acc.BeginRollback()
	if handled, err := acc.AddCycleReward(7, addr, 5); err != nil || !handled {
		t.Fatalf("add: handled=%v err=%v", handled, err)
	}
	acc.CommitRollback(snap)

	if acc.cycle != 7 || acc.rewards[addr] != 15 {
		t.Fatalf("committed accumulator = cycle %d rewards %v", acc.cycle, acc.rewards)
	}
}

func TestCycleRewardAccumulatorRollbackScopeReusesJournal(t *testing.T) {
	addr := tcommon.BytesToAddress([]byte{1})
	acc := &cycleRewardAccumulator{cycle: 7, rewards: map[tcommon.Address]int64{addr: 10}}

	// Warm the one-entry journal capacity. Successful historical sync blocks
	// reuse it instead of allocating a full reward-map snapshot.
	snap := acc.BeginRollback()
	_, _ = acc.AddCycleReward(7, addr, 1)
	acc.Restore(snap)

	allocs := testing.AllocsPerRun(100, func() {
		snap := acc.BeginRollback()
		_, _ = acc.AddCycleReward(7, addr, 1)
		acc.Restore(snap)
	})
	if allocs != 0 {
		t.Fatalf("rollback scope allocations = %v, want 0", allocs)
	}
	if acc.rewards[addr] != 10 {
		t.Fatalf("reward after repeated rollback = %d, want 10", acc.rewards[addr])
	}
}
