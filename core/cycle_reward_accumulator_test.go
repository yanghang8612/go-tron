package core

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
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
