package core

import (
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
)

type cycleRewardAccumulator struct {
	cycle   int64
	rewards map[tcommon.Address]int64
}

type cycleRewardAccumulatorSnapshot struct {
	cycle   int64
	rewards map[tcommon.Address]int64
}

func newCycleRewardAccumulator(reader ethdb.KeyValueReader) (*cycleRewardAccumulator, error) {
	cycle, rewards, ok, err := rawdb.ReadCycleRewardPending(reader)
	if err != nil {
		return nil, err
	}
	if !ok || rewards == nil {
		rewards = make(map[tcommon.Address]int64)
	}
	return &cycleRewardAccumulator{cycle: cycle, rewards: rewards}, nil
}

func newEmptyCycleRewardAccumulator() *cycleRewardAccumulator {
	return &cycleRewardAccumulator{rewards: make(map[tcommon.Address]int64)}
}

func (a *cycleRewardAccumulator) AddCycleReward(cycle int64, addr tcommon.Address, delta int64) (bool, error) {
	if a == nil || delta == 0 {
		return true, nil
	}
	if !a.canTrackCycle(cycle) {
		return false, nil
	}
	next := a.rewards[addr] + delta
	if next == 0 {
		delete(a.rewards, addr)
		return true, nil
	}
	a.rewards[addr] = next
	return true, nil
}

func (a *cycleRewardAccumulator) AddCycleRewards(cycle int64, deltas map[tcommon.Address]int64) (bool, error) {
	if a == nil || len(deltas) == 0 {
		return true, nil
	}
	if !a.canTrackCycle(cycle) {
		return false, nil
	}
	for addr, delta := range deltas {
		if delta == 0 {
			continue
		}
		next := a.rewards[addr] + delta
		if next == 0 {
			delete(a.rewards, addr)
			continue
		}
		a.rewards[addr] = next
	}
	return true, nil
}

func (a *cycleRewardAccumulator) PendingCycleReward(cycle int64, addr tcommon.Address) (int64, bool) {
	if a == nil || len(a.rewards) == 0 || a.cycle != cycle {
		return 0, false
	}
	amount, ok := a.rewards[addr]
	if !ok || amount == 0 {
		return 0, false
	}
	return amount, true
}

func (a *cycleRewardAccumulator) Snapshot() cycleRewardAccumulatorSnapshot {
	if a == nil {
		return cycleRewardAccumulatorSnapshot{rewards: make(map[tcommon.Address]int64)}
	}
	return cycleRewardAccumulatorSnapshot{
		cycle:   a.cycle,
		rewards: copyCycleRewardMap(a.rewards),
	}
}

func (a *cycleRewardAccumulator) Restore(snap cycleRewardAccumulatorSnapshot) {
	if a == nil {
		return
	}
	a.cycle = snap.cycle
	a.rewards = copyCycleRewardMap(snap.rewards)
	if a.rewards == nil {
		a.rewards = make(map[tcommon.Address]int64)
	}
}

func (a *cycleRewardAccumulator) FlushCycleToState(statedb *state.StateDB, cycle int64) error {
	if a == nil || statedb == nil || len(a.rewards) == 0 || a.cycle != cycle {
		return nil
	}
	deltas := copyCycleRewardMap(a.rewards)
	a.rewards = make(map[tcommon.Address]int64)
	statedb.SetCycleRewardSink(nil)
	err := statedb.AddCycleRewardsFinal(cycle, deltas)
	statedb.SetCycleRewardSink(a)
	return err
}

func (a *cycleRewardAccumulator) Write(writer ethdb.KeyValueWriter) error {
	if a == nil || len(a.rewards) == 0 {
		return rawdb.DeleteCycleRewardPending(writer)
	}
	return rawdb.WriteCycleRewardPending(writer, a.cycle, a.rewards)
}

// Write persists a captured snapshot of the pending accumulator. The async
// commit worker captures bc.cycleRewards.Snapshot() at handoff (a deep copy)
// and writes it to the committing block's buffer layer, so it is unaffected by
// the foreground advancing bc.cycleRewards for the next block. Byte-identical
// to (*cycleRewardAccumulator).Write for the same contents.
func (snap cycleRewardAccumulatorSnapshot) Write(writer ethdb.KeyValueWriter) error {
	if len(snap.rewards) == 0 {
		return rawdb.DeleteCycleRewardPending(writer)
	}
	return rawdb.WriteCycleRewardPending(writer, snap.cycle, snap.rewards)
}

func (a *cycleRewardAccumulator) canTrackCycle(cycle int64) bool {
	if a == nil {
		return false
	}
	if len(a.rewards) == 0 {
		a.cycle = cycle
		return true
	}
	return a.cycle == cycle
}

func copyCycleRewardMap(in map[tcommon.Address]int64) map[tcommon.Address]int64 {
	if len(in) == 0 {
		return make(map[tcommon.Address]int64)
	}
	out := make(map[tcommon.Address]int64, len(in))
	for addr, amount := range in {
		if amount != 0 {
			out[addr] = amount
		}
	}
	return out
}
