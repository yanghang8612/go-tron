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

	// rollbackJournal records the pre-block value of only the rewards that a
	// block actually changes. Historical sync normally commits every block, so
	// deep-copying the whole (usually 27-entry) reward map solely for the rare
	// failure path wastes a map and its buckets on every successful block.
	rollbackJournal  []cycleRewardRollbackChange
	rollbackBoundary int
	rollbackActive   bool
}

type cycleRewardAccumulatorSnapshot struct {
	cycle   int64
	rewards map[tcommon.Address]int64
}

type cycleRewardAccumulatorRollback struct {
	cycle    int64
	boundary int
}

type cycleRewardRollbackChange struct {
	addr    tcommon.Address
	amount  int64
	existed bool
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
	a.journalReward(addr)
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
		a.journalReward(addr)
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

// BeginRollback starts a lazy rollback scope. Only the first pre-mutation
// value of each address is journaled; taking the snapshot itself is allocation
// free. The accumulator has one foreground owner, so rollback scopes are not
// nested.
func (a *cycleRewardAccumulator) BeginRollback() cycleRewardAccumulatorRollback {
	if a == nil {
		return cycleRewardAccumulatorRollback{}
	}
	if a.rollbackActive {
		panic("cycle reward accumulator: nested rollback scope")
	}
	a.rollbackActive = true
	a.rollbackBoundary = len(a.rollbackJournal)
	return cycleRewardAccumulatorRollback{cycle: a.cycle, boundary: a.rollbackBoundary}
}

func (a *cycleRewardAccumulator) CommitRollback(snap cycleRewardAccumulatorRollback) {
	if a == nil {
		return
	}
	a.validateRollback(snap)
	a.rollbackJournal = a.rollbackJournal[:snap.boundary]
	a.rollbackActive = false
}

func (a *cycleRewardAccumulator) Restore(snap cycleRewardAccumulatorRollback) {
	if a == nil {
		return
	}
	a.validateRollback(snap)
	if a.rewards == nil {
		a.rewards = make(map[tcommon.Address]int64)
	}
	for i := len(a.rollbackJournal) - 1; i >= snap.boundary; i-- {
		change := a.rollbackJournal[i]
		if change.existed {
			a.rewards[change.addr] = change.amount
		} else {
			delete(a.rewards, change.addr)
		}
	}
	a.rollbackJournal = a.rollbackJournal[:snap.boundary]
	a.cycle = snap.cycle
	a.rollbackActive = false
}

func (a *cycleRewardAccumulator) validateRollback(snap cycleRewardAccumulatorRollback) {
	if !a.rollbackActive || snap.boundary != a.rollbackBoundary || snap.boundary < 0 || snap.boundary > len(a.rollbackJournal) {
		panic("cycle reward accumulator: invalid rollback scope")
	}
}

func (a *cycleRewardAccumulator) journalReward(addr tcommon.Address) {
	if a == nil || !a.rollbackActive {
		return
	}
	for i := len(a.rollbackJournal) - 1; i >= a.rollbackBoundary; i-- {
		if a.rollbackJournal[i].addr == addr {
			return
		}
	}
	amount, existed := a.rewards[addr]
	a.rollbackJournal = append(a.rollbackJournal, cycleRewardRollbackChange{
		addr:    addr,
		amount:  amount,
		existed: existed,
	})
}

func (a *cycleRewardAccumulator) journalAllRewards() {
	if a == nil || !a.rollbackActive {
		return
	}
	for addr := range a.rewards {
		a.journalReward(addr)
	}
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

func (a *cycleRewardAccumulator) FlushCycleToState(statedb *state.StateDB, cycle int64) error {
	if a == nil || statedb == nil || len(a.rewards) == 0 || a.cycle != cycle {
		return nil
	}
	deltas := copyCycleRewardMap(a.rewards)
	a.journalAllRewards()
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
