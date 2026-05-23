package dpos

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
)

// WitnessStore is the native rooted witness capsule surface needed by
// ApplyBlockStatistics.
type WitnessStore interface {
	GetWitness(common.Address) *types.Witness
	SetWitnessCapsule(*types.Witness) error
}

// ApplyBlockStatistics updates per-witness production counters and the
// BLOCK_FILLED_SLOTS rolling window after a block has been processed.
// Mirrors java-tron consensus.dpos.StatisticManager.applyBlock.
//
// previousHeadTimestamp is the timestamp of the chain head BEFORE this block
// was inserted (i.e. the parent block's time on a linear extension).
// activeWitnesses is the schedule used by GetScheduledWitness for missed-slot
// attribution; isMaintenance is computed from previousHeadTimestamp to match
// the way java-tron's DposSlot.getSlot interprets the slot calculator.
func ApplyBlockStatistics(
	witnesses WitnessStore,
	dp *state.DynamicProperties,
	block *types.Block,
	previousHeadTimestamp int64,
	activeWitnesses []common.Address,
	genesisTimestamp int64,
	isMaintenance bool,
) {
	blockNum := int64(block.Number())
	blockTime := block.Timestamp()
	producer := block.WitnessAddress()

	wc := loadOrInitWitness(witnesses, producer)
	wc.SetTotalProduced(wc.TotalProduced() + 1)
	wc.SetLatestBlockNum(blockNum)
	wc.SetLatestSlotNum(AbsoluteSlot(blockTime, genesisTimestamp))
	_ = witnesses.SetWitnessCapsule(wc)

	var slot int64 = 1
	if blockNum != 1 {
		slot = SlotForTime(blockTime, previousHeadTimestamp, genesisTimestamp,
			isMaintenance, params.MaintenanceSkipSlots)
	}

	for i := int64(1); i < slot; i++ {
		missed := GetScheduledWitness(i, previousHeadTimestamp, genesisTimestamp,
			activeWitnesses, isMaintenance, params.MaintenanceSkipSlots)
		if missed == (common.Address{}) {
			continue
		}
		m := loadOrInitWitness(witnesses, missed)
		m.SetTotalMissed(m.TotalMissed() + 1)
		_ = witnesses.SetWitnessCapsule(m)
		dp.ApplyBlockToFilledSlots(false)
	}

	dp.ApplyBlockToFilledSlots(true)
}

func loadOrInitWitness(witnesses WitnessStore, addr common.Address) *types.Witness {
	if w := witnesses.GetWitness(addr); w != nil {
		return w
	}
	return types.NewWitness(addr, "")
}
