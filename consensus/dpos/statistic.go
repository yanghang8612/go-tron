package dpos

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
)

// KVReadWriter is the narrow ethdb capability ApplyBlockStatistics needs:
// per-witness ReadWitness lookups plus WriteWitness updates. Both
// rawdb.NewMemoryDatabase() (an ethdb.KeyValueStore) and
// blockbuffer.Buffer satisfy this interface, letting callers route the
// writes either to disk directly or through the fork-rewind buffer.
type KVReadWriter interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
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
//
// Witness records are written via rawdb.WriteWitness — the in-memory statedb
// witness cache is not used because nothing else mutates these counters in the
// same applyBlock pass.
func ApplyBlockStatistics(
	db KVReadWriter,
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

	wc := loadOrInitWitness(db, producer)
	wc.SetTotalProduced(wc.TotalProduced() + 1)
	wc.SetLatestBlockNum(blockNum)
	wc.SetLatestSlotNum(AbsoluteSlot(blockTime, genesisTimestamp))
	rawdb.WriteWitness(db, producer, wc)

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
		m := loadOrInitWitness(db, missed)
		m.SetTotalMissed(m.TotalMissed() + 1)
		rawdb.WriteWitness(db, missed, m)
		dp.ApplyBlockToFilledSlots(false)
	}

	dp.ApplyBlockToFilledSlots(true)
}

func loadOrInitWitness(db ethdb.KeyValueReader, addr common.Address) *types.Witness {
	if w := rawdb.ReadWitness(db, addr); w != nil {
		return w
	}
	return types.NewWitness(addr, "")
}
