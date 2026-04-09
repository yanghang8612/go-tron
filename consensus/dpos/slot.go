package dpos

import "github.com/tronprotocol/go-tron/params"

func AbsoluteSlot(timestamp, genesisTime int64) int64 {
	return (timestamp - genesisTime) / params.BlockProducedInterval
}

func SlotTime(slot int64, headTimestamp, genesisTime int64, isMaintenance bool, maintenanceSkipSlots int64) int64 {
	if slot == 0 {
		return 0
	}
	interval := int64(params.BlockProducedInterval)

	if headTimestamp == genesisTime {
		return genesisTime + slot*interval
	}

	if isMaintenance {
		slot += maintenanceSkipSlots
	}

	aligned := headTimestamp - ((headTimestamp - genesisTime) % interval)
	return aligned + interval*slot
}

func SlotForTime(timestamp, headTimestamp, genesisTime int64, isMaintenance bool, maintenanceSkipSlots int64) int64 {
	firstSlotTime := SlotTime(1, headTimestamp, genesisTime, isMaintenance, maintenanceSkipSlots)
	if timestamp < firstSlotTime {
		return 0
	}
	return (timestamp-firstSlotTime)/int64(params.BlockProducedInterval) + 1
}

func WitnessIndex(absoluteSlot int64, witnessCount int) int {
	if witnessCount <= 0 {
		return 0
	}
	idx := absoluteSlot % int64(witnessCount*params.SingleRepeat)
	return int(idx / params.SingleRepeat)
}
