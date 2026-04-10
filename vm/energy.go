package vm

// Energy cost tiers (Constantinople schedule).
const (
	EnergyZero         uint64 = 0
	EnergyBase         uint64 = 2
	EnergyVeryLow      uint64 = 3
	EnergyLow          uint64 = 5
	EnergyMid          uint64 = 8
	EnergyHigh         uint64 = 10
	EnergySHA3         uint64 = 30
	EnergySHA3Word     uint64 = 6
	EnergySload        uint64 = 200
	EnergySstoreSet    uint64 = 20000
	EnergySstoreReset  uint64 = 5000
	EnergySstoreRefund uint64 = 15000
	EnergyJumpDest     uint64 = 1
	EnergyExp          uint64 = 10
	EnergyExpByte      uint64 = 50
	EnergyCopy         uint64 = 3
	EnergyCall         uint64 = 700
	EnergyCallNewAcct  uint64 = 25000
	EnergyCallValueTx  uint64 = 9000
	EnergyCallStipend  uint64 = 2300
	EnergyCreate       uint64 = 32000
	EnergyBalance      uint64 = 400
	EnergyExtCodeSize  uint64 = 700
	EnergyExtCodeCopy  uint64 = 700
	EnergyExtCodeHash  uint64 = 400
	EnergyLog          uint64 = 375
	EnergyLogTopic     uint64 = 375
	EnergyLogData      uint64 = 8
	EnergyCodeDeposit  uint64 = 200
	EnergySelfDestruct uint64 = 5000
	EnergyMemory       uint64 = 3
	EnergyBlockHash    uint64 = 20
	EnergySelfBalance  uint64 = 5
)

// maxCodeSize is the maximum contract code size (24KB, EIP-170).
const maxCodeSize = 24576

// memoryEnergyCost calculates energy cost for memory expansion.
// Cost = words * 3 + words^2 / 512
func memoryEnergyCost(size uint64) uint64 {
	words := toWordSize(size)
	return words*EnergyMemory + (words*words)/512
}

// toWordSize returns the number of 32-byte words needed for size bytes.
func toWordSize(size uint64) uint64 {
	if size == 0 {
		return 0
	}
	return (size + 31) / 32
}

// memoryExpansionCost returns the additional energy cost for expanding memory
// from its current size to the required size.
func memoryExpansionCost(mem *Memory, offset, size uint64) uint64 {
	if size == 0 {
		return 0
	}
	newSize := offset + size
	if uint64(mem.len()) >= newSize {
		return 0
	}
	newCost := memoryEnergyCost(newSize)
	oldCost := memoryEnergyCost(uint64(mem.len()))
	return newCost - oldCost
}
