package vm

import "github.com/holiman/uint256"

// Energy cost tiers — aligned with java-tron EnergyCost.java.
const (
	EnergyZero         uint64 = 0
	EnergyBase         uint64 = 2  // BASE_TIER
	EnergyVeryLow      uint64 = 3  // VERY_LOW_TIER
	EnergyLow          uint64 = 5  // LOW_TIER
	EnergyMid          uint64 = 8  // MID_TIER
	EnergyHigh         uint64 = 10 // HIGH_TIER
	EnergyExt          uint64 = 20 // EXT_TIER (BALANCE, EXTCODESIZE, etc.)
	EnergySHA3         uint64 = 30
	EnergySHA3Word     uint64 = 6
	EnergySload        uint64 = 50 // java-tron: SLOAD = 50
	EnergySstoreSet    uint64 = 20000
	EnergySstoreReset  uint64 = 5000
	EnergySstoreRefund uint64 = 15000
	EnergyJumpDest     uint64 = 1
	// EnergySpecial mirrors java-tron's `SPECIAL_TIER = 1` (EnergyCost.java:20).
	// Used by JUMPDEST always, and by MLOAD/MSTORE/MSTORE8 only when the
	// `allow_higher_limit_for_max_cpu_time_of_one_tx` proposal (#65) is
	// active, via `OperationRegistry.adjustMemOperations`.
	EnergySpecial      uint64 = 1
	EnergyExp          uint64 = 10
	EnergyExpByte      uint64 = 10 // java-tron: EXP_BYTE_ENERGY = 10
	EnergyCopy         uint64 = 3
	EnergyCall         uint64 = 40    // java-tron: CALL_ENERGY = 40
	EnergyCallNewAcct  uint64 = 25000 // java-tron: NEW_ACCT_CALL = 25000
	EnergyCallValueTx  uint64 = 9000  // java-tron: VT_CALL = 9000
	EnergyCallStipend  uint64 = 2300  // java-tron: STIPEND_CALL = 2300
	EnergyCreate       uint64 = 32000
	EnergyBalance      uint64 = 20 // java-tron: BALANCE = EXT_TIER = 20
	EnergyExtCodeSize  uint64 = 20 // java-tron: EXT_TIER = 20
	EnergyExtCodeCopy  uint64 = 20 // java-tron: EXT_TIER = 20 (base; +per-word)
	EnergyExtCodeHash  uint64 = 400
	EnergyLog          uint64 = 375
	EnergyLogTopic     uint64 = 375
	EnergyLogData      uint64 = 8
	EnergyCodeDeposit  uint64 = 200
	EnergySelfDestruct uint64 = 5000
	EnergyMemory       uint64 = 3
	EnergyBlockHash    uint64 = 20
	EnergySelfBalance  uint64 = 5

	// TRON-specific staking / governance opcode costs (java-tron EnergyCost.java)
	EnergyFreeze                 uint64 = 20000
	EnergyUnfreeze               uint64 = 20000
	EnergyFreezeExpireTime       uint64 = 50
	EnergyFreezeV2               uint64 = 10000
	EnergyUnfreezeV2             uint64 = 10000
	EnergyWithdrawExpireUnfreeze uint64 = 10000
	EnergyCancelAllUnfreezeV2    uint64 = 10000
	EnergyDelegateResource       uint64 = 10000
	EnergyUnDelegateResource     uint64 = 10000
	EnergyVoteWitness            uint64 = 30000
	EnergyWithdrawReward         uint64 = 20000
	EnergyIsContract             uint64 = 100 // checking code existence
	EnergyTokenBalance           uint64 = 20  // same as BALANCE
	EnergyTLoad                  uint64 = 100 // EIP-1153 transient storage
	EnergyTStore                 uint64 = 100
)

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

func checkedMemoryExpansionCost(mem *Memory, offset, size uint64, op OpCode) (uint64, error) {
	if size == 0 {
		return 0, nil
	}
	if offset > tvmMemoryLimit || size > tvmMemoryLimit || offset > tvmMemoryLimit-size {
		return 0, newOutOfMemoryError(op)
	}
	return memoryExpansionCost(mem, offset, size), nil
}

func checkedMemoryExpansionCostWords(mem *Memory, offset, size *uint256.Int, op OpCode) (uint64, uint64, uint64, error) {
	if size.IsZero() {
		return offset.Uint64(), 0, 0, nil
	}
	if !offset.IsUint64() || !size.IsUint64() {
		return 0, 0, 0, newOutOfMemoryError(op)
	}
	off := offset.Uint64()
	sz := size.Uint64()
	cost, err := checkedMemoryExpansionCost(mem, off, sz, op)
	return off, sz, cost, err
}

func checkedMemoryExpansionCostFixed(mem *Memory, offset *uint256.Int, size uint64, op OpCode) (uint64, uint64, error) {
	if size == 0 {
		return offset.Uint64(), 0, nil
	}
	if !offset.IsUint64() {
		return 0, 0, newOutOfMemoryError(op)
	}
	off := offset.Uint64()
	cost, err := checkedMemoryExpansionCost(mem, off, size, op)
	return off, cost, err
}
