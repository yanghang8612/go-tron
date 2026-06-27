package vm

import (
	"encoding/binary"

	"github.com/holiman/uint256"
	tcommon "github.com/tronprotocol/go-tron/common"
)

// PrecompiledContract is the interface for precompiled contracts.
// Run is called with the TVM context, caller address, input bytes, and available energy.
// Returns (output, energyConsumed, error). On ErrOutOfEnergy, energyConsumed == energy.
type PrecompiledContract interface {
	Run(tvm *TVM, caller tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error)
}

type precompiledContractWithStatus interface {
	RunWithStatus(tvm *TVM, caller tcommon.Address, input []byte, energy uint64) ([]byte, uint64, bool, error)
}

func runPrecompile(tvm *TVM, p PrecompiledContract, caller tcommon.Address, input []byte, energy uint64) ([]byte, uint64, bool, error) {
	if ps, ok := p.(precompiledContractWithStatus); ok {
		return ps.RunWithStatus(tvm, caller, input, energy)
	}
	ret, used, err := p.Run(tvm, caller, input, energy)
	return ret, used, err == nil, err
}

// addrFromUint constructs a TRON precompile address from a uint64 discriminant.
//
// TRON uses 21-byte addresses with 0x41 prefix. The CALL opcode converts a
// 256-bit stack value to an address via:
//
//	addr[0]  = 0x41
//	addr[1:] = last 20 bytes of the 256-bit value (big-endian)
//
// So addrFromUint(1) == address 0x41 00…01, which is what Solidity produces
// for address(1) in the TVM.
func addrFromUint(n uint64) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	binary.BigEndian.PutUint64(addr[13:21], n)
	return addr
}

// getPrecompile returns the precompiled contract for addr given the current fork
// configuration, or nil if addr is not a precompile (or its fork gate is inactive).
func getPrecompile(addr tcommon.Address, cfg TVMConfig) PrecompiledContract {
	switch addr {
	// ── Standard TVM precompiles (always active) ─────────────────────────
	case addrFromUint(0x01):
		return &ecRecover{}
	case addrFromUint(0x02):
		return &sha256hash{}
	case addrFromUint(0x03):
		return &tronRipemd160{}
	case addrFromUint(0x04):
		return &dataCopy{}
	case addrFromUint(0x05):
		return &bigModExp{istanbul: cfg.Istanbul, osaka: cfg.Osaka, cpuTimeGuard: cfg.CpuTimeGuard}
	case addrFromUint(0x06):
		return &bn128Add{istanbul: cfg.Istanbul}
	case addrFromUint(0x07):
		return &bn128Mul{istanbul: cfg.Istanbul}
	case addrFromUint(0x08):
		return &bn128Pairing{istanbul: cfg.Istanbul}

	// ── TRON signature precompiles (AllowTvmSolidity059) ─────────────────
	case addrFromUint(0x09):
		if cfg.Solidity059 {
			return &batchValidateSign{}
		}
	case addrFromUint(0x0a):
		if cfg.Solidity059 {
			return &validateMultiSign{}
		}

	// ── Shielded token precompiles (AllowTvmShieldedToken) ───────────────
	case addrFromUint(0x01000001):
		if cfg.ShieldedToken {
			return &verifyMintProof{}
		}
	case addrFromUint(0x01000002):
		if cfg.ShieldedToken {
			return &verifyTransferProof{}
		}
	case addrFromUint(0x01000003):
		if cfg.ShieldedToken {
			return &verifyBurnProof{}
		}
	case addrFromUint(0x01000004):
		if cfg.ShieldedToken {
			return &shieldedMerkleHash{}
		}

	// ── Voting precompiles (AllowTvmVote) ────────────────────────────────
	case addrFromUint(0x01000005):
		if cfg.Vote {
			return &rewardBalance{}
		}
	case addrFromUint(0x01000006):
		if cfg.Vote {
			return &isSrCandidate{}
		}
	case addrFromUint(0x01000007):
		if cfg.Vote {
			return &voteCount{}
		}
	case addrFromUint(0x01000008):
		if cfg.Vote {
			return &usedVoteCount{}
		}
	case addrFromUint(0x01000009):
		if cfg.Vote {
			return &receivedVoteCount{}
		}
	case addrFromUint(0x0100000a):
		if cfg.Vote {
			return &totalVoteCount{}
		}

	// ── StakingV2 precompiles (AllowStakingV2) ───────────────────────────
	case addrFromUint(0x0100000b):
		if cfg.StakingV2 {
			return &getChainParameter{}
		}
	case addrFromUint(0x0100000c):
		if cfg.StakingV2 {
			return &availableUnfreezeV2Size{}
		}
	case addrFromUint(0x0100000d):
		if cfg.StakingV2 {
			return &unfreezableBalanceV2{}
		}
	case addrFromUint(0x0100000e):
		if cfg.StakingV2 {
			return &expireUnfreezeBalanceV2{}
		}
	case addrFromUint(0x0100000f):
		if cfg.StakingV2 {
			return &delegatableResource{}
		}
	case addrFromUint(0x01000010):
		if cfg.StakingV2 {
			return &resourceV2{}
		}
	case addrFromUint(0x01000011):
		if cfg.StakingV2 {
			return &checkUnDelegateResource{}
		}
	case addrFromUint(0x01000012):
		if cfg.StakingV2 {
			return &resourceUsage{}
		}
	case addrFromUint(0x01000013):
		if cfg.StakingV2 {
			return &totalResource{}
		}
	case addrFromUint(0x01000014):
		if cfg.StakingV2 {
			return &totalDelegatedResource{}
		}
	case addrFromUint(0x01000015):
		if cfg.StakingV2 {
			return &totalAcquiredResource{}
		}

	// ── TVM compatibility precompiles (AllowTvmCompatibleEvm) ────────────
	case addrFromUint(0x020003):
		if cfg.Compatibility {
			return &ethRipemd160{}
		}
	case addrFromUint(0x020009):
		if cfg.Compatibility {
			return &blake2F{}
		}

	// ── Osaka precompile (AllowTvmOsaka) ─────────────────────────────────
	case addrFromUint(0x0100):
		if cfg.Osaka {
			return &p256Verify{}
		}
	}
	return nil
}

// ── Input helpers ─────────────────────────────────────────────────────────────

// getInput returns input[offset:offset+size], zero-padded if the input is shorter.
func getInput(input []byte, offset, size uint64) []byte {
	result := make([]byte, size)
	if offset >= uint64(len(input)) {
		return result
	}
	copy(result, input[offset:])
	return result
}

// int64ToBytes32 encodes an int64 as a 32-byte big-endian word (right-aligned).
func int64ToBytes32(v int64) []byte {
	b := make([]byte, 32)
	binary.BigEndian.PutUint64(b[24:], uint64(v))
	return b
}

// tronAddrFromWord decodes a TRON address from a 32-byte ABI word.
// Solidity encodes an address as the last 20 bytes of a 32-byte word; TRON
// adds the 0x41 prefix to reconstruct the full 21-byte address.
func tronAddrFromWord(word []byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	if len(word) >= 32 {
		copy(addr[1:], word[12:32])
	} else if len(word) >= 20 {
		copy(addr[1:], word[len(word)-20:])
	}
	return addr
}

// parseWord32 returns input[offset:offset+32], zero-padded if needed.
func parseWord32(input []byte, offset int) []byte {
	return getInput(input, uint64(offset), 32)
}

// parseUint64FromWord reads a uint64 from the last 8 bytes of a 32-byte word at offset.
func parseUint64FromWord(input []byte, offset int) uint64 {
	w := parseWord32(input, offset)
	return binary.BigEndian.Uint64(w[24:])
}

// parseInt64FromWord reads an int64 from a 32-byte word (using uint64 then cast).
func parseInt64FromWord(input []byte, offset int) int64 {
	return int64(parseUint64FromWord(input, offset))
}

// parseLenIntValueSafe mirrors java PrecompiledContracts.parseLen:
// `new DataWord(parseBytes(data, idx, 32)).intValueSafe()`. A length word occupying
// more than 4 bytes (or whose low-32 is negative) saturates to Integer.MAX_VALUE, not
// its truncated low-64 bits — so a huge length no longer slips past a `> bound` guard
// as a small (or zero) value.
func parseLenIntValueSafe(input []byte, offset int) uint64 {
	w := new(uint256.Int).SetBytes(parseWord32(input, offset))
	return uint64(wordToIntValueSafe(w))
}

// parseInt64SafeFromWord mirrors java DataWord.longValueSafe(): a 32-byte word
// whose value does not fit a signed int64 (any of the high 24 bytes set, or the
// low-8 high bit set) saturates to maxInt64 instead of wrapping/truncating.
// TRON's V2 staking/freeze query precompiles decode their numeric scalar params
// (amount, time) this way; the raw parseInt64FromWord would feed a wrapped or
// negative value where java uses Long.MAX_VALUE.
func parseInt64SafeFromWord(input []byte, offset int) int64 {
	const maxInt64 = int64(^uint64(0) >> 1)
	w := parseWord32(input, offset)
	for _, b := range w[:24] {
		if b != 0 {
			return maxInt64
		}
	}
	if v := int64(binary.BigEndian.Uint64(w[24:])); v >= 0 {
		return v
	}
	return maxInt64
}

// javaIntMaxValue mirrors java's Integer.MAX_VALUE, returned by
// DataWord.intValueSafe on overflow.
const javaIntMaxValue = 2147483647

// wordCount mirrors DataWord.parseArray: words.length = data.length / WORD_SIZE
// (truncating any trailing partial word).
func wordCount(input []byte) int {
	return len(input) / tronPrecompileWordSize
}

// wordIntValueSafe reads the word at word index wordIdx of input and returns its
// value with java DataWord.intValueSafe semantics: if any byte above the low 4
// is non-zero, or the low-4-byte signed int is negative (>= 2^31), it saturates
// to Integer.MAX_VALUE. ok is false when wordIdx is out of the truncated word
// array (mirroring DataWord.parseArray's AIOOBE on words[wordIdx]).
func wordIntValueSafe(input []byte, wordIdx int) (value int, ok bool) {
	if wordIdx < 0 || wordIdx >= wordCount(input) {
		return 0, false
	}
	start := wordIdx * tronPrecompileWordSize
	w := input[start : start+tronPrecompileWordSize]
	// bytesOccupied > 4  ⇔  any non-zero byte in w[0:28].
	for i := 0; i < tronPrecompileWordSize-4; i++ {
		if w[i] != 0 {
			return javaIntMaxValue, true
		}
	}
	// low 4 bytes as a signed 32-bit int (java intValue keeps the low 32 bits).
	v := int32(uint32(w[28])<<24 | uint32(w[29])<<16 | uint32(w[30])<<8 | uint32(w[31]))
	if v < 0 {
		return javaIntMaxValue, true
	}
	return int(v), true
}
