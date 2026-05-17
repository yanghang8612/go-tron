package vm

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/crypto/blake2b"
	tcommon "github.com/tronprotocol/go-tron/common"
	"golang.org/x/crypto/ripemd160"
)

// ── 0x020003 EthRipemd160 ─────────────────────────────────────────────────────
//
// Standard RIPEMD-160 (the actual hash function, unlike 0x03 which is a
// TRON-specific double-SHA256 variant). Result is 20 bytes right-padded to 32.

type ethRipemd160 struct{}

func (c *ethRipemd160) Run(_ *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	cost := 600 + 120*toWordSize(uint64(len(input)))
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	h := ripemd160.New()
	h.Write(input)
	digest := h.Sum(nil) // 20 bytes
	result := make([]byte, 32)
	copy(result[12:], digest) // left-pad to 32 bytes
	return result, cost, nil
}

// ── 0x020009 Blake2F ──────────────────────────────────────────────────────────
//
// Implements the Blake2b F compression function (EIP-152).
// Input must be exactly 213 bytes:
//   - [0:4]   rounds (big-endian uint32)
//   - [4:68]  state vector h[0..7] (little-endian uint64 each)
//   - [68:196] message block m[0..15] (little-endian uint64 each)
//   - [196:212] offset counters t[0..1] (little-endian uint64 each)
//   - [212]   final block indicator (0 or 1)
// Output: 64 bytes (updated state vector, little-endian).

const blake2FInputLen = 213

type blake2F struct{}

func (c *blake2F) Run(_ *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	ret, used, _, err := c.RunWithStatus(nil, tcommon.Address{}, input, energy)
	return ret, used, err
}

func (c *blake2F) RunWithStatus(_ *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, bool, error) {
	if len(input) != blake2FInputLen {
		return make([]byte, 32), 0, false, nil
	}
	if input[212] != 0 && input[212] != 1 {
		return make([]byte, 32), 0, false, nil
	}

	rounds := binary.BigEndian.Uint32(input[0:4])
	cost := uint64(rounds)
	if energy < cost {
		return nil, energy, false, ErrOutOfEnergy
	}

	final := input[212] == 1

	var h [8]uint64
	for i := range h {
		h[i] = binary.LittleEndian.Uint64(input[4+i*8:])
	}
	var m [16]uint64
	for i := range m {
		m[i] = binary.LittleEndian.Uint64(input[68+i*8:])
	}
	var t [2]uint64
	t[0] = binary.LittleEndian.Uint64(input[196:204])
	t[1] = binary.LittleEndian.Uint64(input[204:212])

	blake2b.F(&h, m, t, final, rounds)

	out := make([]byte, 64)
	for i, v := range h {
		binary.LittleEndian.PutUint64(out[i*8:], v)
	}
	return out, cost, true, nil
}
