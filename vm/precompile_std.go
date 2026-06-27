package vm

import (
	"crypto/sha256"
	"errors"
	"math/big"
	"math/bits"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/crypto/bn256"
	tcommon "github.com/tronprotocol/go-tron/common"
)

// ── 0x01 ECRecover ────────────────────────────────────────────────────────────

type ecRecover struct{}

func (c *ecRecover) Run(_ *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	const cost = 3000
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}

	// Input: hash(32) + v(32) + r(32) + s(32)
	in := getInput(input, 0, 128)
	hash := in[0:32]
	r := in[64:96]
	s := in[96:128]

	// java ECRecover.validateV: every byte of the v word except the last must be
	// zero. A dirty high byte → reject (gtron previously read only in[63]).
	for _, b := range in[32:63] {
		if b != 0 {
			return nil, cost, nil
		}
	}
	// java ECKey.validateComponents: v must be exactly 27 or 28 (gtron previously
	// also accepted the raw recovery ids 0/1). r,s ∈ [1,N) is enforced identically
	// by ethcrypto.Ecrecover (libsecp256k1 rejects r/s == 0 or >= N), so no
	// explicit range check is needed to match validateComponents.
	v := in[63]
	if v != 27 && v != 28 {
		return nil, cost, nil
	}

	// Build 65-byte [r | s | v] signature (go-ethereum convention, v in {0,1})
	sig := make([]byte, 65)
	copy(sig[0:32], r)
	copy(sig[32:64], s)
	sig[64] = v - 27

	pubBytes, err := ethcrypto.Ecrecover(hash, sig)
	if err != nil || len(pubBytes) != 65 {
		// java returns EMPTY_BYTE_ARRAY on any failure; gtron previously returned
		// 32 zero bytes (diverging from both java and go-ethereum) — RETURNDATASIZE
		// and the CALL output buffer differed. Return empty to match java.
		return nil, cost, nil
	}

	// Keccak256 of the uncompressed public key (skip the 0x04 prefix byte)
	pubHash := ethcrypto.Keccak256(pubBytes[1:])

	// Ethereum address = last 20 bytes, left-padded to 32
	result := make([]byte, 32)
	copy(result[12:], pubHash[12:])
	return result, cost, nil
}

// ── 0x02 SHA256 ───────────────────────────────────────────────────────────────

type sha256hash struct{}

func (c *sha256hash) Run(_ *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	cost := 60 + 12*toWordSize(uint64(len(input)))
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	h := sha256.Sum256(input)
	return h[:], cost, nil
}

// ── 0x03 TronRipemd160 ────────────────────────────────────────────────────────
//
// java-tron's 0x03 precompile is NOT standard RIPEMD-160.  It computes
// SHA256(SHA256(input)[0:20]), returning that 32-byte hash right-padded with zeros.

type tronRipemd160 struct{}

func (c *tronRipemd160) Run(_ *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	cost := 600 + 120*toWordSize(uint64(len(input)))
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	// First SHA256
	first := sha256.Sum256(input)
	// Truncate to 20 bytes, then SHA256 again
	second := sha256.Sum256(first[:20])
	return second[:], cost, nil
}

// ── 0x04 DataCopy (Identity) ──────────────────────────────────────────────────

type dataCopy struct{}

func (c *dataCopy) Run(_ *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	cost := 15 + 3*toWordSize(uint64(len(input)))
	if energy < cost {
		return nil, energy, ErrOutOfEnergy
	}
	out := make([]byte, len(input))
	copy(out, input)
	return out, cost, nil
}

// ── 0x05 BigModExp ────────────────────────────────────────────────────────────
//
// Energy formula follows java-tron's legacy EIP-198-style pricing, with
// TIP-7883 pricing when Osaka is active.

type bigModExp struct {
	istanbul     bool
	osaka        bool
	cpuTimeGuard bool // VERSION_4_8_1_1: degenerate-input OutOfTime guard
}

func (c *bigModExp) Run(_ *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	ret, used, _, err := c.RunWithStatus(nil, tcommon.Address{}, input, energy)
	return ret, used, err
}

func (c *bigModExp) RunWithStatus(_ *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, bool, error) {
	var (
		// java PrecompiledContracts.parseLen decodes each length via
		// DataWord.intValueSafe() (saturate to Integer.MAX_VALUE for a >4-byte or
		// negative word), NOT raw low-64 bits — otherwise an expLen word of 2^64
		// truncates to 0 and skips the VERSION_4_8_1_1 degenerate OutOfTime guard.
		baseLen = parseLenIntValueSafe(input, 0)
		expLen  = parseLenIntValueSafe(input, 32)
		modLen  = parseLenIntValueSafe(input, 64)
	)

	// Skip the 96-byte header
	data := input
	if len(data) > 96 {
		data = data[96:]
	} else {
		data = nil
	}

	// Retrieve the leading 32 bytes of exp for adjusted exponent length
	var expHead big.Int
	if uint64(len(data)) > baseLen {
		expData := getInput(data, baseLen, min64(expLen, 32))
		expHead.SetBytes(expData)
	}

	cost := c.calcCost(baseLen, expLen, modLen, &expHead)
	if energy < cost {
		return nil, energy, false, ErrOutOfEnergy
	}
	if c.osaka && (baseLen > 1024 || expLen > 1024 || modLen > 1024) {
		return []byte{}, cost, false, nil
	}

	// Handle edge cases
	if baseLen == 0 && modLen == 0 {
		// java-tron PrecompiledContracts.ModExp (MUtil.checkCPUTimeForModExp): under
		// VERSION_4_8_1_1 this degenerate input with expLen > UPPER_BOUND(1024) aborts
		// the tx with OutOfTime instead of cheaply succeeding. Reachable only pre-Osaka
		// — the Osaka upper-bound reject above already short-circuits expLen>1024.
		if c.cpuTimeGuard && expLen > 1024 {
			return nil, cost, false, ErrAlreadyTimeOut
		}
		return []byte{}, cost, true, nil
	}

	base := new(big.Int).SetBytes(getInput(data, 0, baseLen))
	exp := new(big.Int).SetBytes(getInput(data, baseLen, expLen))
	mod := new(big.Int).SetBytes(getInput(data, baseLen+expLen, modLen))

	var result []byte
	if mod.Sign() == 0 {
		// java PrecompiledContracts.ModExp: post-Osaka (TIP-7883) returns modLen
		// zero-bytes for a zero modulus, pre-Osaka returns EMPTY_BYTE_ARRAY.
		if c.osaka {
			result = make([]byte, modLen)
		} else {
			result = []byte{}
		}
	} else {
		r := new(big.Int).Exp(base, exp, mod)
		// Left-pad result to modLen
		rb := r.Bytes()
		result = make([]byte, modLen)
		if uint64(len(rb)) <= modLen {
			copy(result[modLen-uint64(len(rb)):], rb)
		}
	}
	return result, cost, true, nil
}

func (c *bigModExp) calcCost(baseLen, expLen, modLen uint64, expHead *big.Int) uint64 {
	if c.osaka {
		return c.calcOsakaCost(baseLen, expLen, modLen, expHead)
	}
	maxLen := max64(baseLen, modLen)
	multComplexity := berlinMultComplexity(maxLen)

	adjExpLen := c.adjustedExpLen(expLen, expHead)
	if adjExpLen == 0 {
		adjExpLen = 1
	}

	hi, gas := bits.Mul64(multComplexity, adjExpLen)
	if hi != 0 {
		return ^uint64(0)
	}
	gas /= 20
	return gas
}

func (c *bigModExp) calcOsakaCost(baseLen, expLen, modLen uint64, expHead *big.Int) uint64 {
	const minEnergy = uint64(500)
	maxLen := max64(baseLen, modLen)
	var multComplexity uint64
	if maxLen <= 32 {
		multComplexity = 16
	} else {
		words := (maxLen + 7) / 8
		hi, square := bits.Mul64(words, words)
		if hi != 0 {
			return ^uint64(0)
		}
		hi, multComplexity = bits.Mul64(2, square)
		if hi != 0 {
			return ^uint64(0)
		}
	}

	iterCount := c.osakaIterationCount(expLen, expHead)
	hi, cost := bits.Mul64(multComplexity, iterCount)
	if hi != 0 {
		return ^uint64(0)
	}
	if cost < minEnergy {
		return minEnergy
	}
	return cost
}

func (c *bigModExp) osakaIterationCount(expLen uint64, expHead *big.Int) uint64 {
	var highestBit uint64
	if expHead.Sign() != 0 {
		highestBit = uint64(expHead.BitLen() - 1)
	}
	var iter uint64
	if expLen <= 32 {
		iter = highestBit
	} else {
		tail := expLen - 32
		if tail > (^uint64(0)-highestBit)/16 {
			return ^uint64(0)
		}
		iter = 16*tail + highestBit
	}
	if iter == 0 {
		return 1
	}
	return iter
}

func (c *bigModExp) adjustedExpLen(expLen uint64, expHead *big.Int) uint64 {
	if expLen <= 32 {
		if expHead.Sign() == 0 {
			return 0
		}
		return uint64(expHead.BitLen() - 1)
	}
	// expLen > 32
	adj := uint64(0)
	if expHead.Sign() != 0 {
		adj = uint64(expHead.BitLen() - 1)
	}
	return 8*(expLen-32) + adj
}

// berlinMultComplexity computes f(words) for EIP-2565.
func berlinMultComplexity(words uint64) uint64 {
	switch {
	case words <= 64:
		return words * words
	case words <= 1024:
		return words*words/4 + 96*words - 3072
	default:
		return words*words/16 + 480*words - 199680
	}
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// ── 0x06 BN128Add ─────────────────────────────────────────────────────────────

type bn128Add struct {
	istanbul bool
}

func (c *bn128Add) Run(_ *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	ret, used, _, err := c.RunWithStatus(nil, tcommon.Address{}, input, energy)
	return ret, used, err
}

func (c *bn128Add) RunWithStatus(_ *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, bool, error) {
	cost := uint64(500)
	if c.istanbul {
		cost = 150
	}
	if energy < cost {
		return nil, energy, false, ErrOutOfEnergy
	}
	ret, err := runBN128Add(input)
	if err != nil {
		return []byte{}, cost, false, nil
	}
	return ret, cost, true, nil
}

func runBN128Add(input []byte) ([]byte, error) {
	x, err := newBN128G1(getInput(input, 0, 64))
	if err != nil {
		return nil, err
	}
	y, err := newBN128G1(getInput(input, 64, 64))
	if err != nil {
		return nil, err
	}
	res := new(bn256.G1)
	res.Add(x, y)
	return res.Marshal(), nil
}

// ── 0x07 BN128Mul ─────────────────────────────────────────────────────────────

type bn128Mul struct {
	istanbul bool
}

func (c *bn128Mul) Run(_ *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	ret, used, _, err := c.RunWithStatus(nil, tcommon.Address{}, input, energy)
	return ret, used, err
}

func (c *bn128Mul) RunWithStatus(_ *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, bool, error) {
	cost := uint64(40000)
	if c.istanbul {
		cost = 6000
	}
	if energy < cost {
		return nil, energy, false, ErrOutOfEnergy
	}
	ret, err := runBN128Mul(input)
	if err != nil {
		return []byte{}, cost, false, nil
	}
	return ret, cost, true, nil
}

func runBN128Mul(input []byte) ([]byte, error) {
	p, err := newBN128G1(getInput(input, 0, 64))
	if err != nil {
		return nil, err
	}
	k := new(big.Int).SetBytes(getInput(input, 64, 32))
	res := new(bn256.G1)
	res.ScalarMult(p, k)
	return res.Marshal(), nil
}

// ── 0x08 BN128Pairing ─────────────────────────────────────────────────────────

const bn128PairSize = 192

type bn128Pairing struct {
	istanbul bool
}

func (c *bn128Pairing) Run(_ *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, error) {
	ret, used, _, err := c.RunWithStatus(nil, tcommon.Address{}, input, energy)
	return ret, used, err
}

func (c *bn128Pairing) RunWithStatus(_ *TVM, _ tcommon.Address, input []byte, energy uint64) ([]byte, uint64, bool, error) {
	var cost uint64
	pairs := uint64(0)
	if len(input) > 0 {
		pairs = uint64(len(input)) / bn128PairSize
	}
	if c.istanbul {
		cost = 45000 + 34000*pairs
	} else {
		cost = 100000 + 80000*pairs
	}
	if energy < cost {
		return nil, energy, false, ErrOutOfEnergy
	}
	ret, err := runBN128Pairing(input)
	if err != nil {
		return []byte{}, cost, false, nil
	}
	return ret, cost, true, nil
}

var (
	errBN128BadPairingInput = errors.New("bad bn128 pairing input size")
	bn128True32             = func() []byte { b := make([]byte, 32); b[31] = 1; return b }()
	bn128False32            = make([]byte, 32)
)

func runBN128Pairing(input []byte) ([]byte, error) {
	if len(input)%bn128PairSize != 0 {
		return nil, errBN128BadPairingInput
	}
	var (
		g1s []*bn256.G1
		g2s []*bn256.G2
	)
	for i := 0; i < len(input); i += bn128PairSize {
		g1, err := newBN128G1(input[i : i+64])
		if err != nil {
			return nil, err
		}
		g2, err := newBN128G2(input[i+64 : i+192])
		if err != nil {
			return nil, err
		}
		g1s = append(g1s, g1)
		g2s = append(g2s, g2)
	}
	if bn256.PairingCheck(g1s, g2s) {
		return bn128True32, nil
	}
	return bn128False32, nil
}

func newBN128G1(blob []byte) (*bn256.G1, error) {
	p := new(bn256.G1)
	if _, err := p.Unmarshal(blob); err != nil {
		return nil, err
	}
	return p, nil
}

func newBN128G2(blob []byte) (*bn256.G2, error) {
	p := new(bn256.G2)
	if _, err := p.Unmarshal(blob); err != nil {
		return nil, err
	}
	return p, nil
}
