package muhash3072

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/big"

	"golang.org/x/crypto/chacha20"
)

const fieldBytes = 384
const fieldBits = fieldBytes * 8

// State is the complete canonical group element, fixed-width little endian.
// Restoring State resumes an accumulator with denominator one.
type State [fieldBytes]byte

var modulus = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 3072), big.NewInt(1103717))
var fieldMask = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), fieldBits), big.NewInt(1))
var fieldCorrection = big.NewInt(1103717)
var errState = errors.New("invalid MuHash3072 state")
var errZeroElement = errors.New("MuHash3072 mapped element is zero")
var stateMagic = []byte{'G', 'M', 'H', '3', '0', '7', '2', 0}

const stateVersion uint16 = 1

// Accumulator is exclusively owned; copying this struct is forbidden. Clone
// deep-copies math/big storage. The zero value is invalid, never the identity.
type Accumulator struct {
	num, den         big.Int
	mapping, product big.Int
	reduction        reductionScratch
}

// reductionScratch is owned by one accumulator or one Finalize call. Globals
// above are read-only; high/low never borrow the product receiver's storage.
type reductionScratch struct{ high, low big.Int }

// reduceProduct replaces x by x mod p for 0 <= x < 2^(2*fieldBits).
// Callers only pass products of two canonical field elements. For B=2^3072,
// c=1103717 and p=B-c, fold1=low+c*high < (c+1)*B; fold2 < B+c^2 < 2*p.
// Thus two folds followed by at most one subtraction yield a canonical value.
// This is not a reduction API for unbounded or negative big.Int inputs.
func (s *reductionScratch) reduceProduct(x *big.Int) {
	if x.BitLen() > fieldBits {
		for range 2 {
			// Capture both halves before changing x. The distinct receivers
			// retain owned scratch, including across successive Insert/Remove.
			s.high.Rsh(x, fieldBits)
			s.low.And(x, fieldMask)
			s.high.Mul(&s.high, fieldCorrection)
			x.Add(&s.low, &s.high)
		}
	}
	if x.Cmp(modulus) >= 0 {
		x.Sub(x, modulus)
	}
}

type Finalized struct {
	Digest [32]byte
	State  State
}

func NewAccumulator() *Accumulator {
	a := new(Accumulator)
	a.num.SetInt64(1)
	a.den.SetInt64(1)
	return a
}
func (a *Accumulator) valid() error {
	if a == nil || a.num.Sign() <= 0 || a.den.Sign() <= 0 || a.num.Cmp(modulus) >= 0 || a.den.Cmp(modulus) >= 0 {
		return errState
	}
	return nil
}
func (a *Accumulator) Clone() (*Accumulator, error) {
	if e := a.valid(); e != nil {
		return nil, e
	}
	c := new(Accumulator)
	c.num.Set(&a.num)
	c.den.Set(&a.den)
	return c, nil
}
func toLE(n *big.Int) (out [fieldBytes]byte) {
	var be [fieldBytes]byte
	n.FillBytes(be[:])
	for i := range be {
		out[fieldBytes-1-i] = be[i]
	}
	return out
}

func (a *Accumulator) mapElement(raw []byte) (*big.Int, error) {
	key := sha256.Sum256(raw)
	var nonce [12]byte
	c, e := chacha20.NewUnauthenticatedCipher(key[:], nonce[:])
	if e != nil {
		return nil, e
	}
	var stream [fieldBytes]byte
	c.XORKeyStream(stream[:], stream[:])
	// The stream is local and no longer needed in little-endian order. Reuse
	// its fixed storage while SetBytes copies into accumulator-owned limbs.
	for i, j := 0, len(stream)-1; i < j; i, j = i+1, j-1 {
		stream[i], stream[j] = stream[j], stream[i]
	}
	n := a.mapping.SetBytes(stream[:])
	// The 384-byte mapping is already below 2^3072, so at most one
	// subtraction is needed. Do not normalize values read by FromState.
	if n.Cmp(modulus) >= 0 {
		n.Sub(n, modulus)
	}
	if n.Sign() == 0 {
		return nil, errZeroElement
	}
	return n, nil
}
func (a *Accumulator) Insert(raw []byte) error {
	if e := a.valid(); e != nil {
		return e
	}
	n, e := a.mapElement(raw)
	if e != nil {
		return e
	}
	a.product.Mul(&a.num, n)
	a.reduction.reduceProduct(&a.product)
	// Move the two exclusively owned limb buffers; no reference to either
	// field survives this move. This is not a general shallow-copy operation.
	a.num, a.product = a.product, a.num
	return nil
}
func (a *Accumulator) Remove(raw []byte) error {
	if e := a.valid(); e != nil {
		return e
	}
	n, e := a.mapElement(raw)
	if e != nil {
		return e
	}
	a.product.Mul(&a.den, n)
	a.reduction.reduceProduct(&a.product)
	a.den, a.product = a.product, a.den
	return nil
}

// Finalize is observational like the official Python reference. It always pays
// ModInverse, fixed-width LE serialization and SHA256, including identity/no-op.
func (a *Accumulator) Finalize() (Finalized, error) {
	var out Finalized
	if e := a.valid(); e != nil {
		return out, e
	}
	inverse := new(big.Int).ModInverse(&a.den, modulus)
	if inverse == nil {
		return out, errState
	}
	n := new(big.Int).Mul(&a.num, inverse)
	// Finalize is observational: never mutate the accumulator's scratch or
	// field elements, and never share temporary storage with its parent.
	var scratch reductionScratch
	scratch.reduceProduct(n)
	if n.Sign() == 0 {
		return out, errState
	}
	out.State = State(toLE(n))
	out.Digest = sha256.Sum256(out.State[:])
	return out, nil
}
func (a *Accumulator) Digest() ([32]byte, error) {
	var empty [32]byte
	c, e := a.Clone()
	if e != nil {
		return empty, e
	}
	f, e := c.Finalize()
	if e != nil {
		return empty, e
	}
	return f.Digest, nil
}

// FromState validates and owns a canonical complete state. Invalid state is
// never interpreted as the empty set.
func FromState(state State) (*Accumulator, error) {
	a := NewAccumulator()
	var be [fieldBytes]byte
	for i := range state {
		be[len(be)-1-i] = state[i]
	}
	a.num.SetBytes(be[:])
	if err := a.valid(); err != nil {
		return nil, err
	}
	return a, nil
}
func ValidateState(state State) error { _, err := FromState(state); return err }
func StateDigest(state State) ([32]byte, error) {
	if err := ValidateState(state); err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(state[:]), nil
}
func EmptyState() State     { var state State; state[0] = 1; return state }
func EmptyDigest() [32]byte { state := EmptyState(); return sha256.Sum256(state[:]) }
func EncodeState(state State) ([]byte, error) {
	if err := ValidateState(state); err != nil {
		return nil, err
	}
	out := append([]byte(nil), stateMagic...)
	out = binary.BigEndian.AppendUint16(out, stateVersion)
	return append(out, state[:]...), nil
}
func DecodeState(raw []byte) (State, error) {
	var state State
	if len(raw) != len(stateMagic)+2+fieldBytes || !bytes.Equal(raw[:len(stateMagic)], stateMagic) || binary.BigEndian.Uint16(raw[len(stateMagic):]) != stateVersion {
		return state, errState
	}
	copy(state[:], raw[len(stateMagic)+2:])
	if err := ValidateState(state); err != nil {
		return State{}, err
	}
	return state, nil
}
