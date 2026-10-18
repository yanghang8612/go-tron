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

// State is the complete canonical group element, fixed-width little endian.
// Restoring State resumes an accumulator with denominator one.
type State [fieldBytes]byte

var modulus = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 3072), big.NewInt(1103717))
var errState = errors.New("invalid MuHash3072 state")
var errZeroElement = errors.New("MuHash3072 mapped element is zero")
var stateMagic = []byte{'G', 'M', 'H', '3', '0', '7', '2', 0}

const stateVersion uint16 = 1

// Accumulator is exclusively owned; copying this struct is forbidden. Clone
// deep-copies math/big storage. The zero value is invalid, never the identity.
type Accumulator struct{ num, den big.Int }
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
func fromLE(b []byte) *big.Int {
	be := make([]byte, len(b))
	for i := range b {
		be[len(b)-1-i] = b[i]
	}
	return new(big.Int).SetBytes(be)
}
func toLE(n *big.Int) (out [fieldBytes]byte) {
	var be [fieldBytes]byte
	n.FillBytes(be[:])
	for i := range be {
		out[fieldBytes-1-i] = be[i]
	}
	return out
}

func mapElement(raw []byte) (*big.Int, error) {
	key := sha256.Sum256(raw)
	var nonce [12]byte
	c, e := chacha20.NewUnauthenticatedCipher(key[:], nonce[:])
	if e != nil {
		return nil, e
	}
	var stream [fieldBytes]byte
	c.XORKeyStream(stream[:], stream[:])
	n := fromLE(stream[:])
	n.Mod(n, modulus)
	if n.Sign() == 0 {
		return nil, errZeroElement
	}
	return n, nil
}
func (a *Accumulator) Insert(raw []byte) error {
	if e := a.valid(); e != nil {
		return e
	}
	n, e := mapElement(raw)
	if e != nil {
		return e
	}
	a.num.Mul(&a.num, n)
	a.num.Mod(&a.num, modulus)
	return nil
}
func (a *Accumulator) Remove(raw []byte) error {
	if e := a.valid(); e != nil {
		return e
	}
	n, e := mapElement(raw)
	if e != nil {
		return e
	}
	a.den.Mul(&a.den, n)
	a.den.Mod(&a.den, modulus)
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
	n.Mod(n, modulus)
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
	a.num.Set(fromLE(state[:]))
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
