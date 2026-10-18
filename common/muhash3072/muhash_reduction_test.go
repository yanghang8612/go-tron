package muhash3072

import (
	"crypto/sha256"
	"math/big"
	"math/rand"
	"sync"
	"testing"

	"golang.org/x/crypto/chacha20"
)

func TestBoundedReductionAgainstGenericModulo(t *testing.T) {
	b := new(big.Int).Lsh(big.NewInt(1), fieldBits)
	p := new(big.Int).Sub(new(big.Int).Set(b), big.NewInt(1103717))
	p2 := new(big.Int).Mul(p, p)
	b2 := new(big.Int).Mul(b, b)
	pm1 := new(big.Int).Sub(new(big.Int).Set(p), big.NewInt(1))
	xs := []*big.Int{big.NewInt(0), big.NewInt(1), new(big.Int).Set(pm1), new(big.Int).Set(p), new(big.Int).Add(p, big.NewInt(1)),
		new(big.Int).Sub(b, big.NewInt(1)), new(big.Int).Set(b), new(big.Int).Add(b, big.NewInt(1)),
		new(big.Int).Sub(b2, big.NewInt(1)), new(big.Int).Sub(p2, big.NewInt(1)), new(big.Int).Set(p2), new(big.Int).Mul(pm1, pm1)}
	r := rand.New(rand.NewSource(20261018))
	for range 4096 {
		var raw [fieldBytes * 2]byte
		if _, e := r.Read(raw[:]); e != nil {
			t.Fatal(e)
		}
		xs = append(xs, new(big.Int).SetBytes(raw[:]))
	}
	maskBefore, cBefore, pBefore := new(big.Int).Set(fieldMask), new(big.Int).Set(fieldCorrection), new(big.Int).Set(modulus)
	var scratch reductionScratch
	for i, original := range xs {
		got := new(big.Int).Set(original)
		want := new(big.Int).Mod(new(big.Int).Set(original), p)
		scratch.reduceProduct(got)
		if got.Cmp(want) != 0 || got.Sign() < 0 || got.Cmp(p) >= 0 {
			t.Fatalf("case%d reduction differs", i)
		}
		// Reuse the same receiver and the same high/low storage after a large
		// input. Small values must not retain high words or previous carries.
		got.SetInt64(1)
		scratch.reduceProduct(got)
		if got.Cmp(big.NewInt(1)) != 0 {
			t.Fatal("scratch retained previous state")
		}
	}
	if fieldMask.Cmp(maskBefore) != 0 || fieldCorrection.Cmp(cBefore) != 0 || modulus.Cmp(pBefore) != 0 {
		t.Fatal("read-only constants changed")
	}
}

// This reference deliberately keeps generic Mod for mapping, each product and
// finalization; no bounded helper or production LE helpers are shared.
func genericMap(t *testing.T, raw []byte) *big.Int {
	t.Helper()
	key := sha256.Sum256(raw)
	var nonce [12]byte
	c, e := chacha20.NewUnauthenticatedCipher(key[:], nonce[:])
	if e != nil {
		t.Fatal(e)
	}
	var stream, be [fieldBytes]byte
	c.XORKeyStream(stream[:], stream[:])
	for i := range stream {
		be[len(be)-1-i] = stream[i]
	}
	n := new(big.Int).SetBytes(be[:])
	n.Mod(n, modulus)
	if n.Sign() == 0 {
		t.Fatal("reference mapped zero")
	}
	return n
}
func genericFinal(num, den *big.Int) Finalized {
	inverse := new(big.Int).ModInverse(den, modulus)
	x := new(big.Int).Mul(num, inverse)
	x.Mod(x, modulus)
	var out Finalized
	var be [fieldBytes]byte
	x.FillBytes(be[:])
	for i := range be {
		out.State[fieldBytes-1-i] = be[i]
	}
	out.Digest = sha256.Sum256(out.State[:])
	return out
}

func TestBoundedReductionOperationsAgainstGenericModulo(t *testing.T) {
	r := rand.New(rand.NewSource(20261019))
	a := NewAccumulator()
	num, den := big.NewInt(1), big.NewInt(1)
	for i := range 1024 {
		raw := make([]byte, r.Intn(65))
		if _, e := r.Read(raw); e != nil {
			t.Fatal(e)
		}
		n := genericMap(t, raw)
		if i%3 == 0 {
			if e := a.Remove(raw); e != nil {
				t.Fatal(e)
			}
			den.Mul(den, n)
			den.Mod(den, modulus)
		} else {
			if e := a.Insert(raw); e != nil {
				t.Fatal(e)
			}
			num.Mul(num, n)
			num.Mod(num, modulus)
		}
		if i%16 == 15 {
			got, e := a.Finalize()
			if e != nil || got != genericFinal(num, den) {
				t.Fatal(i, e, "generic full operation mismatch")
			}
			if i%256 == 255 {
				a, e = FromState(got.State)
				if e != nil {
					t.Fatal(e)
				}
			}
		}
	}
}

func TestReductionScratchCloneAndFinalizeOwnership(t *testing.T) {
	a := NewAccumulator()
	for i := range 32 {
		if e := a.Insert([]byte{byte(i)}); e != nil {
			t.Fatal(e)
		}
	}
	want, e := a.Finalize()
	if e != nil {
		t.Fatal(e)
	}
	hi, lo, num, den := new(big.Int).Set(&a.reduction.high), new(big.Int).Set(&a.reduction.low), new(big.Int).Set(&a.num), new(big.Int).Set(&a.den)
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, e := a.Clone()
			if e != nil {
				t.Error(e)
				return
			}
			if c.reduction.high.Sign() != 0 || c.reduction.low.Sign() != 0 {
				t.Error("clone borrowed scratch")
			}
			raw := []byte{0xff, byte(i)}
			if e := c.Insert(raw); e != nil {
				t.Error(e)
			}
			if e := c.Remove(raw); e != nil {
				t.Error(e)
			}
			got, e := c.Finalize()
			if e != nil || got != want {
				t.Error(e, "clone modified parent group state")
			}
		}(i)
	}
	wg.Wait()
	again, e := a.Finalize()
	if e != nil || again != want || a.num.Cmp(num) != 0 || a.den.Cmp(den) != 0 || a.reduction.high.Cmp(hi) != 0 || a.reduction.low.Cmp(lo) != 0 {
		t.Fatal("Finalize or clone mutated parent storage", e)
	}
}

func TestReductionDoesNotNormalizeInvalidStoredState(t *testing.T) {
	for _, n := range []*big.Int{big.NewInt(0), new(big.Int).Set(modulus), new(big.Int).Add(modulus, big.NewInt(1)), new(big.Int).Set(fieldMask)} {
		state := State(toLE(n))
		if _, e := FromState(state); e == nil {
			t.Fatal("stored field silently normalized")
		}
		if _, e := StateDigest(state); e == nil {
			t.Fatal("bad state received digest")
		}
	}
}
