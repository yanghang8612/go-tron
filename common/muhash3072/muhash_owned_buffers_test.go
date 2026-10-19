package muhash3072

import (
	"fmt"
	"math/big"
	"testing"
)

func ownedFields(a *Accumulator) []*big.Int {
	return []*big.Int{&a.num, &a.den, &a.mapping, &a.product, &a.reduction.high, &a.reduction.low}
}

func assertDisjointOwnedFields(t *testing.T, accumulators ...*Accumulator) {
	t.Helper()
	seen := make(map[*big.Word]int)
	for ai, a := range accumulators {
		for fi, n := range ownedFields(a) {
			words := n.Bits()
			words = words[:cap(words)]
			for i := range words {
				id := ai*6 + fi
				if old, ok := seen[&words[i]]; ok && old != id {
					t.Fatalf("owned buffers overlap: %d and %d", old, id)
				}
				seen[&words[i]] = id
			}
		}
	}
}

func assertObservationalFinalize(t *testing.T, a *Accumulator, want Finalized) {
	t.Helper()
	fields := ownedFields(a)
	saved := make([]big.Int, len(fields))
	for i, n := range fields {
		saved[i].Set(n)
	}
	got, err := a.Finalize()
	if err != nil || got != want {
		t.Fatal("finalize oracle mismatch", err)
	}
	for i, n := range fields {
		if n.Cmp(&saved[i]) != 0 {
			t.Fatalf("Finalize modified owned field %d", i)
		}
	}
}

func TestOwnedProductRotationsAgainstGenericModulo(t *testing.T) {
	for _, start := range []*big.Int{big.NewInt(1), new(big.Int).Sub(modulus, big.NewInt(1))} {
		for _, mode := range []string{"insert", "remove", "alternate"} {
			t.Run(fmt.Sprintf("%d/%s", start.BitLen(), mode), func(t *testing.T) {
				a, err := FromState(State(toLE(start)))
				if err != nil {
					t.Fatal(err)
				}
				num, den := new(big.Int).Set(start), big.NewInt(1)
				for i := range 96 {
					raw := []byte{0xa9, byte(i), byte(i >> 8)}
					mapped := genericMap(t, raw)
					remove := mode == "remove" || (mode == "alternate" && i&1 == 1)
					if remove {
						err = a.Remove(raw)
						den.Mul(den, mapped)
						den.Mod(den, modulus)
					} else {
						err = a.Insert(raw)
						num.Mul(num, mapped)
						num.Mod(num, modulus)
					}
					if err != nil || a.num.Cmp(num) != 0 || a.den.Cmp(den) != 0 {
						t.Fatal("rotation differs from generic Mod", i, err)
					}
					assertDisjointOwnedFields(t, a)
					assertObservationalFinalize(t, a, genericFinal(num, den))
				}
			})
		}
	}
}

func TestOwnedMappingProductWarmCloneIsolation(t *testing.T) {
	parent := NewAccumulator()
	for i := range 16 {
		if err := parent.Insert([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		if err := parent.Remove([]byte{0xff, byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	child, err := parent.Clone()
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range ownedFields(child)[2:] {
		if n.Sign() != 0 || len(n.Bits()) != 0 {
			t.Fatal("clone inherited scratch")
		}
	}
	assertDisjointOwnedFields(t, parent, child)
	want, err := parent.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	for i := range 32 {
		raw := []byte{0x7b, byte(i)}
		for _, a := range []*Accumulator{child, parent} {
			if err := a.Insert(raw); err != nil {
				t.Fatal(err)
			}
			assertDisjointOwnedFields(t, parent, child)
			if err := a.Remove(raw); err != nil {
				t.Fatal(err)
			}
			assertDisjointOwnedFields(t, parent, child)
			assertObservationalFinalize(t, a, want)
		}
	}
}

func TestOwnedBuffersInvalidStateRejectsWithoutMutation(t *testing.T) {
	for _, bad := range []*big.Int{big.NewInt(0), new(big.Int).Set(modulus), new(big.Int).Add(modulus, big.NewInt(1))} {
		a := NewAccumulator()
		a.num.Set(bad)
		a.mapping.SetInt64(7)
		a.product.SetInt64(11)
		a.reduction.high.SetInt64(13)
		a.reduction.low.SetInt64(17)
		fields := ownedFields(a)
		saved := make([]big.Int, len(fields))
		for i, n := range fields {
			saved[i].Set(n)
		}
		for _, operation := range []func([]byte) error{a.Insert, a.Remove} {
			if operation([]byte("invalid")) == nil {
				t.Fatal("accepted invalid group state")
			}
			for i, n := range fields {
				if n.Cmp(&saved[i]) != 0 {
					t.Fatalf("failed operation modified field %d", i)
				}
			}
		}
	}
}
