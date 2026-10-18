package muhash3072

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"os"
	"testing"
)

type operation struct {
	Op      string
	Element string `json:"element_hex"`
}
type expected struct {
	Digest string `json:"digest_raw_sha256_hex"`
	State  string `json:"state_le384_hex"`
}

func decodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, e := hex.DecodeString(s)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func apply(t *testing.T, a *Accumulator, ops []operation) {
	t.Helper()
	for _, o := range ops {
		var e error
		switch o.Op {
		case "insert":
			e = a.Insert(decodeHex(t, o.Element))
		case "remove":
			e = a.Remove(decodeHex(t, o.Element))
		default:
			t.Fatal(o.Op)
		}
		if e != nil {
			t.Fatal(e)
		}
	}
}
func check(t *testing.T, a *Accumulator, w expected) State {
	t.Helper()
	f, e := a.Finalize()
	if e != nil {
		t.Fatal(e)
	}
	if hex.EncodeToString(f.Digest[:]) != w.Digest || hex.EncodeToString(f.State[:]) != w.State {
		t.Fatal("official/Python canonical state or digest differs")
	}
	return f.State
}
func TestBitcoinV30VectorsCanonicalReload(t *testing.T) {
	var input struct {
		Vectors []struct {
			Name       string
			Operations []operation
			expected
		}
	}
	b, e := os.ReadFile("testdata/bitcoin-v30-vectors.json")
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(b, &input); e != nil {
		t.Fatal(e)
	}
	if len(input.Vectors) != 22 {
		t.Fatal(len(input.Vectors))
	}
	for _, v := range input.Vectors {
		t.Run(v.Name, func(t *testing.T) {
			a := NewAccumulator()
			apply(t, a, v.Operations)
			check(t, a, v.expected)
			normalized := NewAccumulator()
			for _, o := range v.Operations {
				apply(t, normalized, []operation{o})
				f, e := normalized.Finalize()
				if e != nil {
					t.Fatal(e)
				}
				encoded, e := EncodeState(f.State)
				if e != nil {
					t.Fatal(e)
				}
				state, e := DecodeState(encoded)
				if e != nil {
					t.Fatal(e)
				}
				normalized, e = FromState(state)
				if e != nil {
					t.Fatal(e)
				}
			}
			check(t, normalized, v.expected)
		})
	}
}
func TestRandomCanonicalReloadDifferential(t *testing.T) {
	var input struct {
		Phases []struct {
			Operations []operation
			Expected   expected
		}
	}
	b, e := os.ReadFile("testdata/canonical-reload.json")
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(b, &input); e != nil {
		t.Fatal(e)
	}
	a := NewAccumulator()
	for _, p := range input.Phases {
		apply(t, a, p.Operations)
		state := check(t, a, p.Expected)
		raw, e := EncodeState(state)
		if e != nil {
			t.Fatal(e)
		}
		round, e := DecodeState(raw)
		if e != nil {
			t.Fatal(e)
		}
		a, e = FromState(round)
		if e != nil {
			t.Fatal(e)
		}
		check(t, a, p.Expected)
	}
}
func TestStateStrictAndParentOwnership(t *testing.T) {
	a := NewAccumulator()
	if e := a.Insert([]byte("a")); e != nil {
		t.Fatal(e)
	}
	c, e := a.Clone()
	if e != nil {
		t.Fatal(e)
	}
	before, _ := a.Finalize()
	if e = c.Remove([]byte("a")); e != nil {
		t.Fatal(e)
	}
	after, _ := a.Finalize()
	if before != after {
		t.Fatal("Clone aliased parent")
	}
	raw, e := EncodeState(before.State)
	if e != nil {
		t.Fatal(e)
	}
	for _, bad := range [][]byte{nil, raw[:len(raw)-1], append(bytes.Clone(raw), 0)} {
		if _, e = DecodeState(bad); e == nil {
			t.Fatal("accepted malformed state")
		}
	}
	zero := State{}
	if _, e = FromState(zero); e == nil {
		t.Fatal("accepted zero")
	}
	for _, n := range []*big.Int{modulus, new(big.Int).Add(modulus, big.NewInt(1))} {
		state := State(toLE(n))
		if _, e = FromState(state); e == nil {
			t.Fatal("accepted out-of-range state")
		}
	}
	raw[0] ^= 1
	if _, e = DecodeState(raw); e == nil {
		t.Fatal("accepted wrong magic")
	}
}
