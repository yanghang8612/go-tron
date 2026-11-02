package main

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"
)

func TestResourceSparseExactRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(70830019))
	for i := 0; i < 30000; i++ {
		r := resourceFields{frozen: i&1 != 0, optimized: i&2 != 0}
		for j := range r.words {
			if (i>>uint(j))&1 != 0 {
				r.words[j] = rng.Uint64()
			}
		}
		if r.frozen {
			if i&4 != 0 {
				r.frozenBalance = rng.Uint64()
			}
			if i&8 != 0 {
				r.frozenExpiry = rng.Uint64()
			}
			if i%7 == 0 {
				r.frozenUnknown = []byte{0xf8, 0x3f, 0x7f}
			}
		}
		if i%11 == 0 {
			r.unknown = []byte{0xfa, 0x3f, 3, 0x80, 0, 0xff}
		}
		old := encodeResourceV1(r)
		new, err := compactResource(old)
		if err != nil {
			t.Fatal(i, err)
		}
		got, err := expandResource(new)
		if err != nil || !bytes.Equal(got, old) {
			t.Fatalf("case %d: old=%x candidate=%x expanded=%x err=%v", i, old, new, got, err)
		}
	}
}
func TestResourceSparsePresenceAndMalformed(t *testing.T) {
	empty, err := compactResource(encodeResourceV1(resourceFields{}))
	if err != nil {
		t.Fatal(err)
	}
	present, err := compactResource(encodeResourceV1(resourceFields{frozen: true}))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(empty, present) || len(empty) != 8 || len(present) != 8 {
		t.Fatal("lost frozen message presence", empty, present)
	}
	bad := func(mask uint16, tail ...byte) []byte {
		b := append([]byte(nil), resourceV2...)
		b = binary.LittleEndian.AppendUint16(b, mask)
		return append(b, tail...)
	}
	for _, b := range [][]byte{nil, empty[:7], append(append([]byte(nil), empty...), 0), bad(1), bad(1, 0), bad(1, 0x80, 0), bad(1<<11, 1), bad(1<<15, 1, 0xff), bad(1<<14, 0), bad(1<<14, 2, 1)} {
		if _, err := expandResource(b); err == nil {
			t.Fatalf("accepted malformed %x", b)
		}
	}
	for _, b := range [][]byte{resourceV1, append(encodeResourceV1(resourceFields{}), 0), append(append([]byte(nil), resourceV1...), 0x80, 0)} {
		if _, err := compactResource(b); err == nil {
			t.Fatalf("accepted malformed old %x", b)
		}
	}
}
func BenchmarkResourceSparseExpand(b *testing.B) {
	v, _ := compactResource(encodeResourceV1(resourceFields{words: [10]uint64{800, 1735997900, 0, 0, 0, 0, 0, 28800000}, frozen: true, frozenBalance: 100000000}))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := expandResource(v); err != nil {
			b.Fatal(err)
		}
	}
}
