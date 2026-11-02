package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"testing"
)

func testRows(n int, repeated bool) []row {
	r := make([]row, n)
	for i := range r {
		k := make([]byte, 66)
		copy(k, []byte("owner/generation/domain/storage-prefix"))
		binary.BigEndian.PutUint64(k[58:], uint64(2*i+1))
		v := []byte{2, byte(i + 1)}
		if repeated {
			v = bytes.Repeat([]byte{255}, 33)
			v[0] = 1
		}
		r[i] = row{k, v}
	}
	return r
}

func TestPageExactWrappedRoundTripAndBoundedMiss(t *testing.T) {
	for _, n := range []int{1, 2, 127, 128} {
		for _, rep := range []bool{false, true} {
			r := testRows(n, rep)
			b, e := encodePage(r)
			if e != nil {
				t.Fatal(e)
			}
			p, e := parsePage(b)
			if e != nil {
				t.Fatal(e)
			}
			if p.n != n {
				t.Fatal("count")
			}
			for _, x := range r {
				v, ok, e := lookupPage(r[n-1].key, b, x.key)
				if e != nil || !ok || !bytes.Equal(v, x.value) {
					t.Fatalf("n %d repeated %v lookup: %v", n, rep, e)
				}
			}
			missing := append([]byte(nil), r[0].key...)
			missing[65]++
			_, ok, e := lookupPage(r[n-1].key, b, missing)
			if e != nil || ok {
				t.Fatal("invented missing row")
			}
			foreign := append([]byte(nil), r[0].key...)
			foreign[5] ^= 1
			_, ok, e = lookupPage(r[n-1].key, b, foreign)
			if e != nil || ok {
				t.Fatal("invented foreign owner")
			}
		}
	}
}

func TestPagePresenceExactAndMalformed(t *testing.T) {
	r := testRows(5, false)
	r[0].value = []byte{2}
	r[1].value = []byte{1}
	r[2].value = []byte{1, 0}
	r[3].value = bytes.Repeat([]byte{255}, 33)
	r[3].value[0] = 1
	b, e := encodePage(r)
	if e != nil {
		t.Fatal(e)
	}
	for _, x := range r {
		v, ok, e := lookupPage(r[4].key, b, x.key)
		if e != nil || !ok || !bytes.Equal(v, x.value) {
			t.Fatal("presence/value changed")
		}
	}
	for i := 0; i < len(b); i++ {
		if _, e := parsePage(b[:i]); e == nil {
			t.Fatalf("accepted truncated page %d", i)
		}
	}
	bad := append([]byte(nil), b...)
	bad[8] = 9
	if _, e := parsePage(bad); e == nil {
		t.Fatal("accepted flag")
	}
	bad = append([]byte(nil), b...)
	bad[4] = 129
	bad[5] = 0
	if _, e := parsePage(bad); e == nil {
		t.Fatal("accepted oversized page")
	}
}

func TestPageRejectsDuplicateOrDifferentIdentity(t *testing.T) {
	r := testRows(2, false)
	r[1].key = append([]byte(nil), r[0].key...)
	if _, e := encodePage(r); e == nil {
		t.Fatal("duplicate")
	}
	r = testRows(2, false)
	r[1].key[20] ^= 1
	if _, e := encodePage(r); e == nil {
		t.Fatal("different prefix")
	}
	if _, e := encodePage(testRows(129, false)); e == nil {
		t.Fatal("unbounded")
	}
}

func TestOrdinaryFallbackAndEverySharedTailLength(t *testing.T) {
	r := testRows(1, false)
	for _, v := range [][]byte{{1}, {2}, {2, 1}, {1, 0}} {
		got, ok, e := lookupPage(r[0].key, v, r[0].key)
		if e != nil || !ok || !bytes.Equal(got, v) {
			t.Fatal("ordinary wrapped fallback changed")
		}
	}
	for shared := 0; shared <= 16; shared++ {
		r = testRows(2, false)
		if shared == 16 {
			r = r[:1]
		} else {
			for i := range r {
				for j := 50; j < 66; j++ {
					r[i].key[j] = 0
				}
				r[i].key[50+shared] = byte(i)
			}
		}
		b, e := encodePage(r)
		if e != nil {
			t.Fatal(e)
		}
		p, e := parsePage(b)
		if e != nil || p.width != 16-shared {
			t.Fatalf("shared %d layout %v width %d", shared, e, p.width)
		}
		for _, x := range r {
			v, ok, e := lookupPage(r[len(r)-1].key, b, x.key)
			if e != nil || !ok || !bytes.Equal(v, x.value) {
				t.Fatalf("shared %d differs: %v", shared, e)
			}
		}
	}
}

func FuzzPageRoundTrip(f *testing.F) {
	for _, n := range []byte{0, 1, 15, 63, 126, 127} {
		f.Add([]byte{n, 0, 1, 255})
	}
	f.Fuzz(func(t *testing.T, seed []byte) {
		if len(seed) == 0 {
			return
		}
		n := 1 + int(seed[0])%128
		r := make([]row, n)
		for i := range r {
			k := make([]byte, 66)
			counter := make([]byte, len(seed)+8)
			copy(counter, seed)
			binary.BigEndian.PutUint64(counter[len(seed):], uint64(i))
			h := sha256.Sum256(counter)
			copy(k[50:], h[16:])
			v := append([]byte{2}, h[:int(h[0])%32]...)
			r[i] = row{k, v}
		}
		sort.Slice(r, func(i, j int) bool { return bytes.Compare(r[i].key, r[j].key) < 0 })
		b, e := encodePage(r)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = parsePage(b); e != nil {
			t.Fatal(e)
		}
		for _, x := range r {
			got, ok, e := lookupPage(r[n-1].key, b, x.key)
			if e != nil || !ok || !bytes.Equal(got, x.value) {
				t.Fatal("fuzz row differs", e)
			}
		}
	})
}
