package snapshots

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math/rand"
	"testing"
)

func accountDeltaFixture(seed int64) ([]byte, []accountDeltaValue) {
	rng := rand.New(rand.NewSource(seed))
	var raw []byte
	var notes []accountDeltaValue
	values := map[uint32][]byte{}
	for i := 0; i < 220; i++ {
		id := uint32(rng.Intn(9))
		v := append([]byte(nil), values[id]...)
		if len(v) == 0 {
			v = make([]byte, 42+rng.Intn(60))
			rng.Read(v)
		}
		if i%11 == 0 {
			v = append(v, byte(i))
		}
		if i%7 != 0 {
			v[rng.Intn(len(v))] ^= byte(1 + rng.Intn(255))
		}
		raw = append(raw, make([]byte, 21)...)
		notes = append(notes, accountDeltaValue{uint32(len(raw)), uint32(len(v)), id})
		raw = append(raw, v...)
		values[id] = v
	}
	return raw, notes
}

func TestAccountDeltaPageOracleAndDepth(t *testing.T) {
	for seed := int64(0); seed < 15; seed++ {
		raw, notes := accountDeltaFixture(seed)
		for _, depth := range []uint8{1, 4, 8} {
			encoded, stats, err := encodeAccountDeltaPage(raw, notes, depth)
			if err != nil {
				t.Fatal(err)
			}
			got, observed, err := decodeAccountDeltaPage(encoded, uint64(len(raw)), nil)
			if err != nil || !bytes.Equal(got, raw) || stats != observed || stats.MaxDepth > depth || stats.Deltas == 0 {
				t.Fatalf("seed%d depth%d: %+v/%+v %v", seed, depth, stats, observed, err)
			}
		}
	}
}

func TestAccountDeltaPageCorruptionAndBudgets(t *testing.T) {
	raw, notes := accountDeltaFixture(1)
	encoded, _, err := encodeAccountDeltaPage(raw, notes, 4)
	if err != nil {
		t.Fatal(err)
	}
	for _, at := range []int{0, 4, 8, 9, 12, len(encoded) - 1} {
		bad := bytes.Clone(encoded)
		bad[at] ^= 0xff
		if _, _, err := decodeAccountDeltaPage(bad, uint64(len(raw)), nil); err == nil {
			t.Fatalf("accepted corrupt byte%d", at)
		}
	}
	for _, n := range []int{0, 1, 43, 44, len(encoded) - 1} {
		if _, _, err := decodeAccountDeltaPage(encoded[:n], uint64(len(raw)), nil); err == nil {
			t.Fatalf("accepted length%d", n)
		}
	}
	for _, n := range []uint64{0, accountDeltaPageMax + 1, ^uint64(0)} {
		if _, _, err := decodeAccountDeltaPage(encoded, n, nil); err == nil {
			t.Fatalf("accepted expected%d", n)
		}
	}
	for _, n := range []accountDeltaValue{{0, 0, 1}, {0, accountDeltaValueMax + 1, 1}, {uint32(len(raw)), 1, 1}} {
		if _, _, err := encodeAccountDeltaPage(raw, []accountDeltaValue{n}, 1); err == nil {
			t.Fatal("accepted invalid note")
		}
	}
	if _, _, err := encodeAccountDeltaPage(raw, []accountDeltaValue{notes[1], notes[0]}, 1); err == nil {
		t.Fatal("accepted unordered notes")
	}
	if _, _, err := encodeAccountDeltaPage(raw, notes, 9); err == nil {
		t.Fatal("accepted depth9")
	}
}

func TestAccountDeltaPageRejectsForgedReferenceAndDepth(t *testing.T) {
	// A syntactically valid forward/opaque reference cannot become an anchor.
	for _, body := range [][]byte{{3, 1}, {0, 1, 7, 3, 1}, {1, 1, 7, 3, 1, 3, 1}} {
		encoded := make([]byte, accountDeltaHeaderSize)
		copy(encoded, "ADP1")
		binary.BigEndian.PutUint32(encoded[4:8], 3)
		encoded[8] = 1
		sum := sha256.Sum256([]byte{7, 7, 7})
		copy(encoded[12:], sum[:])
		encoded = append(encoded, body...)
		if _, _, err := decodeAccountDeltaPage(encoded, 3, nil); err == nil {
			t.Fatalf("accepted bad reference/depth%v", body)
		}
	}
}

func FuzzAccountDeltaPageDecoder(f *testing.F) {
	raw, notes := accountDeltaFixture(42)
	encoded, _, _ := encodeAccountDeltaPage(raw, notes, 4)
	f.Add(encoded, uint32(len(raw)))
	f.Fuzz(func(t *testing.T, p []byte, n uint32) {
		if len(p) > 2*accountDeltaPageMax+accountDeltaHeaderSize {
			return
		}
		got, s, err := decodeAccountDeltaPage(p, uint64(n), nil)
		if err == nil && (len(got) != int(n) || s.MaxDepth > accountDeltaDepthMax) {
			t.Fatal("invalid successful decode")
		}
	})
}

func TestAccountDeltaColumnsOracle(t *testing.T) {
	raw, notes := accountDeltaFixture(2026)
	for _, templates := range []bool{false, true} {
		ns := notes
		if templates {
			ns = accountDeltaTemplateNotes(raw, notes)
		}
		for _, deltas := range []bool{false, true} {
			for _, depth := range []uint8{1, 4, 8} {
				enc, s, e := encodeAccountDeltaColumns(raw, ns, depth, deltas)
				if e != nil {
					t.Fatal(e)
				}
				got, ds, e := decodeAccountDeltaColumns(enc, uint64(len(raw)), nil)
				if e != nil || !bytes.Equal(raw, got) || s != ds {
					t.Fatalf("%v/%v depth%d: %+v/%+v %v", templates, deltas, depth, s, ds, e)
				}
				for _, at := range []int{0, 4, 8, 12, 16, 20, 24, len(enc) - 1} {
					bad := bytes.Clone(enc)
					bad[at] ^= 0xff
					if _, _, e := decodeAccountDeltaColumns(bad, uint64(len(raw)), nil); e == nil {
						t.Fatalf("accepted corruption%d", at)
					}
				}
			}
		}
	}
}
