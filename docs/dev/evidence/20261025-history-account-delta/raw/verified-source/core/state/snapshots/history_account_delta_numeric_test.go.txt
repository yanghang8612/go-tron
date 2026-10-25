package snapshots

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func accountDeltaNumericFixture(balance, clock uint64) []byte {
	core := []byte("GTRC\x02")
	core = binary.BigEndian.AppendUint32(core, 1<<28|1<<3|1<<9|1<<10|1<<27)
	core = binary.AppendUvarint(core, accountDeltaZigBits(balance))
	core = binary.AppendUvarint(core, accountDeltaZigBits(1500000000000))
	core = binary.AppendUvarint(core, accountDeltaZigBits(clock))
	core = binary.AppendUvarint(core, 5)
	core = append(core, []byte{0xf8, 1, 0x80, 0xff, 7}...)
	payload := wrapAccountDeltaRLP([]byte{5}, core, false)
	payload = append(payload, 0x80, 0x80, 0x80)
	return wrapAccountDeltaRLP(nil, payload, true)
}
func TestAccountDeltaNumericExactArithmetic(t *testing.T) {
	values := []uint64{0, 1, 63, 64, 127, 128, 1 << 32, 1<<63 - 1, 1 << 63, ^uint64(0)}
	for _, a := range values {
		for _, b := range values {
			if a == b {
				continue
			}
			old := accountDeltaNumericFixture(a, 1500000000000+a)
			next := accountDeltaNumericFixture(b, 1500000000000+b)
			patch := accountDeltaNumericPatch(old, next)
			if len(patch) == 0 || patch[0] != 2 {
				t.Fatal("numeric candidate missing", a, b)
			}
			r := accountDeltaParser{patch[1:]}
			got, e := decodeAccountDeltaNumericPatch(&r, old)
			if e != nil || len(r.p) != 0 || !bytes.Equal(got, next) {
				t.Fatalf("numeric %d -> %d: %v", a, b, e)
			}
		}
	}
	old := accountDeltaNumericFixture(999999999, 123456789)
	next := bytes.Clone(old)
	next[len(next)-5] ^= 1
	if p := accountDeltaNumericPatch(old, next); p != nil {
		t.Fatal("opaque bytes must remain literal")
	}
	for _, mask := range []uint64{0, 1, 1 << 27, 1 << 32} {
		r := accountDeltaParser{binary.AppendUvarint(nil, mask)}
		if _, e := decodeAccountDeltaNumericPatch(&r, old); e == nil {
			t.Fatal("accepted invalid numeric mask", mask)
		}
	}
}
func TestAccountDeltaNumericPageDepthAndOracle(t *testing.T) {
	var raw []byte
	var notes []accountDeltaValue
	for i := 0; i < 200; i++ {
		raw = append(raw, 0xfe, byte(i))
		v := accountDeltaNumericFixture(uint64(123456789123456+i*17), uint64(1530000000000+i*3000))
		notes = append(notes, accountDeltaValue{uint32(len(raw)), uint32(len(v)), uint32(i % 3)})
		raw = append(raw, v...)
	}
	for _, depth := range []uint8{1, 4, 8} {
		enc, s, e := encodeAccountDeltaColumnsOrder(raw, notes, depth, true, true)
		if e != nil {
			t.Fatal(e)
		}
		got, ds, e := decodeAccountDeltaColumns(enc, uint64(len(raw)), nil)
		if e != nil || !bytes.Equal(got, raw) || s != ds || s.Numeric == 0 || s.MaxDepth > depth {
			t.Fatalf("numeric depth%d: %+v/%+v %v", depth, s, ds, e)
		}
		bad := bytes.Clone(enc)
		bad[len(bad)-1] ^= 1
		if _, _, e = decodeAccountDeltaColumns(bad, uint64(len(raw)), nil); e == nil {
			t.Fatal("numeric corruption accepted")
		}
	}
}

func FuzzAccountDeltaNumericExactBytes(f *testing.F) {
	f.Add(accountDeltaNumericFixture(123456789000000, 1500000000000), accountDeltaNumericFixture(123456789000017, 1500000003000))
	f.Fuzz(func(t *testing.T, base, value []byte) {
		if len(base) > accountDeltaValueMax || len(value) > accountDeltaValueMax {
			return
		}
		p := accountDeltaNumericPatch(base, value)
		if len(p) == 0 {
			return
		}
		r := accountDeltaParser{p[1:]}
		got, e := decodeAccountDeltaNumericPatch(&r, base)
		if e != nil || len(r.p) != 0 || !bytes.Equal(got, value) {
			t.Fatalf("numeric patch changes canonical bytes: %v", e)
		}
	})
}
