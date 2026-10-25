package snapshots

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

func recordColumnFixture() ([]byte, []uint32) {
	raw := bytes.Repeat([]byte{0xf3}, 37)
	var starts []uint32
	keys := []uint32{0, math.MaxUint32, 1, 0, 65535}
	txs := []uint64{0, 1, 1, math.MaxUint64 - 1, math.MaxUint64}
	for i, key := range keys {
		starts = append(starts, uint32(len(raw)))
		n := uint32(i * 37)
		raw = binary.BigEndian.AppendUint32(raw, 17+n)
		raw = binary.BigEndian.AppendUint32(raw, key)
		raw = binary.BigEndian.AppendUint64(raw, txs[i])
		raw = append(raw, byte(i%2))
		raw = binary.BigEndian.AppendUint32(raw, n)
		raw = append(raw, bytes.Repeat([]byte{byte(i + 1)}, int(n))...)
	}
	raw = append(raw, []byte("partial header")...)
	return raw, starts
}

func TestHistoryRecordColumnsRoundtrip(t *testing.T) {
	raw, starts := recordColumnFixture()
	for mode := uint32(0); mode < 3; mode++ {
		p, e := encodeHistoryRecordColumns(raw, starts, mode)
		if e != nil {
			t.Fatal(mode, e)
		}
		for _, dst := range [][]byte{nil, make([]byte, 5, len(raw)), make([]byte, 0, 1)} {
			got, e := decodeHistoryRecordColumns(p, uint64(len(raw)), dst)
			if e != nil || !bytes.Equal(got, raw) {
				t.Fatal(mode, "oracle", e)
			}
		}
		for cut := 0; cut < len(p); cut++ {
			if _, e := decodeHistoryRecordColumns(p[:cut], uint64(len(raw)), nil); e == nil {
				t.Fatal(mode, "truncation accepted", cut)
			}
		}
		for at := range p {
			bad := bytes.Clone(p)
			bad[at] ^= 0x80
			if _, e := decodeHistoryRecordColumns(bad, uint64(len(raw)), nil); e == nil {
				t.Fatal(mode, "corruption accepted", at)
			}
		}
	}
}

func TestHistoryRecordColumnsCrossingValue(t *testing.T) {
	for tail := 0; tail < 25; tail++ {
		raw := binary.BigEndian.AppendUint32(nil, math.MaxUint32)
		raw = binary.BigEndian.AppendUint32(raw, math.MaxUint32)
		raw = binary.BigEndian.AppendUint64(raw, math.MaxUint64)
		raw = append(raw, 1)
		raw = binary.BigEndian.AppendUint32(raw, math.MaxUint32-17)
		raw = append(raw, make([]byte, tail)...)
		for mode := uint32(0); mode < 3; mode++ {
			p, e := encodeHistoryRecordColumns(raw, []uint32{0}, mode)
			if e != nil {
				t.Fatal(e)
			}
			got, e := decodeHistoryRecordColumns(p, uint64(len(raw)), nil)
			if e != nil || !bytes.Equal(got, raw) {
				t.Fatal(tail, mode, e)
			}
		}
	}
}

func TestHistoryRecordColumnsRejectLayout(t *testing.T) {
	raw, starts := recordColumnFixture()
	for _, bad := range [][]uint32{nil, {0}, {starts[0], starts[2]}, {starts[0], starts[1] + 1}, {math.MaxUint32}} {
		if _, e := encodeHistoryRecordColumns(raw, bad, 0); e == nil {
			t.Fatal("bad layout accepted", bad)
		}
	}
	for _, mode := range []uint32{3, math.MaxUint32} {
		if _, e := encodeHistoryRecordColumns(raw, starts, mode); e == nil {
			t.Fatal("mode")
		}
	}
	p, e := encodeHistoryRecordColumns(raw, starts, 0)
	if e != nil {
		t.Fatal(e)
	}
	for _, n := range []uint64{0, 1, uint64(len(raw) - 1), historyRecordColumnPageMax + 1, math.MaxUint64} {
		if _, e := decodeHistoryRecordColumns(p, n, nil); e == nil {
			t.Fatal("output bound", n)
		}
	}
}

func FuzzHistoryRecordColumnsDecode(f *testing.F) {
	raw, starts := recordColumnFixture()
	for mode := uint32(0); mode < 3; mode++ {
		p, e := encodeHistoryRecordColumns(raw, starts, mode)
		if e != nil {
			f.Fatal(e)
		}
		f.Add(p, uint32(len(raw)))
	}
	f.Fuzz(func(t *testing.T, p []byte, n uint32) {
		if len(p) > 2*historyRecordColumnPageMax+historyRecordColumnHeader {
			return
		}
		_, _ = decodeHistoryRecordColumns(p, uint64(n), nil)
	})
}

// A nonminimal number can reconstruct identical canonical bytes and pass the
// page digest. Reject the encoded ambiguity independently of that digest.
func TestHistoryRecordColumnsRejectNoncanonicalNumber(t *testing.T) {
	raw, starts := recordColumnFixture()
	encoded, e := encodeHistoryRecordColumns(raw, starts, 0)
	if e != nil {
		t.Fatal(e)
	}
	for field := 0; field < 3; field++ {
		bad := bytes.Clone(encoded)
		at := historyRecordColumnHeader
		for i := 0; i < field; i++ {
			at += int(binary.BigEndian.Uint32(encoded[16+i*4 : 20+i*4]))
		}
		if bad[at] != 0 {
			t.Fatal("fixture first number must be zero")
		}
		bad = append(bad[:at], append([]byte{0x80}, bad[at:]...)...)
		slot := 16 + field*4
		binary.BigEndian.PutUint32(bad[slot:slot+4], binary.BigEndian.Uint32(bad[slot:slot+4])+1)
		if _, e = decodeHistoryRecordColumns(bad, uint64(len(raw)), nil); e == nil {
			t.Fatal("nonminimal number accepted", field)
		}
	}
}
