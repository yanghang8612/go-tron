package snapshots

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// Vary sizes, key locality, compressibility, flags and incomplete last records.
// The stateless encoder stays independent and is the byte-for-byte oracle.
func recordColumnEncoderPage(seed int64, size int) ([]byte, []uint32) {
	rng := rand.New(rand.NewSource(seed))
	raw := make([]byte, min(size, int(seed%41)))
	rng.Read(raw)
	var starts []uint32
	var tx uint64
	for len(raw)+21 <= size {
		starts = append(starts, uint32(len(raw)))
		n := rng.Intn(256)
		if seed%3 == 0 {
			n = 0
		}
		key := rng.Uint32()
		if seed%2 == 0 {
			key = uint32(len(starts) / 3)
		}
		tx += uint64(rng.Intn(8))
		raw = binary.BigEndian.AppendUint32(raw, 17+uint32(n))
		raw = binary.BigEndian.AppendUint32(raw, key)
		raw = binary.BigEndian.AppendUint64(raw, tx)
		raw = append(raw, byte(rng.Intn(2)))
		raw = binary.BigEndian.AppendUint32(raw, uint32(n))
		value := make([]byte, min(n, size-len(raw)))
		if seed%5 != 0 {
			rng.Read(value)
		}
		raw = append(raw, value...)
	}
	return raw, starts
}

func TestRecordColumnReusableEncoderExactBytes(t *testing.T) {
	enc, _, err := cbCodec()
	if err != nil {
		t.Fatal(err)
	}
	var e recordColumnPageEncoder
	check := func(raw []byte, starts []uint32) {
		t.Helper()
		before, offsets := bytes.Clone(raw), append([]uint32(nil), starts...)
		if len(starts) > 0 {
			if err := e.prepare(raw, starts); err != nil {
				t.Fatal(err)
			}
			// Descending and repeated order catches overlapping scratch writes.
			for _, mode := range []int{2, 1, 0, 1, 2, 0} {
				want, err := encodeHistoryRecordColumns(raw, starts, uint32(mode))
				if err != nil || !bytes.Equal(e.transformed(mode), want) {
					t.Fatalf("transform mode %d differs: %v", mode, err)
				}
			}
		}
		want, err := encodeRecordColumnStoredPage(raw, starts, enc)
		if err != nil {
			t.Fatal(err)
		}
		got, err := e.encode(raw, starts, enc)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("stored bytes differ: %v", err)
		}
		out, _, err := decodeRecordColumnStoredPage(got, uint64(len(raw)), nil, nil)
		if err != nil || !bytes.Equal(out, raw) || !bytes.Equal(raw, before) {
			t.Fatalf("canonical bytes or input changed: %v", err)
		}
		for i := range starts {
			if starts[i] != offsets[i] {
				t.Fatal("mutated annotations")
			}
		}
	}
	raw, starts := recordColumnFixture()
	check(raw, starts)
	for seed := int64(0); seed < 60; seed++ {
		size := []int{historyRecordColumnPageMax, 21, 127, 4096, 65535}[seed%5]
		raw, starts = recordColumnEncoderPage(seed, size)
		check(raw, starts)
		if seed%7 == 0 {
			check(raw, nil)
		}
	}
	for tail := 0; tail < 25; tail++ {
		raw = binary.BigEndian.AppendUint32(nil, math.MaxUint32)
		raw = binary.BigEndian.AppendUint32(raw, math.MaxUint32)
		raw = binary.BigEndian.AppendUint64(raw, math.MaxUint64)
		raw = append(raw, 1)
		raw = binary.BigEndian.AppendUint32(raw, math.MaxUint32-17)
		check(append(raw, make([]byte, tail)...), []uint32{0})
	}
}

func TestRecordColumnReusableEncoderRejectAndRecover(t *testing.T) {
	enc, _, err := cbCodec()
	if err != nil {
		t.Fatal(err)
	}
	var e recordColumnPageEncoder
	raw, starts := recordColumnFixture()
	for _, bad := range [][]uint32{{0}, {starts[0], starts[2]}, {starts[0], starts[1] + 1}, {math.MaxUint32}} {
		if _, err := e.encode(raw, bad, enc); err == nil {
			t.Fatal("invalid annotations accepted", bad)
		}
	}
	for _, slot := range []int{0, 16, 17} {
		bad := bytes.Clone(raw)
		bad[int(starts[0])+slot] = 255
		if _, err := e.encode(bad, starts, enc); err == nil {
			t.Fatal("invalid header accepted", slot)
		}
	}
	bad := bytes.Clone(raw)
	binary.BigEndian.PutUint64(bad[starts[1]+8:starts[1]+16], 2)
	if _, err := e.encode(bad, starts, enc); err == nil {
		t.Fatal("decreasing tx accepted")
	}
	for _, bad := range [][]byte{nil, make([]byte, historyRecordColumnPageMax+1)} {
		if _, err := e.encode(bad, nil, enc); err == nil {
			t.Fatal("invalid page bound accepted")
		}
	}
	want, _ := encodeRecordColumnStoredPage(raw, starts, enc)
	got, err := e.encode(raw, starts, enc)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatal("encoder failed to recover after rejected input", err)
	}
}

func TestRecordColumnReusableEncoderAllocationBound(t *testing.T) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	raw, starts := recordColumnEncoderPage(6, historyRecordColumnPageMax)
	var e recordColumnPageEncoder
	old := testing.AllocsPerRun(5, func() {
		if _, err := encodeRecordColumnStoredPage(raw, starts, enc); err != nil {
			t.Fatal(err)
		}
	})
	next := testing.AllocsPerRun(5, func() {
		if _, err := e.encode(raw, starts, enc); err != nil {
			t.Fatal(err)
		}
	})
	if next >= old/8 {
		t.Fatalf("allocation reduction too small: %.0f -> %.0f", old, next)
	}
	retained := cap(e.txs) + cap(e.lengths) + cap(e.flags) + cap(e.literals) + cap(e.packed) + cap(e.best) + cap(e.candidate)
	for _, key := range e.keys {
		retained += cap(key)
	}
	if retained > 6*historyRecordColumnPageMax {
		t.Fatal("encoder retained more than six pages", retained)
	}
	t.Logf("allocations/page %.0f -> %.0f; retained scratch %d B", old, next, retained)
}

func TestRecordColumnStreamEncoderReferenceAndReset(t *testing.T) {
	w, err := newRecordColumnStreamWriter(t.TempDir(), 128)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	for pass := 0; pass < 2; pass++ {
		raw := bytes.Repeat([]byte{byte(pass)}, 128)
		var offsets []uint64
		if _, err := w.Write(raw); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 9000; i++ {
			offsets = append(offsets, uint64(len(raw)))
			if err := w.NoteRecordHeader(uint64(len(raw))); err != nil {
				t.Fatal(err)
			}
			n := (i*37)%300 + 1
			frame := binary.BigEndian.AppendUint32(nil, uint32(17+n))
			frame = binary.BigEndian.AppendUint32(frame, uint32(i*7919))
			frame = binary.BigEndian.AppendUint64(frame, uint64(i/3+pass))
			frame = append(frame, byte((i+pass)%2))
			frame = binary.BigEndian.AppendUint32(frame, uint32(n))
			frame = append(frame, bytes.Repeat([]byte{byte(i)}, n)...)
			raw = append(raw, frame...)
			if _, err := w.Write(frame); err != nil {
				t.Fatal(err)
			}
		}
		if pass == 0 {
			if cap(w.recordHeaders.encoder.packed) == 0 {
				t.Fatal("fixture never flushed a page")
			}
			packed := &w.recordHeaders.encoder.packed[0]
			if err := w.Reset(); err != nil {
				t.Fatal(err)
			}
			if len(w.recordHeaders.starts) != 0 || len(w.recordHeaders.offsets) != 0 || w.recordHeaders.haveLast || &w.recordHeaders.encoder.packed[0] != packed {
				t.Fatal("reset lost scratch or retained annotations")
			}
			continue
		}
		path := filepath.Join(w.dir, "reference.seg")
		if err := w.Finish(path); err != nil {
			t.Fatal(err)
		}
		blob, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		r, err := openCompressedBlockReader(path)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		enc, _, _ := cbCodec()
		for i, block := range r.table {
			n, err := compressedBlockExpectedLen(r.table, r.uncSize, i)
			if err != nil {
				t.Fatal(err)
			}
			start := block.uncompressedStart
			var starts []uint32
			for _, offset := range offsets {
				if offset >= start && offset+21 <= start+n {
					starts = append(starts, uint32(offset-start))
				}
			}
			want, err := encodeRecordColumnStoredPage(raw[start:start+n], starts, enc)
			physical := compressedBlockHeaderSize + block.compressedStart
			if err != nil || !bytes.Equal(blob[physical:physical+block.compressedLen], want) {
				t.Fatal("stream page differs from stateless oracle", i, err)
			}
		}
	}
}

func BenchmarkRecordColumnPageEncoder(b *testing.B) {
	raw, starts := recordColumnEncoderPage(17, historyRecordColumnPageMax)
	enc, _, err := cbCodec()
	if err != nil {
		b.Fatal(err)
	}
	for _, reuse := range []bool{false, true} {
		name := "stateless"
		if reuse {
			name = "reused"
		}
		b.Run(name, func(b *testing.B) {
			var e recordColumnPageEncoder
			if reuse {
				if _, err := e.encode(raw, starts, enc); err != nil {
					b.Fatal(err)
				}
			}
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if reuse {
					_, err = e.encode(raw, starts, enc)
				} else {
					_, err = encodeRecordColumnStoredPage(raw, starts, enc)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
