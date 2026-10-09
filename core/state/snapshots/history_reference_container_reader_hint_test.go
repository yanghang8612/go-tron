package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"
)

func largeSequentialReferenceContainer(t testing.TB, spans int) []byte {
	t.Helper()
	const chunkSize = 256
	raw := make([]byte, chunkSize)
	for i := range raw {
		raw[i] = byte(i)
	}
	h := historyReferenceHeader{logical: uint64(spans), chunkDir: historyReferenceHeaderSize + chunkSize,
		chunks: 1, spans: uint64(spans)}
	h.spanDir = h.chunkDir + historyReferenceChunkEntrySize
	h.physical = h.spanDir + uint64(spans)*historyReferenceSpanEntrySize
	data := make([]byte, h.physical)
	copy(data[historyReferenceHeaderSize:], raw)
	chunk := (historyReferenceChunk{offset: historyReferenceHeaderSize, stored: chunkSize, raw: chunkSize,
		codec: historyReferenceCodecRaw, digest: sha256.Sum256(raw)}).encode()
	copy(data[h.chunkDir:], chunk[:])
	for i := 0; i < spans; i++ {
		span := (historyReferenceSpan{logical: uint64(i), chunk: 0, offset: uint32(i % chunkSize), length: 1}).encode()
		copy(data[h.spanDir+uint64(i)*historyReferenceSpanEntrySize:], span[:])
	}
	var err error
	h.metadata, err = historyReferenceMetadataHash(context.Background(), bytes.NewReader(data), h)
	if err != nil {
		t.Fatal(err)
	}
	header := h.encode()
	copy(data, header[:])
	return data
}

func TestHistoryReferenceSequentialHintReadAtEquivalenceAndIOLimit(t *testing.T) {
	const spans, start, requests = 1 << 19, 1 << 17, 8192
	data := largeSequentialReferenceContainer(t, spans)
	open := func() (*historyReferenceReader, *referenceCountingReader) {
		count := &referenceCountingReader{ReaderAt: bytes.NewReader(data)}
		r, err := newHistoryReferenceReader(context.Background(), count, uint64(len(data)), nil, 256)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.ValidateLayout(context.Background()); err != nil {
			t.Fatal(err)
		}
		return r, count
	}
	fast, fastInput := open()
	defer fast.Close()
	baseline, baselineInput := open()
	defer baseline.Close()
	fastStart, baselineStart := fastInput.reads, baselineInput.reads
	fastByteStart, baselineByteStart := fastInput.bytes, baselineInput.bytes
	for i := 0; i < requests; i++ {
		offset := start + i*4
		var got, want [4]byte
		n, err := fast.ReadAt(got[:], int64(offset))
		// A disabled hint forces exactly the pre-change binary-search path.
		baseline.hintValid = false
		oldN, oldErr := baseline.ReadAt(want[:], int64(offset))
		if n != oldN || err != oldErr || got != want {
			t.Fatalf("offset %d: hint %d/%v/%v, binary %d/%v/%v", offset, n, err, got, oldN, oldErr, want)
		}
		for j, b := range got {
			if b != byte((offset+j)%256) {
				t.Fatalf("offset %d byte %d = %d", offset, j, b)
			}
		}
	}
	fastReads, binaryReads := fastInput.reads-fastStart, baselineInput.reads-baselineStart
	fastBytes, binaryBytes := fastInput.bytes-fastByteStart, baselineInput.bytes-baselineByteStart
	t.Logf("sequential underlying reads hint=%d binary=%d bytes hint=%d binary=%d", fastReads, binaryReads, fastBytes, binaryBytes)
	if binaryReads < 100 || fastReads*4 >= binaryReads*3 || fastBytes >= binaryBytes {
		t.Fatalf("sequential metadata I/O: hint=%d/%d binary=%d/%d", fastReads, fastBytes, binaryReads, binaryBytes)
	}
	for _, query := range []struct{ offset, length int }{
		{start + 1, 7}, {spans - 1, 2}, {spans, 1}, {spans + 7, 1},
		{start + 64, 5}, {start - 1, 8}, {spans / 2, 1}, {0, 16},
	} {
		got, want := make([]byte, query.length), make([]byte, query.length)
		n, err := fast.ReadAt(got, int64(query.offset))
		baseline.hintValid = false
		oldN, oldErr := baseline.ReadAt(want, int64(query.offset))
		if n != oldN || err != oldErr || !bytes.Equal(got, want) {
			t.Fatalf("seek %+v: hint %d/%v, binary %d/%v", query, n, err, oldN, oldErr)
		}
	}
	if n, err := fast.ReadAt(nil, int64(spans+100)); n != 0 || err != nil {
		t.Fatalf("zero-length read: %d %v", n, err)
	}
	if n, err := fast.ReadAt(make([]byte, 1), -1); n != 0 || !errors.Is(err, errHistoryReferenceCorrupt) {
		t.Fatalf("negative read: %d %v", n, err)
	}
}

func TestHistoryReferenceSequentialHintRetainsChecksAndClose(t *testing.T) {
	data := largeSequentialReferenceContainer(t, 1024)
	ctx, cancel := context.WithCancel(context.Background())
	r, err := newHistoryReferenceReader(ctx, bytes.NewReader(data), uint64(len(data)), nil, 256)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadAt(make([]byte, 2), 101); err != nil || !r.hintValid {
		t.Fatalf("initial hinted read: %v", err)
	}
	cancel()
	if _, err := r.ReadAt(make([]byte, 1), 102); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled hinted read: %v", err)
	}
	if err := r.Close(); err != nil || r.hintValid {
		t.Fatalf("close retained hint: %v", err)
	}
	if _, err := r.ReadAt(make([]byte, 1), 102); !errors.Is(err, errHistoryReferenceClosed) {
		t.Fatalf("closed hinted read: %v", err)
	}

	broken := bytes.Clone(data)
	broken[len(broken)-1] ^= 1
	r, err = newHistoryReferenceReader(context.Background(), bytes.NewReader(broken), uint64(len(broken)), nil, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.ReadAt(make([]byte, 1), 0); !errors.Is(err, errHistoryReferenceCorrupt) {
		t.Fatalf("corrupt metadata accepted: %v", err)
	}
	truncated := bytes.NewReader(data[:len(data)-1])
	r, err = newHistoryReferenceReader(context.Background(), truncated, uint64(len(data)), nil, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.ReadAt(make([]byte, 1), 0); !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		t.Fatalf("truncated metadata accepted: %v", err)
	}
}

func BenchmarkHistoryReferenceSequentialHint(b *testing.B) {
	const spans, start, requests = 1 << 19, 1 << 17, 8192
	data := largeSequentialReferenceContainer(b, spans)
	for _, tc := range []struct {
		name    string
		disable bool
	}{
		{"hint", false}, {"binary-search", true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			r, err := newHistoryReferenceReader(context.Background(), bytes.NewReader(data), uint64(len(data)), nil, 256)
			if err != nil {
				b.Fatal(err)
			}
			defer r.Close()
			if err := r.ValidateLayout(context.Background()); err != nil {
				b.Fatal(err)
			}
			var value [4]byte
			b.ReportAllocs()
			b.SetBytes(4)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if tc.disable {
					r.hintValid = false // reference path from before this change
				}
				if n, err := r.ReadAt(value[:], int64(start+(i%requests)*4)); n != 4 || err != nil {
					b.Fatal(n, err)
				}
			}
		})
	}
}
