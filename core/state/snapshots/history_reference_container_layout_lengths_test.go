package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"testing"
)

// The spans deliberately jump among more chunk-directory pages than the
// reader's page cache holds. Each payload is one byte, so this measures
// authenticated metadata lookups rather than decompression or payload caching.
func interleavedReferenceContainer(t testing.TB) ([]byte, historyReferenceHeader) {
	t.Helper()
	const chunks, spans = 2048, 1 << 16
	h := historyReferenceHeader{
		logical:  spans,
		chunkDir: historyReferenceHeaderSize + chunks,
		chunks:   chunks,
		spans:    spans,
	}
	h.spanDir = h.chunkDir + h.chunks*historyReferenceChunkEntrySize
	h.physical = h.spanDir + h.spans*historyReferenceSpanEntrySize
	data := make([]byte, h.physical)
	for id := uint64(0); id < chunks; id++ {
		value := byte(id)
		data[historyReferenceHeaderSize+id] = value
		chunk := (historyReferenceChunk{offset: historyReferenceHeaderSize + id, stored: 1, raw: 1,
			codec: historyReferenceCodecRaw, digest: sha256.Sum256([]byte{value})}).encode()
		copy(data[h.chunkDir+id*historyReferenceChunkEntrySize:], chunk[:])
	}
	for i := uint64(0); i < spans; i++ {
		// Only even chunks are referenced. Odd chunks must still be checked by
		// ValidateAll despite not contributing to the logical stream.
		span := (historyReferenceSpan{logical: i, chunk: uint32((i * 991 % (chunks / 2)) * 2), length: 1}).encode()
		copy(data[h.spanDir+i*historyReferenceSpanEntrySize:], span[:])
	}
	rehashReferenceContainer(t, data, h)
	return data, h
}

func rehashReferenceContainer(t testing.TB, data []byte, h historyReferenceHeader) {
	t.Helper()
	var err error
	h.metadata, err = historyReferenceMetadataHash(context.Background(), bytes.NewReader(data), h)
	if err != nil {
		t.Fatal(err)
	}
	header := h.encode()
	copy(data, header[:])
}

// frozenReferenceLayout is the pre-change validator. In particular, its span
// pass rereads each referenced chunk directory entry after the chunk pass.
func frozenReferenceLayout(r *historyReferenceReader, ctx context.Context) error {
	if err := r.check(ctx); err != nil {
		return err
	}
	h := r.header
	h.metadata = [sha256.Size]byte{}
	encoded := h.encode()
	digest := sha256.New()
	_, _ = digest.Write(encoded[:])
	var scratch [historyReferenceMetadataPage]byte
	for off := h.chunkDir; off < h.physical; {
		n := min(uint64(len(scratch)), h.physical-off)
		if err := r.metadata(ctx, scratch[:n], off); err != nil {
			return err
		}
		_, _ = digest.Write(scratch[:n])
		off += n
	}
	var got [sha256.Size]byte
	copy(got[:], digest.Sum(nil))
	if got != r.header.metadata {
		return fmt.Errorf("%w: metadata digest", errHistoryReferenceCorrupt)
	}
	physical := uint64(historyReferenceHeaderSize)
	for id := uint32(0); uint64(id) < h.chunks; id++ {
		chunk, err := r.chunk(ctx, id)
		if err != nil {
			return err
		}
		if chunk.offset != physical || chunk.raw == 0 || chunk.raw > historyReferenceMaxChunk || chunk.stored == 0 ||
			(chunk.codec != historyReferenceCodecRaw && chunk.codec != historyReferenceCodecSnappy && chunk.codec != historyReferenceCodecZstd) ||
			chunk.codec == historyReferenceCodecRaw && chunk.stored != chunk.raw ||
			(chunk.codec == historyReferenceCodecSnappy || chunk.codec == historyReferenceCodecZstd) && chunk.stored >= chunk.raw ||
			uint64(chunk.stored) > h.chunkDir-physical {
			return fmt.Errorf("%w: chunk layout", errHistoryReferenceCorrupt)
		}
		physical += uint64(chunk.stored)
	}
	if physical != h.chunkDir {
		return fmt.Errorf("%w: payload coverage", errHistoryReferenceCorrupt)
	}
	logical := uint64(0)
	for index := uint64(0); index < h.spans; index++ {
		span, err := r.span(ctx, index)
		if err != nil {
			return err
		}
		chunk, err := r.chunk(ctx, span.chunk)
		if err != nil {
			return err
		}
		if span.logical != logical || span.length == 0 || span.offset > chunk.raw || span.length > chunk.raw-span.offset ||
			uint64(span.length) > h.logical-logical {
			return fmt.Errorf("%w: span layout", errHistoryReferenceCorrupt)
		}
		logical += uint64(span.length)
	}
	if logical != h.logical {
		return fmt.Errorf("%w: logical coverage", errHistoryReferenceCorrupt)
	}
	return r.check(ctx)
}

func TestHistoryReferenceLayoutLengthTablePreservesValidationAndReads(t *testing.T) {
	data, h := interleavedReferenceContainer(t)
	open := func(data []byte, cache int) (*historyReferenceReader, *referenceCountingReader) {
		counted := &referenceCountingReader{ReaderAt: bytes.NewReader(data)}
		r, err := newHistoryReferenceReader(context.Background(), counted, uint64(len(data)), nil, cache)
		if err != nil {
			t.Fatal(err)
		}
		return r, counted
	}
	for _, cache := range []int{0, 4} {
		fast, fastInput := open(data, cache)
		old, oldInput := open(data, cache)
		if err := fast.ValidateLayout(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := frozenReferenceLayout(old, context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Logf("cache=%d layout metadata reads new=%d original=%d bytes new=%d original=%d", cache,
			fastInput.reads, oldInput.reads, fastInput.bytes, oldInput.bytes)
		if fastInput.reads*10 >= oldInput.reads || fastInput.bytes*10 >= oldInput.bytes {
			t.Fatalf("interleaved spans still reread chunk metadata: new=%d/%d original=%d/%d",
				fastInput.reads, fastInput.bytes, oldInput.reads, oldInput.bytes)
		}
		for _, offset := range []uint64{0, 1, 997, 8191, h.logical - 1, h.logical} {
			var got, want [2]byte
			n, err := fast.ReadAt(got[:], int64(offset))
			if offset == h.logical {
				if n != 0 || !errors.Is(err, io.EOF) {
					t.Fatalf("EOF at %d: %d %v", offset, n, err)
				}
				continue
			}
			want[0] = byte((offset * 991 % (h.chunks / 2)) * 2)
			if offset+1 < h.logical {
				want[1] = byte(((offset + 1) * 991 % (h.chunks / 2)) * 2)
			}
			wantN, wantErr := len(got), error(nil)
			if offset+1 == h.logical {
				wantN, wantErr = 1, io.EOF
			}
			if n != wantN || !errors.Is(err, wantErr) || !bytes.Equal(got[:n], want[:n]) {
				t.Fatalf("read offset=%d got=%x n=%d err=%v want=%x", offset, got, n, err, want)
			}
		}
		if err := fast.ValidateAll(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := fast.Close(); err != nil {
			t.Fatal(err)
		}
		if err := old.Close(); err != nil {
			t.Fatal(err)
		}
	}

	corrupt := func(change func([]byte)) []byte {
		copyData := bytes.Clone(data)
		change(copyData)
		rehashReferenceContainer(t, copyData, h)
		return copyData
	}
	faults := map[string][]byte{
		"bad chunk ID": corrupt(func(b []byte) {
			b[h.spanDir+8] = 0xff
			b[h.spanDir+9] = 0xff
			b[h.spanDir+10] = 0xff
			b[h.spanDir+11] = 0xff
		}),
		"span out of raw bounds": corrupt(func(b []byte) { b[h.spanDir+12] = 1 }),
		"zero raw chunk":         corrupt(func(b []byte) { b[h.chunkDir+15] = 0 }),
		"bad metadata hash":      func() []byte { b := bytes.Clone(data); b[h.spanDir] ^= 1; return b }(),
	}
	for name, damaged := range faults {
		t.Run(name, func(t *testing.T) {
			fast, _ := open(damaged, 0)
			old, _ := open(damaged, 0)
			got, want := fast.ValidateLayout(context.Background()), frozenReferenceLayout(old, context.Background())
			if got == nil || want == nil || got.Error() != want.Error() {
				t.Fatalf("validation changed: new=%v original=%v", got, want)
			}
			fast.Close()
			old.Close()
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancelled, err := newHistoryReferenceReader(ctx, bytes.NewReader(data), uint64(len(data)), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := cancelled.ValidateLayout(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled layout accepted: %v", err)
	}
	cancelled.Close()
	truncated, err := newHistoryReferenceReader(context.Background(), bytes.NewReader(data[:len(data)-1]), uint64(len(data)), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := truncated.ValidateLayout(context.Background()); !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated metadata accepted: %v", err)
	}
	truncated.Close()
	// Payload validation, including chunks unused by spans, remains separate
	// from structural layout validation.
	payload := bytes.Clone(data)
	payload[historyReferenceHeaderSize+1] ^= 1 // chunk 1 is unused by every span
	r, _ := open(payload, 0)
	if err := r.ValidateLayout(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.ValidateAll(context.Background()); !errors.Is(err, errHistoryReferenceCorrupt) {
		t.Fatalf("changed payload accepted: %v", err)
	}
	r.Close()
}

type cancelReferenceReadAt struct {
	io.ReaderAt
	reads, cancelAt int
	cancel          context.CancelFunc
	triggeredOffset int64
}

func (r *cancelReferenceReadAt) ReadAt(p []byte, off int64) (int, error) {
	r.reads++
	if r.reads == r.cancelAt {
		r.triggeredOffset = off
		r.cancel()
	}
	return r.ReaderAt.ReadAt(p, off)
}

func TestHistoryReferenceLayoutLengthTableCancellationCanRetry(t *testing.T) {
	data, h := interleavedReferenceContainer(t)
	digestPages := int((h.physical - h.chunkDir + historyReferenceMetadataPage - 1) / historyReferenceMetadataPage)
	chunkPages := int(h.chunks * historyReferenceChunkEntrySize / historyReferenceMetadataPage)
	for _, tc := range []struct {
		name, phase string
		cancelAt    int
	}{
		{"chunk pass", "chunk", 1 + digestPages + 1},
		{"span pass", "span", 1 + digestPages + chunkPages + 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			input := &cancelReferenceReadAt{ReaderAt: bytes.NewReader(data), cancelAt: tc.cancelAt, cancel: cancel}
			r, err := newHistoryReferenceReader(context.Background(), input, uint64(len(data)), nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.ValidateLayout(ctx); !errors.Is(err, context.Canceled) || r.validated {
				t.Fatalf("cancel left layout valid or wrong error: %v validated=%t", err, r.validated)
			}
			if input.reads != tc.cancelAt {
				t.Fatalf("cancellation did not occur in %s pass: reads=%d want=%d", tc.phase, input.reads, tc.cancelAt)
			}
			if tc.phase == "chunk" && (input.triggeredOffset < int64(h.chunkDir) || input.triggeredOffset >= int64(h.spanDir)) ||
				tc.phase == "span" && (input.triggeredOffset < int64(h.spanDir) || input.triggeredOffset >= int64(h.physical)) {
				t.Fatalf("cancellation read outside %s directory: offset=%d", tc.phase, input.triggeredOffset)
			}
			if err := r.ValidateLayout(context.Background()); err != nil || !r.validated {
				t.Fatalf("fresh context could not retry validation: %v validated=%t", err, r.validated)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			if err := r.ValidateLayout(context.Background()); !errors.Is(err, errHistoryReferenceClosed) {
				t.Fatalf("closed reader accepted validation: %v", err)
			}
		})
	}
}

func BenchmarkHistoryReferenceLayoutInterleaved(b *testing.B) {
	data, _ := interleavedReferenceContainer(b)
	for _, reference := range []bool{false, true} {
		name := "raw-length-table"
		if reference {
			name = "original-chunk-lookup"
		}
		b.Run(name, func(b *testing.B) {
			reads, readBytes := 0, 0
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				counted := &referenceCountingReader{ReaderAt: bytes.NewReader(data)}
				r, err := newHistoryReferenceReader(context.Background(), counted, uint64(len(data)), nil, 0)
				if err == nil {
					if reference {
						err = frozenReferenceLayout(r, context.Background())
					} else {
						err = r.ValidateLayout(context.Background())
					}
				}
				if err != nil {
					b.Fatal(err)
				}
				reads += counted.reads
				readBytes += counted.bytes
				r.Close()
			}
			b.StopTimer()
			b.ReportMetric(float64(reads)/float64(b.N), "readat/op")
			b.ReportMetric(float64(readBytes)/float64(b.N), "requested-B/op")
		})
	}
}
