package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/golang/snappy"
)

func encodedReferenceContainer(t testing.TB, raw, stored []byte, codec uint32, declared uint32) []byte {
	t.Helper()
	if declared == 0 {
		declared = uint32(len(raw))
	}
	h := historyReferenceHeader{
		logical:  uint64(declared),
		chunkDir: historyReferenceHeaderSize + uint64(len(stored)),
		chunks:   1,
		spans:    1,
	}
	h.spanDir = h.chunkDir + historyReferenceChunkEntrySize
	h.physical = h.spanDir + historyReferenceSpanEntrySize
	out := make([]byte, h.physical)
	copy(out[historyReferenceHeaderSize:], stored)
	c := historyReferenceChunk{offset: historyReferenceHeaderSize, stored: uint32(len(stored)), raw: declared, codec: codec, digest: sha256.Sum256(raw)}
	ce := c.encode()
	copy(out[h.chunkDir:], ce[:])
	se := (historyReferenceSpan{chunk: 0, length: declared}).encode()
	copy(out[h.spanDir:], se[:])
	h.metadata, _ = historyReferenceMetadataHash(context.Background(), bytes.NewReader(out), h)
	he := h.encode()
	copy(out, he[:])
	return out
}

func testReferenceZstd(t testing.TB, raw []byte) []byte {
	t.Helper()
	enc, err := newHistoryReferenceZstdEncoder()
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	return enc.EncodeAll(raw, nil)
}

func referenceContainerFixture(t *testing.T, cache int) ([]byte, []byte, *historyReferenceReader) {
	t.Helper()
	dir := t.TempDir()
	w, err := newHistoryReferenceWriter(context.Background(), dir, 256)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Release(); err != nil {
			t.Error(err)
		}
	})
	chunks := [][]byte{bytes.Repeat([]byte("authenticated chunk;"), 5000), make([]byte, historyReferenceMaxChunk)}
	rand.New(rand.NewSource(31)).Read(chunks[1])
	ids := make([]uint32, len(chunks))
	for i, p := range chunks {
		ids[i], err = w.StoreChunkWithDigest(p, sha256.Sum256(p))
		if err != nil {
			t.Fatal(err)
		}
		id, err := w.StoreChunk(p)
		if err != nil || id != ids[i] {
			t.Fatalf("dedup %d: %d %v", i, id, err)
		}
	}
	var want []byte
	header := bytes.Repeat([]byte{0}, 64)
	if _, err = w.WriteLiteral(header); err != nil {
		t.Fatal(err)
	}
	want = append(want, header...)
	for i := 0; i < 300; i++ {
		literal := []byte(fmt.Sprintf("row %05d:", i))
		if _, err = w.Write(literal); err != nil {
			t.Fatal(err)
		}
		want = append(want, literal...)
		which := i % 2
		off := uint32(i * 13)
		n := uint32(1000 + i*3)
		if err := w.WriteSpan(ids[which], off, n); err != nil {
			t.Fatal(err)
		}
		want = append(want, chunks[which][off:off+n]...)
	}
	patch := []byte("patched V6 dictionary commitment")
	if _, err = w.WriteAt(patch, 7); err != nil {
		t.Fatal(err)
	}
	copy(want[7:], patch)
	path := filepath.Join(dir, "fresh.seg")
	size, checksum, err := w.Finalize(path)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if size != uint64(len(data)) || checksum != fmt.Sprintf("sha256:%x", sha256.Sum256(data)) {
		t.Fatal("physical checksum differs")
	}
	stats := w.Stats()
	if stats.StoredChunks != 2 || stats.StoredChunkBytes != uint64(len(chunks[0])+len(chunks[1])) || stats.LogicalBytes != uint64(len(want)) || stats.PhysicalBytes != size {
		t.Fatalf("stats %+v", stats)
	}
	if size >= uint64(len(want)) {
		t.Fatalf("repeated logical bytes not reduced: %d >= %d", size, len(want))
	}
	reader, err := openHistoryReferenceReader(context.Background(), path, cache)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	})
	return data, want, reader
}

func TestHistoryReferenceContainerRandomAccessAndOwnedOutput(t *testing.T) {
	data, want, r := referenceContainerFixture(t, 2*historyReferenceMaxChunk)
	if string(data[:8]) != historyReferenceMagic || r.UncompressedSize() != uint64(len(want)) {
		t.Fatal("format identity")
	}
	if _, err := readStateDomainChangeBinaryHeaderAt(bytes.NewReader(data), stateDomainChangeBinarySegmentMagic); err == nil {
		t.Fatal("legacy reader accepted reference magic")
	}
	if err := r.ValidateLayout(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.ValidateAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	rand := rand.New(rand.NewSource(401))
	for i := 0; i < 1000; i++ {
		off, length := rand.Intn(len(want)+20), rand.Intn(5000)+1
		p := make([]byte, length)
		n, err := r.ReadAt(p, int64(off))
		expected := 0
		if off < len(want) {
			expected = min(length, len(want)-off)
		}
		if n != expected || !bytes.Equal(p[:n], want[min(off, len(want)):min(off+n, len(want))]) || (err == io.EOF) != (n < len(p)) || err != nil && err != io.EOF {
			t.Fatalf("range %d %d: n=%d err=%v", off, length, n, err)
		}
		clear(p)
	}
	actual := make([]byte, len(want))
	if _, err := r.ReadAt(actual, 0); err != nil || !bytes.Equal(actual, want) {
		t.Fatalf("full byte stream: %v", err)
	}
	if n, err := r.ReadAt(nil, math.MaxInt64); n != 0 || err != nil {
		t.Fatalf("zero ReadAt: %d %v", n, err)
	}
	if _, err := r.ReadAt(make([]byte, 1), -1); !errors.Is(err, errHistoryReferenceCorrupt) {
		t.Fatal(err)
	}
}

func TestHistoryReferenceContainerCodecCompatibilityAndBounds(t *testing.T) {
	raw := bytes.Repeat([]byte("independent reference chunk "), 3000)
	codecs := []struct {
		name   string
		codec  uint32
		stored []byte
	}{
		{"raw", historyReferenceCodecRaw, raw},
		{"legacy-snappy", historyReferenceCodecSnappy, snappy.Encode(nil, raw)},
		{"zstd", historyReferenceCodecZstd, testReferenceZstd(t, raw)},
	}
	for _, tc := range codecs {
		t.Run(tc.name, func(t *testing.T) {
			data := encodedReferenceContainer(t, raw, tc.stored, tc.codec, 0)
			r, err := newHistoryReferenceReader(context.Background(), bytes.NewReader(data), uint64(len(data)), nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			got := make([]byte, len(raw))
			if _, err = r.ReadAt(got, 0); err != nil || !bytes.Equal(got, raw) {
				t.Fatalf("round trip: %v", err)
			}
			if err = r.ValidateAll(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}

	for _, codec := range []uint32{2, 4, math.MaxUint32} {
		data := encodedReferenceContainer(t, raw, raw, codec, 0)
		r, err := newHistoryReferenceReader(context.Background(), bytes.NewReader(data), uint64(len(data)), nil, 0)
		if err == nil {
			err = r.ValidateLayout(context.Background())
			_ = r.Close()
		}
		if !errors.Is(err, errHistoryReferenceCorrupt) {
			t.Fatalf("codec %d accepted: %v", codec, err)
		}
	}

	// The frame really expands to 128 KiB, while its authenticated directory
	// claims only 64 KiB. DecodeAllCapLimit must reject it before checksum use.
	bombRaw := bytes.Repeat([]byte{'z'}, historyReferenceMaxChunk)
	bomb := encodedReferenceContainer(t, bombRaw, testReferenceZstd(t, bombRaw), historyReferenceCodecZstd, historyReferenceMaxChunk/2)
	r, err := newHistoryReferenceReader(context.Background(), bytes.NewReader(bomb), uint64(len(bomb)), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.ValidateAll(context.Background()); !errors.Is(err, errHistoryReferenceCorrupt) {
		t.Fatalf("overlong Zstd output accepted: %v", err)
	}
	if err = r.Close(); err != nil || r.zstd != nil {
		t.Fatalf("decoder resource retained: %v", err)
	}

	// A frame whose declared raw size itself exceeds the 128 KiB format bound
	// is rejected by layout validation without invoking the decoder.
	tooLarge := encodedReferenceContainer(t, bombRaw, testReferenceZstd(t, bombRaw), historyReferenceCodecZstd, historyReferenceMaxChunk+1)
	r, err = newHistoryReferenceReader(context.Background(), bytes.NewReader(tooLarge), uint64(len(tooLarge)), nil, 0)
	if err == nil {
		err = r.ValidateLayout(context.Background())
		_ = r.Close()
	}
	if !errors.Is(err, errHistoryReferenceCorrupt) {
		t.Fatalf("oversize declaration accepted: %v", err)
	}
}

func TestHistoryReferenceContainerZstdCorruptionAndTruncation(t *testing.T) {
	raw := bytes.Repeat([]byte("checksum protected Zstd chunk"), 3000)
	stored := testReferenceZstd(t, raw)
	data := encodedReferenceContainer(t, raw, stored, historyReferenceCodecZstd, 0)
	for _, cut := range []int{1, len(stored) / 2, len(stored) - 1} {
		broken := bytes.Clone(data)
		// Keep the physical layout intact while making the independent frame
		// equivalent to a truncated payload followed by unrelated zeros.
		clear(broken[historyReferenceHeaderSize+cut : historyReferenceHeaderSize+len(stored)])
		r, err := newHistoryReferenceReader(context.Background(), bytes.NewReader(broken), uint64(len(broken)), nil, 0)
		if err == nil {
			err = r.ValidateAll(context.Background())
			_ = r.Close()
		}
		if !errors.Is(err, errHistoryReferenceCorrupt) {
			t.Fatalf("truncated frame at %d accepted: %v", cut, err)
		}
	}
	broken := bytes.Clone(data)
	broken[historyReferenceHeaderSize+len(stored)/2] ^= 0x80
	r, err := newHistoryReferenceReader(context.Background(), bytes.NewReader(broken), uint64(len(broken)), nil, 0)
	if err == nil {
		err = r.ValidateAll(context.Background())
		_ = r.Close()
	}
	if !errors.Is(err, errHistoryReferenceCorrupt) {
		t.Fatalf("corrupt Zstd frame accepted: %v", err)
	}
}

func TestHistoryReferenceContainerWriterSelectsSmallestCodec(t *testing.T) {
	dir := t.TempDir()
	w, err := newHistoryReferenceWriter(context.Background(), dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Release()
	compressible := bytes.Repeat([]byte("zstd chooses across the whole independent chunk;"), 2500)
	random := make([]byte, historyReferenceMaxChunk)
	rand.New(rand.NewSource(909)).Read(random)
	for _, raw := range [][]byte{compressible, random} {
		id, err := w.StoreChunk(raw)
		if err != nil {
			t.Fatal(err)
		}
		if err = w.WriteSpan(id, 0, uint32(len(raw))); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(dir, "codec-choice.r1")
	if _, _, err = w.Finalize(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	h, err := decodeHistoryReferenceHeader(data[:historyReferenceHeaderSize], uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	first, err := decodeHistoryReferenceChunk(data[h.chunkDir : h.chunkDir+historyReferenceChunkEntrySize])
	if err != nil || first.codec != historyReferenceCodecZstd {
		t.Fatalf("compressible chunk codec=%d err=%v", first.codec, err)
	}
	second, err := decodeHistoryReferenceChunk(data[h.chunkDir+historyReferenceChunkEntrySize : h.chunkDir+2*historyReferenceChunkEntrySize])
	if err != nil || second.codec != historyReferenceCodecRaw {
		t.Fatalf("incompressible chunk codec=%d err=%v", second.codec, err)
	}
	r, err := openHistoryReferenceReader(context.Background(), path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	// One ReadAt deliberately crosses the independently encoded chunk boundary.
	got := make([]byte, 64)
	off := len(compressible) - 32
	if _, err = r.ReadAt(got, int64(off)); err != nil || !bytes.Equal(got, append(bytes.Clone(compressible[off:]), random[:32]...)) {
		t.Fatalf("cross-chunk read: %v", err)
	}
}

// BenchmarkHistoryReferenceChunkCodecsSynthetic compares only in-memory codec
// work and sizes on generated bytes. It is not an online history throughput or
// production space-savings claim.
func BenchmarkHistoryReferenceChunkCodecsSynthetic(b *testing.B) {
	inputs := map[string][]byte{
		"repeated": bytes.Repeat([]byte("synthetic history owner/block/value;"), historyReferenceMaxChunk/36),
		"random":   make([]byte, historyReferenceMaxChunk),
	}
	rand.New(rand.NewSource(920)).Read(inputs["random"])
	enc, err := newHistoryReferenceZstdEncoder()
	if err != nil {
		b.Fatal(err)
	}
	defer enc.Close()
	dec, err := newHistoryReferenceZstdDecoder()
	if err != nil {
		b.Fatal(err)
	}
	defer dec.Close()
	for name, raw := range inputs {
		for _, codec := range []string{"snappy", "zstd"} {
			b.Run(name+"/"+codec+"/encode", func(b *testing.B) {
				b.SetBytes(int64(len(raw)))
				b.ReportAllocs()
				var stored []byte
				for i := 0; i < b.N; i++ {
					if codec == "snappy" {
						stored = snappy.Encode(stored[:0], raw)
					} else {
						stored = enc.EncodeAll(raw, stored[:0])
					}
				}
				b.ReportMetric(float64(len(stored)), "stored-B")
			})
			var stored []byte
			if codec == "snappy" {
				stored = snappy.Encode(nil, raw)
			} else {
				stored = enc.EncodeAll(raw, nil)
			}
			b.Run(name+"/"+codec+"/decode", func(b *testing.B) {
				b.SetBytes(int64(len(raw)))
				b.ReportAllocs()
				out := make([]byte, 0, len(raw))
				for i := 0; i < b.N; i++ {
					if codec == "snappy" {
						out, err = snappy.Decode(out[:0], stored)
					} else {
						out, err = dec.DecodeAll(stored, out[:0])
					}
					if err != nil || len(out) != len(raw) {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(len(stored)), "stored-B")
			})
		}
	}
}

func sealReferenceMetadata(t *testing.T, data []byte) {
	t.Helper()
	h, err := decodeHistoryReferenceHeader(data[:historyReferenceHeaderSize], uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	h.metadata, err = historyReferenceMetadataHash(context.Background(), bytes.NewReader(data), h)
	if err != nil {
		t.Fatal(err)
	}
	b := h.encode()
	copy(data, b[:])
}

func TestHistoryReferenceContainerCorruptionAndTruncation(t *testing.T) {
	data, _, _ := referenceContainerFixture(t, 0)
	h, err := decodeHistoryReferenceHeader(data[:128], uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	mutations := []struct {
		name   string
		seal   bool
		change func([]byte)
	}{
		{"metadata checksum", false, func(b []byte) { b[h.spanDir] ^= 1 }},
		{"chunk gap", true, func(b []byte) { binary.BigEndian.PutUint64(b[h.chunkDir:], 129) }},
		{"chunk overflow", true, func(b []byte) { binary.BigEndian.PutUint64(b[h.chunkDir:], math.MaxUint64) }},
		{"raw cap", true, func(b []byte) { binary.BigEndian.PutUint32(b[h.chunkDir+12:], historyReferenceMaxChunk+1) }},
		{"codec", true, func(b []byte) { binary.BigEndian.PutUint32(b[h.chunkDir+16:], 2) }},
		{"chunk reserved", true, func(b []byte) { b[h.chunkDir+63] = 1 }},
		{"span gap", true, func(b []byte) { binary.BigEndian.PutUint64(b[h.spanDir:], 1) }},
		{"span overlap", true, func(b []byte) { binary.BigEndian.PutUint64(b[h.spanDir+32:], 0) }},
		{"span overflow", true, func(b []byte) { binary.BigEndian.PutUint64(b[h.spanDir:], math.MaxUint64) }},
		{"reference chain or bad ID", true, func(b []byte) { binary.BigEndian.PutUint32(b[h.spanDir+8:], uint32(h.chunks)) }},
		{"span raw offset", true, func(b []byte) { binary.BigEndian.PutUint32(b[h.spanDir+12:], math.MaxUint32) }},
		{"zero span", true, func(b []byte) { binary.BigEndian.PutUint32(b[h.spanDir+16:], 0) }},
		{"span reserved", true, func(b []byte) { b[h.spanDir+31] = 1 }},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			b := bytes.Clone(data)
			tc.change(b)
			if tc.seal {
				sealReferenceMetadata(t, b)
			}
			r, err := newHistoryReferenceReader(context.Background(), bytes.NewReader(b), uint64(len(b)), nil, 0)
			if err == nil {
				err = r.ValidateLayout(context.Background())
				_ = r.Close()
			}
			if !errors.Is(err, errHistoryReferenceCorrupt) {
				t.Fatalf("malformed layout accepted: %v", err)
			}
		})
	}
	for _, size := range []int{0, 7, 127, 128, int(h.chunkDir) - 1, int(h.spanDir) - 1, len(data) - 1} {
		t.Run(fmt.Sprintf("truncated-%d", size), func(t *testing.T) {
			_, err := newHistoryReferenceReader(context.Background(), bytes.NewReader(data[:size]), uint64(size), nil, 0)
			if err == nil {
				t.Fatal("truncated container opened")
			}
		})
	}
	for _, offset := range []int{8, 12, 24, 40, 56, 127} {
		b := bytes.Clone(data)
		b[offset] = 0xff
		if _, err := newHistoryReferenceReader(context.Background(), bytes.NewReader(b), uint64(len(b)), nil, 0); err == nil {
			t.Fatalf("bad header %d accepted", offset)
		}
	}
	b := append(bytes.Clone(data), 0)
	if _, err := newHistoryReferenceReader(context.Background(), bytes.NewReader(b), uint64(len(b)), nil, 0); err == nil {
		t.Fatal("trailing bytes accepted")
	}
	// Payload integrity is not replaced by authenticated layout metadata.
	b = bytes.Clone(data)
	b[128] ^= 1
	r, err := newHistoryReferenceReader(context.Background(), bytes.NewReader(b), uint64(len(b)), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err = r.ValidateLayout(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = r.ValidateAll(context.Background()); !errors.Is(err, errHistoryReferenceCorrupt) {
		t.Fatalf("chunk corruption not detected: %v", err)
	}
}

func TestHistoryReferenceContainerStoreDigestBoundsAndPrefix(t *testing.T) {
	for _, kind := range []string{"false digest", "collision", "oversize", "empty", "span", "source patch", "prefix patch"} {
		t.Run(kind, func(t *testing.T) {
			w, err := newHistoryReferenceWriter(context.Background(), t.TempDir(), 16)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Release()
			id, err := w.StoreChunk([]byte("stored immutable"))
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "false digest":
				_, err = w.StoreChunkWithDigest([]byte("new"), [32]byte{})
			case "collision":
				_, err = w.StoreChunkWithDigest([]byte("differ immutable"), sha256.Sum256([]byte("stored immutable")))
			case "oversize":
				_, err = w.StoreChunk(make([]byte, historyReferenceMaxChunk+1))
			case "empty":
				_, err = w.StoreChunk(nil)
			case "span":
				err = w.WriteSpan(id, math.MaxUint32, 1)
			case "source patch":
				if err = w.WriteSpan(id, 0, 8); err == nil {
					_, err = w.WriteAt([]byte{0}, 0)
				}
			case "prefix patch":
				_, err = w.Write([]byte("literal"))
				if err == nil {
					_, err = w.WriteAt([]byte{0}, 16)
				}
			}
			if err == nil {
				t.Fatal("invalid input accepted")
			}
			path := filepath.Join(t.TempDir(), "failed.seg")
			if _, _, err := w.Finalize(path); err == nil {
				t.Fatal("poisoned writer finalized")
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatal("failed output exists", err)
			}
		})
	}
	w, err := newHistoryReferenceWriter(context.Background(), t.TempDir(), 3*historyReferenceMaxChunk)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Release()
	literal := bytes.Repeat([]byte{'a'}, 3*historyReferenceMaxChunk+10)
	if _, err := w.Write(literal); err != nil {
		t.Fatal(err)
	}
	patch := bytes.Repeat([]byte{'b'}, 20)
	if _, err := w.WriteAt(patch, historyReferenceMaxChunk-10); err != nil {
		t.Fatal(err)
	}
	copy(literal[historyReferenceMaxChunk-10:], patch)
	path := filepath.Join(t.TempDir(), "patched.seg")
	if _, _, err := w.Finalize(path); err != nil {
		t.Fatal(err)
	}
	r, err := openHistoryReferenceReader(context.Background(), path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	actual := make([]byte, len(literal))
	if _, err := r.ReadAt(actual, 0); err != nil || !bytes.Equal(actual, literal) {
		t.Fatal("flushed literal patch", err)
	}
	if _, _, err := w.Finalize(path); !errors.Is(err, errHistoryReferenceClosed) {
		t.Fatal("double finalize", err)
	}
}

type referenceCountingReader struct {
	io.ReaderAt
	reads, bytes int
	block        chan struct{}
	entered      chan struct{}
	once         sync.Once
}

func (r *referenceCountingReader) ReadAt(p []byte, off int64) (int, error) {
	if r.block != nil {
		r.once.Do(func() { close(r.entered) })
		<-r.block
	}
	r.reads++
	r.bytes += len(p)
	return r.ReaderAt.ReadAt(p, off)
}

func TestHistoryReferenceContainerReadBudgetCancellationAndClose(t *testing.T) {
	data, want, _ := referenceContainerFixture(t, 0)
	count := &referenceCountingReader{ReaderAt: bytes.NewReader(data)}
	ctx, cancel := context.WithCancel(context.Background())
	r, err := newHistoryReferenceReader(ctx, count, uint64(len(data)), nil, historyReferenceMaxChunk*3)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ValidateLayout(ctx); err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 1)
	if _, err := r.ReadAt(p, 100); err != nil || p[0] != want[100] {
		t.Fatal(err)
	}
	reads := count.reads
	if _, err := r.ReadAt(p, 100); err != nil || count.reads != reads {
		t.Fatal("cached read issued I/O", err)
	}
	if r.cacheBytes > r.cacheLimit || len(r.cache) > historyReferenceMaxCacheEntries {
		t.Fatal("cache bound")
	}
	cancel()
	if _, err := r.ReadAt(p, 100); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled cache hit", err)
	}
	if err := r.ValidateLayout(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatal("scope cancellation overridden", err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if r.cacheBytes != 0 || len(r.cache) != 0 {
		t.Fatal("close retained cache")
	}
	if _, err := r.ReadAt(p, 0); !errors.Is(err, errHistoryReferenceClosed) {
		t.Fatal(err)
	}
	// One-byte cold reads pay for one independent <=128KiB chunk plus bounded
	// directory pages, never a reconstruction from the segment start.
	count = &referenceCountingReader{ReaderAt: bytes.NewReader(data)}
	r, err = newHistoryReferenceReader(context.Background(), count, uint64(len(data)), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err = r.ValidateLayout(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := count.bytes
	if _, err = r.ReadAt(p, int64(len(want)-50)); err != nil {
		t.Fatal(err)
	}
	if count.bytes-before > historyReferenceMaxChunk+24*historyReferenceMetadataPage {
		t.Fatal("unbounded random read", count.bytes-before)
	}
	// Close waits for the in-flight read instead of closing its file underneath.
	count.block, count.entered = make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() { _, err := r.ReadAt(p, 100); done <- err }()
	<-count.entered
	closed := make(chan error, 1)
	go func() { closed <- r.Close() }()
	select {
	case <-closed:
		t.Fatal("Close passed active read")
	case <-time.After(10 * time.Millisecond):
	}
	close(count.block)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}

func TestHistoryReferenceContainerCancelFailureCleanupAndEmpty(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	w, err := newHistoryReferenceWriter(ctx, dir, 8)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	cancel()
	path := filepath.Join(dir, "not-created.seg")
	if _, _, err := w.Finalize(path); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := w.Release(); err != nil {
		t.Fatal(err)
	}
	if err := w.Release(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatal("scratch leaked", entries, err)
	}
	w, err = newHistoryReferenceWriter(context.Background(), dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Release()
	if _, _, err := w.Finalize(path); err != nil {
		t.Fatal(err)
	}
	r, err := openHistoryReferenceReader(context.Background(), path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := r.ValidateAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n, err := r.ReadAt(make([]byte, 1), 0); n != 0 || err != io.EOF {
		t.Fatal(n, err)
	}
	// Existing files are never overwritten, even if they are empty placeholders.
	w2, err := newHistoryReferenceWriter(context.Background(), dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Release()
	before, _ := os.ReadFile(path)
	if _, _, err := w2.Finalize(path); !os.IsExist(err) {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("overwrote foreign output")
	}
}

type referenceErrorCloser struct {
	err   error
	calls int
}

func (c *referenceErrorCloser) Close() error { c.calls++; return c.err }

type referenceShortReader struct {
	data   []byte
	failAt int
	calls  int
	err    error
}

func (r *referenceShortReader) ReadAt(p []byte, off int64) (int, error) {
	r.calls++
	if r.calls == r.failAt {
		return 0, r.err
	}
	return bytes.NewReader(r.data).ReadAt(p, off)
}

func TestHistoryReferenceContainerFailuresBudgetsAndUnusedPayload(t *testing.T) {
	data, _, _ := referenceContainerFixture(t, 0)
	sentinel := errors.New("physical read fault")
	for _, fail := range []int{1, 2, 3, 5} {
		input := &referenceShortReader{data: data, failAt: fail, err: sentinel}
		r, err := newHistoryReferenceReader(context.Background(), input, uint64(len(data)), nil, 0)
		if err == nil {
			err = r.ValidateAll(context.Background())
			_ = r.Close()
		}
		if !errors.Is(err, sentinel) {
			t.Fatalf("failure %d: %v", fail, err)
		}
	}
	short := &referenceShortReader{data: data, failAt: 1}
	if _, err := newHistoryReferenceReader(context.Background(), short, uint64(len(data)), nil, 0); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	closer := &referenceErrorCloser{err: sentinel}
	r, err := newHistoryReferenceReader(context.Background(), bytes.NewReader(data), uint64(len(data)), closer, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil || closer.calls != 1 {
		t.Fatal(err, closer.calls)
	}
	for _, limit := range []int{-1, historyReferenceMaxCache + 1} {
		if _, err := newHistoryReferenceReader(context.Background(), bytes.NewReader(data), uint64(len(data)), nil, limit); !errors.Is(err, errHistoryReferenceBudget) {
			t.Fatal(err)
		}
	}
	if _, err := newHistoryReferenceWriter(context.Background(), t.TempDir(), historyReferenceMaxPrefix+1); !errors.Is(err, errHistoryReferenceBudget) {
		t.Fatal(err)
	}
	w, err := newHistoryReferenceWriter(context.Background(), t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Release()
	if err := w.budget(historyReferenceMaxChunks, 0, 1); err != nil {
		t.Fatal(err)
	}
	for _, counts := range [][3]uint64{{historyReferenceMaxChunks + 1, 0, 0}, {0, historyReferenceMaxSpans + 1, 0}, {historyReferenceMaxChunks, historyReferenceMaxSpans, 0}, {0, 0, historyReferenceMaxPhysical}} {
		if err := w.budget(counts[0], counts[1], counts[2]); !errors.Is(err, errHistoryReferenceBudget) {
			t.Fatal(counts, err)
		}
	}
	// A corrupted scratch file cannot produce a finalized self-attestation.
	id, err := w.StoreChunk([]byte("an unused authenticated source chunk"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("logical data")); err != nil {
		t.Fatal(err)
	}
	if err := w.flushLiteral(); err != nil {
		t.Fatal(err)
	}
	chunk, err := w.chunk(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.raw.WriteAt([]byte{'X'}, int64(chunk.offset)); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "failed.seg")
	if _, _, err := w.Finalize(path); !errors.Is(err, errHistoryReferenceCorrupt) {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("partial output not removed", err)
	}
	// Validated but unused source data remains checked by full-file verification.
	w2, err := newHistoryReferenceWriter(context.Background(), t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Release()
	if _, err := w2.StoreChunk([]byte("unused source chunk")); err != nil {
		t.Fatal(err)
	}
	if _, err := w2.Write([]byte("literal only")); err != nil {
		t.Fatal(err)
	}
	path = filepath.Join(t.TempDir(), "valid.seg")
	if _, _, err := w2.Finalize(path); err != nil {
		t.Fatal(err)
	}
	unused, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	unused[128] ^= 1
	r, err = newHistoryReferenceReader(context.Background(), bytes.NewReader(unused), uint64(len(unused)), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	p := make([]byte, len("literal only"))
	if _, err := r.ReadAt(p, 0); err != nil || string(p) != "literal only" {
		t.Fatal(err)
	}
	if err := r.ValidateAll(context.Background()); !errors.Is(err, errHistoryReferenceCorrupt) {
		t.Fatal("unused corruption skipped", err)
	}
}

func TestHistoryReferenceContainerCacheEntryAndPayloadBound(t *testing.T) {
	w, err := newHistoryReferenceWriter(context.Background(), t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Release()
	for i := 0; i < 700; i++ {
		var raw [16]byte
		binary.BigEndian.PutUint64(raw[:], uint64(i))
		binary.BigEndian.PutUint64(raw[8:], uint64(i)^0x183949829401)
		id, err := w.StoreChunk(raw[:])
		if err != nil {
			t.Fatal(err)
		}
		if err := w.WriteSpan(id, 0, 16); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(t.TempDir(), "many.seg")
	if _, _, err := w.Finalize(path); err != nil {
		t.Fatal(err)
	}
	for _, budget := range []int{0, 32, historyReferenceMaxCache} {
		r, err := openHistoryReferenceReader(context.Background(), path, budget)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.ValidateAll(context.Background()); err != nil {
			t.Fatal(err)
		}
		if r.cacheBytes > budget || len(r.cache) > historyReferenceMaxCacheEntries || len(r.cacheOrder) > historyReferenceMaxCacheEntries {
			t.Fatal("cache bound", budget, r.cacheBytes, len(r.cache))
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
