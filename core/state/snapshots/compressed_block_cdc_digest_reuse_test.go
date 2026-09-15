package snapshots

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/tronprotocol/go-tron/core/state/statecodec"
	"github.com/tronprotocol/go-tron/internal/historychunk"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// Synthetic complete legacy delegation rows: adjacent versions insert one
// address in a large aggregate list. This is the measured data shape, not a
// copy of production addresses, records or a production-throughput estimate.
func cdcLegacyListShape(t testing.TB, addresses, versions int) []byte {
	t.Helper()
	rng := rand.New(rand.NewSource(915))
	arena := make([]byte, addresses*21)
	_, _ = rng.Read(arena)
	all := make([][]byte, addresses)
	for i := range all {
		all[i] = arena[i*21 : (i+1)*21 : (i+1)*21]
	}
	out := []byte("synthetic cold record metadata")
	for version := 0; version < versions; version++ {
		at := 97 + version*193
		list := append([][]byte(nil), all[:at]...)
		inserted := make([]byte, 21)
		binary.BigEndian.PutUint64(inserted[13:], uint64(version+1))
		list = append(list, inserted)
		list = append(list, all[at:]...)
		raw, err := statecodec.MarshalDelegationIndex(&corepb.DelegatedResourceAccountIndex{Account: all[0], FromAccounts: list, ToAccounts: all[:3], Timestamp: int64(version + 1)})
		if err != nil {
			t.Fatal(err)
		}
		out = binary.BigEndian.AppendUint64(out, uint64(version))
		out = binary.BigEndian.AppendUint32(out, uint32(len(raw)))
		out = append(out, raw...)
	}
	return out
}

func cdcDigestWriterForTest(t testing.TB, data []byte, workers, prefix int, frozen, collisions bool, level zstd.EncoderLevel) ([]byte, cdcWriteStats) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "out.seg")
	var stream historyCompressedStream
	var stats func() cdcWriteStats
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level), zstd.WithEncoderConcurrency(cdcMaxCompressionWorkers), zstd.WithWindowSize(historychunk.MaxSize), zstd.WithEncoderCRC(true))
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	if frozen {
		w, err := newFrozenCDCStreamWriterWorkers(context.Background(), dir, prefix, workers)
		if err != nil {
			t.Fatal(err)
		}
		w.enc = enc
		stream, stats = w, func() cdcWriteStats { return cdcFrozenStats(w.stats) }
	} else {
		w, err := newCDCStreamWriterWorkers(context.Background(), dir, prefix, workers)
		if err != nil {
			t.Fatal(err)
		}
		w.enc = enc
		if collisions {
			w.fingerprintForTest = func([]byte) uint64 { return 7 }
		}
		stream, stats = w, func() cdcWriteStats { return w.stats }
	}
	defer stream.Abort()
	for off := 0; off < len(data); {
		step := 193031
		if workers > 1 && off > 0 {
			step = 9 << 20
		}
		end := min(len(data), off+step)
		if _, err := stream.Write(data[off:end]); err != nil {
			t.Fatal(err)
		}
		off = end
	}
	// Exercise retained-prefix changes after reference/anchor queues exist.
	replacement := []byte("changed prefix")
	if _, err := stream.WriteAt(replacement, 0); err != nil {
		t.Fatal(err)
	}
	metadata, err := stream.FinishWithMetadataContext(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.size != uint64(len(encoded)) || metadata.checksum != sha256.Sum256(encoded) {
		t.Fatal("file checksum changed or bypassed")
	}
	decoded, err := decompressBlockBlob(encoded)
	expected := bytes.Clone(data)
	copy(expected, replacement)
	if err != nil || !bytes.Equal(decoded, expected) {
		t.Fatal("full decoded stream differs", err)
	}
	return encoded, stats()
}

func TestCDCDigestReuseFrozenFullFileOracle(t *testing.T) {
	random := make([]byte, 2<<20)
	rand.New(rand.NewSource(791)).Read(random)
	fixtures := []struct {
		name string
		raw  []byte
	}{
		{"legacy-lists", cdcLegacyListShape(t, 16000, 7)},
		{"random", random},
		{"same", bytes.Repeat([]byte("same small account value"), 100000)},
	}
	for _, fixture := range fixtures {
		for _, workers := range []int{1, 2, 4, 8} {
			for _, level := range []zstd.EncoderLevel{zstd.SpeedFastest, zstd.SpeedDefault} {
				t.Run(fmt.Sprintf("%s/workers%d/level%d", fixture.name, workers, level), func(t *testing.T) {
					prefix := 64
					if workers%4 == 0 {
						prefix = historychunk.MaxSize
					}
					want, oldStats := cdcDigestWriterForTest(t, fixture.raw, workers, prefix, true, false, level)
					for _, collision := range []bool{false, true} {
						got, stats := cdcDigestWriterForTest(t, fixture.raw, workers, prefix, false, collision, level)
						if !bytes.Equal(got, want) {
							t.Fatalf("collision=%v exact serialized output differs", collision)
						}
						if stats.Anchors != oldStats.Anchors || stats.References != oldStats.References || stats.ReusedBytes != oldStats.ReusedBytes {
							t.Fatal("anchor choice differs")
						}
						if !collision && fixture.name == "legacy-lists" && (stats.ReusedDigestChunks == 0 || stats.ReusedDigestBytes == 0) {
							t.Fatal("fixture did not exercise digest reuse")
						}
					}
				})
			}
		}
	}
}

func newCDCDigestDictionaryTestWriter() *cdcStreamWriter {
	return &cdcStreamWriter{dictionary: make(map[[sha256.Size]byte]*list.Element), fingerprints: make(map[cdcChunkFingerprint]*list.Element), fingerprintSeed: maphash.MakeSeed()}
}

func TestCDCDigestReuseCollisionLengthAndOwnedBytes(t *testing.T) {
	w := newCDCDigestDictionaryTestWriter()
	w.fingerprintForTest = func([]byte) uint64 { return 1 }
	add := func(raw []byte, ordinal uint32) *cdcDictionaryEntry {
		w.chunk = bytes.Clone(raw)
		digest, fp := w.chunkDigest()
		if digest != sha256.Sum256(raw) {
			t.Fatal("incorrect digest")
		}
		return w.remember(digest, fp, cdcEntry{anchor: cdcAnchor}, ordinal)
	}
	a := bytes.Repeat([]byte{7}, 128)
	old := add(a, 1)
	// Caller and producer buffers must not mutate the retained equality proof.
	a[0] = 99
	w.chunk[0] = 88
	if old.bytes[0] != 7 {
		t.Fatal("dictionary aliases caller/producer bytes")
	}
	add(bytes.Repeat([]byte{8}, 128), 2) // same length and forced fingerprint
	w.chunk = bytes.Repeat([]byte{7}, 128)
	digest, _ := w.chunkDigest()
	if digest != old.digest || w.stats.ReusedDigestChunks != 0 {
		t.Fatal("fingerprint collision was treated as equality")
	}
	w.chunk = bytes.Repeat([]byte{8}, 128)
	digest, _ = w.chunkDigest()
	if digest != sha256.Sum256(w.chunk) || w.stats.ReusedDigestChunks != 1 {
		t.Fatal("equal owned bytes did not reuse SHA")
	}
	w.chunk = bytes.Repeat([]byte{8}, 127)
	_, _ = w.chunkDigest()
	if w.stats.ReusedDigestChunks != 1 {
		t.Fatal("different length reused SHA")
	}
	// Even a stale secondary pointer cannot revive a removed primary anchor.
	element := w.dictionary[digest]
	delete(w.dictionary, digest)
	w.lru.Remove(element)
	w.dictionaryBytes -= len(element.Value.(*cdcDictionaryEntry).bytes)
	w.chunk = bytes.Repeat([]byte{8}, 128)
	_, _ = w.chunkDigest()
	if w.stats.ReusedDigestChunks != 1 {
		t.Fatal("retired primary anchor reused")
	}
}

func TestCDCDigestReuseBoundedEvictionAndReentry(t *testing.T) {
	for _, size := range []int{64, historychunk.MaxSize} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			w := newCDCDigestDictionaryTestWriter()
			count := cdcDictionaryEntries + 1
			if size == historychunk.MaxSize {
				count = cdcDictionaryBytes/size + 1
			}
			var first *cdcDictionaryEntry
			for i := 0; i < count; i++ {
				w.chunk = make([]byte, size)
				binary.BigEndian.PutUint64(w.chunk, uint64(i))
				digest, fp := w.chunkDigest()
				entry := w.remember(digest, fp, cdcEntry{anchor: cdcAnchor}, uint32(i+1))
				if i == 0 {
					first = entry
				}
				if len(w.fingerprints) > len(w.dictionary) || len(w.fingerprints) > cdcDictionaryEntries || w.dictionaryBytes > cdcDictionaryBytes {
					t.Fatal("unbounded secondary index")
				}
			}
			candidate := w.fingerprints[first.fingerprint]
			if w.dictionary[first.digest] != nil || candidate != nil && candidate.Value.(*cdcDictionaryEntry) == first {
				t.Fatal("evicted identity retained by index")
			}
			w.chunk = make([]byte, size)
			digest, fp := w.chunkDigest()
			if w.stats.ReusedDigestChunks != 0 {
				t.Fatal("evicted content reused SHA")
			}
			entry := w.remember(digest, fp, cdcEntry{anchor: cdcAnchor}, uint32(count+1))
			if entry == first || entry.ordinal == first.ordinal {
				t.Fatal("evicted anchor resurrected")
			}
			_, _ = w.chunkDigest()
			if w.stats.ReusedDigestChunks != 1 {
				t.Fatal("reentered bytes not reusable")
			}
			for fp, el := range w.fingerprints {
				e := el.Value.(*cdcDictionaryEntry)
				if e.fingerprint != fp || w.dictionary[e.digest] != el {
					t.Fatal("secondary index retains a non-primary identity")
				}
			}
		})
	}
}

func TestCDCDigestReuseLiteralReferenceDistanceOracle(t *testing.T) {
	for _, workers := range []int{1, 4} {
		dir := t.TempDir()
		actual, err := newCDCStreamWriterWorkers(context.Background(), dir, 64, workers)
		if err != nil {
			t.Fatal(err)
		}
		defer actual.Abort()
		actual.fingerprintForTest = func([]byte) uint64 { return 0 }
		old, err := newFrozenCDCStreamWriterWorkers(context.Background(), dir, 64, workers)
		if err != nil {
			t.Fatal(err)
		}
		defer old.Abort()
		prefix := bytes.Repeat([]byte{33}, 64)
		_, _ = actual.Write(prefix)
		_, _ = old.Write(prefix)
		if workers > 1 {
			if err := actual.startPipeline(); err != nil {
				t.Fatal(err)
			}
			if err := old.startPipeline(); err != nil {
				t.Fatal(err)
			}
		}
		chunk := make([]byte, 64)
		emit := func(id uint64) {
			binary.BigEndian.PutUint64(chunk, id)
			actual.chunk = append(actual.chunk[:0], chunk...)
			actual.logical += uint64(len(chunk))
			old.chunk = append(old.chunk[:0], chunk...)
			old.logical += uint64(len(chunk))
			if err := actual.flushChunk(); err != nil {
				t.Fatal(err)
			}
			if err := old.flushChunk(); err != nil {
				t.Fatal(err)
			}
		}
		emit(1)
		for i := 2; i <= cdcEntriesPerPage; i++ {
			emit(uint64(i))
		}
		emit(1) // a reference across a directory-page boundary, never ref-to-ref
		emit(1)
		for i := cdcEntriesPerPage + 1; i <= cdcEntriesPerPage+cdcDictionaryEntries+2; i++ {
			emit(uint64(i))
		}
		emit(1) // primary entry evicted: must become a new literal anchor
		emit(1)
		a, b := filepath.Join(dir, "new.seg"), filepath.Join(dir, "old.seg")
		if err := actual.Finish(a); err != nil {
			t.Fatal(err)
		}
		if err := old.Finish(b); err != nil {
			t.Fatal(err)
		}
		got, _ := os.ReadFile(a)
		want, _ := os.ReadFile(b)
		if !bytes.Equal(got, want) {
			t.Fatal("literal/reference distance or LRU differs")
		}
		r, err := openCDCReader(bytes.NewReader(got), uint64(len(got)), got[:48])
		if err != nil {
			t.Fatal(err)
		}
		before, _, err := r.entry(cdcEntriesPerPage + 1)
		if err != nil || before.anchor != 1 {
			t.Fatal("long-distance reference differs", err, before)
		}
		end, _, err := r.entry(r.count - 1)
		if err != nil || end.anchor != uint32(r.count-2) {
			t.Fatal("reentry did not select new literal", err, end)
		}
	}
	// The reserved uint32 sentinel remains rejected before hashing/admission.
	dir := t.TempDir()
	w, err := newCDCStreamWriter(context.Background(), dir, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	old, err := newFrozenCDCStreamWriterWorkers(context.Background(), dir, 64, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Abort()
	w.count, old.count = uint64(math.MaxUint32-1), uint64(math.MaxUint32-1)
	w.chunk, old.chunk = []byte{1}, []byte{1}
	a, b := w.flushChunk(), old.flushChunk()
	if a == nil || b == nil || a.Error() != b.Error() || len(w.fingerprints) != 0 {
		t.Fatal("ordinal sentinel behavior changed", a, b)
	}
}

func BenchmarkCDCDigestReuseLegacyLists(b *testing.B) {
	for _, input := range []struct {
		name string
		data []byte
	}{
		{"LargeLegacyLists", cdcLegacyListShape(b, 95000, 16)},
		{"LowReuse", func() []byte { p := make([]byte, 8<<20); rand.New(rand.NewSource(91)).Read(p); return p }()},
	} {
		for _, workers := range []int{1, 4} {
			for _, frozen := range []bool{true, false} {
				b.Run(fmt.Sprintf("%s/workers%d/frozen%v", input.name, workers, frozen), func(b *testing.B) {
					dir := b.TempDir()
					path := filepath.Join(dir, "out.seg")
					write := func(frozen bool) cdcWriteStats {
						var stream historyCompressedStream
						var stats func() cdcWriteStats
						if frozen {
							w, err := newFrozenCDCStreamWriterWorkers(context.Background(), dir, historychunk.MaxSize, workers)
							if err != nil {
								b.Fatal(err)
							}
							stream = w
							stats = func() cdcWriteStats { return cdcFrozenStats(w.stats) }
						} else {
							w, err := newCDCStreamWriterWorkers(context.Background(), dir, historychunk.MaxSize, workers)
							if err != nil {
								b.Fatal(err)
							}
							stream = w
							stats = func() cdcWriteStats { return w.stats }
						}
						defer stream.Abort()
						if _, err := stream.Write(input.data); err != nil {
							b.Fatal(err)
						}
						if _, err := stream.FinishWithMetadataContext(context.Background(), path); err != nil {
							b.Fatal(err)
						}
						return stats()
					}
					write(true)
					expected, err := os.ReadFile(path)
					if err != nil {
						b.Fatal(err)
					}
					write(false)
					actual, err := os.ReadFile(path)
					if err != nil || !bytes.Equal(actual, expected) {
						b.Fatal("full writer bytes differ from frozen oracle", err)
					}
					stats := write(frozen)
					encoded, err := os.ReadFile(path)
					if err != nil {
						b.Fatal(err)
					}
					decoded, err := decompressBlockBlob(encoded)
					if err != nil || !bytes.Equal(decoded, input.data) {
						b.Fatal("full writer roundtrip", err)
					}
					encodedBytes := len(encoded)
					b.SetBytes(int64(len(input.data)))
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						stats = write(frozen)
					}
					b.StopTimer()
					b.ReportMetric(float64(encodedBytes), "encoded-B")
					b.ReportMetric(float64(stats.ReusedDigestBytes), "SHA-reused-B/op")
					b.ReportMetric(float64(stats.ReusedDigestChunks), "SHA-reused-chunks/op")
				})
			}
		}
	}
}

func TestCDCDigestReuseResetAndCanceledFinishReleaseIndex(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir := t.TempDir()
	w, err := newCDCStreamWriterWorkers(ctx, dir, 64, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	data := cdcRepeatedInput(128<<10, 3)
	if _, err = w.Write(data); err != nil {
		t.Fatal(err)
	}
	if len(w.fingerprints) == 0 {
		t.Fatal("fixture has no secondary entries")
	}
	if err = w.Reset(); err != nil {
		t.Fatal(err)
	}
	if len(w.fingerprints) != 0 || len(w.dictionary) != 0 || w.lru.Len() != 0 {
		t.Fatal("Reset retained old identities")
	}
	if _, err = w.Write(data); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err = w.Finish(filepath.Join(dir, "canceled.seg")); err == nil {
		t.Fatal("canceled Finish succeeded")
	}
	if w.fingerprints != nil || w.dictionary != nil || w.lru.Len() != 0 || w.pipeline != nil {
		t.Fatal("canceled Finish retained dictionary or workers")
	}
	if _, err = os.Stat(filepath.Join(dir, "canceled.seg")); !os.IsNotExist(err) {
		t.Fatal("canceled file published", err)
	}
}

func cdcFrozenStats(s frozenCDCWriteStats) cdcWriteStats {
	return cdcWriteStats{InputBytes: s.InputBytes, AnchorBytes: s.AnchorBytes, ReusedBytes: s.ReusedBytes, Anchors: s.Anchors, References: s.References, Pipeline: s.Pipeline}
}
