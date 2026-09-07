package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

func v6StreamChanges(from, count, txsPerBlock uint64, valueSize int) []*rawdb.StateDomainChange {
	changes := make([]*rawdb.StateDomainChange, 0, count)
	for i := uint64(0); i < count; i++ {
		tx := from + i
		c := binaryStateDomainChange((tx-1)/txsPerBlock+1, tx, 1, fmt.Sprintf("slot/%05d", (i*7919)%count))
		c.Prev = bytes.Repeat([]byte{byte(i), byte(i >> 8), 0xff}, valueSize/3+2)[:valueSize+int(i%3)]
		switch i % 19 {
		case 0:
			c.Prev, c.PrevExists = nil, false
		case 1:
			c.Prev = nil // present empty value is distinct from absence
		}
		changes = append(changes, c)
	}
	return changes
}

// The existing V6 fixture writes plain bytes. Explicitly compress its history
// here so these tests/benchmarks exercise the production compressed V6 path.
func compressV6StreamFixture(t testing.TB, dir string, refs []SegmentRef, chunkSize int) {
	t.Helper()
	for i := range refs {
		if refs[i].Kind != SegmentHistory {
			continue
		}
		path := filepath.Join(dir, refs[i].Path)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := compressBlobToFile(dir, path, data, chunkSize); err != nil {
			t.Fatal(err)
		}
		refs[i].Size, refs[i].Checksum, err = stateDomainChangeBinaryFileMetadata(path)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestHistoryV6StreamMatchesPlainMerge(t *testing.T) {
	t.Setenv("GTRON_HISTORY_COMPACTION_SOURCE_WORKERS", "2")
	for _, format := range []string{"1", "2"} {
		t.Run("format"+format, func(t *testing.T) {
			t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", format)
			first, second := v6StreamChanges(1, 513, 3, 80), v6StreamChanges(514, 513, 3, 80)
			// Force multi-block payloads, followed immediately by tiny records.
			first[17].Prev = bytes.Repeat([]byte{0xa3}, 70<<10)
			second[31].Prev = bytes.Repeat([]byte{0x5c}, 35<<10)
			var expected map[SegmentKind][]byte
			for _, chunk := range []int{0, 257, 4096, 16384} {
				t.Run(fmt.Sprintf("chunk%d", chunk), func(t *testing.T) {
					dir := t.TempDir()
					refs := writeV6StateDomainHistorySegmentForTest(t, dir, 1, 513, first)
					refs = append(refs, writeV6StateDomainHistorySegmentForTest(t, dir, 514, 1026, second)...)
					if chunk != 0 {
						compressV6StreamFixture(t, dir, refs, chunk)
					}
					if err := PublishManifest(dir, NewManifest(1, 1026, refs)); err != nil {
						t.Fatal(err)
					}
					result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{})
					if err != nil || !result.Merged {
						t.Fatalf("merge=%v err=%v", result.Merged, err)
					}
					got := make(map[SegmentKind][]byte)
					for _, ref := range result.Segments {
						got[ref.Kind], err = os.ReadFile(filepath.Join(dir, ref.Path))
						if err != nil {
							t.Fatal(err)
						}
					}
					if expected == nil {
						expected = got
					} else if !reflect.DeepEqual(got, expected) {
						t.Fatal("compressed source merge differs from plain source merge bytes")
					}
					if _, err := VerifyLoadedManifestFiles(dir, NewManifest(1, 1026, result.Segments), VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}

func TestHistoryV6StreamRejectsMalformedSource(t *testing.T) {
	t.Setenv("GTRON_HISTORY_COMPACTION_SOURCE_WORKERS", "2")
	for _, fault := range []string{"key-id", "missing-range", "marker", "length", "order", "trailing", "checksum"} {
		t.Run(fault, func(t *testing.T) {
			t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "2")
			dir := t.TempDir()
			refs := writeV6StateDomainHistorySegmentForTest(t, dir, 1, 6, v6StreamChanges(1, 6, 1, 256))
			refs = append(refs, writeV6StateDomainHistorySegmentForTest(t, dir, 7, 12, v6StreamChanges(7, 6, 1, 256))...)
			path := filepath.Join(dir, refs[0].Path)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			r := bytes.NewReader(data)
			header, err := readStateDomainChangeBinaryHeaderAt(r, stateDomainChangeBinarySegmentMagic)
			if err != nil {
				t.Fatal(err)
			}
			_, off, err := stateDomainChangeBinaryTxRangeTableBoundsAt(r, uint64(len(data)), refs[0], header)
			if err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "key-id":
				binary.BigEndian.PutUint32(data[off+4:off+8], ^uint32(0))
			case "missing-range":
				binary.BigEndian.PutUint64(data[off+8:off+16], 0)
			case "marker":
				data[off+16] = 2
			case "length":
				binary.BigEndian.PutUint32(data[off:off+4], ^uint32(0))
			case "order":
				next := off + 4 + uint64(binary.BigEndian.Uint32(data[off:off+4]))
				binary.BigEndian.PutUint64(data[off+8:off+16], 2)
				binary.BigEndian.PutUint64(data[next+8:next+16], 1)
			case "trailing":
				data = append(data, 0xa5)
			}
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			compressV6StreamFixture(t, dir, refs, 257)
			if fault == "checksum" {
				refs[0].Checksum = "sha256:" + strings.Repeat("0", 64)
			}
			if err := PublishManifest(dir, NewManifest(1, 12, refs)); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(filepath.Join(dir, ManifestFile))
			if err != nil {
				t.Fatal(err)
			}
			files := historyCancellationFiles(t, dir)
			result, err := CompactHistoryDomainContext(context.Background(), dir, SegmentDatasetStateDomainChange, CompactionConfig{DeleteObsolete: true})
			if err == nil || result.Merged {
				t.Fatalf("malformed merge accepted: %+v %v", result, err)
			}
			after, readErr := os.ReadFile(filepath.Join(dir, ManifestFile))
			if readErr != nil || !bytes.Equal(before, after) || !reflect.DeepEqual(files, historyCancellationFiles(t, dir)) {
				t.Fatalf("failed merge changed manifest/files: %v", readErr)
			}
		})
	}
}

func BenchmarkHistoryV6StreamMerge(b *testing.B) {
	for _, shape := range []struct {
		name        string
		txsPerBlock uint64
		valueSize   int
	}{{"many-ranges", 1, 64}, {"mainnet-density", 100, 256}, {"large-values", 100, 16384}} {
		b.Run(shape.name, func(b *testing.B) {
			b.StopTimer()
			const count = uint64(8192)
			first := v6StreamChanges(1, count, shape.txsPerBlock, shape.valueSize)
			second := v6StreamChanges(count+1, count, shape.txsPerBlock, shape.valueSize)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir := b.TempDir()
				refs := writeV6StateDomainHistorySegmentForTest(b, dir, 1, count, first)
				refs = append(refs, writeV6StateDomainHistorySegmentForTest(b, dir, count+1, count*2, second)...)
				compressV6StreamFixture(b, dir, refs, historyCompressChunkSize)
				if err := PublishManifest(dir, NewManifest(1, count*2, refs)); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{})
				if err != nil || !result.Merged {
					b.Fatalf("merged=%v err=%v", result.Merged, err)
				}
				b.StopTimer()
				if err := os.RemoveAll(dir); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type v6StreamInterruptWriter struct {
	cancel context.CancelFunc
	err    error
	wrote  bool
}

func (w *v6StreamInterruptWriter) Write(p []byte) (int, error) {
	w.wrote = true
	if w.cancel != nil {
		w.cancel()
	}
	if w.err != nil {
		return 0, w.err
	}
	return len(p), nil
}

func TestHistoryV6StreamInterruptsActiveCopy(t *testing.T) {
	for _, cancelCopy := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%v", cancelCopy), func(t *testing.T) {
			t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "2")
			dir := t.TempDir()
			const count = uint64(8192)
			refs := writeV6StateDomainHistorySegmentForTest(t, dir, 1, count, v6StreamChanges(1, count, 3, 256))
			compressV6StreamFixture(t, dir, refs, 4096)
			progress := newHistoryCompactionProgress(SegmentDatasetStateDomainChange, 1, count, 1)
			defer progress.finish(nil)
			selection := historyCompactionSelection{candidates: []historyCompactionCandidate{{history: refs[0], companions: refs[1:]}}}
			sources, err := collectStateDomainChangeBinaryCompactionSources(context.Background(), dir, selection, progress)
			if err != nil {
				t.Fatal(err)
			}
			build, err := newStateDomainChangeV6Build(etl.Options{TempDir: filepath.Join(dir, "etl")}, dir, "output.seg")
			if err != nil {
				t.Fatal(err)
			}
			defer build.Close()
			if err := collectStateDomainChangeBinarySegmentV6Keys(context.Background(), dir, build, sources[0]); err != nil {
				t.Fatal(err)
			}
			if err := build.FinishDictionaryContext(context.Background()); err != nil {
				t.Fatal(err)
			}
			index, err := os.Create(filepath.Join(dir, "output.idx"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := index.Close(); err != nil {
					t.Error(err)
				}
			}()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := errors.New("injected destination write failure")
			sink := &v6StreamInterruptWriter{err: want}
			if cancelCopy {
				sink.cancel, sink.err, want = cancel, nil, context.Canceled
			}
			writer := newStateDomainChangeHistoryRecordWriterV6(sink, index, build, refs[0], count, sources[0].recordOffset)
			defer writer.Release()
			err = copyStateDomainChangeBinarySegmentPayload(ctx, dir, writer, build, sources[0], progress, 0)
			if !errors.Is(err, want) || !sink.wrote || writer.count == 0 || writer.count >= count {
				t.Fatalf("copy wrote=%v count=%d err=%v, want active copy interrupted by %v", sink.wrote, writer.count, err, want)
			}
		})
	}
}

// Optional same-file component benchmark. The fixture must contain only an
// isolated manifest and pinned immutable history files; no chain DB is opened.
// Each iteration writes into its own temporary directory and validates the
// published trio outside the timed merge.
func BenchmarkHistoryV6StreamProduction(b *testing.B) {
	b.StopTimer()
	fixture := os.Getenv("GTRON_V6_STREAM_FIXTURE")
	if fixture == "" {
		b.Skip("GTRON_V6_STREAM_FIXTURE is not set")
	}
	manifest, err := LoadManifest(fixture)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dir := b.TempDir()
		for _, ref := range manifest.Segments {
			target := filepath.Join(dir, ref.Path)
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				b.Fatal(err)
			}
			if err := os.Link(filepath.Join(fixture, ref.Path), target); err != nil {
				b.Fatal(err)
			}
		}
		if err := PublishManifest(dir, NewManifest(manifest.VisibleTxStart, manifest.VisibleTxEnd, manifest.Segments)); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{})
		b.StopTimer()
		if err != nil || !result.Merged {
			b.Fatalf("merged=%v err=%v", result.Merged, err)
		}
		if _, err := VerifyLoadedManifestFiles(dir, NewManifest(result.FromTxNum, result.ToTxNum, result.Segments), VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
			b.Fatal(err)
		}
		for _, ref := range result.Segments {
			b.Logf("output %s %d %s", ref.Kind, ref.Size, ref.Checksum)
		}
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
	}
}
