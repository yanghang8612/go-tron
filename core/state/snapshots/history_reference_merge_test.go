package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func referenceMergeFixture(t *testing.T, mode string) (string, DomainCfg, historyCompactionSelection, []stateDomainChangeBinaryCompactionSource) {
	t.Helper()
	dir := t.TempDir()
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	first, second := v6StreamChanges(1, 7, 3, 200), v6StreamChanges(8, 11, 3, 200)
	first[2].TxNum = first[1].TxNum
	first[3].Prev = bytes.Repeat([]byte("large previous version"), 100000)
	// This is accepted by the old cold format and must survive a merge, even
	// though the new hot-source builder does not generate absent/nonempty rows.
	first[4].PrevExists = false
	first[4].Prev = []byte("legacy absent bytes")
	second[4].TxNum = second[3].TxNum
	second[5].Prev = append([]byte(nil), first[3].Prev...)
	left := writeV6StateDomainHistorySegmentForTest(t, dir, 1, 7, first)
	var right []SegmentRef
	if mode == "legacy-v5" || mode == "legacy-v2" {
		right = writeCompactionStateDomainChangeSegment(t, dir, 8, 18, second...)
		if mode == "legacy-v2" {
			rewriteStateDomainChangeHistoryAsV2(t, dir, right, second...)
		}
	} else {
		right = writeV6StateDomainHistorySegmentForTest(t, dir, 8, 18, second)
	}
	if mode == "codec1" || mode == "codec2" || mode == "codec3" {
		t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", mode[len(mode)-1:])
		compressV6StreamFixture(t, dir, right, 4096)
	}
	var err error
	left, _, err = ReencodeHistoryReferenceTrioContext(context.Background(), dir, left)
	if err != nil {
		t.Fatal(err)
	}
	if mode == "r1" {
		right, _, err = ReencodeHistoryReferenceTrioContext(context.Background(), dir, right)
		if err != nil {
			t.Fatal(err)
		}
	}
	selection := historyCompactionSelection{fromTxNum: 1, toTxNum: 18, aggregationSteps: 2, candidates: []historyCompactionCandidate{{history: left[0], companions: left[1:]}, {history: right[0], companions: right[1:]}}}
	sources, err := collectStateDomainChangeBinaryCompactionSources(context.Background(), dir, selection, nil)
	if err != nil {
		t.Fatal(err)
	}
	return dir, cfg, selection, sources
}

func referenceMergeKindBytes(t *testing.T, dir string, refs []SegmentRef) map[SegmentKind][]byte {
	t.Helper()
	out := make(map[SegmentKind][]byte)
	for _, ref := range refs {
		var b []byte
		var err error
		if ref.Kind == SegmentHistory {
			b = referenceBuildLogicalBytes(t, dir, ref)
		} else {
			b, err = os.ReadFile(filepath.Join(dir, ref.Path))
			if err != nil {
				t.Fatal(err)
			}
		}
		out[ref.Kind] = b
	}
	return out
}

func TestHistoryReferenceMergeFullOldTrioOracle(t *testing.T) {
	for _, mode := range []string{"r1", "raw", "codec1", "codec2", "codec3", "legacy-v5", "legacy-v2"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "auto")
			dir, cfg, selection, sources := referenceMergeFixture(t, mode)
			// Call the unchanged pre-R1 writer directly, so the oracle cannot be
			// redirected by the production dispatch hook being implemented separately.
			old, idx, acc, err := writeCompactedStateDomainChangeBinaryFiles(context.Background(), dir, cfg, selection, sources, nil)
			if err != nil {
				t.Fatal(err)
			}
			want := referenceMergeKindBytes(t, dir, []SegmentRef{old, acc, idx})
			inputs := make(map[string][32]byte)
			for _, candidate := range selection.candidates {
				for _, ref := range append([]SegmentRef{candidate.history}, candidate.companions...) {
					raw, err := os.ReadFile(filepath.Join(dir, ref.Path))
					if err != nil {
						t.Fatal(err)
					}
					inputs[ref.Path] = sha256.Sum256(raw)
				}
			}
			refs, err := compactStateDomainChangeReferenceHistoryRunContext(context.Background(), dir, cfg, selection, sources, nil)
			if err != nil {
				t.Fatal(err)
			}
			gotBytes := referenceMergeKindBytes(t, dir, refs)
			for kind, expected := range want {
				actual := gotBytes[kind]
				if !bytes.Equal(actual, expected) {
					at := 0
					for at < min(len(actual), len(expected)) && actual[at] == expected[at] {
						at++
					}
					t.Fatalf("complete %s differs: lengths %d/%d first offset %d got=%x want=%x", kind, len(actual), len(expected), at, actual[at:min(len(actual), at+24)], expected[at:min(len(expected), at+24)])
				}
			}
			history := referenceBuildHistoryRef(t, refs)
			physical, err := os.ReadFile(filepath.Join(dir, history.Path))
			if err != nil || string(physical[:8]) != historyReferenceMagic {
				t.Fatal("merge lost R1", err)
			}
			if _, err := VerifyLoadedManifestFiles(dir, NewManifest(1, 18, refs), VerifyManifestOptions{RequireChecksums: true, RequireRegistered: true}); err != nil {
				t.Fatal(err)
			}
			for path, hash := range inputs {
				raw, err := os.ReadFile(filepath.Join(dir, path))
				if err != nil || sha256.Sum256(raw) != hash {
					t.Fatal("input changed", path, err)
				}
			}
			// The output owns every chunk; remove all admitted source trios and still
			// perform complete logical and companion reads, not a header-only check.
			for path := range inputs {
				if path != history.Path {
					if err := os.Remove(filepath.Join(dir, path)); err != nil {
						t.Fatal(err)
					}
				}
			}
			if !reflect.DeepEqual(referenceMergeKindBytes(t, dir, refs), want) {
				t.Fatal("merge depends on removed inputs")
			}
		})
	}
}

func TestHistoryReferenceMergeV6DoesNotMaterializePrev(t *testing.T) {
	dir, _, _, sources := referenceMergeFixture(t, "r1")
	if err := os.MkdirAll(filepath.Join(dir, "etl"), 0700); err != nil {
		t.Fatal(err)
	}
	w, err := newHistoryReferenceWriter(context.Background(), filepath.Join(dir, "etl"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Release()
	var rows, large int
	for _, source := range sources {
		err = scanReferenceMergeSource(context.Background(), dir, source, w, func(row *rawdb.StateDomainChange, prev uint64, spans []historyReferenceValueSpan) error {
			rows++
			if len(row.Prev) != 0 || len(row.Next) != 0 {
				t.Fatal("V6 expanded Prev or Next")
			}
			var n uint64
			for _, span := range spans {
				n += uint64(span.length)
			}
			if n != prev {
				t.Fatal("wrong total")
			}
			if prev > 1<<20 {
				large++
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if rows != 18 || large != 2 {
		t.Fatalf("fixture not exercised rows=%d large=%d", rows, large)
	}
}

func TestHistoryReferenceMergeSpanIntervalsAndOwnership(t *testing.T) {
	_, want, r := referenceContainerFixture(t, historyReferenceMaxChunk)
	t.Cleanup(func() { _ = r.Close() })
	w, err := newHistoryReferenceWriter(context.Background(), t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Release()
	mapping := make([]uint32, r.header.chunks)
	for i := range mapping {
		mapping[i] = math.MaxUint32
	}
	var expected []byte
	for _, interval := range [][2]uint64{{0, 1}, {63, 4}, {77, 100000}, {uint64(len(want)) - 1, 1}, {0, uint64(len(want))}} {
		spans, err := r.copyValueSpans(context.Background(), w, mapping, interval[0], interval[1])
		if err != nil {
			t.Fatal(err)
		}
		for _, span := range spans {
			if err := w.WriteSpan(span.chunk, span.offset, span.length); err != nil {
				t.Fatal(err)
			}
		}
		expected = append(expected, want[interval[0]:interval[0]+interval[1]]...)
		// Returned metadata may be mutated without touching source or destination.
		for i := range spans {
			spans[i] = historyReferenceValueSpan{math.MaxUint32, 0, 0}
		}
	}
	count := w.Stats().StoredChunks
	if _, err := r.copyValueSpans(context.Background(), w, mapping, 0, uint64(len(want))); err != nil || w.Stats().StoredChunks != count {
		t.Fatal("reimported mapped chunks", err)
	}
	for _, bad := range [][2]uint64{{math.MaxUint64, 1}, {uint64(len(want)), 1}, {0, math.MaxUint64}} {
		if _, err := r.copyValueSpans(context.Background(), w, mapping, bad[0], bad[1]); err == nil {
			t.Fatal("bad interval accepted", bad)
		}
	}
	if _, err := r.copyValueSpans(context.Background(), w, mapping[:0], 0, 1); err == nil {
		t.Fatal("wrong mapping accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.copyValueSpans(ctx, w, mapping, 0, 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	output := filepath.Join(w.dir, "joined.r1")
	if _, _, err := w.Finalize(output); err != nil {
		t.Fatal(err)
	}
	result, err := openHistoryReferenceReader(context.Background(), output, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	got, err := io.ReadAll(io.NewSectionReader(result, 0, int64(result.UncompressedSize())))
	if err != nil || !bytes.Equal(got, expected) {
		t.Fatal("interval reconstruction", err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.copyValueSpans(context.Background(), w, mapping, 0, 1); !errors.Is(err, errHistoryReferenceClosed) {
		t.Fatal("expired source", err)
	}
}

func TestHistoryReferenceMergeCancellationAndSourceFailure(t *testing.T) {
	for _, fault := range []string{"already-cancelled", "active-cancel", "bad-source-checksum", "missing-source", "bad-range", "too-many-sources"} {
		t.Run(fault, func(t *testing.T) {
			dir, cfg, selection, sources := referenceMergeFixture(t, "r1")
			var active []SegmentRef
			for _, c := range selection.candidates {
				active = append(active, c.history)
				active = append(active, c.companions...)
			}
			if err := PublishManifest(dir, NewManifest(1, 18, active)); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(filepath.Join(dir, ManifestFile))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch fault {
			case "already-cancelled":
				cancel()
			case "bad-source-checksum":
				sources[0].history.Checksum = "sha256:" + string(bytes.Repeat([]byte{'0'}, 64))
			case "missing-source":
				if err := os.Remove(filepath.Join(dir, sources[0].history.Path)); err != nil {
					t.Fatal(err)
				}
			case "bad-range":
				sources[1].history.FromTxNum++
			case "too-many-sources":
				sources = make([]stateDomainChangeBinaryCompactionSource, historyReferenceMergeMaxSources+1)
			case "active-cancel":
				w, err := newHistoryReferenceWriter(ctx, dir, 0)
				if err != nil {
					t.Fatal(err)
				}
				count := 0
				err = scanReferenceMergeSource(ctx, dir, sources[0], w, func(*rawdb.StateDomainChange, uint64, []historyReferenceValueSpan) error {
					count++
					cancel()
					return nil
				})
				if !errors.Is(err, context.Canceled) || count != 1 {
					t.Fatal("active cancel", err, count)
				}
				if err := w.Release(); err != nil {
					t.Fatal(err)
				}
			}
			refs, err := compactStateDomainChangeReferenceHistoryRunContext(ctx, dir, cfg, selection, sources, nil)
			if err == nil || len(refs) != 0 {
				t.Fatal("fault succeeded", fault, err)
			}
			after, readErr := os.ReadFile(filepath.Join(dir, ManifestFile))
			if readErr != nil || !bytes.Equal(before, after) {
				t.Fatal("failure published", readErr)
			}
			matches, _ := filepath.Glob(filepath.Join(dir, "etl", ".history-reference-*"))
			if len(matches) != 0 {
				t.Fatal("scratch leaked", matches)
			}
			matches, _ = filepath.Glob(filepath.Join(dir, "etl", "history-reference-merge-*.spool"))
			if len(matches) != 0 {
				t.Fatal("spool leaked", matches)
			}
		})
	}
}

func TestHistoryReferenceMergeRejectsSemanticV6Corruption(t *testing.T) {
	for _, fault := range []string{"marker", "length", "key", "tx", "trailing"} {
		t.Run(fault, func(t *testing.T) {
			dir, _, _, sources := referenceMergeFixture(t, "raw")
			source := sources[1]
			raw, err := os.ReadFile(filepath.Join(dir, source.history.Path))
			if err != nil {
				t.Fatal(err)
			}
			off := source.recordOffset
			switch fault {
			case "marker":
				raw[off+16] = 2
			case "length":
				binary.BigEndian.PutUint32(raw[off:off+4], math.MaxUint32)
			case "key":
				binary.BigEndian.PutUint32(raw[off+4:off+8], math.MaxUint32)
			case "tx":
				binary.BigEndian.PutUint64(raw[off+8:off+16], 0)
			case "trailing":
				raw = append(raw, 0)
				source.segmentSize++
				source.history.Size++
			}
			if err = os.WriteFile(filepath.Join(dir, source.history.Path), raw, 0600); err != nil {
				t.Fatal(err)
			}
			w, err := newHistoryReferenceWriter(context.Background(), dir, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Release()
			err = scanReferenceMergeSource(context.Background(), dir, source, w, func(*rawdb.StateDomainChange, uint64, []historyReferenceValueSpan) error { return nil })
			if err == nil {
				t.Fatal("corrupt source accepted")
			}
		})
	}
}

func TestHistoryReferenceMergeValidatesUnusedChunks(t *testing.T) {
	dir, _, _, sources := referenceMergeFixture(t, "r1")
	source := sources[0]
	logical := referenceBuildLogicalBytes(t, dir, source.history)
	w, err := newHistoryReferenceWriter(context.Background(), dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Release()
	if _, err = w.StoreChunk([]byte("never referenced but authenticated")); err != nil {
		t.Fatal(err)
	}
	if err = historyReferenceTranscodeBytes(w, logical); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(w.dir, "unused.r1")
	if _, _, err = w.Finalize(path); err != nil {
		t.Fatal(err)
	}
	physical, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	physical[historyReferenceHeaderSize] ^= 0x40
	target := filepath.Join(dir, source.history.Path)
	if err = os.WriteFile(target, physical, 0600); err != nil {
		t.Fatal(err)
	}
	source.history.Size, source.history.Checksum, err = stateDomainChangeBinaryFileMetadata(target)
	if err != nil {
		t.Fatal(err)
	}
	dst, err := newHistoryReferenceWriter(context.Background(), dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Release()
	rows := 0
	err = scanReferenceMergeSource(context.Background(), dir, source, dst, func(*rawdb.StateDomainChange, uint64, []historyReferenceValueSpan) error { rows++; return nil })
	if !errors.Is(err, errHistoryReferenceCorrupt) || rows != int(source.segmentHeader.count) {
		t.Fatal("unused payload not authenticated after referenced rows", err, rows)
	}
}

// A source table is reopened after record collection. Alter only one hash byte
// at that exact phase: header/count/size/ranges remain legal, so only a final
// comparison with the admitted physical SHA rejects the substituted table.
type referenceMergeTableMutationContext struct {
	context.Context
	progress *historyCompactionProgress
	mutate   func()
	once     sync.Once
}

func (c *referenceMergeTableMutationContext) Err() error {
	if c.progress.phase.Load() == historyCompactionPhaseWriteTxRanges {
		c.once.Do(c.mutate)
	}
	return c.Context.Err()
}
func TestHistoryReferenceMergeRevalidatesAfterFinalSourceRead(t *testing.T) {
	dir, cfg, selection, sources := referenceMergeFixture(t, "raw")
	source := sources[1]
	if source.txRangeCount == 0 {
		t.Fatal("fixture missing tx table")
	}
	path := filepath.Join(dir, source.history.Path)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	progress := newHistoryCompactionProgress(cfg.Dataset, selection.fromTxNum, selection.toTxNum, len(sources))
	var mutationErr error
	mutated := false
	ctx := &referenceMergeTableMutationContext{Context: context.Background(), progress: progress}
	ctx.mutate = func() {
		// The final block occurs only in the second source, avoiding the existing
		// overlapping-boundary hash agreement check as an accidental test oracle.
		offset := stateDomainChangeBinaryTxRangeTableStart(source.segmentHeader.version) + 8 + (source.txRangeCount-1)*stateDomainChangeBinaryTxRangeSize + 8
		if offset >= uint64(len(original)) {
			mutationErr = errors.New("fixture offset out of bounds")
			return
		}
		f, e := os.OpenFile(path, os.O_WRONLY, 0)
		if e != nil {
			mutationErr = e
			return
		}
		_, e = f.WriteAt([]byte{original[offset] ^ 0x80}, int64(offset))
		mutationErr = errors.Join(e, f.Close())
		mutated = mutationErr == nil
	}
	refs, err := compactStateDomainChangeReferenceHistoryRunContext(ctx, dir, cfg, selection, sources, progress)
	progress.finish(err)
	if mutationErr != nil || !mutated {
		t.Fatalf("phase mutation not exercised: %v", mutationErr)
	}
	if err == nil || !strings.Contains(err.Error(), "checksum") || len(refs) != 0 {
		t.Fatalf("changed final table accepted: refs%d err%v", len(refs), err)
	}
	after, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if len(after) != len(original) || bytes.Equal(after, original) {
		t.Fatal("fixture did not preserve size/change bytes")
	}
	if _, e = os.Stat(filepath.Join(dir, cfg.HistoryPath(selection.fromTxNum, selection.toTxNum))); !os.IsNotExist(e) {
		t.Fatal("final artifact installed before source revalidation", e)
	}
}
