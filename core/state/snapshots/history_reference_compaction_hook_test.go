package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

type boundedPrevReader struct {
	data    []byte
	maxRead int
	reads   int
	cancel  context.CancelFunc
}

func (r *boundedPrevReader) ReadAt(p []byte, off int64) (int, error) {
	r.reads++
	if len(p) > r.maxRead {
		r.maxRead = len(p)
	}
	n := copy(p, r.data[off:])
	if r.cancel != nil && r.reads == 1 {
		r.cancel()
	}
	if n != len(p) {
		return n, io.EOF
	}
	return n, nil
}

func streamedV6WriterFixture(t *testing.T) (*stateDomainChangeHistoryRecordWriter, *bytes.Buffer, *rawdb.StateDomainChange, uint32) {
	t.Helper()
	dir := t.TempDir()
	change := v6StreamChanges(1, 1, 1, 1)[0]
	change.Prev, change.PrevExists, change.Next, change.NextExists = nil, true, nil, false
	build, err := newStateDomainChangeV6Build(etl.Options{TempDir: filepath.Join(dir, "etl")}, dir, "out.seg")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(build.Close)
	key := appendStateDomainChangeBinaryAccessorLookupKey(nil, change.FlatDomain, change.Owner, change.Generation, change.Domain, change.Key)
	if err = build.CollectLogicalKey(key); err != nil {
		t.Fatal(err)
	}
	if err = build.FinishDictionaryContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	keyID, err := build.KeyID(key)
	if err != nil {
		t.Fatal(err)
	}
	index, err := os.Create(filepath.Join(dir, "out.idx"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = index.Close() })
	sink := new(bytes.Buffer)
	w := newStateDomainChangeHistoryRecordWriterV6(sink, index, build, SegmentRef{FromTxNum: 1, ToTxNum: 1}, 1, 0)
	t.Cleanup(w.Release)
	return w, sink, change, keyID
}

func TestWriteStreamedV6ChangeBoundsPrevReads(t *testing.T) {
	w, sink, change, keyID := streamedV6WriterFixture(t)
	prev := bytes.Repeat([]byte("bounded-prev"), (3*stateDomainChangeHistoryWriteBufferSize)/12+97)
	r := &boundedPrevReader{data: prev}
	if err := w.WriteStreamedV6Change(context.Background(), change, keyID, r, 0, uint64(len(prev)), make([]byte, stateDomainChangeHistoryWriteBufferSize)); err != nil {
		t.Fatal(err)
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if r.reads < 4 || r.maxRead > stateDomainChangeHistoryWriteBufferSize {
		t.Fatalf("reads=%d max=%d", r.reads, r.maxRead)
	}
	got := sink.Bytes()
	if len(got) != 21+len(prev) || !bytes.Equal(got[21:], prev) {
		t.Fatal("streamed Prev differs")
	}
}

func TestWriteStreamedV6ChangeCancelsBetweenChunks(t *testing.T) {
	w, _, change, keyID := streamedV6WriterFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	r := &boundedPrevReader{data: make([]byte, 3*stateDomainChangeHistoryWriteBufferSize), cancel: cancel}
	err := w.WriteStreamedV6Change(ctx, change, keyID, r, 0, uint64(len(r.data)), make([]byte, stateDomainChangeHistoryWriteBufferSize))
	if !errors.Is(err, context.Canceled) || r.reads != 1 {
		t.Fatalf("reads=%d err=%v", r.reads, err)
	}
	if w.count != 0 {
		t.Fatal("canceled record became publishable")
	}
}

func TestHistoryReferenceCompactionHookBusyBoundaries(t *testing.T) {
	small := historyReferenceCompactionInputBudget{HasReference: true, Records: 1, LogicalBytes: 100, ChunksUpper: 1, SpansUpper: 1, RawUpper: 100}
	large := small
	large.ChunksUpper = historyReferenceMaxChunks
	combined := small
	combined.ChunksUpper = historyReferenceMaxChunks / 2
	old := large
	old.HasReference = false
	for _, tc := range []struct {
		name       string
		inputs     []historyReferenceCompactionInputBudget
		maxSources uint64
		from, to   uint64
	}{
		{"oversized estimate streams", []historyReferenceCompactionInputBudget{large, small, small}, 2, 1, 2},
		{"late oversized estimate streams", []historyReferenceCompactionInputBudget{small, small, large}, 4, 1, 3},
		{"combined boundary streams", []historyReferenceCompactionInputBudget{small, combined, combined}, 4, 1, 3},
		{"old only unchanged", []historyReferenceCompactionInputBudget{old, old}, 2, 1, 2},
		{"all estimates stream", []historyReferenceCompactionInputBudget{large, large}, 2, 1, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			costs := make([]historyCompactionInputCost, len(tc.inputs))
			for i := range costs {
				costs[i] = historyCompactionInputCost{bytes: 1, records: 1, logicalBytes: 1}
			}
			candidates, read := budgetTestLeaves(costs...)
			calls := make(map[uint64]int)
			selection, ok, err := selectBudgetedHistoryCompactionLeaves(context.Background(), candidates, CompactionConfig{MaxSources: tc.maxSources}, read,
				func(c historyCompactionCandidate) (historyReferenceCompactionInputBudget, error) {
					calls[c.history.FromTxNum]++
					return tc.inputs[c.history.FromTxNum-1], nil
				})
			if err != nil || ok != (tc.from != 0) || selection.fromTxNum != tc.from || selection.toTxNum != tc.to {
				t.Fatalf("selection %+v ok%v err%v", selection, ok, err)
			}
			if selection.outputMode == historyCompactionOutputStreaming && tc.name == "old only unchanged" {
				t.Fatal("old-only inputs unexpectedly selected streaming output")
			}
			if strings.Contains(tc.name, "stream") && selection.outputMode != historyCompactionOutputStreaming {
				t.Fatal("combined R1 overflow did not select streaming output")
			}
			for _, n := range calls {
				if n != 1 {
					t.Fatal("source budget read repeatedly", calls)
				}
			}
			if tc.from == 0 && !strings.HasPrefix(selection.referenceDeferReason, "reference-") {
				t.Fatal("missing defer reason")
			}
		})
	}
}

func TestHistoryReferenceStreamingFallbackRetriesBoundaryCandidate(t *testing.T) {
	giB := uint64(1 << 30)
	costs := []historyCompactionInputCost{
		{bytes: 1, logicalBytes: 1700 << 20, records: 1},
		{bytes: 1, logicalBytes: giB, records: 1},
		{bytes: 1, logicalBytes: giB, records: 1},
	}
	candidates, read := budgetTestLeaves(costs...)
	large := historyReferenceCompactionInputBudget{HasReference: true, Records: 1, LogicalBytes: 1, ChunksUpper: historyReferenceMaxChunks}
	selection, ok, err := selectBudgetedHistoryCompactionLeaves(context.Background(), candidates, CompactionConfig{}, read, func(historyCompactionCandidate) (historyReferenceCompactionInputBudget, error) { return large, nil })
	if err != nil || !ok || selection.fromTxNum != 2 || selection.toTxNum != 3 || selection.outputMode != historyCompactionOutputStreaming {
		t.Fatal("boundary candidate was not retried under fixed zero-config limits", selection, ok, err)
	}
}

func TestHistoryReferenceStreamingFallbackStopsBeforeFixedLimit(t *testing.T) {
	giB := uint64(1 << 30)
	costs := []historyCompactionInputCost{{bytes: 1, logicalBytes: giB, records: 1}, {bytes: 1, logicalBytes: giB, records: 1}, {bytes: 1, logicalBytes: giB, records: 1}}
	candidates, read := budgetTestLeaves(costs...)
	large := historyReferenceCompactionInputBudget{HasReference: true, Records: 1, LogicalBytes: 1, ChunksUpper: historyReferenceMaxChunks}
	selection, ok, err := selectBudgetedHistoryCompactionLeaves(context.Background(), candidates, CompactionConfig{}, read, func(historyCompactionCandidate) (historyReferenceCompactionInputBudget, error) { return large, nil })
	if err != nil || !ok || selection.fromTxNum != 1 || selection.toTxNum != 2 || selection.inputLogicalBytes != 2*giB {
		t.Fatal("streaming group crossed fixed logical limit", selection, ok, err)
	}
}

func TestHistoryReferenceCompactionHookAlignedContinues(t *testing.T) {
	costs := make([]historyCompactionInputCost, 4)
	candidates, _ := budgetTestLeaves(costs...)
	small := historyReferenceCompactionInputBudget{HasReference: true, Records: 1, LogicalBytes: 100, ChunksUpper: 1, SpansUpper: 1}
	for _, oldOnly := range []bool{false, true} {
		calls := make(map[uint64]int)
		cache := make(map[uint64]historyReferenceCompactionInputBudget)
		read := func(c historyCompactionCandidate) (historyReferenceCompactionInputBudget, error) {
			if in, ok := cache[c.history.FromTxNum]; ok {
				return in, nil
			}
			calls[c.history.FromTxNum]++
			in := small
			in.HasReference = !oldOnly
			if c.history.FromTxNum == 1 {
				in.ChunksUpper = historyReferenceMaxChunks
			}
			cache[c.history.FromTxNum] = in
			return in, nil
		}
		selection, ok, err := selectReferenceBudgetedHistoryCompactionRun(context.Background(), candidates, 2, 4, read)
		if err != nil || !ok {
			t.Fatal(selection, ok, err)
		}
		if oldOnly {
			want, found := selectHistoryCompactionCandidatesRunAtLeast(candidates, 2, 4)
			if !found || !reflect.DeepEqual(selection, want) {
				t.Fatal("old-only aligned policy changed", selection, want)
			}
		} else if selection.fromTxNum != 2 || selection.toTxNum != 3 {
			t.Fatal("later eligible group stranded", selection)
		}
		for _, n := range calls {
			if n != 1 {
				t.Fatal("uncached budget read", calls)
			}
		}
	}
	sentinel := errors.New("source header unavailable")
	if _, _, err := selectReferenceBudgetedHistoryCompactionRun(context.Background(), candidates, 2, 4, func(historyCompactionCandidate) (historyReferenceCompactionInputBudget, error) {
		return small, sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatal("probe error lost", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := selectReferenceBudgetedHistoryCompactionRun(ctx, candidates, 2, 4, func(historyCompactionCandidate) (historyReferenceCompactionInputBudget, error) {
		t.Fatal("canceled selector read source")
		return small, nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// A split at the center would permanently strand the two small middle leaves.
// Exercise both individually oversized boundaries and individually legal
// neighbors whose combined bound alone exceeds the limit.
func TestHistoryReferenceCompactionHookAlignedCrossingBoundaries(t *testing.T) {
	for _, individualOversized := range []bool{false, true} {
		candidates, _ := budgetTestLeaves(make([]historyCompactionInputCost, 4)...)
		small := historyReferenceCompactionInputBudget{HasReference: true, Records: 1, LogicalBytes: 100, ChunksUpper: 10, SpansUpper: 1}
		large := small
		large.ChunksUpper = historyReferenceMaxChunks - 10
		if individualOversized {
			large.ChunksUpper = historyReferenceMaxChunks
		} else if budget := estimateHistoryReferenceCompactionBudget([]historyReferenceCompactionInputBudget{large}); budget.Deferred {
			t.Fatal("fixture must fit by itself", budget)
		}
		inputs := []historyReferenceCompactionInputBudget{large, small, small, large}
		cache := make(map[uint64]historyReferenceCompactionInputBudget)
		calls := make(map[uint64]int)
		selection, ok, err := selectReferenceBudgetedHistoryCompactionRun(context.Background(), candidates, 2, 4,
			func(c historyCompactionCandidate) (historyReferenceCompactionInputBudget, error) {
				if input, found := cache[c.history.FromTxNum]; found {
					return input, nil
				}
				calls[c.history.FromTxNum]++
				input := inputs[c.history.FromTxNum-1]
				cache[c.history.FromTxNum] = input
				return input, nil
			})
		if err != nil || !ok || selection.fromTxNum != 2 || selection.toTxNum != 3 {
			t.Fatal("middle legal pair stranded", individualOversized, selection, ok, err)
		}
		for _, n := range calls {
			if n != 1 {
				t.Fatal("source budget read more than once", calls)
			}
		}
	}
}

func referenceHookTrioFixture(t *testing.T, count int) (string, [][]SegmentRef) {
	t.Helper()
	dir, old, manifest := referenceMigrationFixture(t, count)
	var trios [][]SegmentRef
	manifest.Segments = nil
	for _, source := range old {
		refs, _, err := ReencodeHistoryReferenceTrioContext(context.Background(), dir, source)
		if err != nil {
			t.Fatal(err)
		}
		trios = append(trios, refs)
		manifest.Segments = append(manifest.Segments, refs...)
	}
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}
	return dir, trios
}

func TestHistoryReferenceCompactionHookRealMergeOracle(t *testing.T) {
	for _, busy := range []bool{false, true} {
		t.Run(map[bool]string{false: "aligned", true: "busy"}[busy], func(t *testing.T) {
			dir, trios := referenceHookTrioFixture(t, 2)
			cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
			selection := historyCompactionSelection{fromTxNum: 1, toTxNum: 4, aggregationSteps: 2}
			for _, refs := range trios {
				selection.candidates = append(selection.candidates, historyCompactionCandidate{history: refs[0], companions: refs[1:]})
			}
			sources, err := collectStateDomainChangeBinaryCompactionSources(context.Background(), dir, selection, nil)
			if err != nil {
				t.Fatal(err)
			}
			old, idx, acc, err := writeCompactedStateDomainChangeBinaryFiles(context.Background(), dir, cfg, selection, sources, nil)
			if err != nil {
				t.Fatal(err)
			}
			want := referenceMergeKindBytes(t, dir, []SegmentRef{old, idx, acc})
			result, err := CompactHistoryDomainContext(context.Background(), dir, SegmentDatasetStateDomainChange, CompactionConfig{BusyLeafOnly: busy, MaxSteps: 2, MaxSources: 2, DeleteObsolete: true})
			if err != nil || !result.Merged || result.FromTxNum != 1 || result.ToTxNum != 4 {
				t.Fatal(result, err)
			}
			ref := referenceBuildHistoryRef(t, result.Segments)
			data, err := os.ReadFile(filepath.Join(dir, ref.Path))
			if err != nil || string(data[:8]) != historyReferenceMagic {
				t.Fatal("R1 routing lost", err)
			}
			if !reflect.DeepEqual(referenceMergeKindBytes(t, dir, result.Segments), want) {
				t.Fatal("complete virtual/companion output differs from original merger")
			}
			for _, refs := range trios {
				for _, ref := range refs {
					if _, err := os.Stat(filepath.Join(dir, ref.Path)); !os.IsNotExist(err) {
						t.Fatal("old source not retired", ref.Path, err)
					}
				}
			}
		})
	}
}

func TestHistoryReferenceCompactionStreamingFallbackOracle(t *testing.T) {
	for _, mode := range []string{"r1", "codec3"} {
		t.Run(mode, func(t *testing.T) {
			dir, cfg, selection, sources := referenceMergeFixture(t, mode)
			wantRefs, err := compactStateDomainChangeReferenceHistoryRunContext(context.Background(), dir, cfg, selection, sources, nil)
			if err != nil {
				t.Fatal(err)
			}
			want := referenceMergeKindBytes(t, dir, wantRefs)
			selection.outputMode = historyCompactionOutputStreaming
			got, err := compactStateDomainChangeBinaryHistoryRunContext(context.Background(), dir, cfg, selection)
			if err != nil {
				t.Fatal(err)
			}
			history := referenceBuildHistoryRef(t, got)
			data, err := os.ReadFile(filepath.Join(dir, history.Path))
			if err != nil || string(data[:8]) == historyReferenceMagic {
				t.Fatal("fallback did not produce compressed V6", err)
			}
			if !reflect.DeepEqual(referenceMergeKindBytes(t, dir, got), want) {
				t.Fatal("streaming fallback changed logical/index output")
			}
		})
	}
}

// Capacity admission intentionally trusts only declarations, not complete
// payloads. Give an actual R1 file an oversized logical declaration: selecting
// it for a writer would fail full validation. Later valid trios must still be
// selected, while the deferred file remains byte-for-byte untouched.
func TestHistoryReferenceCompactionHookOversizedHeaderBoundary(t *testing.T) {
	for _, busy := range []bool{false, true} {
		for _, all := range []bool{false, true} {
			name := map[bool]string{false: "aligned", true: "busy"}[busy] + map[bool]string{false: "/later-valid", true: "/all-deferred"}[all]
			t.Run(name, func(t *testing.T) {
				dir, trios := referenceHookTrioFixture(t, 4)
				m, err := LoadProductionManifest(dir)
				if err != nil {
					t.Fatal(err)
				}
				for i := range m.Segments {
					ref := &m.Segments[i]
					if ref.Kind != SegmentHistory || !all && ref.FromTxNum != 1 && ref.FromTxNum != 7 {
						continue
					}
					data, err := os.ReadFile(filepath.Join(dir, ref.Path))
					if err != nil {
						t.Fatal(err)
					}
					binary.BigEndian.PutUint64(data[16:24], historyReferenceMaxLogical)
					if err = os.WriteFile(filepath.Join(dir, ref.Path), data, 0600); err != nil {
						t.Fatal(err)
					}
					ref.Checksum = checksumBytes(data)
				}
				if err = PublishManifest(dir, m); err != nil {
					t.Fatal(err)
				}
				before := referenceMigrationFiles(t, dir)
				result, err := CompactHistoryDomainContext(context.Background(), dir, SegmentDatasetStateDomainChange, CompactionConfig{BusyLeafOnly: busy, MaxSteps: 2, MaxSources: 2, DeleteObsolete: true})
				if err != nil {
					t.Fatal(result, err)
				}
				if all {
					if result.Merged || !result.Deferred || !strings.HasPrefix(result.DeferReason, "reference-") || !reflect.DeepEqual(before, referenceMigrationFiles(t, dir)) {
						t.Fatal("defer invoked a writer or lost reason", result)
					}
				} else {
					if !result.Merged || result.FromTxNum != 3 || result.ToTxNum != 6 {
						t.Fatal("later leaves stranded", result)
					}
					path := trios[0][0].Path
					data, err := os.ReadFile(filepath.Join(dir, path))
					if err != nil || checksumBytes(data) != before[path] {
						t.Fatal("deferred source changed", err)
					}
					if bytes.Equal(data[:8], []byte(historyReferenceMagic)) == false {
						t.Fatal("fixture not R1")
					}
				}
			})
		}
	}
}
