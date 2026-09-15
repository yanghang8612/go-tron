package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

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
		{"skip oversized", []historyReferenceCompactionInputBudget{large, small, small}, 2, 2, 3},
		{"return preceding ready group", []historyReferenceCompactionInputBudget{small, small, large}, 4, 1, 2},
		{"combined boundary", []historyReferenceCompactionInputBudget{small, combined, combined}, 4, 1, 2},
		{"old only unchanged", []historyReferenceCompactionInputBudget{old, old}, 2, 1, 2},
		{"all deferred", []historyReferenceCompactionInputBudget{large, large}, 2, 0, 0},
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
