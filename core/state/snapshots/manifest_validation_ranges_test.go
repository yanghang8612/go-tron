package snapshots

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"testing"
)

func assertManifestValidationOracle(t *testing.T, m *Manifest) {
	t.Helper()
	before, _ := json.Marshal(m)
	want, got := frozenManifestValidate(m), m.Validate()
	if (want == nil) != (got == nil) {
		t.Fatalf("base validation differs: old=%v new=%v", want, got)
	}
	// Multiple invalid families may be visited in a different map order; exact
	// message equivalence is checked separately for one-fault inputs below.
	if m != nil {
		want, got = frozenValidateProductionHistorySegments(m), m.ValidateProduction()
		if (want == nil) != (got == nil) || want != nil && want.Error() != got.Error() {
			t.Fatalf("production validation differs: old=%v new=%v", want, got)
		}
	}
	after, _ := json.Marshal(m)
	if !bytes.Equal(before, after) {
		t.Fatal("validation mutated caller-owned input")
	}
}

func TestManifestValidationRangesIndependentOverlapOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(9142026))
	for trial := 0; trial < 500; trial++ {
		var refs []SegmentRef
		valid := true
		for i := 0; i < 24; i++ {
			from := uint64(rng.Intn(500))
			to := from + uint64(rng.Intn(6))
			if trial%3 == 0 {
				from, to = uint64(i*10), uint64(i*10+9)
			}
			if trial%5 == 0 {
				from, to = math.MaxUint64-to, math.MaxUint64-from
			}
			for _, previous := range refs {
				// Independent O(N²) closed-interval oracle, with no end+1
				// arithmetic that could overflow at MaxUint64.
				if from <= previous.ToTxNum && previous.FromTxNum <= to {
					valid = false
				}
			}
			refs = append(refs, SegmentRef{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog,
				FromTxNum: from, ToTxNum: to, Path: fmt.Sprintf("event-%d.seg", i)})
		}
		rng.Shuffle(len(refs), func(i, j int) { refs[i], refs[j] = refs[j], refs[i] })
		m := &Manifest{Version: CurrentManifestVersion, Segments: refs}
		assertManifestValidationOracle(t, m)
		if got := m.Validate() == nil; got != valid {
			t.Fatalf("trial %d: accepted=%v want=%v", trial, got, valid)
		}
	}
	for _, bounds := range [][4]uint64{{0, 0, 1, 1}, {0, 1, 1, 2}, {0, 10, 0, 5}, {0, 0, 0, 0}, {math.MaxUint64 - 1, math.MaxUint64 - 1, math.MaxUint64, math.MaxUint64}} {
		m := NewManifest(0, 0, []SegmentRef{
			{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, Path: "a.seg", FromTxNum: bounds[0], ToTxNum: bounds[1]},
			{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, Path: "b.seg", FromTxNum: bounds[2], ToTxNum: bounds[3]},
		})
		want, got := frozenManifestValidate(m), m.Validate()
		if (want == nil) != (got == nil) || want != nil && want.Error() != got.Error() {
			t.Fatalf("boundary %v: old=%v new=%v", bounds, want, got)
		}
	}
}

func TestManifestValidationRangesPreserveAllChecks(t *testing.T) {
	assertManifestValidationOracle(t, nil)
	for _, mutation := range []string{"none", "version", "visible", "chain", "kind", "dataset", "domain", "inverted", "outside", "absolute", "parent", "unclean", "duplicate", "steps", "missing-index", "orphan", "retired-kind", "retired-path", "legacy-json"} {
		t.Run(mutation, func(t *testing.T) {
			m := manifestValidationFixture(3)
			history, index := -1, -1
			for i := range m.Segments {
				if m.Segments[i].FromTxNum == 0 && m.Segments[i].Kind == SegmentHistory {
					history = i
				}
				if m.Segments[i].FromTxNum == 0 && m.Segments[i].Kind == SegmentInverted {
					index = i
				}
			}
			switch mutation {
			case "version":
				m.Version++
			case "visible":
				m.VisibleTxStart, m.VisibleTxEnd = 1, 0
			case "chain":
				m.Chain = &ChainIdentity{GenesisHash: "invalid"}
			case "kind":
				m.Segments[history].Kind = "unknown"
			case "dataset":
				m.Segments[history].Dataset = "unknown"
			case "domain":
				m.Segments[history].Domain = 1
			case "inverted":
				m.Segments[history].FromTxNum = 10
			case "outside":
				m.VisibleTxStart = 1
			case "absolute":
				m.Segments[history].Path = "/escape.seg"
			case "parent":
				m.Segments[history].Path = "../escape.seg"
			case "unclean":
				m.Segments[history].Path = "a/../escape.seg"
			case "duplicate":
				m.Segments = append(m.Segments, m.Segments[history])
			case "steps":
				m.Segments[index].AggregationSteps = 2
			case "missing-index":
				m.Segments = append(m.Segments[:index], m.Segments[index+1:]...)
			case "orphan":
				m.Segments = append(m.Segments[:history], m.Segments[history+1:]...)
			case "retired-kind":
				m.Retired = []SegmentRef{{Kind: "unknown", Path: "retired"}}
			case "retired-path":
				m.Retired = []SegmentRef{m.Segments[history]}
				m.Retired[0].Path = "../retired"
			case "legacy-json":
				m = NewManifest(0, 9, []SegmentRef{{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, Path: "history/legacy.json", ToTxNum: 9}})
			}
			assertManifestValidationOracle(t, m)
			want, got := frozenManifestValidate(m), m.Validate()
			if want != nil && want.Error() != got.Error() {
				t.Fatalf("error changed: old=%v new=%v", want, got)
			}
		})
	}
}

func TestManifestValidationKindFiltersMatchUnvalidatedOracle(t *testing.T) {
	// Exercise the helpers directly on arbitrary inputs too: filtering unrelated
	// kinds must not rely on public Validate having already rejected the input.
	kinds := []SegmentKind{SegmentHistory, SegmentLatest, SegmentAccessor, SegmentInverted, SegmentBTree, SegmentEventLog, SegmentEventLogIndex, SegmentChainFreezer, "unknown"}
	datasets := []SegmentDataset{"", SegmentDatasetStateDomainChange, SegmentDatasetAccountLatest, SegmentDatasetEventLog, "unknown"}
	for _, kind := range kinds {
		for _, dataset := range datasets {
			for _, ext := range []string{"seg", "json", "kv", "idx", "lidx", "bt", "LIDX", "BT"} {
				m := &Manifest{Segments: []SegmentRef{{Kind: kind, Dataset: dataset, Path: "sample." + ext, FromTxNum: 1, ToTxNum: 2}}}
				byPath := map[string]*SegmentRef{m.Segments[0].Path: &m.Segments[0]}
				for _, pair := range [][2]error{
					{frozenValidateHistoryBinaryCompanionTriples(m, byPath), validateHistoryBinaryCompanionTriples(m, byPath)},
					{frozenValidateLatestBinaryCompanionTriples(m), validateLatestBinaryCompanionTriples(m)},
					{frozenValidateProductionHistorySegments(m), validateProductionHistorySegments(m)},
				} {
					if (pair[0] == nil) != (pair[1] == nil) || pair[0] != nil && pair[0].Error() != pair[1].Error() {
						t.Fatalf("kind=%s dataset=%s ext=%s: old=%v new=%v", kind, dataset, ext, pair[0], pair[1])
					}
				}
			}
		}
	}
}
