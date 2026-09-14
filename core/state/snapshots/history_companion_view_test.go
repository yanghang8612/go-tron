package snapshots

import (
	"reflect"
	"testing"
)

func TestHistoryCompanionViewDetachedOwnership(t *testing.T) {
	m := cachedManifestFixture(4)
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	history := SegmentRef{}
	for _, ref := range m.Segments {
		if ref.Kind == SegmentHistory {
			history = ref
			break
		}
	}
	wantIndex, _ := cfg.HistoryIndexRef(m, history)
	wantAccessor, _ := cfg.HistoryAccessorRef(m, history)
	view := NewHistoryCompanionView(m)
	if m.lookup != nil || view == nil || view.manifest.lookup == nil {
		t.Fatal("view must own its index and leave the input unindexed")
	}
	if view.manifest.Chain != nil || view.manifest.Progress != nil || view.manifest.Retired != nil || cap(view.manifest.Segments) != len(m.Segments) {
		t.Fatal("view retained unrelated manifest fields or active slice slack")
	}
	originalSegments := m.Segments
	for i := range originalSegments {
		originalSegments[i].Path = "changed"
		originalSegments[i].Size++
	}
	*m = Manifest{Segments: []SegmentRef{}}
	index, foundIndex := view.HistoryIndexRef(cfg, history)
	accessor, foundAccessor := view.HistoryAccessorRef(cfg, history)
	if !foundIndex || !foundAccessor || index != wantIndex || accessor != wantAccessor {
		t.Fatal("external Manifest or SegmentRef mutation changed the detached view")
	}
	index.Path = "returned value mutation"
	if again, _ := view.HistoryIndexRef(cfg, history); again != wantIndex {
		t.Fatal("returned ref aliases the view")
	}
}

func TestHistoryCompanionViewMatchesScanIdentity(t *testing.T) {
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for _, kind := range []SegmentKind{SegmentInverted, SegmentAccessor} {
		for _, mutation := range []string{"none", "from", "to", "steps", "dataset", "kind", "path", "duplicate"} {
			t.Run(string(kind)+"/"+mutation, func(t *testing.T) {
				m := manifestValidationFixture(2)
				for i := range m.Segments {
					ref := &m.Segments[i]
					if ref.Kind != kind || ref.FromTxNum != 0 {
						continue
					}
					switch mutation {
					case "from":
						ref.FromTxNum++
					case "to":
						ref.ToTxNum++
					case "steps":
						ref.AggregationSteps = 2
					case "dataset":
						ref.Dataset = SegmentDatasetCode
					case "kind":
						ref.Kind = SegmentLatest
					case "path":
						ref.Path = "missing"
					case "duplicate":
						correct := *ref
						ref.ToTxNum++
						// Two valid duplicates must keep the first matching row,
						// skipping the earlier row with an invalid identity.
						m.Segments = append(m.Segments, correct)
						correct.Size++
						m.Segments = append(m.Segments, correct)
					}
					break
				}
				view := NewHistoryCompanionView(m)
				for _, history := range m.Segments {
					if history.Kind != SegmentHistory {
						continue
					}
					want, wantOK := cfg.HistoryIndexRef(m, history)
					got, gotOK := view.HistoryIndexRef(cfg, history)
					if got != want || gotOK != wantOK {
						t.Fatal("index lookup differs from scan")
					}
					want, wantOK = cfg.HistoryAccessorRef(m, history)
					got, gotOK = view.HistoryAccessorRef(cfg, history)
					if got != want || gotOK != wantOK {
						t.Fatal("accessor lookup differs from scan")
					}
				}
			})
		}
	}
}

func TestHistoryCompanionViewLimitFallsBackWithoutCopy(t *testing.T) {
	m := manifestValidationFixture(2)
	before := cloneManifest(m)
	if NewHistoryCompanionView(nil) != nil {
		t.Fatal("nil input must decline view")
	}
	if got := testing.AllocsPerRun(10, func() {
		if newHistoryCompanionView(m, len(m.Segments)-1) != nil {
			t.Fatal("oversized input indexed")
		}
	}); got != 0 {
		t.Fatalf("oversized fallback allocated %g times", got)
	}
	if !reflect.DeepEqual(before, m) {
		t.Fatal("declined view modified input")
	}
	if newHistoryCompanionView(m, len(m.Segments)) == nil {
		t.Fatal("exact limit should admit")
	}
	// A declined view leaves callers with the unchanged, mutable scan path.
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for _, history := range m.Segments {
		if history.Kind != SegmentHistory {
			continue
		}
		if _, ok := cfg.HistoryIndexRef(m, history); !ok {
			t.Fatal("fallback lost companion")
		}
	}
}
