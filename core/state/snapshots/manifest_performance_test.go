package snapshots

import (
	"fmt"
	"math/rand"
	"testing"
)

func manifestValidationFixture(n int) *Manifest {
	refs := make([]SegmentRef, 0, n*5)
	for i := 0; i < n; i++ {
		from, to := uint64(i*10), uint64(i*10+9)
		for j, kind := range []SegmentKind{SegmentHistory, SegmentInverted, SegmentAccessor} {
			refs = append(refs, SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: kind,
				FromTxNum: from, ToTxNum: to, Path: fmt.Sprintf("history/state-domain-change-%d-%d.%s", from, to, []string{"seg", "idx", "kv"}[j])})
		}
		refs = append(refs,
			SegmentRef{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, FromTxNum: from, ToTxNum: to, Path: fmt.Sprintf("log/event-%d.seg", i)},
			SegmentRef{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLogIndex, FromTxNum: from, ToTxNum: to, Path: fmt.Sprintf("log/event-%d.idx", i)})
	}
	return NewManifest(0, uint64(n*10-1), refs)
}

func TestManifestIndexedCompanionIdentity(t *testing.T) {
	base := manifestValidationFixture(128)
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []SegmentKind{SegmentAccessor, SegmentInverted} {
		for _, field := range []string{"from", "to", "steps", "path", "kind", "dataset", "duplicate"} {
			t.Run(string(kind)+"/"+field, func(t *testing.T) {
				m := cloneManifest(base)
				for i := range m.Segments {
					ref := &m.Segments[i]
					if ref.Kind != kind || ref.FromTxNum != 640 {
						continue
					}
					switch field {
					case "from":
						ref.FromTxNum++
					case "to":
						ref.ToTxNum--
					case "steps":
						ref.AggregationSteps = 2
					case "path":
						ref.Path = "history/unrelated." + map[SegmentKind]string{SegmentAccessor: "kv", SegmentInverted: "idx"}[kind]
					case "kind":
						ref.Kind = SegmentLatest
					case "dataset":
						ref.Dataset = SegmentDatasetAccountLatest
					case "duplicate":
						m.Segments = append(m.Segments, *ref)
					}
					break
				}
				if err := m.Validate(); err == nil {
					t.Fatal("mismatched companion accepted")
				}
			})
		}
	}
	// Legacy zero and explicit one denote the same aggregation generation.
	for i := range base.Segments {
		if base.Segments[i].Kind == SegmentHistory {
			base.Segments[i].AggregationSteps = 1
		}
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("legacy steps rejected: %v", err)
	}
}

func TestManifestEventCoverageCursorMatchesFullScan(t *testing.T) {
	rng := rand.New(rand.NewSource(92137))
	for trial := 0; trial < 200; trial++ {
		var refs []SegmentRef
		for i := uint64(0); i < 100; i++ {
			if rng.Intn(8) != 0 {
				refs = append(refs, SegmentRef{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog,
					FromTxNum: i * 10, ToTxNum: i*10 + 9, Path: fmt.Sprintf("event-%d.seg", i)})
			}
		}
		for i := uint64(0); i < 10; i++ {
			from := uint64(rng.Intn(995))
			refs = append(refs, SegmentRef{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLogIndex,
				FromTxNum: from, ToTxNum: from + uint64(rng.Intn(20)), Path: fmt.Sprintf("index-%d.idx", i)})
		}
		m := NewManifest(0, 0, refs)
		events := eventLogRefs(m)
		want := true
		for _, ref := range eventLogIndexRefs(m) {
			want = want && eventLogRangeCoveredByRefs(events, ref.FromTxNum, ref.ToTxNum)
		}
		if got := validateEventLogIndexCompanions(m) == nil; got != want {
			t.Fatalf("trial %d: covered=%v want=%v", trial, got, want)
		}
	}
	// One event file can cover many indexes, including the final uint64 block.
	m := NewManifest(0, 0, []SegmentRef{
		{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLog, FromTxNum: 0, ToTxNum: ^uint64(0), Path: "all.seg"},
		{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLogIndex, FromTxNum: 1, ToTxNum: 2, Path: "first.idx"},
		{Dataset: SegmentDatasetEventLog, Kind: SegmentEventLogIndex, FromTxNum: ^uint64(0), ToTxNum: ^uint64(0), Path: "last.idx"},
	})
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkManifestValidationCatalog(b *testing.B) {
	for _, n := range []int{512, 2048} {
		b.Run(fmt.Sprintf("groups=%d", n), func(b *testing.B) {
			m := manifestValidationFixture(n)
			if err := m.Validate(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := m.Validate(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
