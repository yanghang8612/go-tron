package snapshots

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"

	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

// Preserve the former owned-ref implementation as an independent oracle for
// exact coverage, family sorting, first-error ordering and input ownership.
func legacyEventLogIndexCompanionValidation(manifest *Manifest) error {
	if manifest == nil {
		return nil
	}
	eventRefs := eventLogRefs(manifest)
	indexRefs := eventLogIndexRefs(manifest)
	start := 0
	for _, indexRef := range indexRefs {
		for start < len(eventRefs) && eventRefs[start].ToTxNum < indexRef.FromTxNum {
			start++
		}
		if !eventLogRangeCoveredByRefs(eventRefs[start:], indexRef.FromTxNum, indexRef.ToTxNum) {
			return fmt.Errorf("snapshots: event-log-index segment %q has no continuous event-log coverage for block range [%d,%d]", indexRef.Path, indexRef.FromTxNum, indexRef.ToTxNum)
		}
	}
	return nil
}

func TestValidateEventLogIndexCompanionsMatchesLegacy(t *testing.T) {
	ref := func(kind SegmentKind, from, to uint64, path string) SegmentRef {
		return SegmentRef{Dataset: SegmentDatasetEventLog, Kind: kind, FromTxNum: from, ToTxNum: to, Path: path}
	}
	event := func(a, b uint64) SegmentRef { return ref(SegmentEventLog, a, b, fmt.Sprintf("e-%d-%d", a, b)) }
	index := func(a, b uint64, path string) SegmentRef { return ref(SegmentEventLogIndex, a, b, path) }
	max := ^uint64(0)
	cases := []struct {
		name         string
		manifest     *Manifest
		literal      string
		checkLiteral bool
	}{
		{"nil", nil, "", true}, {"empty", &Manifest{}, "", true},
		{"noIndex", &Manifest{Segments: []SegmentRef{event(1, 4)}}, "", true},
		{"noEvent", &Manifest{Segments: []SegmentRef{index(1, 2, "missing")}}, `snapshots: event-log-index segment "missing" has no continuous event-log coverage for block range [1,2]`, true},
		{"adjacentUnsorted", &Manifest{Segments: []SegmentRef{index(1, 4, "i"), event(3, 4), event(1, 2)}}, "", true},
		{"gap", &Manifest{Segments: []SegmentRef{event(1, 2), event(4, 5), index(1, 5, "gap")}}, `snapshots: event-log-index segment "gap" has no continuous event-log coverage for block range [1,5]`, true},
		{"oneEventSeveralIndexes", &Manifest{Segments: []SegmentRef{event(1, 9), index(2, 3, "a"), index(4, 5, "b"), index(6, 9, "c")}}, "", true},
		{"overlap", &Manifest{Segments: []SegmentRef{event(1, 5), event(3, 8), index(1, 8, "o")}}, "", true},
		{"duplicate", &Manifest{Segments: []SegmentRef{event(1, 5), event(1, 5), index(1, 5, "d"), index(1, 5, "d")}}, "", true},
		{"firstErrorSorted", &Manifest{Segments: []SegmentRef{index(9, 10, "late"), index(1, 2, "early")}}, `snapshots: event-log-index segment "early" has no continuous event-log coverage for block range [1,2]`, true},
		{"maxBoundary", &Manifest{Segments: []SegmentRef{event(max-2, max-1), event(max, max), index(max-2, max, "max")}}, "", true},
		{"inverted", &Manifest{Segments: []SegmentRef{event(1, 8), index(8, 7, "inv")}}, `snapshots: event-log-index segment "inv" has no continuous event-log coverage for block range [8,7]`, true},
	}
	wrongDataset := event(1, 9)
	wrongDataset.Dataset = SegmentDatasetBalanceTrace
	normalized := event(1, 9)
	normalized.Dataset = ""
	wrongKind := event(1, 9)
	wrongKind.Kind = SegmentLatest
	wrongKind.Dataset = ""
	cases = append(cases, struct {
		name         string
		manifest     *Manifest
		literal      string
		checkLiteral bool
	}{"datasetNormalization", &Manifest{Segments: []SegmentRef{wrongDataset, normalized, wrongKind, index(1, 2, "normal")}}, `snapshots: event-log-index segment "normal" has no continuous event-log coverage for block range [1,2]`, true})
	domain0, domain1 := event(100, 200), event(1, 2)
	domain1.Domain = kvdomains.KVDomain(1)
	// Preserve the existing family-order behavior even for a malformed catalog;
	// do not silently replace its ordering with a range-only comparator.
	cases = append(cases, struct {
		name         string
		manifest     *Manifest
		literal      string
		checkLiteral bool
	}{"domainBeforeRange", &Manifest{Segments: []SegmentRef{domain1, domain0, index(1, 2, "domain")}}, `snapshots: event-log-index segment "domain" has no continuous event-log coverage for block range [1,2]`, true})
	random := rand.New(rand.NewSource(20261031))
	for n := 0; n < 300; n++ {
		m := &Manifest{}
		for j := 0; j < random.Intn(40); j++ {
			kind := []SegmentKind{SegmentEventLog, SegmentEventLogIndex, SegmentLatest}[random.Intn(3)]
			from, to := uint64(random.Intn(20)), uint64(random.Intn(20))
			if random.Intn(15) == 0 {
				to = max
			}
			r := ref(kind, from, to, fmt.Sprintf("path-%d", random.Intn(7)))
			r.Domain = kvdomains.KVDomain(random.Intn(3))
			r.Dataset = []SegmentDataset{SegmentDatasetEventLog, "", SegmentDatasetBalanceTrace}[random.Intn(3)]
			m.Segments = append(m.Segments, r)
		}
		cases = append(cases, struct {
			name         string
			manifest     *Manifest
			literal      string
			checkLiteral bool
		}{fmt.Sprintf("random-%d", n), m, "", false})
	}
	errText := func(err error) string {
		if err == nil {
			return ""
		}
		return err.Error()
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before, _ := json.Marshal(tc.manifest)
			want := errText(legacyEventLogIndexCompanionValidation(tc.manifest))
			got := errText(validateEventLogIndexCompanions(tc.manifest))
			if tc.checkLiteral && want != tc.literal {
				t.Fatalf("baseline literal: %q != %q", want, tc.literal)
			}
			if got != want {
				t.Fatalf("candidate error %q, baseline %q", got, want)
			}
			after, _ := json.Marshal(tc.manifest)
			if !bytes.Equal(before, after) {
				t.Fatal("mutated input manifest")
			}
		})
	}
}
