package snapshots

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestAccountDeltaRangesMatchExhaustiveOverlappingEnvelopes(t *testing.T) {
	rng := rand.New(rand.NewSource(81))
	m := &Manifest{}
	for i := 0; i < 1000; i++ {
		from := uint64(rng.Intn(1000))
		m.Segments = append(m.Segments, SegmentRef{Kind: SegmentAccountHistoryRoutingDelta, FromTxNum: from, ToTxNum: from + uint64(rng.Intn(300)), Path: fmt.Sprintf("history/test-%d.route", i)})
	}
	m = manifestLookupView(m)
	x := manifestAccountDeltaRanges(m)
	if x == nil || x != manifestAccountDeltaRanges(m) {
		t.Fatal("immutable index not reused")
	}
	for i := 0; i < 200; i++ {
		from := uint64(rng.Intn(1300))
		h := SegmentRef{FromTxNum: from, ToTxNum: from + uint64(rng.Intn(100))}
		want, got := map[uint32]bool{}, map[uint32]bool{}
		for row, ref := range m.Segments {
			if ref.FromTxNum <= h.FromTxNum && ref.ToTxNum >= h.ToTxNum {
				want[uint32(row)] = true
			}
		}
		x.visit(m, h, func(row uint32, ref SegmentRef) bool { got[row] = true; return false })
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("range %d..%d got %v want %v", h.FromTxNum, h.ToTxNum, got, want)
		}
		calls := 0
		x.visit(m, h, func(uint32, SegmentRef) bool { calls++; return true })
		if calls > 1 || (calls == 0) != (len(want) == 0) {
			t.Fatal("visitor did not stop", calls, len(want))
		}
	}
}

func TestAccountRoutingDeltaSelectsNeededObjectAfterFirst128(t *testing.T) {
	dir, m, _ := accountRoutingFixture(t, 9)
	ref := accountRouteDeltaBuild(t, dir, m, 64)
	var err error
	m, err = publishAccountHistoryDelta(context.Background(), dir, m, ref, nil)
	if err != nil {
		t.Fatal(err)
	}
	// These earlier envelopes intersect the overall query, but not the current
	// source. None is readable; selecting the first 128 in the constructor
	// would permanently omit the valid later proof.
	var fake []SegmentRef
	for i := 0; i < accountDeltaRequestObjects; i++ {
		fake = append(fake, SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentAccountHistoryRoutingDelta, FromTxNum: 1, ToTxNum: 1, Path: fmt.Sprintf("history/missing-earlier-%d.route", i)})
	}
	m = cloneManifest(m)
	m.Segments = append(fake, m.Segments...)
	m = manifestLookupView(m)
	q := StateHistoryQuery{FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: (common.AccountID{0xee}).Address(common.AddressPrefixMainnet)}
	key := stateDomainChangeBinaryAccessorLookupKey(q.FlatDomain, q.Owner, 0, 0, nil)
	r := newAccountDeltaHistoryReader(m, 0, 3, [][]byte{key})
	for _, source := range manifestHistorySources(m).sources {
		h := m.Segments[source.row]
		if h.FromTxNum != 3 {
			continue
		}
		if skipped, err := r.maySkip(context.Background(), dir, h, source.id, key, 0); err != nil || !skipped || r.opened != 1 {
			t.Fatal("late needed proof excluded by unrelated envelopes", skipped, r.opened, err)
		}
	}
}

func TestAccountRoutingDeltaActualObjectLimitStillFallsBack(t *testing.T) {
	dir, m, _ := accountRoutingFixture(t, 9)
	var refs []SegmentRef
	for i := 0; i < 4096; i++ {
		refs = append(refs, SegmentRef{Kind: SegmentAccountHistoryRoutingDelta, FromTxNum: 1, ToTxNum: 3, Path: fmt.Sprintf("history/unreadable-overlap-%d.route", i)})
	}
	m = cloneManifest(m)
	m.Segments = append(m.Segments, refs...)
	m = manifestLookupView(m)
	key := stateDomainChangeBinaryAccessorLookupKey(rawdb.StateFlatDomainAccountLatest, (common.AccountID{0xee}).Address(common.AddressPrefixMainnet), 0, 0, nil)
	r := newAccountDeltaHistoryReader(m, 1, 3, [][]byte{key})
	for pass := 0; pass < 16; pass++ {
		for _, source := range manifestHistorySources(m).sources {
			h := m.Segments[source.row]
			if skipped, err := r.maySkip(context.Background(), dir, h, source.id, key, 1); err != nil || skipped || r.opened != accountDeltaRequestObjects || r.visited > accountDeltaRequestObjects+1 {
				t.Fatal("budget fallback reopened objects or scanned all overlaps", skipped, r.opened, r.visited, err)
			}
		}
	}
}

func TestAccountRoutingDeltaBudgetKeepsCachedExactProofs(t *testing.T) {
	dir, m, qs := accountRoutingFixture(t, 9)
	ref := accountRouteDeltaBuild(t, dir, m, 64)
	var err error
	m, err = publishAccountHistoryDelta(context.Background(), dir, m, ref, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < accountDeltaRequestObjects; i++ {
		m.Segments = append(m.Segments, SegmentRef{Kind: SegmentAccountHistoryRoutingDelta, FromTxNum: 1, ToTxNum: 3, Path: fmt.Sprintf("history/unreadable-cache-%d.route", i)})
	}
	m = manifestLookupView(m)
	key := stateDomainChangeBinaryAccessorLookupKey(qs[0].FlatDomain, qs[0].Owner, 0, 0, nil)
	r := newAccountDeltaHistoryReader(m, 1, 3, [][]byte{key})
	// No current source matches this ID, forcing admission to its cap after
	// retaining the first authenticated object's bound.
	h := SegmentRef{FromTxNum: 1, ToTxNum: 1}
	if skip, e := r.maySkip(context.Background(), dir, h, [32]byte{}, key, 1); e != nil || skip || r.opened != accountDeltaRequestObjects {
		t.Fatal(skip, r.opened, e)
	}
	visited := r.visited
	for _, source := range manifestHistorySources(m).sources {
		h = m.Segments[source.row]
		if h.FromTxNum != 3 {
			continue
		}
		if skip, e := r.maySkip(context.Background(), dir, h, source.id, key, 1); e != nil || !skip || r.visited != visited {
			t.Fatal("cached exact proof lost after cap", skip, r.visited, e)
		}
	}
}

func BenchmarkAccountDeltaRangeLookup(b *testing.B) {
	m := &Manifest{}
	for i := 0; i < 4096; i++ {
		m.Segments = append(m.Segments, SegmentRef{Kind: SegmentAccountHistoryRoutingDelta, FromTxNum: uint64(i * 16), ToTxNum: uint64(i*16 + 15), Path: fmt.Sprintf("history/range-%d.route", i)})
	}
	m = manifestLookupView(m)
	x := manifestAccountDeltaRanges(m)
	for _, indexed := range []bool{false, true} {
		b.Run(fmt.Sprintf("indexed=%v", indexed), func(b *testing.B) {
			hits := 0
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				h := SegmentRef{FromTxNum: uint64(i%4096*16 + 3), ToTxNum: uint64(i%4096*16 + 7)}
				if indexed {
					x.visit(m, h, func(uint32, SegmentRef) bool { hits++; return false })
				} else {
					for _, ref := range m.Segments {
						if ref.FromTxNum <= h.FromTxNum && ref.ToTxNum >= h.ToTxNum {
							hits++
						}
					}
				}
			}
			if hits != b.N {
				b.Fatal(hits, b.N)
			}
		})
	}
}
