package snapshots

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func replaceMetadataRepairFixtureSources(t *testing.T, dir string, from, to uint64) *Manifest {
	t.Helper()
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	for tx := from; tx <= to; tx++ {
		if err := rawdb.WriteStateTxRange(db, tx, common.Hash{byte(tx)}, tx, tx); err != nil {
			t.Fatal(err)
		}
		row := &rawdb.StateDomainChange{BlockNum: tx, BlockHash: common.Hash{byte(tx)}, TxNum: tx, FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.Address{0x41, 9}, Domain: kvdomains.ContractMetadata, Key: []byte("new-meta"), PrevExists: true, Prev: []byte{byte(tx)}}
		if err := rawdb.WriteStateDomainChange(db, row); err != nil {
			t.Fatal(err)
		}
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, from, to, fmt.Sprintf("history/state-domain-change-repair-%d-%d.seg", from, to))
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewAggregator(dir).Integrate(from, to, refs)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func assertMetadataRepairCovered(t *testing.T, dir string, m *Manifest, want int) {
	t.Helper()
	r := newMetadataHistoryRoutingReader(m)
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	count := 0
	for _, h := range m.Segments {
		if h.Kind != SegmentHistory || h.normalizedDataset() != SegmentDatasetStateDomainChange {
			continue
		}
		a, _ := cfg.HistoryAccessorRef(m, h)
		if _, covered, err := r.source(context.Background(), dir, h, systemHistoryRoutingPairID(h, a)); err != nil {
			t.Fatal(err)
		} else if covered {
			count++
		}
	}
	if count != want {
		t.Fatalf("exact covered sources=%d, want %d", count, want)
	}
}

func TestMetadataHistoryRoutingRepairPreservesFullPageBeforeCursor(t *testing.T) {
	dir, m := smallManyRoutingFixtureSources(t, 256)
	m = addMetadataRouting(t, dir, m, 256)
	m = replaceMetadataRepairFixtureSources(t, dir, 128, 128)
	next := uint64(128)
	ref, built, err := buildMetadataHistoryRoutingWithBudget(context.Background(), dir, m, 1, 128, &next, smallSystemRoutingWorkLimit{bytes: 1})
	if err != nil || !built || ref.Rows != 256 || ref.FromTxNum != 1 || ref.ToTxNum != 256 || next != 129 {
		t.Fatalf("repair %+v built=%v next=%d err=%v", ref, built, next, err)
	}
	m, err = NewAggregator(dir).Integrate(m.VisibleTxStart, m.VisibleTxEnd, []SegmentRef{ref})
	if err != nil {
		t.Fatal(err)
	}
	assertMetadataRepairCovered(t, dir, m, 256)
	// This checks both the new positive filter, the old positive filter at
	// source 256, and all the preserved zero-family filters before the cursor.
	if err := checkMetadataHistoryRoutingContext(context.Background(), dir, m, ref); err != nil {
		t.Fatal(err)
	}
}

func TestMetadataHistoryRoutingRepairAcrossTwoOldPages(t *testing.T) {
	dir, m, _ := routingFixture(t)
	m = addMetadataRouting(t, dir, m, 2)
	m = addMetadataRouting(t, dir, m, 1)
	m = replaceMetadataRepairFixtureSources(t, dir, 2, 3)
	ref, built, err := BuildMetadataHistoryRoutingContext(context.Background(), dir, m, 1)
	if err != nil || !built || ref.Rows != 2 || ref.FromTxNum != 1 || ref.ToTxNum != 3 {
		t.Fatalf("cross-page repair %+v %v %v", ref, built, err)
	}
	m, err = NewAggregator(dir).Integrate(m.VisibleTxStart, m.VisibleTxEnd, []SegmentRef{ref})
	if err != nil {
		t.Fatal(err)
	}
	assertMetadataRepairCovered(t, dir, m, 2)
	if err := checkMetadataHistoryRoutingContext(context.Background(), dir, m, ref); err != nil {
		t.Fatal(err)
	}
}

func TestMetadataHistoryRoutingRepairDoesNotTruncateFullPageForAHole(t *testing.T) {
	dir, m := smallManyRoutingFixtureSources(t, 257)
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	rows := make(map[[32]byte]metadataSourceFilter)
	for _, h := range m.Segments {
		if h.Kind != SegmentHistory || h.FromTxNum == 128 {
			continue
		}
		a, _ := cfg.HistoryAccessorRef(m, h)
		filter, err := metadataHistorySourceFilter(context.Background(), dir, h, a)
		if err != nil {
			t.Fatal(err)
		}
		rows[systemHistoryRoutingPairID(h, a)] = filter
	}
	ref, built, err := writeMetadataHistoryRoutingPage(context.Background(), dir, 1, 257, rows)
	if err != nil || !built || ref.Rows != 256 {
		t.Fatal(ref, built, err)
	}
	m, err = NewAggregator(dir).Integrate(1, 257, []SegmentRef{ref})
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	next := uint64(128)
	if _, built, err := buildMetadataHistoryRoutingContext(context.Background(), dir, m, 1, 128, &next); err != nil || built || next != 129 {
		t.Fatal("over-capacity repair must yield without losing current proofs", built, next, err)
	}
	after, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil || string(before) != string(after) {
		t.Fatal("unpublished repair changed the manifest", err)
	}
	assertMetadataRepairCovered(t, dir, m, 256)
}

func TestMetadataHistoryRoutingRepairPublicationRejectsChangedPage(t *testing.T) {
	dir, m, _ := routingFixture(t)
	m = addMetadataRouting(t, dir, m, 3)
	m = replaceMetadataRepairFixtureSources(t, dir, 1, 1)
	ref, built, err := BuildMetadataHistoryRoutingContext(context.Background(), dir, m, 1)
	if err != nil || !built {
		t.Fatal(ref, built, err)
	}
	rows := make(map[[32]byte]metadataSourceFilter)
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	for _, h := range m.Segments {
		if h.Kind != SegmentHistory || h.FromTxNum == 1 {
			continue
		}
		a, _ := cfg.HistoryAccessorRef(m, h)
		filter, err := metadataHistorySourceFilter(context.Background(), dir, h, a)
		if err != nil {
			t.Fatal(err)
		}
		rows[systemHistoryRoutingPairID(h, a)] = filter
	}
	other, _, err := writeMetadataHistoryRoutingPage(context.Background(), dir, 1, 3, rows)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewAggregator(dir).Integrate(1, 3, []SegmentRef{other}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publishMetadataHistoryRoutingContext(context.Background(), dir, m, ref); !errors.Is(err, errMetadataRoutingRefreshConflict) {
		t.Fatal("stale repair replaced another publisher's page", err)
	}
	after, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil || string(before) != string(after) {
		t.Fatal("conflicting repair changed published bytes", err)
	}
}

func TestMetadataHistoryRoutingRepairPublicationKeepsUnrelatedNewPage(t *testing.T) {
	dir, m := smallManyRoutingFixture(t)
	m = addMetadataRouting(t, dir, m, 3)
	m = replaceMetadataRepairFixtureSources(t, dir, 2, 2)
	ref, built, err := BuildMetadataHistoryRoutingContext(context.Background(), dir, m, 1)
	if err != nil || !built {
		t.Fatal(ref, built, err)
	}
	other, built, err := buildMetadataHistoryRoutingContext(context.Background(), dir, m, 256, 4, nil)
	if err != nil || !built || other.FromTxNum != 4 {
		t.Fatal(other, built, err)
	}
	if _, err := NewAggregator(dir).Integrate(m.VisibleTxStart, m.VisibleTxEnd, []SegmentRef{other}); err != nil {
		t.Fatal(err)
	}
	next, err := publishMetadataHistoryRoutingContext(context.Background(), dir, m, ref)
	if err != nil {
		t.Fatal(err)
	}
	assertMetadataRepairCovered(t, dir, next, 70)
	for _, active := range next.Segments {
		if active == other {
			return
		}
	}
	t.Fatal("repair discarded another publisher's unrelated page")
}
