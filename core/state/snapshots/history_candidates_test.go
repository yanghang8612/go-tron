package snapshots

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestHistoryCandidatesJumpNegativeAndRetainUnknownReplacement(t *testing.T) {
	const n = 4096
	m := &Manifest{}
	for i := 0; i < n; i++ {
		h := SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: uint64(i + 1), ToTxNum: uint64(i + 1), Path: fmt.Sprintf("history/state-domain-change-jump-%d.seg", i), Checksum: "sha256:" + fmt.Sprintf("%x", sha256.Sum256([]byte{byte(i), byte(i >> 8)}))}
		m.Segments = append(m.Segments, h)
	}
	// Use the table directly to isolate inverted dispatch from disk codecs.
	m = manifestLookupView(m)
	tab := &historySourceTable{byID: make(map[[32]byte]uint32)}
	route := &systemHistoryRouting{columns: make(map[[32]byte]int), keys: make(map[string][]byte)}
	for i := range n {
		var id [32]byte
		binary.BigEndian.PutUint64(id[:], uint64(i+1))
		tab.sources = append(tab.sources, historySource{row: uint32(i), id: id, paired: true})
		tab.byID[id] = uint32(i)
		route.sources = append(route.sources, id)
		route.columns[id] = i
	}
	m.lookup.historyOnce.Do(func() { m.lookup.history = tab })
	q := StateHistoryQuery{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: common.SystemAccountAddress, Generation: 3, Domain: kvdomains.SystemDynamicProperty, Key: []byte("missing")}
	key := stateDomainChangeBinaryAccessorLookupKey(q.FlatDomain, q.Owner, q.Generation, q.Domain, q.Key)
	queries := []historyCandidateQuery{{query: q, lookup: key}}
	rows, e := historyCandidates(context.Background(), t.TempDir(), m, 1, n, queries, nil, route, nil, newMetadataHistoryRoutingReader(m))
	if e != nil || len(rows) != 0 {
		t.Fatalf("negative candidate count=%d err=%v", len(rows), e)
	}
	route.keys[string(key)] = append([]byte{1}, binary.BigEndian.AppendUint32(nil, 3000)...)
	rows, e = historyCandidates(context.Background(), "", m, 1, n, queries, nil, route, nil, newMetadataHistoryRoutingReader(m))
	if e != nil || len(rows) != 1 || rows[0].row != 3000 {
		t.Fatalf("positive candidates=%+v err=%v", rows, e)
	}
	delete(route.columns, tab.sources[4000].id) // same range, no exact source proof
	rows, e = historyCandidates(context.Background(), "", m, 3002, n, queries, nil, route, nil, newMetadataHistoryRoutingReader(m))
	if e != nil || len(rows) != 1 || rows[0].row != 4000 {
		t.Fatalf("unknown candidate lost: %+v %v", rows, e)
	}
	// Arbitrary domains must remain unknown even if a small-family object has no key.
	queries[0].query.Domain = kvdomains.SystemReward
	queries[0].lookup = stateDomainChangeBinaryAccessorLookupKey(q.FlatDomain, q.Owner, q.Generation, kvdomains.SystemReward, q.Key)
	rows, e = historyCandidates(context.Background(), "", m, n-1, n, queries, nil, route, nil, newMetadataHistoryRoutingReader(m))
	if e != nil || len(rows) != 2 {
		t.Fatalf("unsupported family incorrectly negative: %d %v", len(rows), e)
	}
	// A frequently changing key must disappear from the merge after it has
	// been resolved. Keeping a static union would still visit all n sources
	// while the separate absent key waited for range completion.
	missing := q
	missing.Key = []byte("never-present")
	missingKey := stateDomainChangeBinaryAccessorLookupKey(missing.FlatDomain, missing.Owner, missing.Generation, missing.Domain, missing.Key)
	route.columns[tab.sources[4000].id] = 4000
	all := make([]byte, 1+(n+7)/8)
	for i := range n {
		all[1+i/8] |= 1 << uint(i%8)
	}
	route.keys[string(key)] = all
	plan, e := newHistoryCandidatePlan(context.Background(), "", m, 1, n, []historyCandidateQuery{{query: q, lookup: key}, {query: missing, lookup: missingKey}}, nil, route, nil, newMetadataHistoryRoutingReader(m))
	if e != nil {
		t.Fatal(e)
	}
	if first, ok := plan.next(func([]byte) bool { return true }); !ok || first.row != 0 {
		t.Fatalf("first candidate: %+v %v", first, ok)
	}
	if extra, ok := plan.next(func(k []byte) bool { return string(k) != string(key) }); ok {
		t.Fatalf("resolved key left a spurious candidate: %+v", extra)
	}
}

func TestHistoryCandidatesPartialCompleteAndReplacedManifestParity(t *testing.T) {
	dir, original, queries := routingFixture(t)
	oracle, e := OpenPinnedManager(dir, original)
	if e != nil {
		t.Fatal(e)
	}
	current := original
	for pass := 0; pass < 3; pass++ {
		current = addSmallRoutingFixture(t, dir, current, 1)
		reader, e := OpenPinnedManager(dir, current)
		if e != nil {
			t.Fatal(e)
		}
		for from := uint64(1); from <= 4; from++ {
			got, e := reader.FirstStateDomainChangesContext(context.Background(), from, 4, queries)
			if e != nil {
				t.Fatal(e)
			}
			for i, q := range queries {
				var want *rawdb.StateDomainChange
				e = oracle.IterateStateDomainChangesByKey(from, 4, q.FlatDomain, q.Owner, q.Generation, q.Domain, q.Key, func(row *rawdb.StateDomainChange) (bool, error) { want = row; return false, nil })
				if e != nil || !reflect.DeepEqual(got[i], want) {
					t.Fatalf("pass %d from %d query %d mismatch: %v", pass, from, i, e)
				}
			}
		}
	}
}
