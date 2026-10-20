package snapshots

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestHistoryMixedBatchMatchesScalarAcrossOwnersDomainsAndCuts(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	dir := t.TempDir()
	a, b := common.Address{0x41, 1}, common.Address{0x41, 2}
	queries := []StateHistoryQuery{
		{FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: a},
		{FlatDomain: rawdb.StateFlatDomainKVGeneration, Owner: a},
		{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: a, Generation: 3, Domain: kvdomains.ContractStorage, Key: []byte("one")},
		{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: b, Generation: 7, Domain: kvdomains.SystemReward, Key: []byte("one")},
		{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: b, Generation: 7, Domain: kvdomains.SystemDynamicProperty, Key: []byte("one")},
		{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: a, Generation: 4, Domain: kvdomains.ContractStorage, Key: []byte("one")},
	}
	for tx := uint64(1); tx <= 6; tx++ {
		if e := rawdb.WriteStateTxRange(db, tx, common.Hash{byte(tx)}, tx, tx); e != nil {
			t.Fatal(e)
		}
		for i, q := range queries[:5] {
			row := &rawdb.StateDomainChange{BlockNum: tx, BlockHash: common.Hash{byte(tx)}, TxNum: tx, Seq: uint64(i), FlatDomain: q.FlatDomain, Owner: q.Owner, Generation: q.Generation, Domain: q.Domain, Key: q.Key, PrevExists: true, Prev: []byte{byte(tx), byte(i)}}
			if q.FlatDomain == rawdb.StateFlatDomainKVGeneration {
				row.Prev = rawdb.EncodeStateKVGenerationValue(tx)
			}
			if e := rawdb.WriteStateDomainChange(db, row); e != nil {
				t.Fatal(e)
			}
		}
	}
	first, e := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1, 3, "history/state-domain-change-mixed-1-3.seg")
	if e != nil {
		t.Fatal(e)
	}
	last, e := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 4, 6, "history/state-domain-change-mixed-4-6.seg")
	if e != nil {
		t.Fatal(e)
	}
	if e := PublishManifest(dir, NewManifest(1, 6, append(first, last...))); e != nil {
		t.Fatal(e)
	}
	m, e := OpenManager(dir)
	if e != nil {
		t.Fatal(e)
	}
	queries = append(queries, queries[0])
	for _, from := range []uint64{1, 3, 5, 6, 7} {
		got, e := m.FirstStateDomainChangesContext(context.Background(), from, 9, queries)
		if e != nil {
			t.Fatal(e)
		}
		for i, q := range queries {
			var want *rawdb.StateDomainChange
			e := m.IterateStateDomainChangesByKey(from, 9, q.FlatDomain, q.Owner, q.Generation, q.Domain, q.Key, func(row *rawdb.StateDomainChange) (bool, error) { want = row; return false, nil })
			if e != nil || !reflect.DeepEqual(got[i], want) {
				t.Fatalf("from %d query %d: got %+v want %+v err %v", from, i, got[i], want, e)
			}
		}
		if got[0] != nil {
			got[0].Prev[0] = 255
			if got[len(got)-1].Prev[0] == 255 {
				t.Fatal("duplicate outputs alias")
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := m.FirstStateDomainChangesContext(ctx, 1, 6, queries); !errors.Is(e, context.Canceled) {
		t.Fatal("cancel lost")
	}
	if _, e := m.FirstStateDomainChangesContext(context.Background(), 6, 1, queries); e == nil {
		t.Fatal("inverted range accepted")
	}
}
