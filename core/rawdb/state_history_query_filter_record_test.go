package rawdb

import (
	"bytes"
	"context"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestQueryFilterRecordStandaloneStrictAndCanonicalFallback(t *testing.T) {
	for _, mode := range []string{"valid", "corrupt", "missing", "wrong-block", "missing-source"} {
		t.Run(mode, func(t *testing.T) {
			f := queryKeysFixture(t, false)
			record, err := f.Get(stateChangeQueryFiltersKey(2))
			if err != nil || len(record) > 3000 {
				t.Fatal(len(record), err)
			}
			f.Delete(stateChangeOwnerSpansKey(2)) // Only the small child remains.
			switch mode {
			case "corrupt":
				record[8] ^= 1
				f.Put(stateChangeQueryFiltersKey(2), record)
			case "missing":
				f.Delete(stateChangeQueryFiltersKey(2))
			case "wrong-block":
				f.Put(stateChangeQueryFiltersKey(3), record)
				f.Delete(stateChangeQueryFiltersKey(2))
			case "missing-source":
				f.Delete(stateChangeSetKey(2, 0))
			}
			view, release, err := AcquireStateHistoryReadView(f)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			check := CheckStateHistoryOwnerSpans(context.Background(), view, 2)
			if (mode == "corrupt" || mode == "missing-source") != (check != nil) {
				t.Fatal("orphan child strict validation", mode, check)
			}
			if mode == "missing-source" {
				return
			}
			hint, err := historyQueryDirectoryHint(context.Background(), view, 2, 2)
			if err != nil || mode == "valid" && !hint {
				t.Fatal("standalone child not discovered", hint, err)
			}
			counted := &queryKeyChunkCountView{StateHistoryReadView: view}
			q := newHistoryTailKeyQuery(StateFlatDomainKVLatest, common.SystemAccountAddress, 7, kvdomains.SystemDynamicProperty, map[string][]byte{"missing": []byte("missing")})
			pack, _ := view.Get(stateChangeSetKey(2, 0))
			skip, err := q.skipPack(context.Background(), counted, pack, 2)
			if err != nil || skip != (mode == "valid") {
				t.Fatal("invalid optional child proved absence", mode, skip, err)
			}
			if got := queryKeyOracle(t, view, common.SystemAccountAddress, 7, kvdomains.SystemDynamicProperty, []byte("known")); got == nil {
				t.Fatal("canonical source changed")
			}
		})
	}
}

func TestQueryFilterRecordRetirementHandlesOrphansAndSparseBlocks(t *testing.T) {
	for _, coarse := range []bool{false, true} {
		f := &historyViewTestFactory{KeyValueStore: NewMemoryDatabase()}
		for block := uint64(1); block <= 514; block++ {
			f.Put(stateChangeQueryFiltersKey(block), []byte("orphan"))
		}
		f.Put(stateChangeOwnerSpansKey(513), []byte("parent"))
		f.Put(stateChangeOwnerSpansKey(514), []byte("parent"))
		for pass := 0; pass < 3; pass++ {
			batch := f.NewBatch()
			if err := RetireStateHistoryOwnerSpans(f, batch, 513, coarse); err != nil {
				t.Fatal(err)
			}
			if err := batch.Write(); err != nil {
				t.Fatal(err)
			}
			batch.Close()
		}
		for block := uint64(1); block <= 514; block++ {
			yes, _ := f.Has(stateChangeQueryFiltersKey(block))
			if yes != (block == 514) {
				t.Fatal("orphan blocks retirement", coarse, block, yes)
			}
		}
		if err := deleteHistoryOwnerSpans(f, []uint64{514}); err != nil {
			t.Fatal(err)
		}
		for _, key := range [][]byte{stateChangeOwnerSpansKey(514), stateChangeQueryFiltersKey(514)} {
			if yes, _ := f.Has(key); yes {
				t.Fatal("sparse point deletion missed child", key)
			}
		}
		f.Close()
	}
	start, end := StateHistoryOwnerSpansBounds()
	for _, b := range []uint64{0, ^uint64(0)} {
		key := stateChangeQueryFiltersKey(b)
		if bytes.Compare(key, start) < 0 || bytes.Compare(key, end) >= 0 {
			t.Fatal("child escaped schema range")
		}
	}
}
