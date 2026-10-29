package rawdb

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type queryRangeCountView struct {
	StateHistoryReadView
	tailScans int
}

func (v *queryRangeCountView) NewIterator(prefix, start []byte) ethdb.Iterator {
	if bytes.Equal(prefix, stateChangeSetPrefix) {
		v.tailScans++
	}
	return v.StateHistoryReadView.NewIterator(prefix, start)
}
func TestQueryFilterRangeReplaysOtherOwnerRepairWithoutRescan(t *testing.T) {
	f := queryKeysFixture(t, false)
	owner := common.Address{0x41, 0xcc}
	repair := &StateDomainChange{BlockNum: 3, Seq: 900, TxNum: 3500, FlatDomain: StateFlatDomainAccountLatest, Owner: owner, PrevExists: true, Prev: []byte("repair")}
	if err := WriteStateDomainChangeRow(f, repair); err != nil {
		t.Fatal(err)
	}
	view, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	counted := &queryRangeCountView{StateHistoryReadView: view}
	cache := WithStateHistoryQueryTailCache(counted).(*stateHistoryQueryTailView)
	PlanStateHistoryQueryTailOwners(cache, []common.Address{common.SystemAccountAddress, owner})
	got, err := ReadFirstStateDomainChangeByKeyBlockRangeContext(context.Background(), cache, 1, 5, 1000, 5999, StateFlatDomainKVLatest, common.SystemAccountAddress, 7, kvdomains.SystemDynamicProperty, []byte("missing"))
	if err != nil || got != nil || cache.filterRange == nil || len(cache.filterRange.repairs) != 1 {
		t.Fatal("complete range not recorded", got, err, cache.filterRange)
	}
	scans := counted.tailScans
	got, err = ReadFirstStateDomainChangeByKeyBlockRangeContext(context.Background(), cache, 1, 5, 1000, 5999, StateFlatDomainAccountLatest, owner, 0, 0, nil)
	if err != nil || !reflect.DeepEqual(got, repair) || counted.tailScans != scans {
		t.Fatal("repair lost or tail rescanned", got, err, counted.tailScans, scans)
	}
	got.Prev[0] ^= 1
	got, err = ReadFirstStateDomainChangeByKeyBlockRangeContext(context.Background(), cache, 1, 5, 1000, 5999, StateFlatDomainAccountLatest, owner, 0, 0, nil)
	if err != nil || !reflect.DeepEqual(got, repair) {
		t.Fatal("callback modified repair cache", got, err)
	}
	// A different range must not inherit exact-range negative coverage.
	q := newHistoryTailKeyQuery(StateFlatDomainAccountLatest, owner, 0, 0, map[string][]byte{"key": nil})
	if cache.cachedFilterRange(3, 5, q) != nil {
		t.Fatal("range identity was lost")
	}
}
func TestQueryFilterRangeSaturationUnknownHolesAndBudget(t *testing.T) {
	owner := common.Address{0x41, 0x11}
	q := newHistoryTailKeyQuery(StateFlatDomainAccountLatest, owner, 0, 0, map[string][]byte{"key": nil})
	r := newHistoryTailFilterRange(2, 3)
	r.planEnvelopes(nil, owner)
	var filters [3][]byte
	for g, n := range historyQueryFilterBytes {
		filters[g] = make([]byte, n)
	}
	// Each pack has a missing probe, but their OR is saturated. Exact planned
	// envelope probes must still retain the per-pack negative proof.
	for i := range filters[0] {
		filters[0][i] = 0x55
	}
	r.add(2, 7, filters)
	for i := range filters[0] {
		filters[0][i] = 0xaa
	}
	r.add(3, 7, filters)
	if !q.filtersMayMatch(r.known, r.filters) {
		t.Fatal("test did not saturate OR")
	}
	if r.mayMatch(q) {
		t.Fatal("per-pack envelope negatives lost to OR saturation")
	}
	// General queries fall back on a positive OR; they never infer an intersection.
	other := newHistoryTailKeyQuery(StateFlatDomainAccountLatest, common.Address{0x41, 0x77}, 0, 0, map[string][]byte{"key": nil})
	if !r.mayMatch(other) {
		t.Fatal("positive aggregate proved absence")
	}
	hole := newHistoryTailFilterRange(2, 4)
	hole.add(2, 7, filters)
	hole.add(4, 7, filters)
	if hole.valid {
		t.Fatal("missing block accepted")
	}
	unknown := newHistoryTailFilterRange(2, 2)
	unknown.add(2, 0, filters)
	if !unknown.mayMatch(other) {
		t.Fatal("unknown scope proved absence")
	}
	overflow := newHistoryTailFilterRange(2, 2)
	overflow.addRepair(&StateDomainChange{Prev: make([]byte, historyTailFilterRepairBytes+1)})
	if overflow.valid || len(overflow.repairs) != 0 {
		t.Fatal("repair budget did not fail closed")
	}
}

func TestQueryFilterRangePartialAndInvalidScansDoNotPublish(t *testing.T) {
	for _, mode := range []string{"early-stop", "cancel", "bad-record"} {
		t.Run(mode, func(t *testing.T) {
			f := queryKeysFixture(t, false)
			if mode == "bad-record" {
				f.Put(stateChangeQueryFiltersKey(2), []byte("bad"))
			}
			view, release, err := AcquireStateHistoryReadView(f)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			cache := WithStateHistoryQueryTailCache(view).(*stateHistoryQueryTailView)
			key := []byte("known")
			if mode == "bad-record" {
				key = []byte("missing")
			}
			q := newHistoryTailKeyQuery(StateFlatDomainKVLatest, common.SystemAccountAddress, 7, kvdomains.SystemDynamicProperty, map[string][]byte{"key": key})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			err = iterateFirstStateHistoryKeyTail(ctx, cache, 2, 5, 1000, 5999, common.SystemAccountAddress, q, func(*StateDomainChange) (bool, error) {
				if mode == "cancel" {
					cancel()
					return true, nil
				}
				return false, nil
			})
			if mode == "cancel" && err != context.Canceled {
				t.Fatal("cancellation lost", err)
			}
			if cache.filterRange != nil {
				t.Fatal("partial/untrusted coverage published", mode)
			}
		})
	}
}
