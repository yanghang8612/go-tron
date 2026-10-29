package rawdb

import (
	"bytes"
	"context"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestQueryFiltersRequestCacheReusesAllGroupsAndBindsSource(t *testing.T) {
	f := queryKeysFixture(t, false)
	view, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	count := &queryKeyChunkCountView{StateHistoryReadView: view}
	cache := WithStateHistoryQueryTailCache(count).(*stateHistoryQueryTailView)
	pack, err := view.Get(stateChangeSetKey(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	query := func(domain kvdomains.KVDomain) *historyTailKeyQuery {
		return newHistoryTailKeyQuery(StateFlatDomainKVLatest, common.SystemAccountAddress, 7, domain, map[string][]byte{"missing": []byte("missing")})
	}
	for _, domain := range []kvdomains.KVDomain{kvdomains.SystemDynamicProperty, kvdomains.SystemForkVote, kvdomains.SystemDynamicProperty} {
		if skip, err := query(domain).skipPack(context.Background(), cache, pack, 2); err != nil || !skip {
			t.Fatal(skip, err)
		}
	}
	if count.sidecars != 1 || len(cache.packFilters) != 1 || len(cache.entries) != 0 || len(cache.first) != 0 {
		t.Fatal("metadata was reread or published as complete rows", count.sidecars, cache)
	}
	changed := bytes.Clone(pack)
	changed[len(changed)-1] ^= 1
	if skip, err := query(kvdomains.SystemDynamicProperty).skipPack(context.Background(), cache, changed, 2); err != nil || skip {
		t.Fatal("cached proof accepted different pack", skip, err)
	}
	if count.sidecars != 2 {
		t.Fatal("stale cache failed to recheck source", count.sidecars)
	}
	fresh := WithStateHistoryQueryTailCache(count).(*stateHistoryQueryTailView)
	if fresh.cachedPackFilters(2, pack) != nil {
		t.Fatal("metadata escaped request ownership")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if skip, err := query(kvdomains.SystemDynamicProperty).skipPack(ctx, cache, pack, 2); err != context.Canceled || skip {
		t.Fatal("cached proof hid cancellation", skip, err)
	}
}

func TestQueryFiltersRequestCacheOwnsBytesAndStopsAtBudget(t *testing.T) {
	v := &stateHistoryQueryTailView{}
	r := &historyOwnerSpans{queryKnown: 7}
	for i, size := range historyQueryFilterBytes {
		r.queryFilters[i] = make([]byte, size)
	}
	v.rememberPackFilters(1, r)
	r.queryFilters[0][0] = 0xff
	if v.packFilters[1].filters[0][0] != 0 {
		t.Fatal("cache retained pooled decoder bytes")
	}
	for i := uint64(2); i < 4096; i++ {
		v.rememberPackFilters(i, r)
	}
	if v.packFilterBytes > historyTailPackFilterCacheBytes || len(v.packFilters) >= 4095 || len(v.packFilters) < 1418 {
		t.Fatal("unbounded cache or original lag no longer fits", v.packFilterBytes, len(v.packFilters))
	}
	before := v.packFilterBytes
	v.rememberPackFilters(1, r)
	if v.packFilterBytes != before {
		t.Fatal("duplicate entry charged twice")
	}
}
