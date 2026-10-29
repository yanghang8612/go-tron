package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/golang/snappy"
	"golang.org/x/crypto/blake2b"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestQueryFiltersNeverLoseInsertedCanonicalKeys(t *testing.T) {
	var builder historyQueryFiltersBuilder
	var rows []*StateDomainChange
	// Exceed both legacy key-list limits, retaining repeated/deleted identities.
	for i := 0; i < 5000; i++ {
		var owner common.Address
		binary.BigEndian.PutUint64(owner[12:], uint64(i))
		r := &StateDomainChange{FlatDomain: StateFlatDomainAccountLatest, Owner: owner}
		rows = append(rows, r)
		builder.add(r)
		builder.add(r)
	}
	rows = append(rows, &StateDomainChange{FlatDomain: StateFlatDomainKVLatest, Owner: common.SystemAccountAddress, Generation: 7, Domain: kvdomains.SystemDynamicProperty, Key: []byte("future-property")})
	builder.add(rows[len(rows)-1])
	data := builder.appendTo(nil)
	var parsed historyOwnerSpans
	if err := decodeHistoryQueryFilters(data, &parsed); err != nil || parsed.queryKnown != 7 || len(data) != 2697 {
		t.Fatal("unbounded/incomplete filters", len(data), parsed.queryKnown, err)
	}
	for _, row := range rows {
		g, _ := historyQueryKeyGroup(row.FlatDomain, row.Owner, row.Domain)
		key, ok := stateDomainChangeLatestKeyByKey(row.FlatDomain, row.Owner, row.Generation, row.Domain, row.Key)
		if !ok || !historyQueryFilterContains(parsed.queryFilters[g], historyQueryKeyHash(key)) {
			t.Fatal("false absence for inserted key", row)
		}
	}
	// Complete-empty and saturated must have opposite conservative outcomes.
	if historyQueryFilterContains(parsed.queryFilters[2], historyQueryKeyHash([]byte("not present"))) {
		t.Fatal("empty scope not negative")
	}
	for i := range parsed.queryFilters[0] {
		parsed.queryFilters[0][i] = 0xff
	}
	if !historyQueryFilterContains(parsed.queryFilters[0], historyQueryKeyHash([]byte("unknown"))) {
		t.Fatal("saturated filter proved absence")
	}
	if !historyQueryFilterContains(nil, 0) {
		t.Fatal("unknown filter proved absence")
	}
	for _, bad := range [][]byte{data[:len(data)-1], append(bytes.Clone(data), 0), {8}} {
		if decodeHistoryQueryFilters(bad, &historyOwnerSpans{}) == nil {
			t.Fatal("malformed filter accepted")
		}
	}
}

func TestQueryFiltersV3ReadAndStrictRebuild(t *testing.T) {
	f := queryKeysFixture(t, false)
	v, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	for _, block := range []uint64{2, 3} {
		pack, present, err := readPresentValue(v, stateChangeSetKey(block, 0), "test pack")
		if err != nil || !present {
			release()
			t.Fatal(err)
		}
		materialized, err := materializeStateHistorySharedPack(pack, block, []ethdb.KeyValueReader{v})
		if err != nil {
			release()
			t.Fatal(err)
		}
		raw, pool, err := borrowStateDomainChangeBlockPayload(materialized)
		if err != nil {
			release()
			t.Fatal(err)
		}
		index, err := buildHistoryOwnerSpansFormat(raw, pack, block, nil, 3)
		if pool != nil {
			releaseBorrowedStateDomainChangeBlockPayload(pool)
		}
		if err != nil {
			release()
			t.Fatal(err)
		}
		if err = f.Put(stateChangeOwnerSpansKey(block), index); err != nil {
			release()
			t.Fatal(err)
		}
	}
	release()
	view, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	for _, block := range []uint64{2, 3} {
		if err := CheckStateHistoryOwnerSpans(context.Background(), view, block); err != nil {
			t.Fatal(err)
		}
	}
	pack, _, err := readPresentValue(view, stateChangeSetKey(2, 0), "legacy proof")
	if err != nil {
		t.Fatal(err)
	}
	q := newHistoryTailKeyQuery(StateFlatDomainKVLatest, common.SystemAccountAddress, 7, kvdomains.SystemDynamicProperty, map[string][]byte{"missing": []byte("missing")})
	if skip, err := q.skipPack(context.Background(), view, pack, 2); err != nil || !skip {
		t.Fatal("V3 negative proof lost", skip, err)
	}
	// The original canonical query oracle and repair coverage remain exercised
	// by all TestQueryKeys tests with the normal V4 writer.
}

func TestQueryFiltersSaturatedMetadataUsesCanonicalFallback(t *testing.T) {
	f := queryKeysFixture(t, false)
	for block := uint64(2); block <= 5; block++ {
		if err := f.Delete(stateChangeQueryFiltersKey(block)); err != nil {
			t.Fatal(err)
		}
		index, err := f.Get(stateChangeOwnerSpansKey(block))
		if err != nil {
			t.Fatal(err)
		}
		_, n := binary.Uvarint(index[8:])
		payload, err := snappy.Decode(nil, index[8+n+32:])
		if err != nil {
			t.Fatal(err)
		}
		for i := len(payload) - 2688; i < len(payload); i++ {
			payload[i] = 0xff
		}
		digest := blake2b.Sum256(payload)
		sealed := append([]byte(nil), historyOwnerSpansMagic[:]...)
		sealed = binary.AppendUvarint(sealed, uint64(len(payload)))
		sealed = append(sealed, digest[:]...)
		sealed = append(sealed, snappy.Encode(nil, payload)...)
		if err = f.Put(stateChangeOwnerSpansKey(block), sealed); err != nil {
			t.Fatal(err)
		}
	}
	view, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	counted := &queryKeyChunkCountView{StateHistoryReadView: view}
	got, err := ReadFirstStateDomainChangeByKeyBlockRangeContext(context.Background(), counted, 1, 5, 1000, 5999, StateFlatDomainKVLatest, common.SystemAccountAddress, 7, kvdomains.SystemDynamicProperty, []byte("missing"))
	if err != nil || got != nil || counted.chunks == 0 {
		t.Fatal("saturated bits did not preserve canonical fallback", got, counted.chunks, err)
	}
}

func TestQueryFiltersVersionByteCannotReinterpretLegacyDirectory(t *testing.T) {
	f := queryKeysFixture(t, false)
	view, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	pack, _, err := readPresentValue(view, stateChangeSetKey(2, 0), "legacy source")
	if err != nil {
		t.Fatal(err)
	}
	materialized, err := materializeStateHistorySharedPack(pack, 2, []ethdb.KeyValueReader{view})
	if err != nil {
		t.Fatal(err)
	}
	raw, pool, err := borrowStateDomainChangeBlockPayload(materialized)
	if err != nil {
		t.Fatal(err)
	}
	if pool != nil {
		defer releaseBorrowedStateDomainChangeBlockPayload(pool)
	}
	v3, err := buildHistoryOwnerSpansFormat(raw, pack, 2, nil, 3)
	if err != nil {
		t.Fatal(err)
	}
	copy(v3[:8], historyOwnerSpansMagic[:])
	if _, err = decodeHistoryOwnerSpans(v3, 2); err == nil {
		t.Fatal("V3 exact-key bytes accepted as V4 filter")
	}
	v4, err := buildHistoryOwnerSpans(raw, pack, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	copy(v4[:8], historyOwnerSpansMagicV3[:])
	if _, err = decodeHistoryOwnerSpans(v4, 2); err == nil {
		t.Fatal("V4 filter accepted as V3 exact-key directory")
	}
}
