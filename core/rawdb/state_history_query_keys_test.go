package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type queryKeyChunkCountView struct {
	StateHistoryReadView
	chunks   int
	sidecars int
}

func (v *queryKeyChunkCountView) Get(key []byte) ([]byte, error) {
	if bytes.HasPrefix(key, stateHistorySharedChunkPrefix) || bytes.HasPrefix(key, stateHistorySharedBucketPrefix) {
		v.chunks++
	}
	if bytes.HasPrefix(key, stateChangeOwnerSpansPrefix) {
		v.sidecars++
	}
	return v.StateHistoryReadView.Get(key)
}

func queryKeysFixture(t *testing.T, legacy bool) *historyViewTestFactory {
	t.Helper()
	f := &historyViewTestFactory{KeyValueStore: NewMemoryDatabase()}
	t.Cleanup(func() { f.Close() })
	for block := uint64(2); block <= 5; block++ {
		var rows []*StateDomainChange
		for i := 0; i < 180; i++ {
			row := &StateDomainChange{BlockNum: block, Seq: uint64(i + 1), TxNum: block*1000 + uint64(i), FlatDomain: StateFlatDomainKVLatest, Owner: common.Address{0x41, 0x44}, Generation: 7, Domain: kvdomains.ContractStorage, Key: []byte(fmt.Sprint(i)), PrevExists: true, Prev: bytes.Repeat([]byte{byte(i), byte(block)}, 100)}
			if i == 0 {
				row.Owner = common.SystemAccountAddress
				row.Domain = kvdomains.SystemDynamicProperty
				row.Key = []byte("known")
			}
			rows = append(rows, row)
		}
		if err := WriteStateDomainChangeBlockRows(f, rows); err != nil {
			t.Fatal(err)
		}
		writeHistoryReadViewSharedFixture(t, f.KeyValueStore, block)
		v, release, err := AcquireStateHistoryReadView(f)
		if err != nil {
			t.Fatal(err)
		}
		if legacy {
			if err := f.Delete(stateChangeQueryFiltersKey(block)); err != nil {
				t.Fatal(err)
			}
			pack, err := v.Get(stateChangeSetKey(block, 0))
			if err != nil {
				t.Fatal(err)
			}
			materialized, err := materializeStateHistorySharedPack(pack, block, []ethdb.KeyValueReader{v})
			if err != nil {
				t.Fatal(err)
			}
			raw, pool, err := borrowStateDomainChangeBlockPayload(materialized)
			if err != nil {
				t.Fatal(err)
			}
			index, err := buildHistoryOwnerSpansVersion(raw, pack, block, nil, false)
			if pool != nil {
				releaseBorrowedStateDomainChangeBlockPayload(pool)
			}
			if err != nil || len(index) == 0 {
				t.Fatal("legacy fixture", err)
			}
			if err := f.Put(stateChangeOwnerSpansKey(block), index); err != nil {
				t.Fatal(err)
			}
		} else if built, err := BuildStateHistoryOwnerSpans(context.Background(), v, f, block); err != nil || !built {
			t.Fatal(built, err)
		}
		release()
	}
	if err := WriteStageProgressWithHash(f, StageStateHistoryIndex, 1, common.Hash{1}); err != nil {
		t.Fatal(err)
	}
	return f
}

func queryKeyOracle(t *testing.T, db StateKVHistoryReader, owner common.Address, generation uint64, domain kvdomains.KVDomain, key []byte) *StateDomainChange {
	t.Helper()
	var want *StateDomainChange
	err := IterateStateDomainChangesByBlockRange(db, 2, 5, func(row *StateDomainChange) (bool, error) {
		if stateDomainChangeInTxWindow(row, 1000, 5999) && stateDomainChangeMatchesKey(row, StateFlatDomainKVLatest, owner, generation, domain, key) {
			want = cloneStateDomainChange(row)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return want
}

func TestQueryKeysNegativePackStillReadsPositiveRepair(t *testing.T) {
	f := queryKeysFixture(t, false)
	repair := &StateDomainChange{BlockNum: 3, Seq: 900, TxNum: 3500, FlatDomain: StateFlatDomainKVLatest, Owner: common.SystemAccountAddress, Generation: 7, Domain: kvdomains.SystemDynamicProperty, Key: []byte("future"), PrevExists: true, Prev: []byte("repair")}
	if err := WriteStateDomainChangeRow(f, repair); err != nil {
		t.Fatal(err)
	}
	v, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	counted := &queryKeyChunkCountView{StateHistoryReadView: v}
	cache := WithStateHistoryQueryTailCache(counted).(*stateHistoryQueryTailView)
	got, err := ReadFirstStateDomainChangeByKeyBlockRangeContext(context.Background(), cache, 1, 5, 1000, 5999, StateFlatDomainKVLatest, repair.Owner, 7, repair.Domain, repair.Key)
	if err != nil || !reflect.DeepEqual(got, repair) || counted.chunks != 0 {
		t.Fatal("negative pack hid repair or loaded chunks", got, counted.chunks, err)
	}
	if len(cache.entries) != 0 || len(cache.first) != 0 {
		t.Fatal("filtered scan published complete owner cache")
	}
	got, err = ReadFirstStateDomainChangeByKeyBlockRangeContext(context.Background(), cache, 1, 5, 1000, 5999, StateFlatDomainKVLatest, repair.Owner, 7, repair.Domain, []byte("known"))
	want := queryKeyOracle(t, v, repair.Owner, 7, repair.Domain, []byte("known"))
	if err != nil || !reflect.DeepEqual(got, want) || got == nil || got.BlockNum != 2 {
		t.Fatal("later key inherited filtered absence", got, want, err)
	}
}

func TestQueryKeysRepairCanShadowPackHit(t *testing.T) {
	for _, kind := range []string{"owner", "key", "generation"} {
		t.Run(kind, func(t *testing.T) {
			f := queryKeysFixture(t, false)
			repair := &StateDomainChange{BlockNum: 2, Seq: 1, TxNum: 2000, FlatDomain: StateFlatDomainKVLatest, Owner: common.SystemAccountAddress, Generation: 7, Domain: kvdomains.SystemDynamicProperty, Key: []byte("known"), PrevExists: true, Prev: []byte("shadow")}
			switch kind {
			case "owner":
				repair.Owner = common.Address{0x41, 0x99}
			case "key":
				repair.Key = []byte("other")
			case "generation":
				repair.Generation = 8
			}
			if err := WriteStateDomainChangeRow(f, repair); err != nil {
				t.Fatal(err)
			}
			v, release, err := AcquireStateHistoryReadView(f)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			got, err := ReadFirstStateDomainChangeByKeyBlockRangeContext(context.Background(), WithStateHistoryQueryTailCache(v), 1, 5, 1000, 5999, StateFlatDomainKVLatest, common.SystemAccountAddress, 7, kvdomains.SystemDynamicProperty, []byte("known"))
			want := queryKeyOracle(t, v, common.SystemAccountAddress, 7, kvdomains.SystemDynamicProperty, []byte("known"))
			if err != nil || !reflect.DeepEqual(got, want) || got == nil || got.BlockNum != 3 {
				t.Fatal("stale pack hit survived repair", got, want, err)
			}
		})
	}
}

func TestQueryKeysLegacyTailRetainsCompleteOwnerCache(t *testing.T) {
	f := queryKeysFixture(t, true)
	v, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	counted := &queryKeyChunkCountView{StateHistoryReadView: v}
	cache := WithStateHistoryQueryTailCache(counted).(*stateHistoryQueryTailView)
	got, err := ReadFirstStateDomainChangeByKeyBlockRangeContext(context.Background(), cache, 1, 5, 1000, 5999, StateFlatDomainKVLatest, common.SystemAccountAddress, 7, kvdomains.SystemDynamicProperty, []byte("future"))
	if err != nil || got != nil || counted.chunks == 0 || len(cache.entries) == 0 {
		t.Fatal("legacy fallback lost complete cache", got, counted.chunks, err)
	}
	counted.chunks = 0
	got, err = ReadFirstStateDomainChangeByKeyBlockRangeContext(context.Background(), cache, 1, 5, 1000, 5999, StateFlatDomainKVLatest, common.SystemAccountAddress, 7, kvdomains.SystemDynamicProperty, []byte("known"))
	if err != nil || got == nil || counted.chunks != 0 {
		t.Fatal("legacy owner was rescanned", got, counted.chunks, err)
	}
	if err := CheckStateHistoryOwnerSpans(context.Background(), v, 2); err != nil {
		t.Fatal("legacy strict check", err)
	}
}

func TestQueryKeysDirectoryScopesBoundsAndCancellation(t *testing.T) {
	var b historyQueryKeysBuilder
	row := &StateDomainChange{FlatDomain: StateFlatDomainKVLatest, Owner: common.SystemAccountAddress, Generation: 7, Domain: kvdomains.SystemDynamicProperty, Key: []byte("same")}
	b.add(row)
	var parsed historyOwnerSpans
	if err := decodeHistoryQueryKeys(b.appendTo(nil), &parsed); err != nil || parsed.queryKnown != 7 || len(parsed.queryKeys[1]) != 1 {
		t.Fatal(parsed, err)
	}
	for i := 0; i < 1200; i++ {
		row.Key = []byte(fmt.Sprintf("long-key-%04d", i))
		b.add(row)
	}
	parsed = historyOwnerSpans{}
	if err := decodeHistoryQueryKeys(b.appendTo(nil), &parsed); err != nil || parsed.queryKnown&2 != 0 || parsed.queryKnown&5 != 5 {
		t.Fatal("one scope budget disabled others or proved absence", parsed, err)
	}
	f := queryKeysFixture(t, false)
	v, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	counted := &queryKeyChunkCountView{StateHistoryReadView: v}
	for _, gen := range []uint64{7, 8} {
		key := []byte("future")
		if gen == 8 {
			key = []byte("known")
		}
		got, err := ReadFirstStateDomainChangeByKeyBlockRangeContext(context.Background(), WithStateHistoryQueryTailCache(counted), 1, 5, 1000, 5999, StateFlatDomainKVLatest, common.SystemAccountAddress, gen, kvdomains.SystemDynamicProperty, key)
		if err != nil || got != nil || counted.chunks != 0 {
			t.Fatal("exact generation scope or absent key decompressed pack", got, counted.chunks, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ReadFirstStateKVChangesByKeysBlockRangeContext(ctx, counted, 1, 5, 1000, 5999, common.SystemAccountAddress, 7, kvdomains.SystemDynamicProperty, [][]byte{[]byte("future")}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestQueryKeysBatchRemovesHitsBeforeSkippingLaterPacks(t *testing.T) {
	f := queryKeysFixture(t, false)
	v, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	counted := &queryKeyChunkCountView{StateHistoryReadView: v}
	cache := WithStateHistoryQueryTailCache(counted)
	got, err := ReadFirstStateKVChangesByKeysBlockRangeContext(context.Background(), cache, 1, 5, 1000, 5999, common.SystemAccountAddress, 7, kvdomains.SystemDynamicProperty, [][]byte{[]byte("known"), []byte("future")})
	if err != nil || len(got) != 1 || got["known"] == nil || got["known"].BlockNum != 2 {
		t.Fatal(got, err)
	}
	firstBlockChunks := counted.chunks
	if firstBlockChunks < 1 {
		t.Fatal("positive first pack not authenticated")
	}
	pack, err := v.Get(stateChangeSetKey(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	counted.chunks = 0
	if _, err := materializeStateHistorySharedPack(pack, 2, []ethdb.KeyValueReader{counted}); err != nil {
		t.Fatal(err)
	}
	if firstBlockChunks != counted.chunks {
		t.Fatal("later negative packs loaded chunks", firstBlockChunks, counted.chunks)
	}
}

func TestQueryKeysBadMissingAndStaleMetadataFallBack(t *testing.T) {
	for _, mode := range []string{"checksum", "missing", "stale"} {
		t.Run(mode, func(t *testing.T) {
			f := queryKeysFixture(t, false)
			key := []byte("known")
			switch mode {
			case "checksum":
				index, err := f.Get(stateChangeOwnerSpansKey(2))
				if err != nil {
					t.Fatal(err)
				}
				index[len(index)-1] ^= 1
				if err := f.Put(stateChangeOwnerSpansKey(2), index); err != nil {
					t.Fatal(err)
				}
			case "missing":
				if err := f.Delete(stateChangeOwnerSpansKey(2)); err != nil {
					t.Fatal(err)
				}
			case "stale":
				key = []byte("future")
				rows := []*StateDomainChange{{BlockNum: 2, Seq: 1, TxNum: 2000, FlatDomain: StateFlatDomainKVLatest, Owner: common.SystemAccountAddress, Generation: 7, Domain: kvdomains.SystemDynamicProperty, Key: key, PrevExists: true, Prev: []byte("new source")}}
				pack := encodeBorrowedStateDomainChangeTestBlock(t, rows)
				if err := f.Put(stateChangeSetKey(2, 0), pack); err != nil {
					t.Fatal(err)
				}
			}
			v, release, err := AcquireStateHistoryReadView(f)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			got, err := ReadFirstStateDomainChangeByKeyBlockRangeContext(context.Background(), WithStateHistoryQueryTailCache(v), 1, 5, 1000, 5999, StateFlatDomainKVLatest, common.SystemAccountAddress, 7, kvdomains.SystemDynamicProperty, key)
			want := queryKeyOracle(t, v, common.SystemAccountAddress, 7, kvdomains.SystemDynamicProperty, key)
			if err != nil || !reflect.DeepEqual(got, want) || got == nil || got.BlockNum != 2 {
				t.Fatal("invalid optional metadata became absence", got, want, err)
			}
		})
	}
}

func TestQueryKeysBorrowedVisitorValidatesAllGroupsWithoutKeyRetention(t *testing.T) {
	var builder historyQueryKeysBuilder
	for i := 0; i < 200; i++ {
		var owner common.Address
		owner[18] = byte(i >> 8)
		owner[19] = byte(i)
		builder.add(&StateDomainChange{FlatDomain: StateFlatDomainAccountLatest, Owner: owner})
	}
	raw := builder.appendTo(nil)
	var full, borrowed historyOwnerSpans
	if err := decodeHistoryQueryKeys(raw, &full); err != nil {
		t.Fatal(err)
	}
	seen := 0
	if err := decodeHistoryQueryKeysVisit(raw, &borrowed, func(group int, key []byte) {
		seen++
		if group != 0 || len(key) == 0 {
			t.Fatal("wrong borrowed key")
		}
	}); err != nil {
		t.Fatal(err)
	}
	if seen != len(full.queryKeys[0]) || seen != 200 || borrowed.queryKnown != full.queryKnown {
		t.Fatal("incomplete membership validation", seen)
	}
	for _, keys := range borrowed.queryKeys {
		if len(keys) != 0 {
			t.Fatal("query retained decoded key strings")
		}
	}
	ordinary := testing.AllocsPerRun(20, func() {
		var parsed historyOwnerSpans
		if err := decodeHistoryQueryKeys(raw, &parsed); err != nil {
			panic(err)
		}
	})
	visited := testing.AllocsPerRun(20, func() {
		var parsed historyOwnerSpans
		if err := decodeHistoryQueryKeysVisit(raw, &parsed, func(int, []byte) {}); err != nil {
			panic(err)
		}
	})
	if visited >= ordinary/4 {
		t.Fatalf("borrowed query still allocates per directory key: full=%v visited=%v", ordinary, visited)
	}
	// A visitor that has already found a match must still reject a malformed
	// later group. Optional metadata can never turn partial validation into absence.
	bad := binary.AppendUvarint(nil, 7)
	bad = binary.AppendUvarint(bad, 1)
	bad = binary.AppendUvarint(bad, 1)
	bad = append(bad, 'a')
	bad = binary.AppendUvarint(bad, 0)
	bad = binary.AppendUvarint(bad, 2)
	for i := 0; i < 2; i++ {
		bad = binary.AppendUvarint(bad, 1)
		bad = append(bad, 'z')
	}
	if err := decodeHistoryQueryKeysVisit(bad, &historyOwnerSpans{}, func(int, []byte) {}); err == nil {
		t.Fatal("visitor accepted duplicate keys in later group")
	}
}
