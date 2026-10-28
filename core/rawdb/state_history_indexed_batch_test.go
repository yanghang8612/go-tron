package rawdb

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type indexedBatchCountingStore struct {
	ethdb.KeyValueStore
	blocks int
}

func (s *indexedBatchCountingStore) NewIterator(prefix, start []byte) ethdb.Iterator {
	if bytes.HasPrefix(prefix, stateChangeSetPrefix) {
		s.blocks++
	}
	return s.KeyValueStore.NewIterator(prefix, start)
}

func TestIndexedKVBatchSharesCandidateBlocksAndRetriesFalsePostings(t *testing.T) {
	db := &indexedBatchCountingStore{KeyValueStore: NewMemoryDatabase()}
	defer db.Close()
	owner := common.Address{0x41, 7}
	var keys [][]byte
	for i := 0; i < 48; i++ {
		keys = append(keys, []byte(fmt.Sprintf("key-%02d", i)))
	}
	for _, block := range []uint64{2, 4, 6} {
		var rows []*StateDomainChange
		for i, key := range keys {
			q := &StateDomainChange{BlockNum: block, TxNum: block, Seq: uint64(i + 1), FlatDomain: StateFlatDomainKVLatest, Owner: owner, Generation: 3, Domain: kvdomains.SystemDynamicProperty, Key: key, PrevExists: true, Prev: []byte{byte(block), byte(i)}}
			rows = append(rows, q)
			if err := WriteStateDomainChangePostingIndex(db, q); err != nil {
				t.Fatal(err)
			}
		}
		if err := WriteStateDomainChangeBlockRows(db, rows); err != nil {
			t.Fatal(err)
		}
	}
	// A false posting at block one must not return a value or hide the real block.
	fake := &StateDomainChange{BlockNum: 1, FlatDomain: StateFlatDomainKVLatest, Owner: owner, Generation: 3, Domain: kvdomains.SystemDynamicProperty, Key: keys[0]}
	if err := WriteStateDomainChangePostingIndex(db, fake); err != nil {
		t.Fatal(err)
	}
	if err := WriteStageProgressWithHash(db, StageStateHistoryIndex, 6, common.Hash{6}); err != nil {
		t.Fatal(err)
	}
	keys = append(keys, keys[0], []byte("absent"))
	for _, target := range []uint64{0, 2, 4, 6} {
		db.blocks = 0
		got, err := ReadFirstStateKVChangesByKeysBlockRangeContext(context.Background(), db, target, 6, target, 6, owner, 3, kvdomains.SystemDynamicProperty, keys)
		if err != nil {
			t.Fatal(err)
		}
		scanned := db.blocks
		for _, key := range keys {
			want, e := ReadFirstStateDomainChangeByKeyBlockRange(db, target, 6, target, 6, StateFlatDomainKVLatest, owner, 3, kvdomains.SystemDynamicProperty, key)
			if e != nil || !reflect.DeepEqual(got[string(key)], want) {
				t.Fatalf("target %d key %q mismatch: %v", target, key, e)
			}
		}
		wantScans := 1
		if target == 0 {
			wantScans = 3
		}
		if target == 6 {
			wantScans = 0
		}
		if scanned != wantScans {
			t.Fatalf("target %d decoded %d candidate blocks for 48 keys, want %d", target, scanned, wantScans)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ReadFirstStateKVChangesByKeysBlockRangeContext(ctx, db, 0, 6, 0, 6, owner, 3, kvdomains.SystemDynamicProperty, keys); err != context.Canceled {
		t.Fatal(err)
	}
}

// Auxiliary coverage must not make a near-head query traverse the whole
// posting history when one pinned canonical scan is cheaper.
func TestIndexedKVShortWindowUsesPinnedCanonicalRows(t *testing.T) {
	f, _ := queryTailFixture(t)
	if err := WriteStageProgressWithHash(f, StageStateHistoryIndex, 10, common.Hash{10}); err != nil {
		t.Fatal(err)
	}
	view, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	counted := &shortWindowCountView{StateHistoryReadView: view}
	cached := WithStateHistoryQueryTailCache(counted)
	// Include duplicate keys, absent keys, generation changes, shared packs,
	// positive-sequence repairs, and same-block transaction cut boundaries.
	for _, block := range []uint64{2, 3, 8, 9} {
		var rows []*StateDomainChange
		if err := IterateStateDomainChanges(view, block, func(r *StateDomainChange) (bool, error) {
			rows = append(rows, cloneStateDomainChange(r))
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
		for _, q := range rows {
			if q.FlatDomain != StateFlatDomainKVLatest {
				continue
			}
			targetTx := block*100 + 1
			got, err := ReadFirstStateDomainChangeByKeyBlockRangeContext(context.Background(), cached, block-1, block, targetTx, block*100+100, q.FlatDomain, q.Owner, q.Generation, q.Domain, q.Key)
			var want *StateDomainChange
			if e := IterateStateDomainChanges(view, block, func(r *StateDomainChange) (bool, error) {
				if stateDomainChangeInTxWindow(r, targetTx, block*100+100) && stateDomainChangeMatchesKey(r, q.FlatDomain, q.Owner, q.Generation, q.Domain, q.Key) {
					want = cloneStateDomainChange(r)
					return false, nil
				}
				return true, nil
			}); e != nil {
				t.Fatal(e)
			}
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("block %d scalar mismatch: %v", block, err)
			}
			batch, e := ReadFirstStateKVChangesByKeysBlockRangeContext(context.Background(), cached, block-1, block, targetTx, block*100+100, q.Owner, q.Generation, q.Domain, [][]byte{q.Key, q.Key, []byte("absent")})
			if e != nil || !reflect.DeepEqual(batch[string(q.Key)], want) || batch["absent"] != nil {
				t.Fatalf("batch mismatch: %v", e)
			}
		}
	}
	if counted.postings != 0 {
		t.Fatalf("short window scanned %d posting prefixes", counted.postings)
	}
	if preferDirectStateHistoryWindow(cached, 0, stateHistoryDirectWindowBlocks+1) || preferDirectStateHistoryWindow(view, 0, 1) {
		t.Fatal("direct policy escaped request/range bound")
	}
}

type shortWindowCountView struct {
	StateHistoryReadView
	postings int
}

func (v *shortWindowCountView) NewIterator(prefix, start []byte) ethdb.Iterator {
	if bytes.HasPrefix(prefix, stateChangePostingPrefix) {
		v.postings++
	}
	return v.StateHistoryReadView.NewIterator(prefix, start)
}
