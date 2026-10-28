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
