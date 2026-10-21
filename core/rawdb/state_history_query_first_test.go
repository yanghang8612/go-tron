package rawdb

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func TestQueryFirstTailSummaryMatchesRepairedOracle(t *testing.T) {
	f, owner := queryTailFixture(t)
	view, closeView, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer closeView()
	counted := &queryTailCountView{StateHistoryReadView: view}
	cache := WithStateHistoryQueryTailCache(counted).(*stateHistoryQueryTailView)
	cache.maxRows = 128
	miss := []byte("not-in-history")
	if row, err := ReadFirstStateDomainChangeByKeyBlockRange(cache, 1, 9, 100, 1000, StateFlatDomainKVLatest, owner, 1, kvdomains.ContractStorage, miss); err != nil || row != nil {
		t.Fatal(row, err)
	}
	if counted.scans != 1 || !cache.rejected[owner] || cache.first[owner] == nil {
		t.Fatalf("summary missing: scans=%d rejected=%v first=%v", counted.scans, cache.rejected[owner], cache.first[owner])
	}
	var keys [][]byte
	for i := 0; i < 16; i++ {
		keys = append(keys, []byte{byte(i)})
	}
	keys = append(keys, miss)
	for _, flat := range []StateFlatDomain{StateFlatDomainAccountLatest, StateFlatDomainKVGeneration, StateFlatDomainKVLatest} {
		for gen := uint64(0); gen < 2; gen++ {
			for _, key := range keys {
				var want *StateDomainChange
				if err := IterateStateDomainChangesByBlockRange(view, 2, 9, func(row *StateDomainChange) (bool, error) {
					if stateDomainChangeInTxWindow(row, 100, 1000) && stateDomainChangeMatchesKey(row, flat, owner, gen, kvdomains.ContractStorage, key) {
						want = cloneStateDomainChange(row)
						return false, nil
					}
					return true, nil
				}); err != nil {
					t.Fatal(err)
				}
				got, err := ReadFirstStateDomainChangeByKeyBlockRange(cache, 1, 9, 100, 1000, flat, owner, gen, kvdomains.ContractStorage, key)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("flat %v gen %d key %x: got %+v want %+v err %v", flat, gen, key, got, want, err)
				}
				if got != nil {
					got.Seq = 999
					if len(got.Prev) > 0 {
						got.Prev[0] ^= 0xff
					}
					if len(got.Key) > 0 {
						got.Key[0] ^= 0xff
					}
				}
			}
		}
	}
	batch, err := ReadFirstStateKVChangesByKeysBlockRange(cache, 1, 9, 100, 1000, owner, 1, kvdomains.ContractStorage, keys)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		want, err := ReadFirstStateDomainChangeByKeyBlockRange(view, 1, 9, 100, 1000, StateFlatDomainKVLatest, owner, 1, kvdomains.ContractStorage, key)
		if err != nil || !reflect.DeepEqual(batch[string(key)], want) {
			t.Fatal("batch mismatch", err)
		}
	}
	if counted.scans != 1 {
		t.Fatalf("summary rescanned %d times", counted.scans)
	}
	if cache.used > cache.budget || cache.rows > cache.maxRows {
		t.Fatal("summary over budget")
	}
	// Generic history remains a full iterator, even after a first-only summary.
	all := 0
	if err := iterateStateHistoryTailByOwner(context.Background(), cache, 2, 9, owner, func(*StateDomainChange) (bool, error) { all++; return true, nil }); err != nil {
		t.Fatal(err)
	}
	if all <= len(cache.first[owner].rows) {
		t.Fatal("generic history replaced by first rows")
	}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := ReadFirstStateDomainChangeByKeyBlockRange(cache, 1, 9, 100, 1000, StateFlatDomainKVLatest, owner, 1, kvdomains.ContractStorage, miss); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if counted.scans != 2 {
		t.Fatal("concurrent summary rescan", counted.scans)
	}
}

func TestQueryFirstTailExactWindowAndNonmonotoneTx(t *testing.T) {
	f := &historyViewTestFactory{KeyValueStore: NewMemoryDatabase()}
	defer f.Close()
	owner := common.Address{0x41, 9}
	key := []byte("key")
	for i, tx := range []uint64{20, 15} {
		if err := WriteStateDomainChangeRow(f, &StateDomainChange{BlockNum: 2, Seq: uint64(i + 1), TxNum: tx, FlatDomain: StateFlatDomainKVLatest, Owner: owner, Domain: kvdomains.ContractStorage, Key: key, Prev: []byte{byte(tx)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := WriteStageProgressWithHash(f, StageStateHistoryIndex, 1, common.Hash{1}); err != nil {
		t.Fatal(err)
	}
	view, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	counted := &queryTailCountView{StateHistoryReadView: view}
	cache := WithStateHistoryQueryTailCache(counted).(*stateHistoryQueryTailView)
	cache.maxRows = 1
	if _, err := ReadFirstStateDomainChangeByKeyBlockRange(cache, 1, 2, 1, 25, StateFlatDomainKVLatest, owner, 0, kvdomains.ContractStorage, []byte("missing")); err != nil {
		t.Fatal(err)
	}
	if cache.first[owner] == nil {
		t.Fatal("summary missing")
	}
	for _, head := range []uint64{25, 18, 25} {
		got, err := ReadFirstStateDomainChangeByKeyBlockRange(cache, 1, 2, 1, head, StateFlatDomainKVLatest, owner, 0, kvdomains.ContractStorage, key)
		if err != nil || got == nil {
			t.Fatal(got, err)
		}
		want := uint64(20)
		if head == 18 {
			want = 15
		}
		if got.TxNum != want {
			t.Fatalf("head %d got %d want %d", head, got.TxNum, want)
		}
	}
}

func TestQueryFirstTailIncompleteAndFailedScansNeverPublish(t *testing.T) {
	for _, mode := range []string{"early", "budget", "cancel", "cancel-mid", "callback-error", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			f, owner := queryTailFixture(t)
			if mode == "corrupt" {
				if err := f.Put(stateChangeSetKey(2, 0), append(append([]byte(nil), stateDomainChangeBlockEnvelopeMagic[:]...), stateDomainChangeBlockSharedVersion, 0xff)); err != nil {
					t.Fatal(err)
				}
			}
			view, release, err := AcquireStateHistoryReadView(f)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			cache := WithStateHistoryQueryTailCache(view).(*stateHistoryQueryTailView)
			cache.maxRows = 128
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "budget" {
				cache.budget = 129
			}
			if mode == "cancel" {
				cancel()
			}
			err = iterateFirstStateHistoryTail(ctx, cache, 2, 9, 100, 1000, owner, func(row *StateDomainChange) (bool, error) {
				if len(row.Prev) > 0 {
					row.Prev = bytes.Clone(row.Prev)
				}
				if mode == "cancel-mid" {
					cancel()
				}
				if mode == "callback-error" {
					return false, errors.New("callback error")
				}
				return mode != "early", nil
			})
			if (mode == "cancel" || mode == "cancel-mid") && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if (mode == "corrupt" || mode == "callback-error") && err == nil {
				t.Fatal("corrupt absence")
			}
			if len(cache.first) != 0 {
				t.Fatal("incomplete summary retained")
			}
		})
	}
}
