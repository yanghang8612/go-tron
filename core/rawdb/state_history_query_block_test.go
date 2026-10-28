package rawdb

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
)

type queryBlockCountView struct {
	StateHistoryReadView
	scans int
}

func (v *queryBlockCountView) NewIterator(prefix, start []byte) ethdb.Iterator {
	if bytes.HasPrefix(prefix, stateChangeSetPrefix) {
		v.scans++
	}
	return v.StateHistoryReadView.NewIterator(prefix, start)
}

func TestQueryBlockCacheRepairedRowsOwnershipAndNestedRead(t *testing.T) {
	f, _ := queryTailFixture(t)
	view, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	counted := &queryBlockCountView{StateHistoryReadView: view}
	cached := WithStateHistoryQueryTailCache(counted)
	for _, block := range []uint64{2, 3, 10} {
		var want []*StateDomainChange
		if err := IterateStateDomainChanges(view, block, func(r *StateDomainChange) (bool, error) {
			want = append(want, cloneStateDomainChange(r))
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
		before := counted.scans
		for i := 0; i < 3; i++ {
			var got []*StateDomainChange
			err := IterateStateDomainChanges(cached, block, func(r *StateDomainChange) (bool, error) {
				got = append(got, cloneStateDomainChange(r))
				if len(r.Prev) > 0 {
					r.Prev[0] ^= 255
				}
				r.Seq = 999
				return true, nil
			})
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("block %d mismatch: %v", block, err)
			}
		}
		if counted.scans-before != 1 {
			t.Fatalf("block %d scanned %d times", block, counted.scans-before)
		}
	}
	if err := IterateStateDomainChanges(cached, 2, func(*StateDomainChange) (bool, error) {
		return false, IterateStateDomainChanges(cached, 3, func(*StateDomainChange) (bool, error) { return false, nil })
	}); err != nil {
		t.Fatal(err)
	}
	// Reads share immutable entries but every consumer owns its row buffers.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := IterateStateDomainChanges(cached, 2, func(r *StateDomainChange) (bool, error) { r.Key = nil; return true, nil }); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := IterateStateDomainChangesContext(ctx, cached, 2, func(*StateDomainChange) (bool, error) { t.Error("callback after cancellation"); return true, nil }); err != context.Canceled {
		t.Fatal(err)
	}
}

func TestQueryBlockCacheErrorsAndAdmissionBounds(t *testing.T) {
	f, _ := queryTailFixture(t)
	if err := f.Put(stateChangeSetKey(12, 2), []byte("corrupt")); err != nil {
		t.Fatal(err)
	}
	row := &StateDomainChange{BlockNum: 12, Seq: 1, TxNum: 1, FlatDomain: StateFlatDomainAccountLatest, PrevExists: true, Prev: []byte("before error")}
	if err := WriteStateDomainChangeRow(f, row); err != nil {
		t.Fatal(err)
	}
	view, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	counted := &queryBlockCountView{StateHistoryReadView: view}
	cached := WithStateHistoryQueryTailCache(counted).(*stateHistoryQueryTailView)
	for i := 0; i < 2; i++ {
		if err := IterateStateDomainChanges(cached, 12, func(*StateDomainChange) (bool, error) { return false, nil }); err != nil {
			t.Fatalf("early stop exposed later error: %v", err)
		}
		if err := IterateStateDomainChanges(cached, 12, func(*StateDomainChange) (bool, error) { return true, nil }); err == nil {
			t.Fatal("corrupt suffix hidden")
		}
	}
	if counted.scans != 1 {
		t.Fatal(counted.scans)
	}
	want := errors.New("callback")
	if err := IterateStateDomainChanges(cached, 12, func(*StateDomainChange) (bool, error) { return false, want }); err != want {
		t.Fatal(err)
	}
	// Force rejection at the budget boundary. Admission may inspect once;
	// subsequent requests stream directly, without repeated failed admission.
	cached.blockBytes = historyQueryBlockBytes
	for i := 0; i < 2; i++ {
		if err := IterateStateDomainChanges(cached, 2, func(*StateDomainChange) (bool, error) { return false, nil }); err != nil {
			t.Fatal(err)
		}
	}
	if !cached.blockRejected[2] || counted.scans != 4 || cached.blocks[2] != nil {
		t.Fatalf("overflow cache admitted/rescanned: scans=%d", counted.scans)
	}
}
