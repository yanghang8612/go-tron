package rawdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

type queryTailCountView struct {
	StateHistoryReadView
	scans int
}

func (v *queryTailCountView) NewIterator(prefix, start []byte) ethdb.Iterator {
	if bytes.Equal(prefix, stateChangeSetPrefix) {
		v.scans++
	}
	return v.StateHistoryReadView.NewIterator(prefix, start)
}

func queryTailFixture(t *testing.T) (*historyViewTestFactory, common.Address) {
	t.Helper()
	f := &historyViewTestFactory{KeyValueStore: NewMemoryDatabase()}
	t.Cleanup(func() { f.KeyValueStore.Close() })
	owner := common.Address{0x41, 0x22}
	other := common.Address{0x41, 0x33}
	rng := rand.New(rand.NewSource(765))
	for block := uint64(2); block <= 9; block++ {
		var rows []*StateDomainChange
		for seq := uint64(1); seq <= 64; seq++ {
			who := owner
			if seq%3 == 0 {
				who = other
			}
			rows = append(rows, &StateDomainChange{BlockNum: block, Seq: seq, TxNum: block*100 + seq/4, FlatDomain: StateFlatDomain(1 + seq%3), Owner: who, Generation: seq % 2, Domain: kvdomains.ContractStorage, Key: []byte{byte(rng.Intn(12))}, PrevExists: seq%2 == 0, Prev: bytes.Repeat([]byte{byte(seq)}, 50)})
		}
		for _, row := range rows {
			if row.FlatDomain == StateFlatDomainKVGeneration {
				row.Prev = make([]byte, 8)
			}
		}
		if err := WriteStateDomainChangeBlockRows(f, rows); err != nil {
			t.Fatal(err)
		}
		if block%2 == 0 {
			writeHistoryReadViewSharedFixture(t, f.KeyValueStore, block)
		}
		// Replace one matching row by a different owner and vice versa, plus
		// append beyond the packed sequence range.
		for _, seq := range []uint64{2, 3, 70} {
			row := cloneStateDomainChange(rows[0])
			row.Seq = seq
			row.TxNum = block*100 + seq/4
			if seq == 2 {
				row.Owner = other
			}
			row.Prev = []byte("repair")
			row.Key = []byte{byte(seq)}
			if err := WriteStateDomainChangeRow(f, row); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := WriteStateDomainChangeRow(f, &StateDomainChange{BlockNum: 10, TxNum: 1010, Seq: 0, FlatDomain: StateFlatDomainKVLatest, Owner: owner, Domain: kvdomains.ContractStorage, Key: []byte("legacy-zero"), PrevExists: true, Prev: []byte("zero")}); err != nil {
		t.Fatal(err)
	}
	if err := WriteStageProgressWithHash(f, StageStateHistoryIndex, 1, common.Hash{1}); err != nil {
		t.Fatal(err)
	}
	return f, owner
}

func TestQueryTailOwnerScanMatchesLogicalOracle(t *testing.T) {
	f, owner := queryTailFixture(t)
	view, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	for from := uint64(1); from <= 10; from++ {
		for to := from; to <= 10; to++ {
			var want, got []*StateDomainChange
			if err := IterateStateDomainChangesByBlockRange(view, from, to, func(row *StateDomainChange) (bool, error) {
				if row.Owner == owner {
					want = append(want, cloneStateDomainChange(row))
				}
				return true, nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := iterateStateHistoryOwnerTail(context.Background(), view, from, to, owner, func(row *StateDomainChange) (bool, error) {
				got = append(got, cloneStateDomainChange(row))
				return true, nil
			}); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("range [%d,%d] oracle mismatch", from, to)
			}
		}
	}
}

func TestQueryTailCachePinsBoundsTxSequenceAndOwnership(t *testing.T) {
	f, owner := queryTailFixture(t)
	view, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	counted := &queryTailCountView{StateHistoryReadView: view}
	cached := WithStateHistoryQueryTailCache(counted)
	// Build once across the whole range, then narrow blocks, transaction cuts,
	// generation, domain and key. Include misses and same-transaction sequences.
	var observed []*StateDomainChange
	if err := iterateStateHistoryTailByOwner(context.Background(), cached, 2, 9, owner, func(row *StateDomainChange) (bool, error) { observed = append(observed, row); return true, nil }); err != nil {
		t.Fatal(err)
	}
	for _, row := range observed {
		if len(row.Prev) > 0 {
			row.Prev[0] ^= 0xff
		}
		row.Seq = 999
	}
	rng := rand.New(rand.NewSource(987))
	for i := 0; i < 250; i++ {
		from := uint64(1 + rng.Intn(8))
		to := from + 1 + uint64(rng.Intn(int(9-from)))
		targetTx := from*100 + uint64(rng.Intn(16))
		headTx := to*100 + uint64(rng.Intn(18))
		flat := StateFlatDomain(1 + rng.Intn(3))
		gen := uint64(rng.Intn(2))
		key := []byte{byte(rng.Intn(14))}
		var want *StateDomainChange
		err := IterateStateDomainChangesByBlockRange(view, from+1, to, func(row *StateDomainChange) (bool, error) {
			if stateDomainChangeInTxWindow(row, targetTx, headTx) && stateDomainChangeMatchesKey(row, flat, owner, gen, kvdomains.ContractStorage, key) {
				want = cloneStateDomainChange(row)
				return false, nil
			}
			return true, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		got, err := ReadFirstStateDomainChangeByKeyBlockRange(cached, from, to, targetTx, headTx, flat, owner, gen, kvdomains.ContractStorage, key)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("iteration %d got=%+v want=%+v err=%v", i, got, want, err)
		}
	}
	if counted.scans != 1 {
		t.Fatalf("tail rescanned %d times, want one", counted.scans)
	}
	// A source mutation/reorg cannot alter a cached or uncached pinned read.
	if err := DeleteStateDomainChanges(f, 9); err != nil {
		t.Fatal(err)
	}
	var got []*StateDomainChange
	if err := iterateStateHistoryTailByOwner(context.Background(), cached, 9, 9, owner, func(row *StateDomainChange) (bool, error) { got = append(got, row); return true, nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 {
		t.Fatal("pinned rows lost after source change")
	}
	if WithStateHistoryQueryTailCache(f) != f {
		t.Fatal("moving source was cached")
	}
}

func TestQueryTailCacheBudgetCancellationAndCorruption(t *testing.T) {
	f, owner := queryTailFixture(t)
	view, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	cache := WithStateHistoryQueryTailCache(view).(*stateHistoryQueryTailView)
	cache.budget = 129
	cache.maxRows = 1
	calls := 0
	if err := iterateStateHistoryTailByOwner(context.Background(), cache, 2, 9, owner, func(row *StateDomainChange) (bool, error) { calls++; return false, nil }); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || len(cache.entries) != 0 || cache.used != 0 {
		t.Fatalf("partial cache escaped: %d/%d/%d", calls, len(cache.entries), cache.used)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := iterateStateHistoryTailByOwner(ctx, cache, 2, 9, owner, func(*StateDomainChange) (bool, error) { t.Fatal("canceled callback"); return true, nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
	// A corrupt shared pack must fail even for an owner absent from the pack.
	if err := f.Put(stateChangeSetKey(2, 0), append(append([]byte(nil), stateDomainChangeBlockEnvelopeMagic[:]...), stateDomainChangeBlockSharedVersion, 0xff)); err != nil {
		t.Fatal(err)
	}
	bad, badRelease, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer badRelease()
	badCache := WithStateHistoryQueryTailCache(bad).(*stateHistoryQueryTailView)
	if err := iterateStateHistoryTailByOwner(context.Background(), badCache, 2, 9, common.Address{9}, func(*StateDomainChange) (bool, error) { return true, nil }); err == nil {
		t.Fatal("corruption became absence")
	}
	if len(badCache.entries) != 0 {
		t.Fatal("failed scan cached")
	}
}

func TestQueryTailCacheConcurrentReuse(t *testing.T) {
	f, owner := queryTailFixture(t)
	view, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	counted := &queryTailCountView{StateHistoryReadView: view}
	cache := WithStateHistoryQueryTailCache(counted)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := iterateStateHistoryTailByOwner(context.Background(), cache, 2, 9, owner, func(row *StateDomainChange) (bool, error) { row.Key[0] = 99; return true, nil }); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if counted.scans != 1 {
		t.Fatalf("scans=%d", counted.scans)
	}
}

func BenchmarkQueryTailDensePointReads(b *testing.B) {
	db := NewMemoryDatabase()
	defer db.Close()
	const blocks = 128
	for block := uint64(2); block <= blocks+1; block++ {
		rows := make([]*StateDomainChange, 4096)
		for i := range rows {
			owner := common.Address{0x41, byte(i%200 + 1)}
			rows[i] = &StateDomainChange{BlockNum: block, Seq: uint64(i + 1), TxNum: block*4096 + uint64(i), FlatDomain: StateFlatDomainKVLatest, Owner: owner, Generation: 1, Domain: kvdomains.ContractStorage, Key: []byte(fmt.Sprint(i)), PrevExists: true, Prev: bytes.Repeat([]byte{byte(i)}, 32)}
		}
		if err := WriteStateDomainChangeBlockRows(db, rows); err != nil {
			b.Fatal(err)
		}
	}
	owner := common.Address{0x41, 0xfe} // unchanged metadata/account owner, all misses
	base := &stateHistoryReadView{reader: db, iteratee: db, pinned: true}
	for _, mode := range []string{"owning-oracle", "borrowed", "query-cache", "early-owning-oracle", "early-borrowed", "early-query-cache"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				early := strings.HasPrefix(mode, "early-")
				kind := strings.TrimPrefix(mode, "early-")
				source := StateKVHistoryReader(base)
				if kind == "query-cache" {
					source = WithStateHistoryQueryTailCache(base)
				}
				for read := 0; read < 8; read++ {
					visit := func(row *StateDomainChange) (bool, error) { return !early, nil }
					var err error
					if kind == "owning-oracle" {
						err = IterateStateDomainChangesByBlockRangeContext(context.Background(), source, 2, blocks+1, visit)
					} else {
						queryOwner := owner
						if early {
							queryOwner = common.Address{0x41, 1}
						}
						err = iterateStateHistoryTailByOwner(context.Background(), source, 2, blocks+1, queryOwner, visit)
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

type queryTailCancelView struct {
	StateHistoryReadView
	cancel context.CancelFunc
}

func (v *queryTailCancelView) NewIterator(prefix, start []byte) ethdb.Iterator {
	return &queryTailCancelIterator{Iterator: v.StateHistoryReadView.NewIterator(prefix, start), cancel: v.cancel}
}

type queryTailCancelIterator struct {
	ethdb.Iterator
	cancel context.CancelFunc
	moves  int
}

func (it *queryTailCancelIterator) Next() bool {
	ok := it.Iterator.Next()
	it.moves++
	if it.moves == 3 {
		it.cancel()
	}
	return ok
}
func TestQueryTailCanceledBuildAndNegativeIsolation(t *testing.T) {
	f, owner := queryTailFixture(t)
	view, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache := WithStateHistoryQueryTailCache(&queryTailCancelView{StateHistoryReadView: view, cancel: cancel}).(*stateHistoryQueryTailView)
	if err := iterateStateHistoryTailByOwner(ctx, cache, 2, 9, owner, func(*StateDomainChange) (bool, error) { t.Fatal("incomplete scan published rows"); return true, nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-scan cancel: %v", err)
	}
	if len(cache.entries) != 0 {
		t.Fatal("partial canceled entry retained")
	}
	missing := common.Address{0x41, 0xfe}
	cache = WithStateHistoryQueryTailCache(view).(*stateHistoryQueryTailView)
	empty := func(*StateDomainChange) (bool, error) { t.Fatal("expected pinned absence"); return true, nil }
	if err := iterateStateHistoryTailByOwner(context.Background(), cache, 2, 9, missing, empty); err != nil {
		t.Fatal(err)
	}
	row := &StateDomainChange{BlockNum: 9, Seq: 80, TxNum: 980, FlatDomain: StateFlatDomainKVLatest, Owner: missing, Domain: kvdomains.ContractStorage, Key: []byte("new"), PrevExists: true, Prev: []byte("created")}
	if err := WriteStateDomainChangeRow(f, row); err != nil {
		t.Fatal(err)
	}
	if err := iterateStateHistoryTailByOwner(context.Background(), cache, 2, 9, missing, empty); err != nil {
		t.Fatal(err)
	}
	fresh, closeFresh, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFresh()
	count := 0
	if err := iterateStateHistoryTailByOwner(context.Background(), WithStateHistoryQueryTailCache(fresh), 2, 9, missing, func(*StateDomainChange) (bool, error) { count++; return true, nil }); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("new snapshot inherited stale negative: %d", count)
	}
	for n := 0; n < 20; n++ {
		if err := iterateStateHistoryTailByOwner(context.Background(), cache, 2, 9, common.Address{0x41, byte(n + 100)}, func(*StateDomainChange) (bool, error) { return true, nil }); err != nil {
			t.Fatal(err)
		}
	}
	if len(cache.entries) > stateHistoryQueryTailOwners || cache.used > cache.budget || cache.rows > cache.maxRows {
		t.Fatalf("cache limits: %d/%d/%d", len(cache.entries), cache.used, cache.rows)
	}
}

type queryTailMoveView struct {
	StateHistoryReadView
	moves int
}

func (v *queryTailMoveView) NewIterator(prefix, start []byte) ethdb.Iterator {
	return &queryTailMoveIterator{Iterator: v.StateHistoryReadView.NewIterator(prefix, start), view: v}
}

type queryTailMoveIterator struct {
	ethdb.Iterator
	view *queryTailMoveView
}

func (it *queryTailMoveIterator) Next() bool { it.view.moves++; return it.Iterator.Next() }
func TestQueryTailEarlyHitDoesNotFillEntireRange(t *testing.T) {
	f, owner := queryTailFixture(t)
	view, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	base := &queryTailMoveView{StateHistoryReadView: view}
	cached := WithStateHistoryQueryTailCache(base).(*stateHistoryQueryTailView)
	calls := 0
	if err := iterateStateHistoryTailByOwner(context.Background(), cached, 2, 9, owner, func(row *StateDomainChange) (bool, error) {
		calls++
		if row.BlockNum != 2 {
			t.Fatalf("first hit: %d", row.BlockNum)
		}
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || base.moves > 5 || len(cached.entries) != 0 {
		t.Fatalf("positive query scanned ahead or cached partial range: calls=%d moves=%d entries=%d", calls, base.moves, len(cached.entries))
	}
	// With a tiny budget an exhaustive query must still use just one scan.
	cached.budget = 129
	base.moves = 0
	if err := iterateStateHistoryTailByOwner(context.Background(), cached, 2, 9, owner, func(*StateDomainChange) (bool, error) { return true, nil }); err != nil {
		t.Fatal(err)
	}
	firstMoves := base.moves
	base.moves = 0
	if err := iterateStateHistoryTailByOwner(context.Background(), cached, 2, 9, owner, func(*StateDomainChange) (bool, error) { return true, nil }); err != nil {
		t.Fatal(err)
	}
	if base.moves != firstMoves || !cached.rejected[owner] {
		t.Fatalf("budget fallback rescanned: first=%d next=%d", firstMoves, base.moves)
	}
}
