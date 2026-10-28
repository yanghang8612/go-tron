package state

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

type historyMemoPinnedDB struct{ readerDB }

func (historyMemoPinnedDB) IsPinnedKeyValueView() bool { return true }

type historyMemoCountingDB struct {
	readerDB
	iterators int
}

func (*historyMemoCountingDB) IsPinnedKeyValueView() bool { return true }
func (d *historyMemoCountingDB) NewIterator(prefix, start []byte) ethdb.Iterator {
	d.iterators++
	return d.readerDB.NewIterator(prefix, start)
}

type historyMemoCold struct {
	rows         []*rawdb.StateDomainChange
	calls, mixed int
	plans        [][]snapshots.StateHistoryQuery
	err          error
	onMixed      func()
}

func (c *historyMemoCold) IterateStateDomainChanges(uint64, uint64, func(*rawdb.StateDomainChange) (bool, error)) error {
	return errors.New("unexpected full scan")
}
func (c *historyMemoCold) resolve(from, to uint64, queries []snapshots.StateHistoryQuery) []*rawdb.StateDomainChange {
	out := make([]*rawdb.StateDomainChange, len(queries))
	for i, q := range queries {
		for _, row := range c.rows {
			if row.TxNum < from || row.TxNum > to || row.FlatDomain != q.FlatDomain || row.Owner != q.Owner || q.FlatDomain == rawdb.StateFlatDomainKVLatest && (q.Generation != row.Generation || q.Domain != row.Domain || string(q.Key) != string(row.Key)) {
				continue
			}
			if out[i] == nil || stateDomainChangeLess(row, out[i]) {
				out[i] = cloneHistoryDomainChange(row)
			}
		}
	}
	return out
}
func (c *historyMemoCold) FirstStateDomainChangesByKeysContext(_ context.Context, from, to uint64, flat rawdb.StateFlatDomain, owner common.Address, generation uint64, domain kvdomains.KVDomain, keys [][]byte) (map[string]*rawdb.StateDomainChange, error) {
	c.calls++
	if c.err != nil {
		return nil, c.err
	}
	queries := make([]snapshots.StateHistoryQuery, len(keys))
	for i, key := range keys {
		queries[i] = snapshots.StateHistoryQuery{FlatDomain: flat, Owner: owner, Generation: generation, Domain: domain, Key: key}
	}
	out := map[string]*rawdb.StateDomainChange{}
	for i, row := range c.resolve(from, to, queries) {
		if row != nil {
			out[string(keys[i])] = row
		}
	}
	return out, nil
}
func (c *historyMemoCold) FirstStateDomainChangesContext(_ context.Context, from, to uint64, queries []snapshots.StateHistoryQuery) ([]*rawdb.StateDomainChange, error) {
	c.mixed++
	c.plans = append(c.plans, append([]snapshots.StateHistoryQuery(nil), queries...))
	if c.onMixed != nil {
		c.onMixed()
	}
	return c.resolve(from, to, queries), c.err
}

func newHistoryMemoReader(t *testing.T, cold *historyMemoCold, pinned bool) *PersistentHistoryReader {
	t.Helper()
	db := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { db.Close() })
	var source readerDB = db
	if pinned {
		source = historyMemoPinnedDB{db}
	}
	for block := uint64(1); block <= 3; block++ {
		if err := rawdb.WriteStateTxRange(db, block, common.Hash{byte(block)}, block, block); err != nil {
			t.Fatal(err)
		}
	}
	r := NewPersistentHistoryReaderWithColdHistory(source, nil, 3, cold)
	if err := r.SetHotHistoryBlockRange(1, 3); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestHistoryRequestMemoPinsIdentityAbsenceAndOwnedResults(t *testing.T) {
	owner := testAddr(0x71)
	q := snapshots.StateHistoryQuery{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Generation: 7, Domain: kvdomains.SystemReward, Key: []byte("reward")}
	row := &rawdb.StateDomainChange{BlockNum: 2, TxNum: 2, Seq: 1, FlatDomain: q.FlatDomain, Owner: owner, Generation: 7, Domain: q.Domain, Key: []byte("reward"), PrevExists: true, Prev: []byte("old")}
	cold := &historyMemoCold{rows: []*rawdb.StateDomainChange{row}}
	r := newHistoryMemoReader(t, cold, true)
	read := func(q snapshots.StateHistoryQuery, from, to uint64) *rawdb.StateDomainChange {
		t.Helper()
		v, e := r.firstStateDomainChangeByKey(from, to, q.FlatDomain, q.Owner, q.Generation, q.Domain, q.Key)
		if e != nil {
			t.Fatal(e)
		}
		return v
	}
	got := read(q, 1, 3)
	got.Prev[0] = 'X'
	got.Key[0] = 'X'
	if string(read(q, 1, 3).Prev) != "old" || cold.calls != 1 {
		t.Fatalf("memo alias/calls: %d", cold.calls)
	}
	missing := q
	missing.Key = []byte("absent")
	if read(missing, 1, 3) != nil || read(missing, 1, 3) != nil || cold.calls != 2 {
		t.Fatal("negative result was not memoized")
	}
	for _, other := range []snapshots.StateHistoryQuery{{FlatDomain: q.FlatDomain, Owner: owner, Generation: 8, Domain: q.Domain, Key: q.Key}, {FlatDomain: q.FlatDomain, Owner: owner, Generation: 7, Domain: kvdomains.ContractStorage, Key: q.Key}, {FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: owner}} {
		if read(other, 1, 3) != nil {
			t.Fatal("identity leak")
		}
	}
	if read(q, 2, 3) != nil || read(q, 1, 2) == nil || cold.calls != 7 {
		t.Fatalf("transaction cut leak/calls %d", cold.calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r.SetContext(ctx)
	if _, e := r.firstStateDomainChangeByKey(1, 3, q.FlatDomain, q.Owner, q.Generation, q.Domain, q.Key); !errors.Is(e, context.Canceled) {
		t.Fatalf("cached read ignored cancel: %v", e)
	}
	r.SetContext(context.Background())
	if err := r.SetHotHistoryBlockRange(1, 3); err != nil {
		t.Fatal(err)
	}
	read(q, 1, 3)
	if cold.calls != 8 {
		t.Fatal("range change retained memo")
	}
}

func TestHistoryRequestMemoMovingViewAndNewRequestDoNotShareNegatives(t *testing.T) {
	cold := &historyMemoCold{}
	owner := testAddr(0x72)
	for _, pinned := range []bool{false, true} {
		r := newHistoryMemoReader(t, cold, pinned)
		before := cold.calls
		for i := 0; i < 2; i++ {
			if _, e := r.firstStateDomainChangeByKey(1, 3, rawdb.StateFlatDomainAccountLatest, owner, 0, 0, nil); e != nil {
				t.Fatal(e)
			}
		}
		want := 2
		if pinned {
			want = 1
		}
		if cold.calls-before != want {
			t.Fatalf("pinned=%v calls=%d", pinned, cold.calls-before)
		}
	}
	cold.rows = []*rawdb.StateDomainChange{{BlockNum: 2, TxNum: 2, FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: owner, PrevExists: true, Prev: []byte("new request")}}
	r := newHistoryMemoReader(t, cold, true)
	v, e := r.firstStateDomainChangeByKey(1, 3, rawdb.StateFlatDomainAccountLatest, owner, 0, 0, nil)
	if e != nil || v == nil || string(v.Prev) != "new request" {
		t.Fatalf("cross request negative: %v %v", v, e)
	}
}

func TestHistoryRequestMixedPrefetchOverlapErrorsAndBatchReuse(t *testing.T) {
	owner := testAddr(0x73)
	q := snapshots.StateHistoryQuery{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Generation: 9, Domain: kvdomains.SystemReward, Key: []byte("one")}
	coldRow := &rawdb.StateDomainChange{BlockNum: 2, TxNum: 2, Seq: 2, FlatDomain: q.FlatDomain, Owner: owner, Generation: 9, Domain: q.Domain, Key: q.Key, PrevExists: true, Prev: []byte("cold")}
	for _, seq := range []uint64{1, 3} {
		cold := &historyMemoCold{rows: []*rawdb.StateDomainChange{coldRow}}
		r := newHistoryMemoReader(t, cold, true)
		hot := *coldRow
		hot.Seq = seq
		hot.Prev = []byte("hot")
		if e := rawdb.WriteStateDomainChange(r.db.(historyMemoPinnedDB).readerDB.(ethdb.KeyValueWriter), &hot); e != nil {
			t.Fatal(e)
		}
		missing := q
		missing.Key = []byte("missing")
		if e := r.prefetchFirst(1, 3, []snapshots.StateHistoryQuery{q, q, missing}); e != nil {
			t.Fatal(e)
		}
		values, e := r.readStateKVBatchAsOf(owner, 9, q.Domain, [][]byte{q.Key, missing.Key}, 1, 3)
		want := "cold"
		if seq == 1 {
			want = "hot"
		}
		if e != nil || string(values["one"]) != want || len(values) != 1 || cold.calls != 0 || cold.mixed != 1 {
			t.Fatalf("seq %d: values %q err %v calls %d/%d", seq, values, e, cold.calls, cold.mixed)
		}
	}
	cold := &historyMemoCold{rows: []*rawdb.StateDomainChange{coldRow}, err: errors.New("partial cold batch")}
	r := newHistoryMemoReader(t, cold, true)
	if e := r.prefetchFirst(1, 3, []snapshots.StateHistoryQuery{q}); e == nil || len(r.firstMemo) != 0 {
		t.Fatal("published failed batch")
	}
	if _, e := r.firstStateDomainChangeByKey(1, 3, q.FlatDomain, q.Owner, q.Generation, q.Domain, q.Key); e == nil || len(r.firstMemo) != 0 {
		t.Fatal("cached an error")
	}
	cold.err = nil
	ctx, cancel := context.WithCancel(context.Background())
	r.SetContext(ctx)
	cold.onMixed = cancel
	if e := r.prefetchFirst(1, 3, []snapshots.StateHistoryQuery{q}); !errors.Is(e, context.Canceled) || len(r.firstMemo) != 0 {
		t.Fatal("published canceled batch")
	}
}

func TestHistoryRequestPrefetchSharesHotTailWithoutCrossingColdBounds(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	owner := testAddr(0x78)
	query := func(key string, generation uint64) snapshots.StateHistoryQuery {
		return snapshots.StateHistoryQuery{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Generation: generation, Domain: kvdomains.SystemReward, Key: []byte(key)}
	}
	queries := []snapshots.StateHistoryQuery{query("early", 1), query("late", 1), query("hot", 1), query("absent", 1), query("hot", 2)}
	row := func(q snapshots.StateHistoryQuery, block, seq uint64, value string) *rawdb.StateDomainChange {
		return &rawdb.StateDomainChange{BlockNum: block, TxNum: block, Seq: seq, FlatDomain: q.FlatDomain, Owner: q.Owner, Generation: q.Generation, Domain: q.Domain, Key: q.Key, PrevExists: true, Prev: []byte(value)}
	}
	for _, change := range []*rawdb.StateDomainChange{row(queries[0], 2, 1, "earlier-hot-seq"), row(queries[1], 3, 1, "later-hot-tx"), row(queries[2], 3, 2, "hot-only"), row(queries[4], 3, 3, "other-generation")} {
		if err := rawdb.WriteStateDomainChange(db, change); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageStateHistoryIndex, 1, common.Hash{1}); err != nil {
		t.Fatal(err)
	}
	cold := &historyMemoCold{rows: []*rawdb.StateDomainChange{row(queries[0], 2, 2, "later-cold-seq"), row(queries[1], 2, 2, "earlier-cold-tx")}}
	counted := &historyMemoCountingDB{readerDB: db}
	r := NewPersistentHistoryReaderWithColdHistory(counted, nil, 3, cold)
	if err := r.SetHotHistoryBlockRange(1, 3); err != nil {
		t.Fatal(err)
	}
	if err := r.prefetchFirst(1, 3, queries); err != nil {
		t.Fatal(err)
	}
	// Two equal cold bounds share a pass; hot-only and absent keys share a
	// second pass. The other generation has its own pass and identity.
	if counted.iterators != 3 {
		t.Fatalf("hot owner-tail passes = %d, want 3", counted.iterators)
	}
	for i, want := range []string{"earlier-hot-seq", "earlier-cold-tx", "hot-only", "", "other-generation"} {
		got, ok := r.memoFirst(1, 3, queries[i])
		if !ok || (got == nil) != (want == "") || got != nil && string(got.Prev) != want {
			t.Fatalf("query %d: row=%+v cached=%v, want %q", i, got, ok, want)
		}
	}
}

func TestHistoryRequestMemoBoundsRetainedRowsAndPayload(t *testing.T) {
	r := newHistoryMemoReader(t, &historyMemoCold{}, true)
	for i := 0; i < historyFirstMemoEntries+10; i++ {
		r.rememberFirst(1, 3, snapshots.StateHistoryQuery{FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: testAddr(0x76), Domain: kvdomains.ContractStorage, Key: []byte(fmt.Sprint(i))}, nil)
	}
	if len(r.firstMemo) != historyFirstMemoEntries || r.firstMemoBytes > historyFirstMemoBytes {
		t.Fatal("unbounded entries")
	}
	r.firstMemo = nil
	r.firstMemoBytes = 0
	q := snapshots.StateHistoryQuery{FlatDomain: rawdb.StateFlatDomainAccountLatest}
	r.rememberFirst(1, 3, q, &rawdb.StateDomainChange{Prev: []byte(strings.Repeat("x", historyFirstMemoBytes))})
	if len(r.firstMemo) != 0 || r.firstMemoBytes != 0 {
		t.Fatal("retained oversized value")
	}
}

func TestHistoryExecutionPrefetchUsesHistoricalGenerationAndRetriesFaults(t *testing.T) {
	caller, contract := testAddr(0x74), testAddr(0x75)
	envelope := func(gen uint64) []byte {
		return encodeStateAccountV2Fields(StateAccountVersion, []byte{1}, EmptyKVRoot, gen, common.Hash{})
	}
	cold := &historyMemoCold{rows: []*rawdb.StateDomainChange{{BlockNum: 2, TxNum: 2, FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: contract, PrevExists: true, Prev: envelope(7)}}}
	r := newHistoryMemoReader(t, cold, true)
	r.latest = &recordingHotStateLatestReader{account: map[common.Address][]byte{contract: envelope(9), common.SystemAccountAddress: envelope(4)}}
	if err := r.PrefetchExecutionAt(1, caller, contract, []int32{27, 35}); err != nil {
		t.Fatal(err)
	}
	if cold.mixed != 2 {
		t.Fatalf("plans %d", cold.mixed)
	}
	meta, forks := 0, 0
	for _, q := range cold.plans[1] {
		if q.Domain == kvdomains.ContractMetadata {
			meta++
			if q.Generation != 7 || q.Owner != contract {
				t.Fatal("used latest contract generation")
			}
		}
		if q.Domain == kvdomains.SystemForkVote {
			forks++
			if q.Generation != 4 || q.Owner != common.SystemAccountAddress {
				t.Fatal("system identity lost")
			}
		}
	}
	if meta != 1 || forks != 2 {
		t.Fatalf("metadata/forks %d/%d", meta, forks)
	}
	if _, _, err := r.readStateKVAsOf(contract, 7, kvdomains.ContractMetadata, contractMetaKVKey, 1, 3); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.readStateAccountLatestAsOf(caller, 1, 3); err != nil {
		t.Fatal(err)
	}
	if cold.calls != 0 {
		t.Fatal("prefetched negative retraversed cold history")
	}
	cold.err = errors.New("speculative failure")
	fresh := newHistoryMemoReader(t, cold, true)
	if err := fresh.PrefetchExecutionAt(1, caller, contract, []int32{27, 35}); err != nil || len(fresh.firstMemo) != 0 {
		t.Fatal("speculative error became authoritative")
	}
	if _, _, err := fresh.readStateAccountLatestAsOf(contract, 1, 3); err == nil {
		t.Fatal("demand read swallowed the failure")
	}
}

type historyMemoManifestCold struct {
	*historyMemoCold
	manifest *snapshots.Manifest
}

func (c *historyMemoManifestCold) Manifest() *snapshots.Manifest { return c.manifest }

func TestHistoryExecutionPrefetchBatchesFullyIndexedHotSpan(t *testing.T) {
	cold := &historyMemoCold{}
	r := newHistoryMemoReader(t, cold, true)
	r.coldHistory = &historyMemoManifestCold{historyMemoCold: cold}
	contract := testAddr(0x7b)
	envelope := func(gen uint64) []byte {
		return encodeStateAccountV2Fields(StateAccountVersion, []byte{1}, EmptyKVRoot, gen, common.Hash{})
	}
	r.latest = &recordingHotStateLatestReader{account: map[common.Address][]byte{common.SystemAccountAddress: envelope(4), contract: envelope(7)}}
	if err := r.PrefetchExecutionAt(2, common.Address{}, contract, nil); err != nil {
		t.Fatal(err)
	}
	if cold.mixed != 0 {
		t.Fatal("unindexed hot-only hint unexpectedly performed reads")
	}
	db := r.db.(historyMemoPinnedDB).readerDB.(ethdb.KeyValueWriter)
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageStateHistoryIndex, 3, common.Hash{3}); err != nil {
		t.Fatal(err)
	}
	if err := r.PrefetchExecutionAt(2, common.Address{}, contract, nil); err != nil {
		t.Fatal(err)
	}
	if cold.mixed != 2 || len(r.firstMemo) == 0 {
		t.Fatalf("fully indexed span not batched: plans=%d memo=%d", cold.mixed, len(r.firstMemo))
	}
}

func TestHistoryExecutionPrefetchRangeHintDoesNotProveAbsence(t *testing.T) {
	owner := testAddr(0x79)
	for _, end := range []uint64{0, 1, 2} {
		t.Run(fmt.Sprint(end), func(t *testing.T) {
			row := &rawdb.StateDomainChange{BlockNum: 2, TxNum: 2, FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: owner, PrevExists: true, Prev: []byte("demand-authoritative")}
			cold := &historyMemoCold{rows: []*rawdb.StateDomainChange{row}}
			r := newHistoryMemoReader(t, cold, true)
			hint := &historyMemoManifestCold{historyMemoCold: cold}
			if end != 0 {
				hint.manifest = &snapshots.Manifest{Segments: []snapshots.SegmentRef{{Dataset: snapshots.SegmentDatasetStateDomainChange, Kind: snapshots.SegmentHistory, FromTxNum: 1, ToTxNum: end}}}
			}
			r.coldHistory = hint
			if err := r.PrefetchExecutionAt(1, owner, testAddr(0x7a), nil); err != nil {
				t.Fatal(err)
			}
			if end <= 1 && (cold.mixed != 0 || len(r.firstMemo) != 0) {
				t.Fatal("out-of-range plan performed reads or cached an absence")
			}
			got, err := r.firstStateDomainChangeByKey(1, 3, row.FlatDomain, owner, 0, 0, nil)
			if err != nil || got == nil || string(got.Prev) != "demand-authoritative" {
				t.Fatalf("manifest hint replaced demand truth: row=%+v err=%v", got, err)
			}
			if end <= 1 && cold.calls != 1 || end == 2 && cold.calls != 0 {
				t.Fatalf("hint end %d: demand/mixed calls %d/%d", end, cold.calls, cold.mixed)
			}
		})
	}
}
