package rawdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"unsafe"

	"github.com/ethereum/go-ethereum/ethdb"
)

func chunkCacheTestView(t *testing.T, db any, budget uint64, entries int) (*stateHistoryChunkCachedView, *StateHistoryChunkCache, *pipelineTestView) {
	t.Helper()
	base, release, err := AcquireStateHistoryReadView(db)
	if err != nil {
		t.Fatal(err)
	}
	observed := &pipelineTestView{StateHistoryReadView: base}
	c := newStateHistoryChunkCache(budget, entries, release)
	t.Cleanup(func() { _ = c.Close() })
	return &stateHistoryChunkCachedView{StateHistoryReadView: observed, cache: c, ctx: context.Background()}, c, observed
}

func TestStateHistoryChunkCacheDefaultFrozenReadOracle(t *testing.T) {
	for _, mode := range []string{"shared", "snappy-shared", "missing", "bad-chunk", "chunk-hash", "bad-digest", "bad-header"} {
		t.Run(mode, func(t *testing.T) {
			db, _ := pipelineFixture(t, mode)
			pack, err := db.Get(stateChangeSetKey(1, 0))
			if err != nil {
				t.Fatal(err)
			}
			base, release, err := AcquireStateHistoryReadView(db)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			a, b := &pipelineTestView{StateHistoryReadView: base}, &pipelineTestView{StateHistoryReadView: base}
			want, we := frozen5148DecodeStateHistorySharedPack(a, pack, 1)
			got, ge := decodeStateHistorySharedPack(b, pack, 1)
			if !bytes.Equal(want, got) || fmt.Sprint(we) != fmt.Sprint(ge) || !reflect.DeepEqual(a.trace, b.trace) {
				t.Fatalf("default changed: errors=%v/%v traces=%v/%v", we, ge, a.trace, b.trace)
			}
			v, c, o := chunkCacheTestView(t, db, StateHistoryChunkCachePayloadBudget, StateHistoryChunkCacheEntryLimit)
			got, ge = decodeStateHistorySharedPack(v, pack, 1)
			if !bytes.Equal(want, got) || fmt.Sprint(we) != fmt.Sprint(ge) || !reflect.DeepEqual(a.trace, o.trace) {
				t.Fatalf("first cache miss changed original validation: %v/%v", we, ge)
			}
			if mode == "missing" || mode == "bad-chunk" || mode == "chunk-hash" || mode == "bad-header" {
				if s := c.Stats(); s.Entries != 0 || s.PayloadBytes != 0 || s.Inserts != 0 {
					t.Fatalf("cached failure: %+v", s)
				}
				_, again := decodeStateHistorySharedPack(v, pack, 1)
				if fmt.Sprint(again) != fmt.Sprint(we) {
					t.Fatal("negative cache changed error", again)
				}
				if mode != "bad-header" && c.Stats().Misses != 2 {
					t.Fatal("did not retry failed miss")
				}
			}
		})
	}
}

func TestStateHistoryChunkCacheOutputOwnershipAndWholePackSHA(t *testing.T) {
	db, _ := pipelineFixture(t, "shared")
	pack, _ := db.Get(stateChangeSetKey(1, 0))
	v, c, observed := chunkCacheTestView(t, db, StateHistoryChunkCachePayloadBudget, StateHistoryChunkCacheEntryLimit)
	first, err := decodeStateHistorySharedPack(v, pack, 1)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Clone(first)
	first[0] ^= 0xff
	reads := len(observed.trace)
	second, err := decodeStateHistorySharedPack(v, pack, 1)
	if err != nil || !bytes.Equal(second, want) || bytes.Equal(first, second) {
		t.Fatal("cache aliases returned output", err)
	}
	if c.Stats().Hits == 0 || len(observed.trace) != reads {
		t.Fatal("second read did not reuse authenticated chunk")
	}
	bad := bytes.Clone(pack)
	pos := len(stateDomainChangeBlockEnvelopeMagic) + 1
	_, n := binary.Uvarint(bad[pos:])
	pos += n
	_, n = binary.Uvarint(bad[pos:])
	pos += n
	bad[pos] ^= 1
	for i := 0; i < 2; i++ {
		if _, err := decodeStateHistorySharedPack(v, bad, 1); err == nil || err.Error() != "rawdb: shared history pack hash mismatch" {
			t.Fatalf("cache hid complete pack SHA: %v", err)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if s := c.Stats(); !s.Closed || s.PayloadBytes != 0 || s.Entries != 0 || s.PeakPayloadBytes == 0 {
		t.Fatalf("close accounting %+v", s)
	}
	if !bytes.Equal(second, want) {
		t.Fatal("output changed after cache close")
	}
}

func TestStateHistoryChunkCacheBucketLengthAndEviction(t *testing.T) {
	db, err := NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	raw := bytes.Repeat([]byte{7}, 32)
	pack, values := ownedReadBenchmarkPack(raw, 1, false)
	for k, value := range values {
		if err := db.Put([]byte(k), value); err != nil {
			t.Fatal(err)
		}
	}
	other, otherValues := ownedReadBenchmarkPack(bytes.Repeat([]byte{8}, 32), 2, false)
	for k, value := range otherValues {
		if err := db.Put([]byte(k), value); err != nil {
			t.Fatal(err)
		}
	}
	v, c, o := chunkCacheTestView(t, db, 32, 1)
	if _, err := decodeStateHistorySharedPack(v, pack, 1); err != nil {
		t.Fatal(err)
	}
	// Same SHA/want in a different physical bucket must still read that bucket.
	differentBucket, _ := ownedReadBenchmarkPack(raw, StateHistoryChunkBucketBlocks+1, false)
	if _, err := decodeStateHistorySharedPack(v, differentBucket, StateHistoryChunkBucketBlocks+1); err == nil {
		t.Fatal("cache crossed physical buckets")
	}
	// Same physical hash with a forged reference length must not reuse the
	// differently sized entry; retain original codec-size validation failure.
	forged := bytes.Clone(pack)
	p := len(stateDomainChangeBlockEnvelopeMagic) + 1
	_, n := binary.Uvarint(forged[p:])
	p += n
	size, n := binary.Uvarint(forged[p:])
	if binary.PutUvarint(forged[p:], size-1) != n {
		t.Fatal("fixture varint width")
	}
	p += n + 32
	_, n = binary.Uvarint(forged[p:])
	p += n
	if binary.PutUvarint(forged[p:], size-1) != n {
		t.Fatal("fixture reference width")
	}
	_, wantErr := frozen5148DecodeStateHistorySharedPack(o, forged, 1)
	if _, err := decodeStateHistorySharedPack(v, forged, 1); wantErr == nil || fmt.Sprint(err) != fmt.Sprint(wantErr) {
		t.Fatalf("forged want accepted: %v/%v", err, wantErr)
	}
	if s := c.Stats(); s.Inserts != 1 || s.Entries != 1 {
		t.Fatal("failed reference admitted", s)
	}
	if _, err := decodeStateHistorySharedPack(v, other, 2); err != nil {
		t.Fatal(err)
	}
	misses := c.Stats().Misses
	if _, err := decodeStateHistorySharedPack(v, pack, 1); err != nil {
		t.Fatal(err)
	}
	if s := c.Stats(); s.Evictions < 2 || s.Misses != misses+1 || s.PeakPayloadBytes > 32 || s.PeakEntries > 1 {
		t.Fatalf("eviction did not reauthenticate: %+v", s)
	}
	// A payload larger than the private test budget falls back without clone.
	v2, c2, _ := chunkCacheTestView(t, db, 16, 2)
	for i := 0; i < 2; i++ {
		if _, err := decodeStateHistorySharedPack(v2, pack, 1); err != nil {
			t.Fatal(err)
		}
	}
	if s := c2.Stats(); s.Hits != 0 || s.Inserts != 0 || s.Entries != 0 || s.Misses != 2 {
		t.Fatal("oversize bypass accounting", s)
	}
}

func TestStateHistoryChunkCacheTwoPassCallbacksAndPinnedSource(t *testing.T) {
	db, from, to, _ := ownedReadBenchmarkFixture(t, "HighReuseRaw")
	base, release, err := AcquireStateHistoryReadView(db)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var want []*StateDomainChange
	if err := frozen3fdIterateHistoryRange(base, from, to, from, to, func(row *StateDomainChange) (bool, error) {
		want = append(want, cloneStateDomainChange(row))
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	view, c, err := AcquireStateHistoryChunkCacheView(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// A later DB mutation must affect neither this cache's misses nor its hits.
	if err := db.Delete(stateChangeSetKey(from, 0)); err != nil {
		t.Fatal(err)
	}
	for _, workers := range []int{2, 4, 8} {
		var got []*StateDomainChange
		err := IterateStateDomainChangesByBlockTxRangePipelinedWithWorkers(context.Background(), view, from, to, from, to, workers, func(row *StateDomainChange) (bool, error) {
			got = append(got, cloneStateDomainChange(row))
			row.Prev[0] ^= 0xff
			return true, nil
		})
		if err != nil || !reflect.DeepEqual(want, got) {
			t.Fatalf("callbacks mutated cache or pinned source: workers=%d %v", workers, err)
		}
	}
	if s := c.Stats(); s.Hits == 0 || s.Misses == 0 || s.PeakEntries > StateHistoryChunkCacheEntryLimit || s.PeakPayloadBytes > StateHistoryChunkCachePayloadBudget {
		t.Fatal("cache bounds or exercise", s)
	}
	borrowed, borrowedRelease, err := AcquireStateHistoryReadView(view)
	if err != nil || borrowed != view {
		t.Fatal("nested acquire unwrapped cache", err)
	}
	if err := borrowedRelease(); err != nil || c.Stats().Closed {
		t.Fatal("nested borrower closed cache", err)
	}
}

func TestStateHistoryChunkCacheAuditedSourceAndCloseOwnership(t *testing.T) {
	db, _ := pipelineFixture(t, "shared")
	base, release, err := AcquireStateHistoryReadView(db)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	for _, source := range []any{struct{ StateHistoryReadView }{base}, pipelineFalseView{base, true, false}, pipelineFalseView{base, false, true}, pipelinePresenceView{&pipelineTestView{StateHistoryReadView: base}}, struct{ ethdb.KeyValueStore }{db}} {
		if v, c, err := AcquireStateHistoryChunkCacheView(context.Background(), source); !errors.Is(err, ErrStateHistoryPipelineView) || v != nil || c != nil {
			t.Fatalf("unaudited source accepted: %T %v", source, err)
		}
	}
	ownedBase, ownedRelease, err := AcquireStateHistoryReadView(db)
	if err != nil {
		t.Fatal(err)
	}
	tracked := &pipelineClosingView{pipelineTestView: &pipelineTestView{StateHistoryReadView: ownedBase}, closeFn: ownedRelease}
	v, c, err := AcquireStateHistoryChunkCacheView(context.Background(), &pipelineFactory{Iteratee: db, snapshot: tracked})
	if err != nil {
		t.Fatal(err)
	}
	if _, nested, err := AcquireStateHistoryChunkCacheView(context.Background(), v); err == nil || nested != nil {
		t.Fatal("nested build cache accepted")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if tracked.closes.Load() != 1 || v.IsPinnedKeyValueView() {
		t.Fatal("cache/snapshot close ownership")
	}
	if _, err := v.Get([]byte("unused")); !errors.Is(err, ErrStateHistoryChunkCacheClosed) {
		t.Fatal("closed view read", err)
	}
	if s := c.Stats(); !s.Closed || s.Entries != 0 || s.PayloadBytes != 0 {
		t.Fatal("closed payload survived", s)
	}
	if unsafe.Sizeof(historyChunkCacheEntry{}) > 80 {
		t.Fatal("re-audit fixed table memory charge")
	}
}

func TestStateHistoryChunkCacheConcurrentMissesAndCanceledHits(t *testing.T) {
	db, keys := pipelineFixture(t, "shared")
	v, c, o := chunkCacheTestView(t, db, StateHistoryChunkCachePayloadBudget, StateHistoryChunkCacheEntryLimit)
	pack, _ := db.Get(stateChangeSetKey(1, 0))
	const workers = 8
	arrived, allow := make(chan struct{}, workers), make(chan struct{})
	o.hook = func(op string, key []byte) error {
		if op == "Get" && bytes.Equal(key, keys[0]) {
			arrived <- struct{}{}
			<-allow
		}
		return nil
	}
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := decodeStateHistorySharedPack(v, pack, 1); errs <- err }()
	}
	for i := 0; i < workers; i++ {
		pipelineAwait(t, arrived)
	}
	close(allow)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if s := c.Stats(); s.Inserts != 1 || s.Entries != 1 || s.Misses != workers || s.Hits != 0 || s.PayloadBytes > StateHistoryChunkCachePayloadBudget {
		t.Fatalf("double misses cloned extra payload: %+v", s)
	}
	before := c.Stats()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reader := historyPipelineReader{StateHistoryReadView: v, ctx: ctx}
	if _, err := decodeStateHistorySharedPack(reader, pack, 1); !errors.Is(err, context.Canceled) {
		t.Fatal("cache hit bypassed job cancellation", err)
	}
	if after := c.Stats(); after.Hits != before.Hits || after.Misses != before.Misses {
		t.Fatal("canceled job reached cache", after)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if s := c.Stats(); !s.Closed || s.Entries != 0 || s.PayloadBytes != 0 {
		t.Fatal("joined concurrent payload not cleared", s)
	}
}

func TestStateHistoryChunkCacheCancellationJoinsBeforeClose(t *testing.T) {
	db, keys := pipelineFixture(t, "shared", "shared", "shared", "shared")
	base, release, err := AcquireStateHistoryReadView(db)
	if err != nil {
		t.Fatal(err)
	}
	started, allow := make(chan struct{}), make(chan struct{})
	var once sync.Once
	closeFailure := errors.New("snapshot release failure")
	tracked := &pipelineClosingView{pipelineTestView: &pipelineTestView{StateHistoryReadView: base, hook: func(op string, key []byte) error {
		if op == "Get" && bytes.Equal(key, keys[0]) {
			once.Do(func() { close(started) })
			<-allow
		}
		return nil
	}}, closeFn: func() error { return errors.Join(release(), closeFailure) }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	v, c, err := AcquireStateHistoryChunkCacheView(ctx, &pipelineFactory{Iteratee: db, snapshot: tracked})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	done := make(chan error, 1)
	go func() {
		err := IterateStateDomainChangesByBlockTxRangePipelinedWithWorkers(ctx, v, 1, 4, 0, 99, 4, borrowedStateDomainChangeNoop)
		done <- errors.Join(err, c.Close())
	}()
	pipelineAwait(t, started)
	cancel()
	select {
	case err := <-done:
		t.Fatalf("returned without joining miss: %v", err)
	default:
	}
	if tracked.closes.Load() != 0 {
		t.Fatal("snapshot closed with active read")
	}
	close(allow)
	if err := <-done; !errors.Is(err, context.Canceled) || !errors.Is(err, closeFailure) || tracked.active.Load() != 0 || tracked.closeActive.Load() != 0 || tracked.closes.Load() != 1 {
		t.Fatalf("cancel/close %v active%d atclose%d closes%d", err, tracked.active.Load(), tracked.closeActive.Load(), tracked.closes.Load())
	}
	if s := c.Stats(); !s.Closed || s.PayloadBytes != 0 || s.Entries != 0 {
		t.Fatal("canceled cache not released", s)
	}
}

func TestStateHistoryChunkCacheClockCapacityAndCollisions(t *testing.T) {
	// All keys are authenticated before private admission; this small cache
	// forces both direct-slot collisions and clock eviction without huge heaps.
	db, err := NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var packs [][]byte
	for i := 0; i < 32; i++ {
		raw := bytes.Repeat([]byte{byte(i)}, 24)
		pack, values := ownedReadBenchmarkPack(raw, 1, false)
		packs = append(packs, pack)
		for k, v := range values {
			if err := db.Put([]byte(k), v); err != nil {
				t.Fatal(err)
			}
		}
	}
	v, c, _ := chunkCacheTestView(t, db, 48, 8)
	for round := 0; round < 3; round++ {
		for i, pack := range packs {
			got, err := decodeStateHistorySharedPack(v, pack, 1)
			if err != nil || sha256.Sum256(got) != sha256.Sum256(bytes.Repeat([]byte{byte(i)}, 24)) {
				t.Fatal("collision changed bytes", err)
			}
			if s := c.Stats(); s.Entries > 2 || s.PayloadBytes > 48 || s.PeakEntries > 2 || s.PeakPayloadBytes > 48 {
				t.Fatal("clock exceeded budget", s)
			}
		}
	}
	if c.Stats().Evictions == 0 {
		t.Fatal("fixture missed eviction path")
	}
}

func TestStateHistoryChunkCacheScopeCancellationCannotBeOverridden(t *testing.T) {
	db, _ := pipelineFixture(t, "shared")
	pack, _ := db.Get(stateChangeSetKey(1, 0))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	v, c, err := AcquireStateHistoryChunkCacheView(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := decodeStateHistorySharedPack(v, pack, 1); err != nil {
		t.Fatal(err)
	}
	before := c.Stats()
	cancel()
	reader := historyPipelineReader{StateHistoryReadView: v, ctx: context.Background()}
	if _, err := decodeStateHistorySharedPack(reader, pack, 1); !errors.Is(err, context.Canceled) {
		t.Fatal("Background job bypassed cache scope cancellation", err)
	}
	if after := c.Stats(); after.Hits != before.Hits || after.Misses != before.Misses {
		t.Fatal("canceled scope performed a lookup", after)
	}
	if _, err := v.Get([]byte("scope")); !errors.Is(err, context.Canceled) {
		t.Fatal("Get bypassed scope", err)
	}
	if _, err := v.Has([]byte("scope")); !errors.Is(err, context.Canceled) {
		t.Fatal("Has bypassed scope", err)
	}
}
