package blockbuffer

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type benchmarkCommitmentCursor struct{}

var benchmarkCommitmentValue = []byte("encoded-branch")

func (benchmarkCommitmentCursor) View(_ []byte, fn func([]byte) error) (bool, error) {
	return true, fn(benchmarkCommitmentValue)
}

func (benchmarkCommitmentCursor) Close() error { return nil }

type benchmarkCommitmentSnapshot struct{}

func (benchmarkCommitmentSnapshot) NewCursor([]byte) (pointread.Cursor, error) {
	return benchmarkCommitmentCursor{}, nil
}

func (benchmarkCommitmentSnapshot) Close() error { return nil }

type checkingCommitmentCursor struct {
	want []byte
}

type blockingCommitmentCursorState struct {
	started   chan struct{}
	release   chan struct{}
	start     sync.Once
	calls     atomic.Int64
	present   bool
	value     []byte
	readErr   error
	readPanic any
	overwrite bool
}

type blockingCommitmentCursor struct {
	state *blockingCommitmentCursorState
}

func (c blockingCommitmentCursor) View(_ []byte, fn func([]byte) error) (bool, error) {
	s := c.state
	s.calls.Add(1)
	s.start.Do(func() { close(s.started) })
	<-s.release
	if s.readPanic != nil {
		panic(s.readPanic)
	}
	if s.readErr != nil {
		return false, s.readErr
	}
	if !s.present {
		return false, nil
	}
	err := fn(s.value)
	if s.overwrite {
		for i := range s.value {
			s.value[i] = 0
		}
	}
	return true, err
}

func (blockingCommitmentCursor) Close() error { return nil }

type blockingCommitmentSnapshot struct {
	state *blockingCommitmentCursorState
}

func (s blockingCommitmentSnapshot) NewCursor([]byte) (pointread.Cursor, error) {
	return blockingCommitmentCursor{state: s.state}, nil
}

func (blockingCommitmentSnapshot) Close() error { return nil }

type sequencedCommitmentSnapshot struct {
	states []*blockingCommitmentCursorState
	next   atomic.Int64
}

func (s *sequencedCommitmentSnapshot) NewCursor([]byte) (pointread.Cursor, error) {
	index := int(s.next.Add(1) - 1)
	if index >= len(s.states) {
		return nil, errors.New("unexpected commitment cursor")
	}
	return blockingCommitmentCursor{state: s.states[index]}, nil
}

func (*sequencedCommitmentSnapshot) Close() error { return nil }

// blockingCommitmentWriter forwards the first mutation either before or after
// parking it. It holds FlushUpTo in the disk-I/O phase while tests capture a
// commitment parent session on both sides of the durable write linearization.
type blockingCommitmentWriter struct {
	target interface {
		Put([]byte, []byte) error
		Delete([]byte) error
	}
	blockAfter bool
	started    chan struct{}
	release    chan struct{}
	once       sync.Once
}

func newBlockingCommitmentWriter(target interface {
	Put([]byte, []byte) error
	Delete([]byte) error
}, blockAfter bool) *blockingCommitmentWriter {
	return &blockingCommitmentWriter{
		target:     target,
		blockAfter: blockAfter,
		started:    make(chan struct{}),
		release:    make(chan struct{}),
	}
}

func (w *blockingCommitmentWriter) wait() {
	w.once.Do(func() {
		close(w.started)
		<-w.release
	})
}

func (w *blockingCommitmentWriter) Put(key, value []byte) error {
	if !w.blockAfter {
		w.wait()
	}
	err := w.target.Put(key, value)
	if w.blockAfter {
		w.wait()
	}
	return err
}

func (w *blockingCommitmentWriter) Delete(key []byte) error {
	if !w.blockAfter {
		w.wait()
	}
	err := w.target.Delete(key)
	if w.blockAfter {
		w.wait()
	}
	return err
}

func (c checkingCommitmentCursor) View(key []byte, fn func([]byte) error) (bool, error) {
	if !bytes.Equal(key, c.want) {
		return false, fmt.Errorf("key = %x, want %x", key, c.want)
	}
	return true, fn([]byte("encoded-branch"))
}

func (checkingCommitmentCursor) Close() error { return nil }

func readSessionBranch(t *testing.T, session pointread.CommitmentParentSession, reader int, prefix []byte) ([]byte, bool, bool) {
	t.Helper()
	var got []byte
	var stable bool
	found, err := rawdb.ViewCommitmentParentBranchInSession(session, reader, prefix, func(value []byte, valueStable bool) error {
		got = append(got, value...)
		stable = valueStable
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return got, found, stable
}

func TestCommitmentParentReadSessionKeepsOverlayAndDurableCut(t *testing.T) {
	disk, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	durablePrefix := []byte{1, 2}
	overridePrefix := []byte{2, 3}
	cachedPrefix := []byte{3, 4}
	if err := rawdb.WriteCommitmentBranch(disk, durablePrefix, []byte("durable-before")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteCommitmentBranch(disk, overridePrefix, []byte("override-before")); err != nil {
		t.Fatal(err)
	}

	buf := New(disk)
	buf.SetBaseReadCacheSize(1<<20, rawdb.CommitmentBranchKeyPrefix)
	cacheKey := append([]byte(rawdb.CommitmentBranchKeyPrefix), cachedPrefix...)
	testBaseReadCacheSet(buf.baseReadCache, cacheKey, []byte("cached-before"))
	buf.BeginBlock(bufHash(1), 1)
	h1, _ := buf.NewestInflight()
	if err := rawdb.WriteCommitmentBranch(buf.ViewLayer(h1), overridePrefix, []byte("override-layer")); err != nil {
		t.Fatal(err)
	}
	if err := buf.CommitInflight(h1); err != nil {
		t.Fatal(err)
	}
	buf.BeginBlock(bufHash(2), 2)
	h2, _ := buf.NewestInflight()
	session, err := buf.ViewLayer(h2).NewCommitmentParentReadSession(17)
	if err != nil || session == nil {
		t.Fatalf("NewCommitmentParentReadSession = (%T,%v)", session, err)
	}
	concrete := session.(*commitmentParentReadSession)
	published := buf.readView.Load()
	if len(concrete.layers) != 1 || len(published.layers) != 1 || &concrete.layers[0] != &published.layers[0] {
		t.Fatal("commitment parent session did not retain the immutable published topology")
	}
	overlayBefore := commitmentParentOverlayResolvedCounter.Snapshot().Count()
	cacheBefore := commitmentParentCacheResolvedCounter.Snapshot().Count()
	durableReadsBefore := commitmentParentDurableReadsCounter.Snapshot().Count()
	durableHitsBefore := commitmentParentDurableHitsCounter.Snapshot().Count()

	// Move the captured overlay into the durable DB, then mutate a row that was
	// already durable. The session must retain both sides of its original cut.
	if err := buf.FlushUpTo(1, disk); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteCommitmentBranch(disk, durablePrefix, []byte("durable-after")); err != nil {
		t.Fatal(err)
	}

	if got, found, stable := readSessionBranch(t, session, 2, overridePrefix); !found || !stable || !bytes.Equal(got, []byte("override-layer")) {
		t.Fatalf("overlay = (%q,%v,stable=%v)", got, found, stable)
	}
	if got, found, stable := readSessionBranch(t, session, 3, cachedPrefix); !found || stable || !bytes.Equal(got, []byte("cached-before")) {
		t.Fatalf("cache = (%q,%v,stable=%v)", got, found, stable)
	}
	if got, found, stable := readSessionBranch(t, session, 1, durablePrefix); !found || stable || !bytes.Equal(got, []byte("durable-before")) {
		t.Fatalf("durable snapshot = (%q,%v,stable=%v)", got, found, stable)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if got := commitmentParentOverlayResolvedCounter.Snapshot().Count() - overlayBefore; got != 1 {
		t.Fatalf("overlay resolved delta = %d, want 1", got)
	}
	if got := commitmentParentCacheResolvedCounter.Snapshot().Count() - cacheBefore; got != 1 {
		t.Fatalf("cache resolved delta = %d, want 1", got)
	}
	if got := commitmentParentDurableReadsCounter.Snapshot().Count() - durableReadsBefore; got != 1 {
		t.Fatalf("durable reads delta = %d, want 1", got)
	}
	if got := commitmentParentDurableHitsCounter.Snapshot().Count() - durableHitsBefore; got != 1 {
		t.Fatalf("durable hits delta = %d, want 1", got)
	}
}

func TestCommitmentParentReadSessionIncludesOnlyOlderInflightLayers(t *testing.T) {
	disk, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	prefix := []byte{6, 7, 8}
	buf := New(disk)
	buf.SetMaxInflight(4)
	buf.BeginBlock(bufHash(1), 1)
	h1, _ := buf.NewestInflight()
	if err := rawdb.WriteCommitmentBranch(buf.ViewLayer(h1), prefix, []byte("older")); err != nil {
		t.Fatal(err)
	}
	buf.BeginBlock(bufHash(2), 2)
	h2, _ := buf.NewestInflight()
	if err := rawdb.WriteCommitmentBranch(buf.ViewLayer(h2), prefix, []byte("bound")); err != nil {
		t.Fatal(err)
	}
	buf.BeginBlock(bufHash(3), 3)
	h3, _ := buf.NewestInflight()
	if err := rawdb.WriteCommitmentBranch(buf.ViewLayer(h3), prefix, []byte("newer")); err != nil {
		t.Fatal(err)
	}

	session, err := buf.ViewLayer(h2).NewCommitmentParentReadSession(17)
	if err != nil || session == nil {
		t.Fatalf("NewCommitmentParentReadSession = (%T,%v)", session, err)
	}
	defer session.Close()
	concrete := session.(*commitmentParentReadSession)
	if len(concrete.inflight) != 1 || concrete.inflight[0] != h1.l {
		t.Fatalf("older inflight layers = %d, want only block 1", len(concrete.inflight))
	}
	if got, found, stable := readSessionBranch(t, session, 6, prefix); !found || !stable || !bytes.Equal(got, []byte("older")) {
		t.Fatalf("parent branch = (%q,%v,stable=%v), want older", got, found, stable)
	}
}

func TestCommitmentParentReadSessionDoesNotWaitForFlushIO(t *testing.T) {
	for _, tc := range []struct {
		name       string
		blockAfter bool
		delete     bool
	}{
		{name: "put-before-durable-write"},
		{name: "put-after-durable-write", blockAfter: true},
		{name: "delete-before-durable-write", delete: true},
		{name: "delete-after-durable-write", blockAfter: true, delete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			disk, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer disk.Close()

			prefix := []byte{4, 5, 6}
			if tc.delete {
				if err := rawdb.WriteCommitmentBranch(disk, prefix, []byte("durable-value")); err != nil {
					t.Fatal(err)
				}
			}
			buf := New(disk)
			buf.BeginBlock(bufHash(1), 1)
			h1, _ := buf.NewestInflight()
			var writeErr error
			if tc.delete {
				writeErr = rawdb.DeleteCommitmentBranch(buf.ViewLayer(h1), prefix)
			} else {
				writeErr = rawdb.WriteCommitmentBranch(buf.ViewLayer(h1), prefix, []byte("layer-value"))
			}
			if writeErr != nil {
				t.Fatal(writeErr)
			}
			if err := buf.CommitInflight(h1); err != nil {
				t.Fatal(err)
			}
			buf.BeginBlock(bufHash(2), 2)
			h2, _ := buf.NewestInflight()

			writer := newBlockingCommitmentWriter(disk, tc.blockAfter)
			flushDone := make(chan error, 1)
			go func() { flushDone <- buf.FlushUpTo(1, writer) }()
			<-writer.started

			type sessionResult struct {
				session pointread.CommitmentParentSession
				err     error
			}
			sessionDone := make(chan sessionResult, 1)
			go func() {
				session, err := buf.ViewLayer(h2).NewCommitmentParentReadSession(17)
				sessionDone <- sessionResult{session: session, err: err}
			}()

			var session pointread.CommitmentParentSession
			select {
			case result := <-sessionDone:
				if result.err != nil || result.session == nil {
					close(writer.release)
					<-flushDone
					t.Fatalf("NewCommitmentParentReadSession = (%T,%v)", result.session, result.err)
				}
				session = result.session
			case <-time.After(2 * time.Second):
				close(writer.release)
				<-flushDone
				t.Fatal("commitment parent session waited for background flush I/O")
			}

			got, found, stable := readSessionBranch(t, session, 4, prefix)
			if tc.delete {
				if found {
					close(writer.release)
					<-flushDone
					t.Fatalf("mid-flush tombstone = (%q,%v,stable=%v)", got, found, stable)
				}
			} else if !found || !stable || !bytes.Equal(got, []byte("layer-value")) {
				close(writer.release)
				<-flushDone
				t.Fatalf("mid-flush branch = (%q,%v,stable=%v)", got, found, stable)
			}
			close(writer.release)
			if err := <-flushDone; err != nil {
				t.Fatal(err)
			}
			got, found, stable = readSessionBranch(t, session, 4, prefix)
			if tc.delete {
				if found {
					t.Fatalf("post-flush tombstone = (%q,%v,stable=%v)", got, found, stable)
				}
			} else if !found || !stable || !bytes.Equal(got, []byte("layer-value")) {
				t.Fatalf("post-flush branch = (%q,%v,stable=%v)", got, found, stable)
			}
			if err := session.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func BenchmarkCommitmentParentReadSessionKeyScratch(b *testing.B) {
	const readers = 17
	keyScratch := borrowCommitmentParentKeyScratch(readers)
	defer returnCommitmentParentKeyScratch(keyScratch)
	session := &commitmentParentReadSession{
		snapshot:   benchmarkCommitmentSnapshot{},
		cursors:    make([]pointread.Cursor, readers),
		keyScratch: keyScratch,
	}
	session.readContexts = borrowCommitmentParentReadContexts(session, readers)
	defer returnCommitmentParentReadContexts(session.readContexts)
	first := []byte(rawdb.CommitmentBranchKeyPrefix)
	second := bytes.Repeat([]byte{0x0a}, 64)
	consume := func([]byte, bool) error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := session.ViewKeyParts(3, first, second, consume); err != nil {
			b.Fatal(err)
		}
	}
}

func TestCommitmentParentReadSessionReadContextDoesNotRetainCallback(t *testing.T) {
	const readers = 1
	keyScratch := borrowCommitmentParentKeyScratch(readers)
	session := &commitmentParentReadSession{
		snapshot:   benchmarkCommitmentSnapshot{},
		cursors:    make([]pointread.Cursor, readers),
		keyScratch: keyScratch,
	}
	session.readContexts = borrowCommitmentParentReadContexts(session, readers)
	defer session.Close()

	first := []byte(rawdb.CommitmentBranchKeyPrefix)
	second := []byte{0x0a}
	wantErr := errors.New("stop first read")
	firstCalls := 0
	if found, err := session.ViewKeyParts(0, first, second, func([]byte, bool) error {
		firstCalls++
		return wantErr
	}); !found || !errors.Is(err, wantErr) {
		t.Fatalf("first read = (found=%v, err=%v), want true/%v", found, err, wantErr)
	}

	secondCalls := 0
	if found, err := session.ViewKeyParts(0, first, second, func([]byte, bool) error {
		secondCalls++
		return nil
	}); !found || err != nil {
		t.Fatalf("second read = (found=%v, err=%v), want true/nil", found, err)
	}
	if firstCalls != 1 || secondCalls != 1 {
		t.Fatalf("callback calls = (%d,%d), want (1,1)", firstCalls, secondCalls)
	}
}

func TestCommitmentParentReadSessionReaderScratchIsolated(t *testing.T) {
	const readers = 17
	keyScratch := borrowCommitmentParentKeyScratch(readers)
	session := &commitmentParentReadSession{
		snapshot:   benchmarkCommitmentSnapshot{},
		cursors:    make([]pointread.Cursor, readers),
		keyScratch: keyScratch,
	}
	session.readContexts = borrowCommitmentParentReadContexts(session, readers)
	defer session.Close()
	first := []byte(rawdb.CommitmentBranchKeyPrefix)
	for reader := range readers {
		second := bytes.Repeat([]byte{byte(reader)}, 64)
		want := append(append([]byte(nil), first...), second...)
		session.cursors[reader] = checkingCommitmentCursor{want: want}
	}

	var wg sync.WaitGroup
	errs := make(chan error, readers)
	for reader := range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			second := bytes.Repeat([]byte{byte(reader)}, 64)
			for range 1_000 {
				if _, err := session.ViewKeyParts(reader, first, second, func([]byte, bool) error { return nil }); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func newBlockingCommitmentParentSession(t *testing.T, readers int, state *blockingCommitmentCursorState) *commitmentParentReadSession {
	t.Helper()
	cache := newBaseReadCacheWithTrunk(1<<20, baseReadCacheTrunkDepth, rawdb.CommitmentBranchKeyPrefix)
	keyScratch := borrowCommitmentParentKeyScratch(readers)
	session := &commitmentParentReadSession{
		cache:        cache,
		cacheVersion: cache.version.Load(),
		snapshot:     blockingCommitmentSnapshot{state: state},
		cursors:      make([]pointread.Cursor, readers),
		keyScratch:   keyScratch,
	}
	session.readContexts = borrowCommitmentParentReadContexts(session, readers)
	return session
}

func waitForCommitmentParentFlightFollowers(t *testing.T, session *commitmentParentReadSession, key []byte, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	hash := layerBloomHashBytes(key)
	shard := &session.flights.shards[hash&(baseReadCacheShardCount-1)]
	for time.Now().Before(deadline) {
		shard.mu.Lock()
		got := 0
		for call := shard.calls[hash]; call != nil; call = call.next {
			if call.matches(key, hash) {
				got = call.followers
				break
			}
		}
		shard.mu.Unlock()
		if got >= want {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("commitment parent flight did not reach %d followers", want)
}

func TestCommitmentParentReadSessionSingleflightSharesPresentValueAndIsolatesCallbacks(t *testing.T) {
	const readers = 8
	wantValue := []byte("shared-durable-branch")
	state := &blockingCommitmentCursorState{
		started:   make(chan struct{}),
		release:   make(chan struct{}),
		present:   true,
		value:     append([]byte(nil), wantValue...),
		overwrite: true,
	}
	session := newBlockingCommitmentParentSession(t, readers, state)
	prefix := []byte{1, 2, 3, 4, 5, 6}
	physicalKey := append([]byte(rawdb.CommitmentBranchKeyPrefix), prefix...)
	wantCallbackErr := errors.New("leader callback stopped")

	leadersBefore := commitmentParentSingleflightLeadersCounter.Snapshot().Count()
	waitersBefore := commitmentParentSingleflightWaitersCounter.Snapshot().Count()
	sharedBefore := commitmentParentSingleflightSharedCounter.Snapshot().Count()
	durableBefore := commitmentParentDurableReadsCounter.Snapshot().Count()
	exactCacheBefore := commitmentParentExactDepthCacheCounters[1].Snapshot().Count()
	exactDurableBefore := commitmentParentExactDepthDurableCounters[1].Snapshot().Count()

	type result struct {
		found  bool
		stable bool
		value  []byte
		err    error
	}
	results := make([]result, readers)
	var wg sync.WaitGroup
	read := func(reader int) {
		defer wg.Done()
		results[reader].found, results[reader].err = rawdb.LegacyCommitmentBranchKeyspace().ViewParentInSession(
			session,
			reader,
			prefix,
			func(value []byte, stable bool) error {
				results[reader].stable = stable
				results[reader].value = append([]byte(nil), value...)
				if reader == 0 {
					return wantCallbackErr
				}
				return nil
			},
		)
	}

	wg.Add(1)
	go read(0)
	<-state.started
	for reader := 1; reader < readers; reader++ {
		wg.Add(1)
		go read(reader)
	}
	waitForCommitmentParentFlightFollowers(t, session, physicalKey, readers-1)
	close(state.release)
	wg.Wait()

	if got := state.calls.Load(); got != 1 {
		t.Fatalf("durable cursor calls = %d, want 1", got)
	}
	for reader, result := range results {
		if !result.found || result.stable || !bytes.Equal(result.value, wantValue) {
			t.Errorf("reader %d = (found=%v,stable=%v,value=%q)", reader, result.found, result.stable, result.value)
		}
		if reader == 0 {
			if !errors.Is(result.err, wantCallbackErr) {
				t.Errorf("leader callback error = %v, want %v", result.err, wantCallbackErr)
			}
		} else if result.err != nil {
			t.Errorf("reader %d inherited leader callback error: %v", reader, result.err)
		}
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if got := commitmentParentSingleflightLeadersCounter.Snapshot().Count() - leadersBefore; got != 1 {
		t.Fatalf("singleflight leaders delta = %d, want 1", got)
	}
	if got := commitmentParentSingleflightWaitersCounter.Snapshot().Count() - waitersBefore; got != readers-1 {
		t.Fatalf("singleflight waiters delta = %d, want %d", got, readers-1)
	}
	if got := commitmentParentSingleflightSharedCounter.Snapshot().Count() - sharedBefore; got != readers-1 {
		t.Fatalf("singleflight shared delta = %d, want %d", got, readers-1)
	}
	if got := commitmentParentDurableReadsCounter.Snapshot().Count() - durableBefore; got != 1 {
		t.Fatalf("durable reads delta = %d, want 1", got)
	}
	if got := commitmentParentExactDepthCacheCounters[1].Snapshot().Count() - exactCacheBefore; got != 0 {
		t.Fatalf("singleflight depth-six cache delta = %d, want 0", got)
	}
	if got := commitmentParentExactDepthDurableCounters[1].Snapshot().Count() - exactDurableBefore; got != 1 {
		t.Fatalf("singleflight depth-six durable delta = %d, want 1", got)
	}
}

func TestCommitmentParentReadSessionSingleflightSharesMissingAndStorageError(t *testing.T) {
	for _, tc := range []struct {
		name    string
		readErr error
	}{
		{name: "missing"},
		{name: "storage-error", readErr: errors.New("durable read failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const readers = 4
			state := &blockingCommitmentCursorState{
				started: make(chan struct{}),
				release: make(chan struct{}),
				readErr: tc.readErr,
			}
			session := newBlockingCommitmentParentSession(t, readers, state)
			prefix := []byte{9, 8, 7, 6, 5, 4}
			physicalKey := append([]byte(rawdb.CommitmentBranchKeyPrefix), prefix...)
			found := make([]bool, readers)
			errs := make([]error, readers)
			var callbackCalls atomic.Int64
			var wg sync.WaitGroup
			read := func(reader int) {
				defer wg.Done()
				found[reader], errs[reader] = rawdb.LegacyCommitmentBranchKeyspace().ViewParentInSession(
					session,
					reader,
					prefix,
					func([]byte, bool) error {
						callbackCalls.Add(1)
						return nil
					},
				)
			}
			wg.Add(1)
			go read(0)
			<-state.started
			for reader := 1; reader < readers; reader++ {
				wg.Add(1)
				go read(reader)
			}
			waitForCommitmentParentFlightFollowers(t, session, physicalKey, readers-1)
			close(state.release)
			wg.Wait()
			if got := state.calls.Load(); got != 1 {
				t.Fatalf("durable cursor calls = %d, want 1", got)
			}
			if got := callbackCalls.Load(); got != 0 {
				t.Fatalf("callback calls = %d, want 0", got)
			}
			for reader := range readers {
				if found[reader] {
					t.Errorf("reader %d unexpectedly found missing value", reader)
				}
				if !errors.Is(errs[reader], tc.readErr) {
					t.Errorf("reader %d error = %v, want %v", reader, errs[reader], tc.readErr)
				}
			}
			if err := session.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCommitmentParentReadSessionSingleflightReleasesFollowerAfterLeaderPanic(t *testing.T) {
	state := &blockingCommitmentCursorState{
		started:   make(chan struct{}),
		release:   make(chan struct{}),
		readPanic: "durable cursor panic",
	}
	session := newBlockingCommitmentParentSession(t, 2, state)
	prefix := []byte{3, 3, 3, 3, 3, 3}
	physicalKey := append([]byte(rawdb.CommitmentBranchKeyPrefix), prefix...)

	leaderRecovered := make(chan any, 1)
	go func() {
		defer func() { leaderRecovered <- recover() }()
		_, _ = rawdb.LegacyCommitmentBranchKeyspace().ViewParentInSession(
			session,
			0,
			prefix,
			func([]byte, bool) error { return nil },
		)
	}()
	<-state.started

	followerDone := make(chan error, 1)
	go func() {
		_, err := rawdb.LegacyCommitmentBranchKeyspace().ViewParentInSession(
			session,
			1,
			prefix,
			func([]byte, bool) error { return nil },
		)
		followerDone <- err
	}()
	waitForCommitmentParentFlightFollowers(t, session, physicalKey, 1)
	close(state.release)

	select {
	case recovered := <-leaderRecovered:
		if recovered != state.readPanic {
			t.Fatalf("leader recovered %v, want %v", recovered, state.readPanic)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("leader panic did not unwind")
	}
	select {
	case err := <-followerDone:
		if !errors.Is(err, errCommitmentParentReadAborted) {
			t.Fatalf("follower error = %v, want %v", err, errCommitmentParentReadAborted)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("follower remained blocked after leader panic")
	}

	hash := layerBloomHashBytes(physicalKey)
	shard := &session.flights.shards[hash&(baseReadCacheShardCount-1)]
	shard.mu.Lock()
	active := shard.calls[hash]
	shard.mu.Unlock()
	if active != nil {
		t.Fatal("panicked flight remained active")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitmentParentReadSessionSingleflightSharesPrefetchWithForeground(t *testing.T) {
	state := &blockingCommitmentCursorState{
		started: make(chan struct{}),
		release: make(chan struct{}),
		present: true,
		value:   []byte("prefetched-branch"),
	}
	session := newBlockingCommitmentParentSession(t, 2, state)
	prefix := []byte{1, 1, 1, 1, 1, 1}
	physicalKey := append([]byte(rawdb.CommitmentBranchKeyPrefix), prefix...)
	var prefetch pointread.CommitmentParentPrefetchSession = session
	exactCacheBefore := commitmentParentExactDepthCacheCounters[1].Snapshot().Count()
	exactDurableBefore := commitmentParentExactDepthDurableCounters[1].Snapshot().Count()

	type prefetchResult struct {
		found bool
		err   error
	}
	prefetchDone := make(chan prefetchResult, 1)
	go func() {
		found, err := rawdb.LegacyCommitmentBranchKeyspace().PrefetchParentInSession(prefetch, 0, prefix)
		prefetchDone <- prefetchResult{found: found, err: err}
	}()
	<-state.started

	type foregroundResult struct {
		found bool
		value []byte
		err   error
	}
	foregroundDone := make(chan foregroundResult, 1)
	go func() {
		var result foregroundResult
		result.found, result.err = rawdb.LegacyCommitmentBranchKeyspace().ViewParentInSession(
			session,
			1,
			prefix,
			func(value []byte, _ bool) error {
				result.value = append([]byte(nil), value...)
				return nil
			},
		)
		foregroundDone <- result
	}()
	waitForCommitmentParentFlightFollowers(t, session, physicalKey, 1)
	close(state.release)

	preResult := <-prefetchDone
	foreground := <-foregroundDone
	if !preResult.found || preResult.err != nil {
		t.Fatalf("prefetch = (%v,%v), want (true,nil)", preResult.found, preResult.err)
	}
	if !foreground.found || foreground.err != nil || string(foreground.value) != "prefetched-branch" {
		t.Fatalf("foreground = (%v,%q,%v)", foreground.found, foreground.value, foreground.err)
	}
	if got := state.calls.Load(); got != 1 {
		t.Fatalf("durable cursor calls = %d, want 1", got)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if got := commitmentParentExactDepthCacheCounters[1].Snapshot().Count() - exactCacheBefore; got != 0 {
		t.Fatalf("shared prefetch depth-six cache delta = %d, want 0", got)
	}
	if got := commitmentParentExactDepthDurableCounters[1].Snapshot().Count() - exactDurableBefore; got != 0 {
		t.Fatalf("shared prefetch depth-six durable delta = %d, want 0", got)
	}
}

func TestCommitmentParentReadSessionForegroundRetriesFailedPrefetchFlight(t *testing.T) {
	prefetchErr := errors.New("speculative prefetch failed")
	prefetchState := &blockingCommitmentCursorState{
		started: make(chan struct{}),
		release: make(chan struct{}),
		readErr: prefetchErr,
	}
	foregroundRelease := make(chan struct{})
	close(foregroundRelease)
	foregroundState := &blockingCommitmentCursorState{
		started: make(chan struct{}),
		release: foregroundRelease,
		present: true,
		value:   []byte("authoritative-branch"),
	}
	snapshot := &sequencedCommitmentSnapshot{states: []*blockingCommitmentCursorState{prefetchState, foregroundState}}
	cache := newBaseReadCacheWithTrunk(1<<20, baseReadCacheTrunkDepth, rawdb.CommitmentBranchKeyPrefix)
	session := &commitmentParentReadSession{
		cache:        cache,
		cacheVersion: cache.version.Load(),
		snapshot:     snapshot,
		cursors:      make([]pointread.Cursor, 2),
		keyScratch:   borrowCommitmentParentKeyScratch(2),
	}
	session.readContexts = borrowCommitmentParentReadContexts(session, 2)
	prefix := []byte{2, 2, 2, 2, 2, 2}
	physicalKey := append([]byte(rawdb.CommitmentBranchKeyPrefix), prefix...)
	type foregroundRetryResult struct {
		found bool
		value []byte
		err   error
	}

	prefetchDone := make(chan error, 1)
	go func() {
		_, err := rawdb.LegacyCommitmentBranchKeyspace().PrefetchParentInSession(session, 0, prefix)
		prefetchDone <- err
	}()
	<-prefetchState.started

	foregroundDone := make(chan foregroundRetryResult, 1)
	go func() {
		var result foregroundRetryResult
		result.found, result.err = rawdb.LegacyCommitmentBranchKeyspace().ViewParentInSession(
			session,
			1,
			prefix,
			func(value []byte, _ bool) error {
				result.value = append([]byte(nil), value...)
				return nil
			},
		)
		foregroundDone <- result
	}()
	waitForCommitmentParentFlightFollowers(t, session, physicalKey, 1)
	close(prefetchState.release)

	if err := <-prefetchDone; !errors.Is(err, prefetchErr) {
		t.Fatalf("prefetch error = %v, want %v", err, prefetchErr)
	}
	foreground := <-foregroundDone
	if !foreground.found || foreground.err != nil || string(foreground.value) != "authoritative-branch" {
		t.Fatalf("foreground retry = (%v,%q,%v)", foreground.found, foreground.value, foreground.err)
	}
	if got := prefetchState.calls.Load(); got != 1 {
		t.Fatalf("prefetch cursor calls = %d, want 1", got)
	}
	if got := foregroundState.calls.Load(); got != 1 {
		t.Fatalf("foreground cursor calls = %d, want 1", got)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitmentParentReadSessionPrefetchAdmitsPresentAndMissing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		present bool
	}{
		{name: "present", present: true},
		{name: "missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			disk, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer disk.Close()
			prefix := []byte{5, 1, 2, 3, 4}
			if tc.present {
				if err := rawdb.WriteCommitmentBranch(disk, prefix, []byte("durable-branch")); err != nil {
					t.Fatal(err)
				}
			}

			buf := New(disk)
			buf.SetBaseReadCacheSizeWithTrunk(1<<20, 4, rawdb.CommitmentBranchKeyPrefix)
			buf.BeginBlock(bufHash(1), 1)
			handle, _ := buf.NewestInflight()
			session, err := buf.ViewLayer(handle).NewCommitmentParentReadSession(2)
			if err != nil || session == nil {
				t.Fatalf("NewCommitmentParentReadSession = (%T,%v)", session, err)
			}
			prefetch := session.(pointread.CommitmentParentPrefetchSession)
			plannedBefore := commitmentParentPrefetchPlannedCounter.Snapshot().Count()
			prefetchDurableBefore := commitmentParentPrefetchDurableCounter.Snapshot().Count()
			prefetchHitsBefore := commitmentParentPrefetchDurableHitCounter.Snapshot().Count()
			cacheBefore := commitmentParentCacheResolvedCounter.Snapshot().Count()
			durableBefore := commitmentParentDurableReadsCounter.Snapshot().Count()
			usefulBefore := baseReadCachePrefetchUsefulCounter.Snapshot().Count()
			commitmentUsefulBefore := commitmentParentPrefetchUsefulCounter.Snapshot().Count()
			depthPlannedBefore := commitmentParentPrefetchDepthPlannedCounters[0].Snapshot().Count()
			depthDurableBefore := commitmentParentPrefetchDepthDurableCounters[0].Snapshot().Count()
			depthUsefulBefore := commitmentParentPrefetchDepthUsefulCounters[0].Snapshot().Count()

			found, err := rawdb.LegacyCommitmentBranchKeyspace().PrefetchParentInSession(prefetch, 0, prefix)
			if err != nil || found != tc.present {
				t.Fatalf("PrefetchParentInSession = (%v,%v), want (%v,nil)", found, err, tc.present)
			}
			got, found, stable := readSessionBranch(t, session, 1, prefix)
			if found != tc.present {
				t.Fatalf("foreground found = %v, want %v", found, tc.present)
			}
			if tc.present && (stable || !bytes.Equal(got, []byte("durable-branch"))) {
				t.Fatalf("foreground branch = (%q,stable=%v)", got, stable)
			}
			if err := session.Close(); err != nil {
				t.Fatal(err)
			}

			if got := commitmentParentPrefetchPlannedCounter.Snapshot().Count() - plannedBefore; got != 1 {
				t.Fatalf("prefetch planned delta = %d, want 1", got)
			}
			if got := commitmentParentPrefetchDurableCounter.Snapshot().Count() - prefetchDurableBefore; got != 1 {
				t.Fatalf("prefetch durable delta = %d, want 1", got)
			}
			wantDurableHits := int64(0)
			if tc.present {
				wantDurableHits = 1
			}
			if got := commitmentParentPrefetchDurableHitCounter.Snapshot().Count() - prefetchHitsBefore; got != wantDurableHits {
				t.Fatalf("prefetch durable-hit delta = %d, want %d", got, wantDurableHits)
			}
			if got := commitmentParentCacheResolvedCounter.Snapshot().Count() - cacheBefore; got != 1 {
				t.Fatalf("foreground cache delta = %d, want 1", got)
			}
			if got := commitmentParentDurableReadsCounter.Snapshot().Count() - durableBefore; got != 0 {
				t.Fatalf("foreground durable delta = %d, want 0", got)
			}
			if got := baseReadCachePrefetchUsefulCounter.Snapshot().Count() - usefulBefore; got != 1 {
				t.Fatalf("useful prefetch delta = %d, want 1", got)
			}
			if got := commitmentParentPrefetchUsefulCounter.Snapshot().Count() - commitmentUsefulBefore; got != 1 {
				t.Fatalf("commitment useful prefetch delta = %d, want 1", got)
			}
			if got := commitmentParentPrefetchDepthPlannedCounters[0].Snapshot().Count() - depthPlannedBefore; got != 1 {
				t.Fatalf("depth-five planned prefetch delta = %d, want 1", got)
			}
			if got := commitmentParentPrefetchDepthDurableCounters[0].Snapshot().Count() - depthDurableBefore; got != 1 {
				t.Fatalf("depth-five durable prefetch delta = %d, want 1", got)
			}
			if got := commitmentParentPrefetchDepthUsefulCounters[0].Snapshot().Count() - depthUsefulBefore; got != 1 {
				t.Fatalf("depth-five useful prefetch delta = %d, want 1", got)
			}
		})
	}
}

func TestCommitmentParentPrefetchDepthBuckets(t *testing.T) {
	for _, tc := range []struct {
		depth int
		want  int
	}{
		{depth: 4, want: -1},
		{depth: 5, want: 0},
		{depth: 6, want: 1},
		{depth: 32, want: 1},
	} {
		if got := commitmentParentPrefetchDepthBucket(tc.depth); got != tc.want {
			t.Fatalf("depth %d bucket = %d, want %d", tc.depth, got, tc.want)
		}
	}
}

func TestCommitmentParentExactDepthBuckets(t *testing.T) {
	for _, tc := range []struct {
		depth int
		want  int
	}{
		{depth: -1, want: -1},
		{depth: 4, want: -1},
		{depth: 5, want: 0},
		{depth: 6, want: 1},
		{depth: 7, want: 2},
		{depth: 8, want: 3},
		{depth: 9, want: -1},
	} {
		if got := commitmentParentExactDepthBucket(tc.depth); got != tc.want {
			t.Fatalf("depth %d exact bucket = %d, want %d", tc.depth, got, tc.want)
		}
	}
}

func TestCommitmentParentReadSessionPublishesExactDepthMetrics(t *testing.T) {
	const readers = 1
	cache := newBaseReadCacheWithTrunk(1<<20, baseReadCacheTrunkDepth, rawdb.CommitmentBranchKeyPrefix)
	session := &commitmentParentReadSession{
		cache:        cache,
		cacheVersion: cache.version.Load(),
		snapshot:     benchmarkCommitmentSnapshot{},
		cursors:      make([]pointread.Cursor, readers),
		keyScratch:   borrowCommitmentParentKeyScratch(readers),
	}
	session.readContexts = borrowCommitmentParentReadContexts(session, readers)

	var exactCacheBefore, exactDurableBefore [4]int64
	for bucket := range exactCacheBefore {
		exactCacheBefore[bucket] = commitmentParentExactDepthCacheCounters[bucket].Snapshot().Count()
		exactDurableBefore[bucket] = commitmentParentExactDepthDurableCounters[bucket].Snapshot().Count()
	}
	aggregateCacheBefore := commitmentParentDepthCacheCounters[0].Snapshot().Count()
	aggregateDurableBefore := commitmentParentDepthDurableCounters[0].Snapshot().Count()
	deepCacheBefore := commitmentParentDepthCacheCounters[1].Snapshot().Count()
	deepDurableBefore := commitmentParentDepthDurableCounters[1].Snapshot().Count()
	totalCacheBefore := commitmentParentCacheResolvedCounter.Snapshot().Count()
	totalDurableBefore := commitmentParentDurableReadsCounter.Snapshot().Count()

	prefix := []byte(rawdb.CommitmentBranchKeyPrefix)
	consume := func([]byte, bool) error { return nil }
	for depth := 4; depth <= 9; depth++ {
		path := bytes.Repeat([]byte{byte(depth)}, depth)
		for attempt := 0; attempt < 2; attempt++ {
			found, err := session.ViewKeyParts(0, prefix, path, consume)
			if err != nil || !found {
				t.Fatalf("depth %d attempt %d = (%v,%v), want (true,nil)", depth, attempt, found, err)
			}
		}
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}

	for bucket := range exactCacheBefore {
		if got := commitmentParentExactDepthCacheCounters[bucket].Snapshot().Count() - exactCacheBefore[bucket]; got != 1 {
			t.Errorf("exact depth %d cache delta = %d, want 1", bucket+5, got)
		}
		if got := commitmentParentExactDepthDurableCounters[bucket].Snapshot().Count() - exactDurableBefore[bucket]; got != 1 {
			t.Errorf("exact depth %d durable delta = %d, want 1", bucket+5, got)
		}
	}
	if got := commitmentParentDepthCacheCounters[0].Snapshot().Count() - aggregateCacheBefore; got != 4 {
		t.Errorf("depth 5-8 cache delta = %d, want 4", got)
	}
	if got := commitmentParentDepthDurableCounters[0].Snapshot().Count() - aggregateDurableBefore; got != 4 {
		t.Errorf("depth 5-8 durable delta = %d, want 4", got)
	}
	if got := commitmentParentDepthCacheCounters[1].Snapshot().Count() - deepCacheBefore; got != 1 {
		t.Errorf("depth 9-16 cache delta = %d, want 1", got)
	}
	if got := commitmentParentDepthDurableCounters[1].Snapshot().Count() - deepDurableBefore; got != 1 {
		t.Errorf("depth 9-16 durable delta = %d, want 1", got)
	}
	if got := commitmentParentCacheResolvedCounter.Snapshot().Count() - totalCacheBefore; got != 6 {
		t.Errorf("total cache delta = %d, want 6", got)
	}
	if got := commitmentParentDurableReadsCounter.Snapshot().Count() - totalDurableBefore; got != 6 {
		t.Errorf("total durable delta = %d, want 6", got)
	}
}

func TestReturnCommitmentParentReadContextsClearsExactDepthMetrics(t *testing.T) {
	ctx := newCommitmentParentReadContext().(*commitmentParentReadContext)
	ctx.exactDepthCached = [4]uint64{1, 2, 3, 4}
	ctx.exactDepthDurable = [4]uint64{5, 6, 7, 8}
	contexts := []*commitmentParentReadContext{ctx}
	returnCommitmentParentReadContexts(contexts)
	if ctx.exactDepthCached != [4]uint64{} || ctx.exactDepthDurable != [4]uint64{} {
		t.Fatalf("pooled exact-depth counters retained cache=%v durable=%v", ctx.exactDepthCached, ctx.exactDepthDurable)
	}
}
