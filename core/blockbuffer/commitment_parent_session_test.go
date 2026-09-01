package blockbuffer

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type benchmarkCommitmentCursor struct{}

func (benchmarkCommitmentCursor) View(_ []byte, fn func([]byte) error) (bool, error) {
	return true, fn([]byte("encoded-branch"))
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
