package blockbuffer

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

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
