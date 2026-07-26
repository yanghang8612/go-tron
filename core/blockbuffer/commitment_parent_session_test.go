package blockbuffer

import (
	"bytes"
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
	if err := rawdb.WriteCommitmentBranch(disk, durablePrefix, []byte("durable-before")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteCommitmentBranch(disk, overridePrefix, []byte("override-before")); err != nil {
		t.Fatal(err)
	}

	buf := New(disk)
	buf.SetBaseReadCacheSize(1 << 20)
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
	defer session.Close()

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
	if got, found, stable := readSessionBranch(t, session, 1, durablePrefix); !found || stable || !bytes.Equal(got, []byte("durable-before")) {
		t.Fatalf("durable snapshot = (%q,%v,stable=%v)", got, found, stable)
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

func TestCommitmentParentReadSessionReaderScratchIsolated(t *testing.T) {
	const readers = 17
	keyScratch := borrowCommitmentParentKeyScratch(readers)
	session := &commitmentParentReadSession{
		snapshot:   benchmarkCommitmentSnapshot{},
		cursors:    make([]pointread.Cursor, readers),
		keyScratch: keyScratch,
	}
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
