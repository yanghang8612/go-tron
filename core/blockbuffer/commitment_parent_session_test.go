package blockbuffer

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func viewSessionBranch(t *testing.T, session pointread.CommitmentParentSession, reader int, prefix []byte) ([]byte, bool, bool) {
	t.Helper()
	var got []byte
	stable := false
	found, err := rawdb.ViewCommitmentParentBranchInSession(session, reader, prefix, func(value []byte, valueStable bool) error {
		got = append(got[:0], value...)
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
	deletedPrefix := []byte{3, 4}
	insertedPrefix := []byte{4, 5}
	for _, entry := range []struct {
		prefix []byte
		value  string
	}{
		{durablePrefix, "durable-before"},
		{overridePrefix, "override-before"},
		{deletedPrefix, "deleted-before"},
	} {
		if err := rawdb.WriteCommitmentBranch(disk, entry.prefix, []byte(entry.value)); err != nil {
			t.Fatal(err)
		}
	}

	buf := New(disk)
	buf.BeginBlock(bufHash(1), 1)
	h1, _ := buf.NewestInflight()
	v1 := buf.ViewLayer(h1)
	if err := rawdb.WriteCommitmentBranch(v1, overridePrefix, []byte("override-after")); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.DeleteCommitmentBranch(v1, deletedPrefix); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteCommitmentBranch(v1, insertedPrefix, []byte("inserted")); err != nil {
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

	// Move the captured overlay into the durable DB and then mutate a key that
	// was durable at capture time. The session must combine its retained layer
	// pointers with the older Pebble snapshot, not observe the later mutation.
	if err := buf.FlushUpTo(1, disk); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteCommitmentBranch(disk, durablePrefix, []byte("durable-after")); err != nil {
		t.Fatal(err)
	}

	if got, found, stable := viewSessionBranch(t, session, 2, overridePrefix); !found || !stable || !bytes.Equal(got, []byte("override-after")) {
		t.Fatalf("override = (%q,%v,stable=%v)", got, found, stable)
	}
	if got, found, _ := viewSessionBranch(t, session, 3, deletedPrefix); found || got != nil {
		t.Fatalf("deleted = (%q,%v), want missing", got, found)
	}
	if got, found, stable := viewSessionBranch(t, session, 4, insertedPrefix); !found || !stable || !bytes.Equal(got, []byte("inserted")) {
		t.Fatalf("inserted = (%q,%v,stable=%v)", got, found, stable)
	}
	if got, found, stable := viewSessionBranch(t, session, 1, durablePrefix); !found || stable || !bytes.Equal(got, []byte("durable-before")) {
		t.Fatalf("durable snapshot = (%q,%v,stable=%v)", got, found, stable)
	}
}

type blockingDelegateWriter struct {
	ethdb.KeyValueWriter
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingDelegateWriter) Put(key, value []byte) error {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return w.KeyValueWriter.Put(key, value)
}

func TestCommitmentParentReadSessionWaitsForActiveFlush(t *testing.T) {
	disk, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	buf := New(disk)
	prefix := []byte{7, 8}
	buf.BeginBlock(bufHash(1), 1)
	h1, _ := buf.NewestInflight()
	if err := rawdb.WriteCommitmentBranch(buf.ViewLayer(h1), prefix, []byte("flushed")); err != nil {
		t.Fatal(err)
	}
	if err := buf.CommitInflight(h1); err != nil {
		t.Fatal(err)
	}
	buf.BeginBlock(bufHash(2), 2)
	h2, _ := buf.NewestInflight()
	w := &blockingDelegateWriter{KeyValueWriter: disk, started: make(chan struct{}), release: make(chan struct{})}
	flushDone := make(chan error, 1)
	go func() { flushDone <- buf.FlushUpTo(1, w) }()
	<-w.started

	sessionDone := make(chan pointread.CommitmentParentSession, 1)
	errDone := make(chan error, 1)
	go func() {
		session, sessionErr := buf.ViewLayer(h2).NewCommitmentParentReadSession(17)
		sessionDone <- session
		errDone <- sessionErr
	}()
	select {
	case session := <-sessionDone:
		if session != nil {
			_ = session.Close()
		}
		close(w.release)
		<-flushDone
		t.Fatal("session captured while flushMu was held")
	case <-time.After(50 * time.Millisecond):
	}
	close(w.release)
	if err := <-flushDone; err != nil {
		t.Fatal(err)
	}
	session := <-sessionDone
	if err := <-errDone; err != nil || session == nil {
		t.Fatalf("session after flush = (%T,%v)", session, err)
	}
	defer session.Close()
	if got, found, stable := viewSessionBranch(t, session, 7, prefix); !found || stable || !bytes.Equal(got, []byte("flushed")) {
		t.Fatalf("post-flush snapshot = (%q,%v,stable=%v)", got, found, stable)
	}
}

func TestCommitmentParentReadSessionWarmsExistingCache(t *testing.T) {
	disk, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	prefix := []byte{9, 10, 11}
	if err := rawdb.WriteCommitmentBranch(disk, prefix, []byte("hot")); err != nil {
		t.Fatal(err)
	}
	buf := New(disk)
	buf.SetBaseReadCacheSize(1 << 20)
	buf.BeginBlock(bufHash(1), 1)
	h, _ := buf.NewestInflight()
	view := buf.ViewLayer(h)
	for attempt := 0; attempt < 3; attempt++ {
		session, err := view.NewCommitmentParentReadSession(17)
		if err != nil || session == nil {
			t.Fatalf("session %d = (%T,%v)", attempt, session, err)
		}
		got, found, stable := viewSessionBranch(t, session, 9, prefix)
		if !found || !bytes.Equal(got, []byte("hot")) {
			t.Fatalf("attempt %d = (%q,%v,stable=%v)", attempt, got, found, stable)
		}
		if attempt == 0 && stable {
			t.Fatal("first cold cursor value unexpectedly reported stable")
		}
		if attempt > 0 && !stable {
			t.Fatalf("attempt %d did not reuse admitted cache value", attempt)
		}
		impl := session.(*commitmentParentReadSession)
		if attempt == 2 && impl.cursors[9] != nil {
			t.Fatal("third read opened a cursor despite the warmed cache")
		}
		if err := session.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
