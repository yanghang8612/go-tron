package blockbuffer

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestHistoryPrefixSettledChecksEveryPendingLayer(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	defer func() { _ = base.Close() }()
	b := New(base)
	if (*Buffer)(nil).HistoryPrefixSettled(0) || (&Buffer{}).HistoryPrefixSettled(0) || !b.HistoryPrefixSettled(^uint64(0)) {
		t.Fatal("nil/missing/empty topology handling changed")
	}
	b.SetMaxInflight(3)
	b.BeginBlock(bufHash(10), 10)
	b.CommitBlock()
	b.BeginBlock(bufHash(20), 20)
	b.BeginBlock(bufHash(30), 30)
	if !b.HistoryPrefixSettled(9) || b.HistoryPrefixSettled(10) || b.HistoryPrefixSettled(25) {
		t.Fatal("committed/inflight boundary was not inclusive")
	}
	if err := b.FlushUpTo(10, base); err != nil {
		t.Fatal(err)
	}
	if !b.HistoryPrefixSettled(19) || b.HistoryPrefixSettled(20) {
		t.Fatal("inflight oldest layer was ignored after committed prefix dropped")
	}
	// Promotion must not create a gap in which the same layer is absent from
	// both topology slices; the check remains false on either side of Commit.
	b.CommitBlock()
	if b.HistoryPrefixSettled(20) {
		t.Fatal("promotion lost the unsettled boundary")
	}
	// Scan all entries, even if topology ordering is unexpectedly broken.
	b.mu.Lock()
	b.layers = append(b.layers, &layer{owner: b, state: layerCommitted, blockHash: bufHash(5), number: 5})
	b.mu.Unlock()
	if b.HistoryPrefixSettled(9) {
		t.Fatal("only the first committed entry was checked")
	}
}

func TestHistoryPrefixSettledRejectsMalformedLayerMetadata(t *testing.T) {
	for _, location := range []string{"committed", "inflight"} {
		for _, malformed := range []string{"nil", "owner-missing", "other-owner", "state", "hash-missing", "lower-later"} {
			t.Run(location+"/"+malformed, func(t *testing.T) {
				base := rawdb.NewMemoryDatabase()
				defer func() { _ = base.Close() }()
				b := New(base)
				state := layerCommitted
				if location == "inflight" {
					state = layerInflight
				}
				first := &layer{owner: b, state: state, blockHash: bufHash(20), number: 20}
				bad := &layer{owner: b, state: state, blockHash: bufHash(30), number: 30}
				switch malformed {
				case "nil":
					bad = nil
				case "owner-missing":
					bad.owner = nil
				case "other-owner":
					bad.owner = New(base)
				case "state":
					bad.state = layerDetached
				case "hash-missing":
					bad.blockHash = [32]byte{}
				case "lower-later":
					bad.number = 5
				}
				if location == "inflight" {
					b.inflight = []*layer{first, bad}
				} else {
					b.layers = []*layer{first, bad}
				}
				if b.HistoryPrefixSettled(10) {
					t.Fatal("malformed topology admitted history deletion")
				}
			})
		}
	}
}

type historyPrefixVisibleBatcher struct {
	ethdb.KeyValueStore
	visible  chan struct{}
	release  chan struct{}
	once     sync.Once
	writeErr error
}

func (s *historyPrefixVisibleBatcher) NewBatch() ethdb.Batch {
	return &historyPrefixVisibleBatch{Batch: s.KeyValueStore.NewBatch(), store: s}
}
func (s *historyPrefixVisibleBatcher) NewBatchWithSize(size int) ethdb.Batch {
	return &historyPrefixVisibleBatch{Batch: s.KeyValueStore.NewBatchWithSize(size), store: s}
}

type historyPrefixVisibleBatch struct {
	ethdb.Batch
	store *historyPrefixVisibleBatcher
}

func (b *historyPrefixVisibleBatch) Write() error {
	if err := b.Batch.Write(); err != nil {
		return err
	}
	b.store.once.Do(func() {
		close(b.store.visible)
		<-b.store.release
	})
	return b.store.writeErr
}

func TestHistoryPrefixSettledRejectsVisibleButUnfinishedFlush(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "ambiguous-error"}[fail], func(t *testing.T) {
			base := rawdb.NewMemoryDatabase()
			defer func() { _ = base.Close() }()
			b := New(base)
			b.BeginBlock(bufHash(10), 10)
			if err := b.Put([]byte("history-row"), []byte("old")); err != nil {
				t.Fatal(err)
			}
			b.CommitBlock()
			store := &historyPrefixVisibleBatcher{KeyValueStore: base, visible: make(chan struct{}), release: make(chan struct{})}
			boom := errors.New("applied batch returned failure")
			if fail {
				store.writeErr = boom
			}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(store.release) }) }
			defer release()
			done := make(chan error, 1)
			go func() { done <- b.FlushUpTo(10, store) }()
			select {
			case <-store.visible:
			case <-time.After(2 * time.Second):
				t.Fatal("flush did not enter batch Write")
			}
			if value, err := base.Get([]byte("history-row")); err != nil || string(value) != "old" {
				t.Fatalf("test did not establish already-visible base row: %q %v", value, err)
			}
			// FlushUpTo currently owns flushMu. A read-only prefix check must
			// return without waiting for its disk-write/drop completion.
			checked := make(chan [2]bool, 1)
			go func() { checked <- [2]bool{b.HistoryPrefixSettled(9), b.HistoryPrefixSettled(10)} }()
			select {
			case got := <-checked:
				if got != [2]bool{true, false} {
					t.Fatalf("visible-but-pending prefix = %v", got)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("prefix check waited for flushMu or disk I/O")
			}
			// Concurrent new work above H does not obscure the old flush layer.
			b.BeginBlock(bufHash(20), 20)
			if b.HistoryPrefixSettled(10) {
				t.Fatal("new inflight layer hid the old flush")
			}
			release()
			if err := <-done; !errors.Is(err, store.writeErr) {
				t.Fatalf("flush error = %v want %v", err, store.writeErr)
			}
			if got := b.HistoryPrefixSettled(10); got != !fail {
				t.Fatal("failed retry layer was lost or successful prefix stayed pending")
			}
			if fail {
				store.writeErr = nil
				if err := b.FlushUpTo(10, store); err != nil {
					t.Fatal(err)
				}
				if !b.HistoryPrefixSettled(10) {
					t.Fatal("successful retry did not settle the old prefix")
				}
			}
			if b.HistoryPrefixSettled(20) {
				t.Fatal("new inflight layer was drained by a read-only check")
			}
		})
	}
}

type historyPrefixFailSecondWriter struct {
	ethdb.KeyValueWriter
	puts int
	err  error
}

func (w *historyPrefixFailSecondWriter) Put(key, value []byte) error {
	w.puts++
	if w.puts == 2 {
		return w.err
	}
	return w.KeyValueWriter.Put(key, value)
}

func TestHistoryPrefixSettledPartialFlushAndDetachedBatch(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	defer func() { _ = base.Close() }()
	b := New(base)
	for _, height := range []uint64{10, 20} {
		b.BeginBlock(bufHash(byte(height)), height)
		if err := b.Put([]byte{byte(height)}, []byte("original")); err != nil {
			t.Fatal(err)
		}
		b.CommitBlock()
	}
	boom := errors.New("second layer failed")
	w := &historyPrefixFailSecondWriter{KeyValueWriter: base, err: boom}
	if err := b.FlushUpTo(20, w); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	if !b.HistoryPrefixSettled(10) || b.HistoryPrefixSettled(20) {
		t.Fatal("partial success did not retain precisely the retry suffix")
	}
	if err := b.FlushUpTo(20, base); err != nil {
		t.Fatal(err)
	}
	b.BeginBlock(bufHash(30), 30)
	batch := b.NewBatch()
	defer batch.Close()
	if err := batch.Put([]byte("delayed-history"), []byte("old batch")); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()
	if err := b.FlushUpTo(30, base); err != nil {
		t.Fatal(err)
	}
	if !b.HistoryPrefixSettled(30) {
		t.Fatal("flushed layer remained pending")
	}
	if err := batch.Write(); err == nil {
		t.Fatal("detached delayed batch was permitted to recreate old history")
	}
	if has, err := base.Has([]byte("delayed-history")); err != nil || has {
		t.Fatalf("detached batch reached base: has=%v err=%v", has, err)
	}
}
