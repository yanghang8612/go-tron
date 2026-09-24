package blockbuffer

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

type gatedPebbleFlush struct {
	ethdb.KeyValueWriter
	batcher     ethdb.Batcher
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
	writeErr    error
}

type gatedPebbleFlushBatch struct {
	ethdb.Batch
	gate *gatedPebbleFlush
}

type failSecondPointFlush struct {
	target  ethdb.KeyValueWriter
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	puts    int
	err     error
}

func (w *failSecondPointFlush) Put(key, value []byte) error {
	w.puts++
	if w.puts == 2 {
		close(w.entered)
		<-w.release
		return w.err
	}
	return w.target.Put(key, value)
}

func (w *failSecondPointFlush) Delete(key []byte) error { return w.target.Delete(key) }
func (w *failSecondPointFlush) unblock()                { w.once.Do(func() { close(w.release) }) }

func (g *gatedPebbleFlush) NewBatch() ethdb.Batch {
	return &gatedPebbleFlushBatch{Batch: g.batcher.NewBatch(), gate: g}
}

func (g *gatedPebbleFlush) NewBatchWithSize(size int) ethdb.Batch {
	return &gatedPebbleFlushBatch{Batch: g.batcher.NewBatchWithSize(size), gate: g}
}

func (b *gatedPebbleFlushBatch) Write() error {
	b.gate.enterOnce.Do(func() { close(b.gate.entered) })
	<-b.gate.release
	if b.gate.writeErr != nil {
		return b.gate.writeErr
	}
	return b.Batch.Write()
}

func (g *gatedPebbleFlush) unblock() { g.releaseOnce.Do(func() { close(g.release) }) }

func newGatedPebbleFlush(db ethdb.KeyValueStore, writeErr error) *gatedPebbleFlush {
	return &gatedPebbleFlush{
		KeyValueWriter: db,
		batcher:        db,
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
		writeErr:       writeErr,
	}
}

func TestFlushConcurrentNewerBatchPreservesSnapshot(t *testing.T) {
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	buffer := New(db)
	buffer.SetBaseReadCacheSize(1 << 20)
	key := []byte("state-kv-latest-v2-shared")
	if err := db.Put(key, []byte("base")); err != nil {
		t.Fatal(err)
	}
	if _, err := buffer.GetNoCopyCached(key); err != nil {
		t.Fatal(err)
	}
	for number, value := range []string{"first", "second"} {
		buffer.BeginBlock(bufHash(byte(number+1)), uint64(number+1))
		if err := buffer.Put(key, []byte(value)); err != nil {
			t.Fatal(err)
		}
		buffer.CommitBlock()
	}
	old, err := buffer.NewReadSnapshotThrough(2)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()

	buffer.BeginBlock(bufHash(3), 3)
	batch := buffer.NewBatch().(*bufferBatch)
	laterKey := []byte("state-kv-latest-v2-later")
	if err := batch.Put(laterKey, []byte("later")); err != nil {
		t.Fatal(err)
	}
	buffer.CommitBlock()

	gate := newGatedPebbleFlush(db, nil)
	defer gate.unblock()
	flushDone := make(chan error, 1)
	go func() { flushDone <- buffer.FlushUpTo(2, gate) }()
	<-gate.entered // Real Pebble batch has been built; disk commit is paused.
	writeDone := make(chan error, 1)
	go func() {
		remaining, err := batch.WriteUpTo(3)
		if err == nil && remaining != 0 {
			err = fmt.Errorf("remaining ops = %d", remaining)
		}
		writeDone <- err
	}()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("newer committed-layer batch blocked behind unrelated flush")
	}
	if got, err := buffer.Get(laterKey); err != nil || string(got) != "later" {
		t.Fatalf("newer overlay before flush completes = (%q,%v)", got, err)
	}
	gate.unblock()
	if err := <-flushDone; err != nil {
		t.Fatal(err)
	}
	if got, err := old.Get(key); err != nil || string(got) != "second" {
		t.Fatalf("old snapshot after flush = (%q,%v)", got, err)
	}
	if _, err := old.Get(laterKey); err == nil {
		t.Fatal("old bounded snapshot saw newer layer")
	}
	current, err := buffer.NewReadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	for _, check := range []struct{ key, want string }{
		{string(key), "second"},
		{string(laterKey), "later"},
	} {
		if got, err := current.Get([]byte(check.key)); err != nil || string(got) != check.want {
			t.Fatalf("current snapshot %q = (%q,%v), want %q", check.key, got, err, check.want)
		}
	}
	if got, err := db.Get(key); err != nil || string(got) != "second" {
		t.Fatalf("durable value = (%q,%v)", got, err)
	}
	if _, err := db.Get(laterKey); err == nil {
		t.Fatal("newer layer was persisted below flush cutoff")
	}
	batch.Close()
}

func TestFlushMixedBatchWaitsBeforeAnyMutationAndReclassifies(t *testing.T) {
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	buffer := New(db)
	buffer.BeginBlock(bufHash(1), 1)
	batch := buffer.NewBatch().(*bufferBatch)
	if err := batch.PutOwnedKeyValue([]byte("frozen"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	buffer.CommitBlock()
	buffer.BeginBlock(bufHash(2), 2)
	if err := batch.PutOwnedKeyValue([]byte("later"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	buffer.CommitBlock()
	// Put the disjoint operation first: a one-pass implementation would apply
	// it before discovering the frozen target, violating batch atomicity.
	batch.ops[0], batch.ops[1] = batch.ops[1], batch.ops[0]
	gate := newGatedPebbleFlush(db, nil)
	defer gate.unblock()
	flushDone := make(chan error, 1)
	go func() { flushDone <- buffer.FlushUpTo(1, gate) }()
	<-gate.entered
	keptDone := make(chan error, 1)
	go func() {
		remaining, err := batch.WriteUpTo(0)
		if err == nil && remaining != 2 {
			err = fmt.Errorf("kept ops = %d, want 2", remaining)
		}
		keptDone <- err
	}()
	select {
	case err := <-keptDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unmatched ops waited for flush even though WriteUpTo kept them")
	}
	writeDone := make(chan error, 1)
	go func() {
		_, err := batch.WriteCommitted(false)
		writeDone <- err
	}()
	select {
	case err := <-writeDone:
		t.Fatalf("mixed batch returned before selected layer flushed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if _, err := buffer.Get([]byte("later")); err == nil {
		t.Fatal("disjoint op applied before frozen target reclassification")
	}
	laterShard := buffer.layers[1].shardForString("later")
	laterShard.mu.RLock()
	gotPending := laterShard.pendingOwnedPuts
	laterShard.mu.RUnlock()
	if gotPending != 1 {
		t.Fatalf("newer owned reservation = %d before wakeup, want 1", gotPending)
	}
	gate.unblock()
	if err := <-flushDone; err != nil {
		t.Fatal(err)
	}
	if err := <-writeDone; err == nil {
		t.Fatal("strict batch did not reject detached target after flush")
	}
	if len(batch.ops) != 2 {
		t.Fatalf("strict batch compacted ops on stale error: %d", len(batch.ops))
	}
	if _, err := buffer.Get([]byte("later")); err == nil {
		t.Fatal("strict batch partially applied newer operation")
	}
	remaining, err := batch.WriteCommitted(true)
	if err != nil || remaining != 0 {
		t.Fatalf("drop-stale retry = remaining %d, err %v", remaining, err)
	}
	if got, err := buffer.Get([]byte("later")); err != nil || string(got) != "new" {
		t.Fatalf("drop-stale retry newer value = (%q,%v)", got, err)
	}
	batch.Close()
}

func TestFlushFailureUnfreezesRetainedLayerForRetry(t *testing.T) {
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	buffer := New(db)
	buffer.SetBaseReadCacheSize(1 << 20)
	buffer.BeginBlock(bufHash(1), 1)
	batch := buffer.NewBatch().(*bufferBatch)
	if err := batch.Put([]byte("retry-key"), []byte("later-write")); err != nil {
		t.Fatal(err)
	}
	if err := buffer.Put([]byte("flush-key"), []byte("before-failure")); err != nil {
		t.Fatal(err)
	}
	buffer.CommitBlock()
	if err := buffer.FlushUpTo(0, db); err != nil {
		t.Fatalf("zero-eligible flush: %v", err)
	}
	forced := fmt.Errorf("forced flush batch failure")
	gate := newGatedPebbleFlush(db, forced)
	defer gate.unblock()
	flushDone := make(chan error, 1)
	go func() { flushDone <- buffer.FlushUpTo(1, gate) }()
	<-gate.entered
	writeDone := make(chan error, 1)
	go func() {
		remaining, err := batch.WriteUpTo(1)
		if err == nil && remaining != 0 {
			err = fmt.Errorf("remaining ops = %d", remaining)
		}
		writeDone <- err
	}()
	select {
	case err := <-writeDone:
		t.Fatalf("batch wrote to frozen layer before failed flush resolved: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	gate.unblock()
	if err := <-flushDone; err != forced {
		t.Fatalf("flush error = %v, want forced failure", err)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("retained-layer late write after failure: %v", err)
	}
	if err := buffer.FlushUpTo(1, db); err != nil {
		t.Fatalf("retry flush: %v", err)
	}
	if got, err := db.Get([]byte("retry-key")); err != nil || string(got) != "later-write" {
		t.Fatalf("retry durable value = (%q,%v)", got, err)
	}
	batch.Close()
}

func TestFlushPartialFailureDropsPrefixClearsCacheAndWakesBatch(t *testing.T) {
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	buffer := New(db)
	buffer.SetBaseReadCacheSize(1 << 20)
	firstKey := []byte("state-kv-latest-v2-first")
	secondKey := []byte("state-kv-latest-v2-second")
	if err := db.Put(firstKey, []byte("base-old")); err != nil {
		t.Fatal(err)
	}
	if _, err := buffer.GetNoCopyCached(firstKey); err != nil {
		t.Fatal(err)
	}
	buffer.BeginBlock(bufHash(1), 1)
	if err := buffer.Put(firstKey, []byte("first-new")); err != nil {
		t.Fatal(err)
	}
	buffer.CommitBlock()
	buffer.BeginBlock(bufHash(2), 2)
	if err := buffer.Put(secondKey, []byte("second-old")); err != nil {
		t.Fatal(err)
	}
	batch := buffer.NewBatch().(*bufferBatch)
	if err := batch.Put(secondKey, []byte("second-late")); err != nil {
		t.Fatal(err)
	}
	buffer.CommitBlock()

	forced := fmt.Errorf("second layer write failed")
	writer := &failSecondPointFlush{
		target: db, entered: make(chan struct{}), release: make(chan struct{}), err: forced,
	}
	defer writer.unblock()
	flushDone := make(chan error, 1)
	go func() { flushDone <- buffer.FlushUpTo(2, writer) }()
	<-writer.entered
	writeDone := make(chan error, 1)
	go func() {
		_, err := batch.WriteUpTo(2)
		writeDone <- err
	}()
	select {
	case err := <-writeDone:
		t.Fatalf("batch wrote into second frozen layer before failure: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	writer.unblock()
	if err := <-flushDone; err != forced {
		t.Fatalf("partial flush error = %v, want %v", err, forced)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("late write to retained layer = %v", err)
	}
	if len(buffer.layers) != 1 || buffer.layers[0].number != 2 {
		t.Fatalf("retained layers after partial failure = %v", buffer.PendingBlocks())
	}
	if _, found, _ := buffer.baseReadCache.getWithEpoch(firstKey); found {
		t.Fatal("partial failure retained promoted cache entry")
	}
	if got, err := buffer.Get(firstKey); err != nil || string(got) != "first-new" {
		t.Fatalf("first layer durable value = (%q,%v)", got, err)
	}
	if got, err := buffer.Get(secondKey); err != nil || string(got) != "second-late" {
		t.Fatalf("second retained overlay = (%q,%v)", got, err)
	}
	if err := buffer.FlushUpTo(2, db); err != nil {
		t.Fatalf("retry after partial failure = %v", err)
	}
	if got, err := db.Get(secondKey); err != nil || string(got) != "second-late" {
		t.Fatalf("second durable value after retry = (%q,%v)", got, err)
	}
	batch.Close()
}

// BenchmarkFlushConcurrentLatestBatch exercises the actual Pebble flush path:
// three overlapping committed layers are merged, sorted, written as a batch,
// and promoted into the base read cache while a range-owned latest-domain batch
// targets a newer committed layer. The timed operation is the foreground batch
// publication after the flush worker has entered FlushUpTo's critical section.
func BenchmarkFlushConcurrentLatestBatch(b *testing.B) {
	const (
		flushKeys = 20_000
		latestOps = 4_096
	)
	keys := make([][]byte, flushKeys)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("state-kv-latest-v2-bench-%08d", i))
	}
	value := bytes.Repeat([]byte{0x5a}, 128)
	var totalFlush time.Duration
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		db, err := rawdb.NewPebbleDB(b.TempDir(), 16, 16)
		if err != nil {
			b.Fatal(err)
		}
		buffer := New(db)
		buffer.SetBaseReadCacheSize(16 << 20)
		for i := 0; i < 2_048; i++ {
			if err := db.Put(keys[i], []byte("durable-before")); err != nil {
				b.Fatal(err)
			}
			if _, err := buffer.GetNoCopyCached(keys[i]); err != nil {
				b.Fatal(err)
			}
		}
		for number := uint64(1); number <= 3; number++ {
			buffer.BeginBlock(bufHash(byte(number)), number)
			for _, key := range keys {
				if err := buffer.Put(key, value); err != nil {
					b.Fatal(err)
				}
			}
			buffer.CommitBlock()
		}
		buffer.BeginBlock(bufHash(4), 4)
		batch := buffer.NewBatchWithSize(latestOps * 64).(*bufferBatch)
		for i := 0; i < latestOps; i++ {
			key := []byte(fmt.Sprintf("state-kv-latest-v2-foreground-%08d", i))
			if err := batch.Put(key, value); err != nil {
				b.Fatal(err)
			}
		}
		buffer.CommitBlock()

		flushDone := make(chan error, 1)
		flushStarted := time.Now()
		go func() { flushDone <- buffer.FlushUpTo(3, db) }()
		// Observe the flush worker holding the shared flush gate. This is a
		// rendezvous only; the measured path still performs real merge/batch/cache
		// work on Pebble and has no artificial hold or sleep.
		deadline := time.Now().Add(10 * time.Second)
		for {
			if !buffer.flushMu.TryLock() {
				break
			}
			buffer.flushMu.Unlock()
			if time.Now().After(deadline) {
				b.Fatal("flush worker did not enter FlushUpTo")
			}
			runtime.Gosched()
		}
		b.StartTimer()
		remaining, err := batch.WriteUpTo(4)
		b.StopTimer()
		if err != nil || remaining != 0 {
			b.Fatalf("WriteUpTo(4) = remaining %d, err %v", remaining, err)
		}
		if err := <-flushDone; err != nil {
			b.Fatal(err)
		}
		totalFlush += time.Since(flushStarted)
		batch.Close()
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(totalFlush.Milliseconds())/float64(b.N), "flush-ms/op")
}

// BenchmarkLatestBatchMostlyKeptNoFlush checks the hot path when a large
// range-owned batch contains only newer operations, with no concurrent flush.
// It guards against an extra preflight scan proportional to the queued batch.
func BenchmarkLatestBatchMostlyKeptNoFlush(b *testing.B) {
	const ops = 80_000
	buffer := New(rawdb.NewMemoryDatabase())
	for number := uint64(1); number <= 20; number++ {
		buffer.BeginBlock(bufHash(byte(number)), number)
		buffer.CommitBlock()
	}
	target := buffer.layers[len(buffer.layers)-1]
	keys := make([]string, ops)
	for i := range keys {
		keys[i] = fmt.Sprintf("state-kv-latest-v2-pending-%08d", i)
	}
	value := []byte("pending")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		batch := &bufferBatch{parent: buffer, ops: make([]bufferBatchOp, ops)}
		for j, key := range keys {
			batch.ops[j] = bufferBatchOp{key: key, value: value, target: target}
		}
		b.StartTimer()
		remaining, err := batch.WriteUpTo(19)
		b.StopTimer()
		if err != nil || remaining != ops {
			b.Fatalf("WriteUpTo(19) = remaining %d, err %v", remaining, err)
		}
		batch.Close()
	}
}
