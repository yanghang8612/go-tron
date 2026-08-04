package etl

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
)

func TestCollectorLoadSortsAndCollapsesLatestOperation(t *testing.T) {
	collector := newTestCollector(t, Options{BufferLimit: 1 << 20})
	defer collector.Close()

	mustPut(t, collector, "c", "old-c")
	mustPut(t, collector, "a", "old-a")
	mustPut(t, collector, "b", "value-b")
	mustPut(t, collector, "a", "value-a")
	mustDelete(t, collector, "c")
	mustPut(t, collector, "c", "value-c")

	writer := newRecordingWriter()
	stats, err := collector.Load(writer)
	if err != nil {
		t.Fatal(err)
	}

	if stats.Collected != 6 || stats.Applied != 3 || stats.AppliedPuts != 3 || stats.AppliedDeletes != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if stats.SpilledRuns != 0 {
		t.Fatalf("in-memory load spilled %d runs, want none", stats.SpilledRuns)
	}
	if got, want := writer.ops, []string{"put:a", "put:b", "put:c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ops = %v, want %v", got, want)
	}
	assertValue(t, writer, "a", "value-a")
	assertValue(t, writer, "b", "value-b")
	assertValue(t, writer, "c", "value-c")
}

func TestCollectorSpillsRunsAndMergesDeletes(t *testing.T) {
	collector := newTestCollector(t, Options{BufferLimit: 1})
	defer collector.Close()

	mustPut(t, collector, "b", "old-b")
	mustPut(t, collector, "a", "value-a")
	mustDelete(t, collector, "b")
	mustPut(t, collector, "c", "value-c")

	writer := newRecordingWriter()
	stats, err := collector.Load(writer)
	if err != nil {
		t.Fatal(err)
	}

	if stats.SpilledRuns < 4 {
		t.Fatalf("spilled runs = %d, want at least 4", stats.SpilledRuns)
	}
	if stats.Applied != 3 || stats.AppliedPuts != 2 || stats.AppliedDeletes != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if got, want := writer.ops, []string{"put:a", "delete:b", "put:c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ops = %v, want %v", got, want)
	}
	assertValue(t, writer, "a", "value-a")
	assertMissing(t, writer, "b")
	assertValue(t, writer, "c", "value-c")
}

func TestCollectorMergesSpilledRunsWithFinalBuffer(t *testing.T) {
	collector := newTestCollector(t, Options{BufferLimit: 50})
	defer collector.Close()

	mustPut(t, collector, "a", "old-a")
	mustPut(t, collector, "b", "value-b")
	mustPut(t, collector, "c", "value-c")
	if len(collector.runFiles) == 0 {
		t.Fatal("collector did not spill its bounded prefix")
	}
	mustPut(t, collector, "a", "new-a")

	writer := newRecordingWriter()
	stats, err := collector.Load(writer)
	if err != nil {
		t.Fatal(err)
	}
	if stats.SpilledRuns < 2 {
		t.Fatalf("spilled runs = %d, want prefix plus final buffer", stats.SpilledRuns)
	}
	if got, want := writer.ops, []string{"put:a", "put:b", "put:c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ops = %v, want %v", got, want)
	}
	assertValue(t, writer, "a", "new-a")
}

func TestCollectorUsesBatchesWhenAvailable(t *testing.T) {
	collector := newTestCollector(t, Options{BufferLimit: 1 << 20, BatchSize: 4})
	defer collector.Close()

	mustPut(t, collector, "a", "1234")
	mustPut(t, collector, "b", "5678")
	mustDelete(t, collector, "c")

	writer := newRecordingBatcher()
	stats, err := collector.Load(writer)
	if err != nil {
		t.Fatal(err)
	}
	if stats.BatchWrites < 2 {
		t.Fatalf("batch writes = %d, want at least 2", stats.BatchWrites)
	}
	if writer.batchWrites != stats.BatchWrites {
		t.Fatalf("writer batch writes = %d, stats = %d", writer.batchWrites, stats.BatchWrites)
	}
	assertValue(t, &writer.recordingWriter, "a", "1234")
	assertValue(t, &writer.recordingWriter, "b", "5678")
	assertMissing(t, &writer.recordingWriter, "c")
}

func TestCollectorLoadInterruptibleRemainsRetryable(t *testing.T) {
	collector := newTestCollector(t, Options{BufferLimit: 1 << 20})
	defer collector.Close()
	mustPut(t, collector, "b", "value-b")
	mustPut(t, collector, "a", "value-a")

	polls := 0
	writer := newRecordingWriter()
	if _, err := collector.LoadInterruptible(writer, func() bool {
		polls++
		return polls > 1
	}); !errors.Is(err, ErrLoadInterrupted) {
		t.Fatalf("interruptible load err = %v, want %v", err, ErrLoadInterrupted)
	}
	if len(writer.ops) != 0 {
		t.Fatalf("interrupted load operations = %v, want none", writer.ops)
	}
	stats, err := collector.Load(writer)
	if err != nil {
		t.Fatalf("retry load: %v", err)
	}
	if stats.Applied != 2 {
		t.Fatalf("retry applied = %d, want 2", stats.Applied)
	}
	if got, want := writer.ops, []string{"put:a", "put:b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("retry ops = %v, want %v", got, want)
	}
}

func TestCollectorLifecycle(t *testing.T) {
	collector := newTestCollector(t, Options{BufferLimit: 1})
	dir := collector.TempDir()
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("temp dir stat: %v", err)
	}
	mustPut(t, collector, "a", "value-a")
	if _, err := collector.Load(newRecordingWriter()); err != nil {
		t.Fatal(err)
	}
	if err := collector.Put([]byte("b"), []byte("value-b")); !errors.Is(err, ErrCollectorLoaded) {
		t.Fatalf("put after load err = %v, want %v", err, ErrCollectorLoaded)
	}
	if err := collector.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp dir stat after close err = %v, want not exist", err)
	}
	if err := collector.Delete([]byte("c")); !errors.Is(err, ErrCollectorClosed) {
		t.Fatalf("delete after close err = %v, want %v", err, ErrCollectorClosed)
	}
}

func newTestCollector(t *testing.T, opts Options) *Collector {
	t.Helper()
	opts.TempDir = t.TempDir()
	collector, err := NewCollector(opts)
	if err != nil {
		t.Fatal(err)
	}
	return collector
}

func mustPut(t *testing.T, c *Collector, key, value string) {
	t.Helper()
	if err := c.Put([]byte(key), []byte(value)); err != nil {
		t.Fatal(err)
	}
}

func mustDelete(t *testing.T, c *Collector, key string) {
	t.Helper()
	if err := c.Delete([]byte(key)); err != nil {
		t.Fatal(err)
	}
}

func assertValue(t *testing.T, writer *recordingWriter, key, want string) {
	t.Helper()
	got, ok := writer.data[key]
	if !ok {
		t.Fatalf("key %q missing", key)
	}
	if string(got) != want {
		t.Fatalf("key %q = %q, want %q", key, string(got), want)
	}
}

func assertMissing(t *testing.T, writer *recordingWriter, key string) {
	t.Helper()
	if _, ok := writer.data[key]; ok {
		t.Fatalf("key %q present, want missing", key)
	}
}

type recordingWriter struct {
	data map[string][]byte
	ops  []string
}

func newRecordingWriter() *recordingWriter {
	return &recordingWriter{data: make(map[string][]byte)}
}

func (w *recordingWriter) Put(key, value []byte) error {
	w.ops = append(w.ops, "put:"+string(key))
	w.data[string(key)] = append([]byte(nil), value...)
	return nil
}

func (w *recordingWriter) Delete(key []byte) error {
	w.ops = append(w.ops, "delete:"+string(key))
	delete(w.data, string(key))
	return nil
}

type recordingBatcher struct {
	recordingWriter
	batchWrites uint64
}

func newRecordingBatcher() *recordingBatcher {
	return &recordingBatcher{recordingWriter: *newRecordingWriter()}
}

func (w *recordingBatcher) NewBatch() ethdb.Batch {
	return w.NewBatchWithSize(0)
}

func (w *recordingBatcher) NewBatchWithSize(_ int) ethdb.Batch {
	return &recordingBatch{parent: w}
}

type recordingBatch struct {
	parent *recordingBatcher
	ops    []recordedOp
	size   int
}

type recordedOp struct {
	delete bool
	key    []byte
	value  []byte
}

func (b *recordingBatch) Put(key, value []byte) error {
	b.ops = append(b.ops, recordedOp{
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
	})
	b.size += len(key) + len(value)
	return nil
}

func (b *recordingBatch) Delete(key []byte) error {
	b.ops = append(b.ops, recordedOp{
		delete: true,
		key:    append([]byte(nil), key...),
	})
	b.size += len(key)
	return nil
}

func (b *recordingBatch) DeleteRange(_, _ []byte) error {
	return fmt.Errorf("delete range unsupported")
}

func (b *recordingBatch) ValueSize() int {
	return b.size
}

func (b *recordingBatch) Write() error {
	for _, op := range b.ops {
		if op.delete {
			if err := b.parent.Delete(op.key); err != nil {
				return err
			}
		} else if err := b.parent.Put(op.key, op.value); err != nil {
			return err
		}
	}
	b.parent.batchWrites++
	return nil
}

func (b *recordingBatch) Reset() {
	b.ops = nil
	b.size = 0
}

func (b *recordingBatch) Replay(w ethdb.KeyValueWriter) error {
	for _, op := range b.ops {
		if op.delete {
			if err := w.Delete(op.key); err != nil {
				return err
			}
		} else if err := w.Put(op.key, op.value); err != nil {
			return err
		}
	}
	return nil
}

func (b *recordingBatch) Close() {}
