package etl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"reflect"
	"slices"
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

func TestCollectorPutCopiesAndPutOwnedTransfers(t *testing.T) {
	collector := newTestCollector(t, Options{BufferLimit: 1 << 20})
	defer collector.Close()

	key, value := []byte("a"), []byte("one")
	if err := collector.Put(key, value); err != nil {
		t.Fatal(err)
	}
	key[0], value[0] = 'z', 'x'

	ownedKey, ownedValue := []byte("b"), []byte("two")
	if err := collector.PutOwned(ownedKey, ownedValue); err != nil {
		t.Fatal(err)
	}
	if &collector.rows[1].key[0] != &ownedKey[0] || &collector.rows[1].value[0] != &ownedValue[0] {
		t.Fatal("PutOwned copied transferred slices")
	}

	writer := newRecordingWriter()
	if _, err := collector.Load(writer); err != nil {
		t.Fatal(err)
	}
	assertValue(t, writer, "a", "one")
	assertValue(t, writer, "b", "two")
}

func TestCollectorPutEncodedUsesStableArenaStorage(t *testing.T) {
	collector := newTestCollector(t, Options{BufferLimit: 1 << 20})
	defer collector.Close()

	var exactStorage []byte
	if err := collector.PutEncodedKeyAsValue(3, func(key []byte) {
		exactStorage = key
		copy(key, "one")
	}); err != nil {
		t.Fatal(err)
	}
	if &collector.rows[0].key[0] != &exactStorage[0] || &collector.rows[0].value[0] != &exactStorage[0] {
		t.Fatal("PutEncodedKeyAsValue did not retain its single arena view")
	}

	var encodedKey, encodedValue []byte
	if err := collector.PutEncoded(1, 3, func(key, value []byte) {
		encodedKey, encodedValue = key, value
		copy(key, "b")
		copy(value, "two")
	}); err != nil {
		t.Fatal(err)
	}
	if &collector.rows[1].key[0] != &encodedKey[0] || &collector.rows[1].value[0] != &encodedValue[0] {
		t.Fatal("PutEncoded did not retain its arena views")
	}
	writer := newRecordingWriter()
	if _, err := collector.Load(writer); err != nil {
		t.Fatal(err)
	}
	assertValue(t, writer, "b", "two")
	assertValue(t, writer, "one", "one")
}

func TestCollectorPutEncodedReusesArenaAcrossSpills(t *testing.T) {
	collector := newTestCollector(t, Options{BufferLimit: 1})
	defer collector.Close()

	var first, second *byte
	if err := collector.PutEncoded(1, 1, func(key, value []byte) {
		first = &key[0]
		key[0], value[0] = 'a', '1'
	}); err != nil {
		t.Fatal(err)
	}
	if err := collector.PutEncoded(1, 1, func(key, value []byte) {
		second = &key[0]
		key[0], value[0] = 'b', '2'
	}); err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("collector replaced arena storage between successful spills")
	}

	writer := newRecordingWriter()
	if _, err := collector.Load(writer); err != nil {
		t.Fatal(err)
	}
	assertValue(t, writer, "a", "1")
	assertValue(t, writer, "b", "2")
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

func TestCollectorSpilledRunRejectsTruncatedHeader(t *testing.T) {
	collector := newTestCollector(t, Options{BufferLimit: 1})
	defer collector.Close()

	mustPut(t, collector, "a", "value-a")
	if len(collector.runFiles) != 1 {
		t.Fatalf("spilled runs = %d, want 1", len(collector.runFiles))
	}
	if err := os.Truncate(collector.runFiles[0], int64(len(runFileMagic)+3)); err != nil {
		t.Fatal(err)
	}
	if _, err := collector.Load(newRecordingWriter()); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated run load err = %v, want %v", err, io.ErrUnexpectedEOF)
	}
}

func TestCollectorSpilledTournamentMatchesInMemory(t *testing.T) {
	inMemory := newTestCollector(t, Options{BufferLimit: 64 << 20})
	spilled := newTestCollector(t, Options{BufferLimit: 16 << 10})
	defer inMemory.Close()
	defer spilled.Close()

	rng := rand.New(rand.NewSource(317))
	for i := 0; i < 20_000; i++ {
		key := []byte(fmt.Sprintf("shared-prefix-%02d-%04d", rng.Intn(16), rng.Intn(2_000)))
		if rng.Intn(7) == 0 {
			if err := inMemory.Delete(key); err != nil {
				t.Fatal(err)
			}
			if err := spilled.Delete(key); err != nil {
				t.Fatal(err)
			}
			continue
		}
		value := make([]byte, rng.Intn(96))
		_, _ = rng.Read(value)
		if err := inMemory.Put(key, value); err != nil {
			t.Fatal(err)
		}
		if err := spilled.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}

	wantWriter, gotWriter := newRecordingWriter(), newRecordingWriter()
	wantStats, err := inMemory.Load(wantWriter)
	if err != nil {
		t.Fatal(err)
	}
	gotStats, err := spilled.Load(gotWriter)
	if err != nil {
		t.Fatal(err)
	}
	if gotStats.SpilledRuns < 2 {
		t.Fatalf("spilled runs = %d, want multiple tournament leaves", gotStats.SpilledRuns)
	}
	if gotStats.Applied != wantStats.Applied || gotStats.AppliedPuts != wantStats.AppliedPuts || gotStats.AppliedDeletes != wantStats.AppliedDeletes {
		t.Fatalf("spilled applied stats = %+v, in-memory = %+v", gotStats, wantStats)
	}
	if !reflect.DeepEqual(gotWriter.ops, wantWriter.ops) || !reflect.DeepEqual(gotWriter.data, wantWriter.data) {
		t.Fatal("spilled tournament output differs from in-memory ordering/collapse")
	}
}

func TestCollectorRetainsEntryBufferAcrossSpills(t *testing.T) {
	collector := newTestCollector(t, Options{BufferLimit: 1})
	defer collector.Close()

	mustPut(t, collector, "a", "value-a")
	if collector.rowBuffer == nil || len(collector.rows) != 0 || cap(collector.rows) == 0 {
		t.Fatalf("entry buffer after spill = ptr %p len %d cap %d", collector.rowBuffer, len(collector.rows), cap(collector.rows))
	}
	buffer := collector.rowBuffer
	capacity := cap(collector.rows)
	mustPut(t, collector, "b", "value-b")
	if collector.rowBuffer != buffer {
		t.Fatal("collector replaced its entry buffer between spills")
	}
	if len(collector.rows) != 0 || cap(collector.rows) != capacity {
		t.Fatalf("entry buffer after second spill = len %d cap %d, want len 0 cap %d", len(collector.rows), cap(collector.rows), capacity)
	}
}

func TestSortedEntryOrderUsesKeyThenSequenceAndResetsPool(t *testing.T) {
	entries := []entry{
		{key: []byte("b"), seq: 1},
		{key: []byte("a"), seq: 2},
		{key: []byte("a"), seq: 1},
	}
	order, err := sortedEntryOrder(entries)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := append([]uint32(nil), (*order)...), []uint32{2, 1, 0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sorted row order = %v, want %v", got, want)
	}
	retained := order
	releaseEntryOrder(&order)
	if order != nil {
		t.Fatal("released sort order still reachable through caller")
	}
	if len(*retained) != 0 {
		t.Fatalf("released sort order retained length %d", len(*retained))
	}
	order, err = sortedEntryOrder(entries[:1])
	if err != nil {
		t.Fatal(err)
	}
	defer releaseEntryOrder(&order)
	if len(*order) != 1 || (*order)[0] != 0 {
		t.Fatalf("reacquired sort order = %v, want [0]", *order)
	}
}

func TestSortedEntryOrderRadixMatchesComparisonOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(91))
	entries := make([]entry, 10_000)
	sequences := rng.Perm(len(entries))
	for i := range entries {
		keyLen := rng.Intn(80)
		entries[i].key = make([]byte, keyLen)
		_, _ = rng.Read(entries[i].key)
		if i > 0 && i%7 == 0 {
			entries[i].key = append(entries[i].key[:0], entries[i-1].key...)
		}
		entries[i].seq = uint64(sequences[i] + 1)
	}
	want := make([]uint32, len(entries))
	for i := range want {
		want[i] = uint32(i)
	}
	sortEntryOrderComparison(want, entries)
	got, err := sortedEntryOrder(entries)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseEntryOrder(&got)
	if !slices.Equal(*got, want) {
		for i := range want {
			if (*got)[i] != want[i] {
				t.Fatalf("radix order differs at %d: got row %d key=%x seq=%d, want row %d key=%x seq=%d", i, (*got)[i], entries[(*got)[i]].key, entries[(*got)[i]].seq, want[i], entries[want[i]].key, entries[want[i]].seq)
			}
		}
	}
}

func TestSortedEntryOrderRadixOrdersLargeDuplicateGroupBySequence(t *testing.T) {
	entries := make([]entry, 512)
	for i := range entries {
		entries[i] = entry{key: []byte("same-long-snapshot-accessor-key"), seq: uint64(len(entries) - i)}
	}
	order, err := sortedEntryOrder(entries)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseEntryOrder(&order)
	for i, row := range *order {
		if got, want := entries[row].seq, uint64(i+1); got != want {
			t.Fatalf("duplicate key sequence at %d = %d, want %d", i, got, want)
		}
	}
}

func TestSortedEntryOrderRadixLongSharedPrefixMatchesComparison(t *testing.T) {
	rng := rand.New(rand.NewSource(441))
	entries := make([]entry, 20_000)
	sequences := rng.Perm(len(entries))
	for i := range entries {
		key := bytes.Repeat([]byte{0x5a}, 32+rng.Intn(64))
		_, _ = rng.Read(key[32:])
		if i > 0 && i%11 == 0 {
			key = append(key[:0], entries[i-1].key...)
		}
		entries[i] = entry{key: key, seq: uint64(sequences[i] + 1)}
	}
	want := make([]uint32, len(entries))
	for i := range want {
		want[i] = uint32(i)
	}
	sortEntryOrderComparison(want, entries)
	got, err := sortedEntryOrder(entries)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseEntryOrder(&got)
	if !slices.Equal(*got, want) {
		t.Fatal("shared-prefix radix order differs from comparison order")
	}
}

func BenchmarkSortedEntryOrder(b *testing.B) {
	for _, size := range []int{10_000, 250_000} {
		b.Run(fmt.Sprintf("rows=%d", size), func(b *testing.B) {
			entries := make([]entry, size)
			for i := range entries {
				key := make([]byte, 49)
				key[0] = byte(i % 9)
				binary.BigEndian.PutUint64(key[1:9], uint64((i*104729)%size))
				copy(key[9:], bytes.Repeat([]byte{byte(i % 251)}, 40))
				entries[i] = entry{key: key, seq: uint64(i + 1)}
			}
			for _, algorithm := range []string{"comparison", "radix"} {
				b.Run(algorithm, func(b *testing.B) {
					b.ReportAllocs()
					for range b.N {
						if algorithm == "radix" {
							order, err := sortedEntryOrder(entries)
							if err != nil {
								b.Fatal(err)
							}
							releaseEntryOrder(&order)
							continue
						}
						order := make([]uint32, len(entries))
						for i := range order {
							order[i] = uint32(i)
						}
						sortEntryOrderComparison(order, entries)
					}
				})
			}
		})
	}
}

func BenchmarkSortedEntryOrderSharedPrefix(b *testing.B) {
	const rows = 250_000
	entries := make([]entry, rows)
	for i := range entries {
		key := bytes.Repeat([]byte{0x5a}, 49)
		binary.BigEndian.PutUint64(key[32:40], uint64((i*104729)%rows))
		key[48] = byte(i % 251)
		entries[i] = entry{key: key, seq: uint64(i + 1)}
	}
	for _, algorithm := range []string{"comparison", "radix"} {
		b.Run(algorithm, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if algorithm == "radix" {
					order, err := sortedEntryOrder(entries)
					if err != nil {
						b.Fatal(err)
					}
					releaseEntryOrder(&order)
					continue
				}
				order := make([]uint32, len(entries))
				for i := range order {
					order[i] = uint32(i)
				}
				sortEntryOrderComparison(order, entries)
			}
		})
	}
}

func BenchmarkCollectorSpilledRunMerge(b *testing.B) {
	const rows = 250_000
	collector := newTestCollector(b, Options{BufferLimit: 600_000})
	b.Cleanup(func() { _ = collector.Close() })
	for i := 0; i < rows; i++ {
		key := make([]byte, 49)
		key[0] = byte(i % 9)
		binary.BigEndian.PutUint64(key[1:9], uint64((i*104729)%rows))
		copy(key[9:], bytes.Repeat([]byte{byte(i % 251)}, 40))
		value := make([]byte, 16)
		binary.BigEndian.PutUint64(value, uint64(i))
		if err := collector.PutOwned(key, value); err != nil {
			b.Fatal(err)
		}
	}
	if err := collector.spillBuffer(); err != nil {
		b.Fatal(err)
	}
	runCount := len(collector.runFiles)
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(runCount), "runs")
	for range b.N {
		if err := collector.mergeRuns(newApplier(discardWriter{}, 1<<20), nil); err != nil {
			b.Fatal(err)
		}
	}
}

type discardWriter struct{}

func (discardWriter) Put([]byte, []byte) error { return nil }
func (discardWriter) Delete([]byte) error      { return nil }

func TestCollectorSuccessfulLoadReleasesEntryBuffer(t *testing.T) {
	collector := newTestCollector(t, Options{BufferLimit: 1 << 20})
	defer collector.Close()

	key, value := []byte("a"), []byte("value-a")
	if err := collector.PutOwned(key, value); err != nil {
		t.Fatal(err)
	}
	buffer := collector.rowBuffer
	if buffer == nil {
		t.Fatal("collector did not acquire an entry buffer")
	}
	if _, err := collector.Load(newRecordingWriter()); err != nil {
		t.Fatal(err)
	}
	if collector.rowBuffer != nil || collector.rows != nil {
		t.Fatalf("loaded collector retained entry buffer: ptr %p rows %v", collector.rowBuffer, collector.rows)
	}
	entries := (*buffer)[:cap(*buffer)]
	if entries[0].key != nil || entries[0].value != nil {
		t.Fatal("released entry buffer retained key/value references")
	}
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

func newTestCollector(t testing.TB, opts Options) *Collector {
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
