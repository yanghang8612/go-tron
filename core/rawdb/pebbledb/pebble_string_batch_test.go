package pebbledb

import (
	"bytes"
	"testing"
)

func TestBatchStringKeyOperationsCopyIntoBatch(t *testing.T) {
	db, err := New(t.TempDir(), 16, 16, "string-batch-test", false, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	deleteKey := "delete-key"
	if err := db.Put([]byte(deleteKey), []byte("old")); err != nil {
		t.Fatal(err)
	}
	batch := db.NewBatch().(*batch)
	defer batch.Close()
	value := []byte("immutable-value")
	want := append([]byte(nil), value...)
	if err := batch.PutString("put-key", value); err != nil {
		t.Fatal(err)
	}
	clear(value)
	if err := batch.DeleteString(deleteKey); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}

	got, err := db.Get([]byte("put-key"))
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("string-key put = (%q,%v), want (%q,nil)", got, err, want)
	}
	if ok, err := db.Has([]byte(deleteKey)); err != nil || ok {
		t.Fatalf("string-key delete = (exists:%v, err:%v), want false/nil", ok, err)
	}
}

func TestBatchPutValueFuncEncodesIntoBatch(t *testing.T) {
	db, err := New(t.TempDir(), 16, 16, "value-func-batch-test", false, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("deferred-key")
	want := []byte("directly-encoded-value")
	batch := db.NewBatch().(*batch)
	defer batch.Close()
	fillCalls := 0
	if err := batch.PutValueFunc(key, len(want), func(dst []byte) error {
		fillCalls++
		if len(dst) != len(want) {
			t.Fatalf("deferred value length = %d, want %d", len(dst), len(want))
		}
		copy(dst, want)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if fillCalls != 1 {
		t.Fatalf("fill calls = %d, want 1", fillCalls)
	}
	if batch.ValueSize() != len(key)+len(want) {
		t.Fatalf("batch value size = %d, want %d", batch.ValueSize(), len(key)+len(want))
	}
	clear(key)
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}

	got, err := db.Get([]byte("deferred-key"))
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("deferred put = (%q,%v), want (%q,nil)", got, err, want)
	}
}

func TestLargeBatchBufferWritesAndResets(t *testing.T) {
	db, err := New(t.TempDir(), 16, 16, "large-batch-test", false, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	batch := db.NewBatchWithSize(2 << 20).(*batch)
	if batch.pooledData == nil {
		t.Fatal("large batch did not borrow a pooled buffer")
	}
	if err := batch.Put([]byte("discarded"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	batch.Reset()
	if batch.ValueSize() != 0 {
		t.Fatalf("reset batch value size = %d, want 0", batch.ValueSize())
	}
	want := bytes.Repeat([]byte{0x5a}, 1<<20)
	if err := batch.Put([]byte("kept"), want); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	batch.Close()

	if ok, err := db.Has([]byte("discarded")); err != nil || ok {
		t.Fatalf("reset key = (exists:%v, err:%v), want false/nil", ok, err)
	}
	got, err := db.Get([]byte("kept"))
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("large-batch value = (len:%d, err:%v), want (len:%d, nil)", len(got), err, len(want))
	}
}

func TestLargeBatchBufferIsReusedAfterClose(t *testing.T) {
	// Empty the target class so this test observes the buffer it returns rather
	// than one left behind by another test.
	class, _, ok := batchBufferClass(2 << 20)
	if !ok {
		t.Fatal("2 MiB batch is not pool eligible")
	}
	for {
		select {
		case <-batchBufferPools[class]:
		default:
			goto drained
		}
	}

drained:
	db, err := New(t.TempDir(), 16, 16, "large-batch-reuse-test", false, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	first := db.NewBatchWithSize(2 << 20).(*batch)
	firstBacking := &first.pooledData[0]
	first.Close()
	second := db.NewBatchWithSize(2 << 20).(*batch)
	defer second.Close()
	if &second.pooledData[0] != firstBacking {
		t.Fatal("large batch did not reuse the buffer returned by Close")
	}
}

func TestLargeBatchBufferIsNotReusedWhenPebbleRetainsIt(t *testing.T) {
	tune := DefaultOptions()
	tune.MemTableSizeBytes = 2 << 20
	db, err := New(t.TempDir(), 16, 16, "retained-large-batch-test", false, tune)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	batch := db.NewBatchWithSize(2 << 20).(*batch)
	if err := batch.Put([]byte("large"), bytes.Repeat([]byte{0x7b}, 1<<20)); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	if batch.pooledData != nil {
		t.Fatal("buffer retained by Pebble remained eligible for reuse")
	}
	batch.Close()
}

func TestBatchBufferClasses(t *testing.T) {
	tests := []struct {
		size     int
		capacity int
		pooled   bool
	}{
		{size: minPooledBatchSize - 1},
		{size: minPooledBatchSize, capacity: 64 << 10, pooled: true},
		{size: (64 << 10) + 1, capacity: 128 << 10, pooled: true},
		{size: 1 << 20, capacity: 1 << 20, pooled: true},
		{size: (1 << 20) + 1, capacity: 2 << 20, pooled: true},
		{size: maxPooledBatchSize, capacity: 32 << 20, pooled: true},
		{size: maxPooledBatchSize + 1},
	}
	for _, test := range tests {
		_, capacity, pooled := batchBufferClass(test.size)
		if capacity != test.capacity || pooled != test.pooled {
			t.Errorf("batchBufferClass(%d) = (capacity:%d, pooled:%v), want (%d,%v)",
				test.size, capacity, pooled, test.capacity, test.pooled)
		}
	}
}

func BenchmarkBatchWithLargeSizeLifecycle(b *testing.B) {
	db, err := New(b.TempDir(), 16, 16, "large-batch-benchmark", false, Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := db.NewBatchWithSize(2 << 20)
		batch.Close()
	}
}

func BenchmarkLargeAndSmallBatchLifecycle(b *testing.B) {
	db, err := New(b.TempDir(), 16, 16, "mixed-batch-benchmark", false, Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		large := db.NewBatchWithSize(2 << 20)
		large.Close()
		small := db.NewBatchWithSize(512 << 10)
		small.Close()
	}
}
