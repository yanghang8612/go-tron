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
