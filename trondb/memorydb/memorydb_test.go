package memorydb

import (
	"testing"
)

func TestPutGet(t *testing.T) {
	db := New()
	defer db.Close()

	if err := db.Put([]byte("key1"), []byte("val1")); err != nil {
		t.Fatal(err)
	}
	val, err := db.Get([]byte("key1"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "val1" {
		t.Fatalf("expected val1, got %s", string(val))
	}
}

func TestHas(t *testing.T) {
	db := New()
	defer db.Close()

	has, _ := db.Has([]byte("missing"))
	if has {
		t.Fatal("should not have key")
	}
	db.Put([]byte("exists"), []byte("v"))
	has, _ = db.Has([]byte("exists"))
	if !has {
		t.Fatal("should have key")
	}
}

func TestDelete(t *testing.T) {
	db := New()
	defer db.Close()

	db.Put([]byte("k"), []byte("v"))
	db.Delete([]byte("k"))
	has, _ := db.Has([]byte("k"))
	if has {
		t.Fatal("key should be deleted")
	}
}

func TestGetMissing(t *testing.T) {
	db := New()
	defer db.Close()

	_, err := db.Get([]byte("nope"))
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestBatch(t *testing.T) {
	db := New()
	defer db.Close()

	batch := db.NewBatch()
	batch.Put([]byte("b1"), []byte("v1"))
	batch.Put([]byte("b2"), []byte("v2"))
	batch.Delete([]byte("b1"))

	// Before write, db should not have keys
	has, _ := db.Has([]byte("b2"))
	if has {
		t.Fatal("batch not yet written")
	}

	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}

	has, _ = db.Has([]byte("b1"))
	if has {
		t.Fatal("b1 should be deleted in batch")
	}
	val, _ := db.Get([]byte("b2"))
	if string(val) != "v2" {
		t.Fatalf("expected v2, got %s", string(val))
	}
}

func TestIterator(t *testing.T) {
	db := New()
	defer db.Close()

	db.Put([]byte("a-1"), []byte("v1"))
	db.Put([]byte("a-2"), []byte("v2"))
	db.Put([]byte("b-1"), []byte("v3"))

	iter := db.NewIterator([]byte("a-"), nil)
	defer iter.Release()

	count := 0
	for iter.Next() {
		count++
	}
	if count != 2 {
		t.Fatalf("expected 2 items with prefix a-, got %d", count)
	}
}
