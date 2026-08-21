package blockbuffer

import (
	"bytes"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func collectRangeDeleteIterator(t *testing.T, it ethdb.Iterator) map[string]string {
	t.Helper()
	defer it.Release()
	got := make(map[string]string)
	for it.Next() {
		got[string(it.Key())] = string(it.Value())
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestBufferDeleteRangeMasksBaseAndPreservesLaterWrites(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	for key, value := range map[string]string{
		"aa": "base-aa",
		"ab": "base-ab",
		"ac": "base-ac",
		"ba": "base-ba",
	} {
		if err := base.Put([]byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}

	buffer := New(base)
	buffer.BeginBlock(bufHash(1), 1)
	if err := buffer.Put([]byte("ab"), []byte("before-range")); err != nil {
		t.Fatal(err)
	}
	if err := buffer.DeleteRange([]byte("a"), []byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := buffer.Put([]byte("ac"), []byte("after-range")); err != nil {
		t.Fatal(err)
	}

	mustNotFound(t, buffer, []byte("aa"))
	mustNotFound(t, buffer, []byte("ab"))
	mustGet(t, buffer, []byte("ac"), []byte("after-range"))
	mustGet(t, buffer, []byte("ba"), []byte("base-ba"))
	if got := collectRangeDeleteIterator(t, buffer.NewIterator(nil, nil)); len(got) != 2 || got["ac"] != "after-range" || got["ba"] != "base-ba" {
		t.Fatalf("iterator after range delete = %#v", got)
	}

	buffer.CommitBlock()
	if err := buffer.Flush(base); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"aa", "ab"} {
		if ok, err := base.Has([]byte(key)); err != nil || ok {
			t.Fatalf("base.Has(%q) = (%v,%v), want false", key, ok, err)
		}
	}
	if got, err := base.Get([]byte("ac")); err != nil || !bytes.Equal(got, []byte("after-range")) {
		t.Fatalf("base.Get(ac) = (%q,%v)", got, err)
	}
}

func TestLayerViewDeleteRangeMasksOlderLayer(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	buffer := New(base)
	buffer.SetMaxInflight(2)

	buffer.BeginBlock(bufHash(1), 1)
	if err := buffer.Put([]byte("a-old"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	buffer.CommitBlock()

	buffer.BeginBlock(bufHash(2), 2)
	handle, ok := buffer.NewestInflight()
	if !ok {
		t.Fatal("missing in-flight layer")
	}
	view := buffer.ViewLayer(handle)
	if err := view.DeleteRange([]byte("a"), []byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := view.Put([]byte("a-new"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if _, err := view.Get([]byte("a-old")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("view.Get(a-old) err = %v, want not found", err)
	}
	if got, err := view.Get([]byte("a-new")); err != nil || !bytes.Equal(got, []byte("new")) {
		t.Fatalf("view.Get(a-new) = (%q,%v)", got, err)
	}
	mustNotFound(t, buffer, []byte("a-old"))
	mustGet(t, buffer, []byte("a-new"), []byte("new"))

	if err := buffer.CommitInflight(handle); err != nil {
		t.Fatal(err)
	}
	if err := buffer.Flush(base); err != nil {
		t.Fatal(err)
	}
	if ok, err := base.Has([]byte("a-old")); err != nil || ok {
		t.Fatalf("base.Has(a-old) = (%v,%v), want false", ok, err)
	}
	if got, err := base.Get([]byte("a-new")); err != nil || !bytes.Equal(got, []byte("new")) {
		t.Fatalf("base.Get(a-new) = (%q,%v)", got, err)
	}
}

func TestDeleteCommitmentBranchesUsesLayerRangeTombstone(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	for i := byte(1); i <= 3; i++ {
		if err := rawdb.WriteCommitmentBranch(base, []byte{i}, []byte{0x01, i}); err != nil {
			t.Fatal(err)
		}
	}

	buffer := New(base)
	buffer.BeginBlock(bufHash(1), 1)
	handle, _ := buffer.NewestInflight()
	view := buffer.ViewLayer(handle)
	if err := rawdb.DeleteCommitmentBranches(view); err != nil {
		t.Fatal(err)
	}
	if got := len(handle.l.loadRangeDeletes()); got != 1 {
		t.Fatalf("range tombstones = %d, want 1", got)
	}
	pointDeletes := 0
	for i := range handle.l.shards {
		pointDeletes += len(handle.l.shards[i].deletes)
	}
	if pointDeletes != 0 {
		t.Fatalf("point tombstones = %d, want 0", pointDeletes)
	}
	rows := 0
	if err := rawdb.IterateCommitmentBranches(view, func(_, _ []byte) (bool, error) {
		rows++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("commitment rows survived range delete: %d", rows)
	}
}
