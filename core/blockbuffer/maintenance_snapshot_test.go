package blockbuffer

import (
	"errors"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestTryNewReadSnapshotDefersDuringRealFlush(t *testing.T) {
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b := New(db)
	b.BeginBlock(bufHash(1), 1)
	if err := b.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()
	gate := newGatedPebbleFlush(db, nil)
	defer gate.unblock()
	done := make(chan error, 1)
	go func() { done <- b.FlushUpTo(1, gate) }()
	<-gate.entered
	result := make(chan error, 1)
	go func() {
		view, err := b.TryNewReadSnapshot()
		if view != nil {
			_ = view.Close()
		}
		result <- err
	}()
	select {
	case err := <-result:
		if !errors.Is(err, ErrReadSnapshotBusy) {
			t.Fatalf("busy snapshot=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("maintenance snapshot waited for the blocked flush")
	}
	gate.unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	view, err := b.TryNewReadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	if value, err := view.Get([]byte("key")); err != nil || string(value) != "value" {
		t.Fatalf("post-flush view=%q err=%v", value, err)
	}
}

func TestTryNewReadSnapshotIncludesInflightAndSurvivesFlush(t *testing.T) {
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b := New(db)
	b.BeginBlock(bufHash(1), 1)
	if err := b.Put([]byte("first"), []byte("committed")); err != nil {
		t.Fatal(err)
	}
	b.CommitBlock()
	b.BeginBlock(bufHash(2), 2)
	if err := b.Put([]byte("second"), []byte("inflight")); err != nil {
		t.Fatal(err)
	}
	view, err := b.TryNewReadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	b.CommitBlock()
	if err := b.FlushUpTo(2, db); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"first": "committed", "second": "inflight"} {
		if value, err := view.Get([]byte(key)); err != nil || string(value) != want {
			t.Fatalf("%s=%q err=%v", key, value, err)
		}
	}
}
