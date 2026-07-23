package blockbuffer

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestBufferReadViewPublicationDoesNotAliasTopology(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	b := New(base)
	b.SetMaxInflight(2)

	initial := b.loadReadView()
	if len(initial.inflight) != 0 || len(initial.layers) != 0 || initial.baseReadCache != nil {
		t.Fatalf("initial view = inflight:%d layers:%d cache:%p", len(initial.inflight), len(initial.layers), initial.baseReadCache)
	}
	b.SetBaseReadCacheSize(1 << 20)
	withCache := b.loadReadView()
	if withCache == initial || withCache.baseReadCache == nil {
		t.Fatal("cache configuration did not publish a fresh read view")
	}
	if initial.baseReadCache != nil {
		t.Fatal("cache publication mutated the previously loaded read view")
	}

	b.BeginBlock([32]byte{1}, 1)
	if err := b.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	h1, ok := b.NewestInflight()
	if !ok {
		t.Fatal("missing block-1 in-flight handle")
	}
	oneInflight := b.loadReadView()
	if len(oneInflight.inflight) != 1 || oneInflight.inflight[0] != h1.l {
		t.Fatalf("block-1 view has %d in-flight layers", len(oneInflight.inflight))
	}

	b.BeginBlock([32]byte{2}, 2)
	if err := b.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	twoInflight := b.loadReadView()
	if len(twoInflight.inflight) != 2 || twoInflight.inflight[0] != h1.l {
		t.Fatalf("two-block view = %#v", twoInflight.inflight)
	}

	// Promotion compacts b.inflight in place. A published view must own a slice
	// copy or this operation would rewrite twoInflight.inflight[0] to block 2.
	if err := b.CommitInflight(h1); err != nil {
		t.Fatal(err)
	}
	afterCommit := b.loadReadView()
	if len(afterCommit.inflight) != 1 || len(afterCommit.layers) != 1 || afterCommit.layers[0] != h1.l {
		t.Fatalf("after commit = inflight:%d layers:%d", len(afterCommit.inflight), len(afterCommit.layers))
	}
	if len(twoInflight.inflight) != 2 || twoInflight.inflight[0] != h1.l {
		t.Fatal("promotion mutated an older published view")
	}
	if got, err := b.Get([]byte("k1")); err != nil || string(got) != "v1" {
		t.Fatalf("Get(k1) = (%q,%v)", got, err)
	}
	if got, err := b.Get([]byte("k2")); err != nil || string(got) != "v2" {
		t.Fatalf("Get(k2) = (%q,%v)", got, err)
	}

	b.DiscardActive()
	afterDiscard := b.loadReadView()
	if len(afterDiscard.inflight) != 0 || len(afterDiscard.layers) != 1 {
		t.Fatalf("after discard = inflight:%d layers:%d", len(afterDiscard.inflight), len(afterDiscard.layers))
	}
	if _, err := b.Get([]byte("k2")); err == nil {
		t.Fatal("discarded in-flight value remains visible")
	}

	if err := b.FlushUpTo(1, base); err != nil {
		t.Fatal(err)
	}
	afterFlush := b.loadReadView()
	if len(afterFlush.layers) != 0 {
		t.Fatalf("flush left %d committed layers in read view", len(afterFlush.layers))
	}
	if len(afterCommit.layers) != 1 {
		t.Fatal("flush mutated an older committed-layer view")
	}
	if got, found, tomb := afterCommit.layers[0].lookup([]byte("k1")); !found || tomb || string(got) != "v1" {
		t.Fatalf("old view no longer owns flushed layer: (%q,%v,%v)", got, found, tomb)
	}
	if got, err := b.Get([]byte("k1")); err != nil || string(got) != "v1" {
		t.Fatalf("Get(k1) after flush = (%q,%v)", got, err)
	}
}

func TestBufferZeroValueReadViewFallback(t *testing.T) {
	b := &Buffer{base: rawdb.NewMemoryDatabase()}
	view := b.loadReadView()
	if view == nil || len(view.inflight) != 0 || len(view.layers) != 0 {
		t.Fatalf("zero-value fallback = %#v", view)
	}
	if _, err := b.GetNoCopy([]byte("missing")); err == nil {
		t.Fatal("zero-value buffer unexpectedly found missing key")
	}
}
