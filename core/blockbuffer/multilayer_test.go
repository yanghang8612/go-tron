package blockbuffer

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

// The multi-active-layer feature underpins async/pipelined commit: the
// foreground writes block N+1's layer while a worker folds + writes block N's
// (older) layer concurrently. These tests exercise the buffer primitive in
// isolation; the wiring into the commit path lands separately and stays behind
// a default-off flag.

// With the default maxInflight==1, a second BeginBlock still panics — the
// single-active guarantee (and thus byte-identity) is preserved.
func TestBuffer_SecondBeginBlockPanicsByDefault(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from second BeginBlock under default maxInflight=1")
		}
	}()
	b := New(rawdb.NewMemoryDatabase())
	b.BeginBlock(bufHash(1), 1)
	b.BeginBlock(bufHash(2), 2)
}

// SetMaxInflight(2) allows two simultaneous in-flight layers; a third still
// panics.
func TestBuffer_MaxInflightGate(t *testing.T) {
	b := New(rawdb.NewMemoryDatabase())
	b.SetMaxInflight(2)
	b.BeginBlock(bufHash(1), 1)
	b.BeginBlock(bufHash(2), 2)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from third BeginBlock under maxInflight=2")
		}
	}()
	b.BeginBlock(bufHash(3), 3)
}

// Two in-flight layers: the foreground writes the NEWEST layer (block N+1) via
// Put, the worker writes the OLDER layer (block N) via a LayerWriter. Reads see
// the newest-first overlay, and each layer's writes are isolated.
func TestBuffer_TwoInflightLayers_WriteIsolationAndOverlay(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	base.Put([]byte("k"), []byte("base"))
	b := New(base)
	b.SetMaxInflight(2)

	// Block N opens, foreground writes it, then hands it to the worker.
	b.BeginBlock(bufHash(1), 1)
	if err := b.Put([]byte("k"), []byte("N")); err != nil {
		t.Fatal(err)
	}
	hN, ok := b.NewestInflight()
	if !ok || hN.Number() != 1 {
		t.Fatalf("NewestInflight() = (%+v,%v), want block 1", hN, ok)
	}

	// Block N+1 opens; foreground Put now targets N+1 (the newest).
	b.BeginBlock(bufHash(2), 2)
	if err := b.Put([]byte("k"), []byte("N+1")); err != nil {
		t.Fatal(err)
	}

	// Worker writes layer N through its view; this must NOT clobber N+1.
	workerView := b.ViewLayer(hN)
	if err := workerView.Put([]byte("worker-only"), []byte("from-N")); err != nil {
		t.Fatal(err)
	}

	// Buffer Get (newest-first) sees N+1's value for "k".
	mustGet(t, b, []byte("k"), []byte("N+1"))
	// The worker's key, written to layer N, is visible through the buffer overlay.
	mustGet(t, b, []byte("worker-only"), []byte("from-N"))

	// The worker's view reads its own layer (N) for "k", NOT N+1's write —
	// LayerView skips other in-flight layers.
	got, err := workerView.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("N")) {
		t.Fatalf("LayerView.Get(k) = %q, want N (must not see the newer in-flight layer)", got)
	}
}

// CommitInflight promotes exactly the handle's layer, and only when it is the
// oldest in-flight (FIFO), keeping the committed stack block-number ordered.
func TestBuffer_CommitInflight_FIFOOnly(t *testing.T) {
	b := New(rawdb.NewMemoryDatabase())
	b.SetMaxInflight(2)

	b.BeginBlock(bufHash(1), 1)
	b.Put([]byte("k"), []byte("N"))
	hN, _ := b.NewestInflight()

	b.BeginBlock(bufHash(2), 2)
	b.Put([]byte("k"), []byte("N+1"))
	hN1, _ := b.NewestInflight()

	// Committing the NEWER layer first is rejected (would break FlushUpTo's
	// block-number-ordered committed stack).
	if err := b.CommitInflight(hN1); err == nil {
		t.Fatal("CommitInflight(newer) should fail: not the oldest in-flight layer")
	}

	// Commit N (oldest), then N+1.
	if err := b.CommitInflight(hN); err != nil {
		t.Fatalf("CommitInflight(N): %v", err)
	}
	if err := b.CommitInflight(hN1); err != nil {
		t.Fatalf("CommitInflight(N+1): %v", err)
	}

	// Committed stack ordered [1,2]; reads see the newest committed value.
	pending := b.PendingBlocks()
	if len(pending) != 2 || pending[0] != bufHash(1) || pending[1] != bufHash(2) {
		t.Fatalf("PendingBlocks = %v, want [hash1 hash2] in order", pending)
	}
	mustGet(t, b, []byte("k"), []byte("N+1"))

	// Re-committing an already-committed handle is rejected.
	if err := b.CommitInflight(hN); err == nil {
		t.Fatal("CommitInflight on an already-committed handle should fail")
	}
}

// DiscardInflight drops the worker's in-flight layer (worker error / orphan)
// without touching the foreground's layer.
func TestBuffer_DiscardInflight_DropsOnlyTarget(t *testing.T) {
	b := New(rawdb.NewMemoryDatabase())
	b.SetMaxInflight(2)

	b.BeginBlock(bufHash(1), 1)
	b.Put([]byte("a"), []byte("N"))
	hN, _ := b.NewestInflight()

	b.BeginBlock(bufHash(2), 2)
	b.Put([]byte("b"), []byte("N+1"))

	b.DiscardInflight(hN)

	// Layer N gone, layer N+1 intact.
	mustNotFound(t, b, []byte("a"))
	mustGet(t, b, []byte("b"), []byte("N+1"))

	// The foreground's layer can still commit normally.
	hN1, _ := b.NewestInflight()
	if err := b.CommitInflight(hN1); err != nil {
		t.Fatalf("CommitInflight(N+1) after discarding N: %v", err)
	}

	// DiscardInflight on a now-absent handle is a no-op.
	b.DiscardInflight(hN)
}

// FlushUpTo must NEVER flush an in-flight layer — only committed ones. This is
// the load-bearing invariant: a layer is flush-eligible only after its fold
// completes and it is committed.
func TestBuffer_FlushUpTo_IgnoresInflightLayers(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	b := New(base)
	b.SetMaxInflight(2)

	// Commit block 1 normally.
	b.BeginBlock(bufHash(1), 1)
	b.Put([]byte("c1"), []byte("v1"))
	h1, _ := b.NewestInflight()
	if err := b.CommitInflight(h1); err != nil {
		t.Fatal(err)
	}

	// Block 2 and 3 remain in flight (not committed).
	b.BeginBlock(bufHash(2), 2)
	b.Put([]byte("c2"), []byte("v2"))
	b.BeginBlock(bufHash(3), 3)
	b.Put([]byte("c3"), []byte("v3"))

	// FlushUpTo(3) must flush ONLY the committed block 1; the in-flight 2 and 3
	// stay in the buffer and must NOT reach the base store.
	if err := b.FlushUpTo(3, base); err != nil {
		t.Fatalf("FlushUpTo: %v", err)
	}
	if v, err := base.Get([]byte("c1")); err != nil || !bytes.Equal(v, []byte("v1")) {
		t.Fatalf("base[c1] = (%q,%v), want v1 flushed", v, err)
	}
	if _, err := base.Get([]byte("c2")); err == nil {
		t.Fatal("base[c2] present: an in-flight layer was flushed (invariant violated)")
	}
	if _, err := base.Get([]byte("c3")); err == nil {
		t.Fatal("base[c3] present: an in-flight layer was flushed (invariant violated)")
	}
	// The in-flight values are still readable through the buffer overlay.
	mustGet(t, b, []byte("c2"), []byte("v2"))
	mustGet(t, b, []byte("c3"), []byte("v3"))
}

// LayerView.NewIterator iterates [its layer + committed + base], skipping the
// other in-flight layer.
func TestLayerView_Iterator_ScopedToLayerAndCommitted(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	base.Put([]byte("p:base"), []byte("vb"))
	b := New(base)
	b.SetMaxInflight(2)

	// Committed block 1 contributes p:committed.
	b.BeginBlock(bufHash(1), 1)
	b.Put([]byte("p:committed"), []byte("vc"))
	h1, _ := b.NewestInflight()
	if err := b.CommitInflight(h1); err != nil {
		t.Fatal(err)
	}

	// Worker layer N (block 2) contributes p:worker.
	b.BeginBlock(bufHash(2), 2)
	b.Put([]byte("p:worker"), []byte("vw"))
	hN, _ := b.NewestInflight()

	// Foreground layer N+1 (block 3) contributes p:fg — the LayerView must NOT
	// surface this.
	b.BeginBlock(bufHash(3), 3)
	b.Put([]byte("p:fg"), []byte("vf"))

	view := b.ViewLayer(hN)
	got := map[string]string{}
	it := view.NewIterator([]byte("p:"), nil)
	for it.Next() {
		got[string(it.Key())] = string(it.Value())
	}
	it.Release()

	want := map[string]string{"p:base": "vb", "p:committed": "vc", "p:worker": "vw"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("LayerView iterator = %v, want %v (must include base+committed+own layer, exclude the newer in-flight layer)", got, want)
	}
}

// -race: the foreground writing the newest layer and the worker writing an
// older layer concurrently must be race-free (both serialize on buffer.mu,
// targeting disjoint layers). Run with `go test -race`.
func TestBuffer_ConcurrentForegroundAndWorkerWrites_RaceFree(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	b := New(base)
	b.SetMaxInflight(2)

	b.BeginBlock(bufHash(1), 1)
	b.Put([]byte("seed"), []byte("N"))
	hN, _ := b.NewestInflight()
	b.BeginBlock(bufHash(2), 2)

	const n = 500
	var wg sync.WaitGroup
	wg.Add(3)
	// Foreground writes layer N+1 (the newest).
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			_ = b.Put([]byte(fmt.Sprintf("fg-%d", i)), []byte("fg"))
		}
	}()
	// Worker writes layer N via its view.
	view := b.ViewLayer(hN)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			_ = view.Put([]byte(fmt.Sprintf("wk-%d", i)), []byte("wk"))
		}
	}()
	// A concurrent off-lock reader (RPC-style) must not race either.
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			_, _ = b.Get([]byte(fmt.Sprintf("fg-%d", i)))
			_, _ = b.Get([]byte(fmt.Sprintf("wk-%d", i)))
		}
	}()
	wg.Wait()

	// Sanity: a sampling of both layers' writes landed and is readable.
	mustGet(t, b, []byte("fg-0"), []byte("fg"))
	mustGet(t, b, []byte("wk-0"), []byte("wk"))
	mustGet(t, b, []byte("seed"), []byte("N"))
}

// -race: the production async path runs FlushUpTo (flush worker, lock-free read
// of committed layers) concurrently with LayerView.Put (commit worker writing
// an OLDER in-flight layer), Buffer.Put (foreground writing the newest layer),
// CommitInflight (commit worker promoting), and Get (off-lock reader). They
// target disjoint layers and serialize the slice/map mutations on b.mu, so the
// committed-layer lock-free read in FlushUpTo stays race-free. Run with -race.
func TestBuffer_FlushUpTo_ConcurrentWithWritersAndCommit_RaceFree(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	b := New(base)
	b.SetMaxInflight(2)

	// Commit blocks 1..3 so there are committed layers for FlushUpTo to drain.
	for i := uint64(1); i <= 3; i++ {
		b.BeginBlock(bufHash(byte(i)), i)
		b.Put([]byte(fmt.Sprintf("c%d", i)), []byte("v"))
		h, _ := b.NewestInflight()
		if err := b.CommitInflight(h); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}

	// Two in-flight layers: 4 (worker-owned) and 5 (foreground).
	b.BeginBlock(bufHash(4), 4)
	hWorker, _ := b.NewestInflight()
	b.BeginBlock(bufHash(5), 5)
	view := b.ViewLayer(hWorker)

	const n = 300
	var wg sync.WaitGroup
	wg.Add(4)
	go func() { // flush worker: drain committed layers ≤3 to base, repeatedly
		defer wg.Done()
		for i := 0; i < n; i++ {
			_ = b.FlushUpTo(3, base)
		}
	}()
	go func() { // commit worker: write the older in-flight layer (4)
		defer wg.Done()
		for i := 0; i < n; i++ {
			_ = view.Put([]byte(fmt.Sprintf("w%d", i)), []byte("w"))
		}
	}()
	go func() { // foreground: write the newest in-flight layer (5)
		defer wg.Done()
		for i := 0; i < n; i++ {
			_ = b.Put([]byte(fmt.Sprintf("f%d", i)), []byte("f"))
		}
	}()
	go func() { // off-lock reader
		defer wg.Done()
		for i := 0; i < n; i++ {
			_, _ = b.Get([]byte(fmt.Sprintf("w%d", i)))
			_, _ = b.Get([]byte(fmt.Sprintf("c1")))
		}
	}()
	wg.Wait()

	// Committed layers ≤3 flushed to base; in-flight layers' writes still in the
	// overlay and NOT on base.
	if v, err := base.Get([]byte("c1")); err != nil || !bytes.Equal(v, []byte("v")) {
		t.Fatalf("base[c1] = (%q,%v), want flushed", v, err)
	}
	if _, err := base.Get([]byte("w0")); err == nil {
		t.Fatal("in-flight worker-layer write leaked to base via FlushUpTo")
	}
	mustGet(t, b, []byte("w0"), []byte("w"))
	mustGet(t, b, []byte("f0"), []byte("f"))
}
