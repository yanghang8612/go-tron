package freezer

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
)

// fakeChain implements ChainSource against an in-memory KV store plus
// per-block raw bytes seeded by plantBlock. The slice-3 runner only
// reads through this interface, so a memorydb is sufficient for every
// test in this file.
type fakeChain struct {
	mu         sync.Mutex
	db         *memorydb.Database
	solidified int64
	// Per-block synthetic content. plantBlock populates all three; the
	// runner asserts that what it appended to ancient matches what
	// plantBlock seeded.
	blockRaw      map[uint64][]byte
	txInfosRaw    map[uint64][]byte
	stateRootRaw  map[uint64][]byte
	blockHashByNo map[uint64]tcommon.Hash
}

// viewingFakeChain advertises RawViewSource and counts whether Runner used the
// callback path or fell back to the allocating slice-returning accessors.
type viewingFakeChain struct {
	*fakeChain
	blockViews int
	txViews    int
	blockReads int
	txReads    int
}

func (f *viewingFakeChain) ReadBlockRaw(n uint64) []byte {
	f.blockReads++
	return f.fakeChain.ReadBlockRaw(n)
}

func (f *viewingFakeChain) ReadTransactionInfosRaw(n uint64) []byte {
	f.txReads++
	return f.fakeChain.ReadTransactionInfosRaw(n)
}

func (f *viewingFakeChain) ViewBlockRaw(n uint64, fn func([]byte) error) (bool, error) {
	f.blockViews++
	f.mu.Lock()
	raw, ok := f.blockRaw[n]
	f.mu.Unlock()
	if !ok {
		return false, nil
	}
	return true, fn(raw)
}

func (f *viewingFakeChain) ViewTransactionInfosRaw(n uint64, fn func([]byte) error) (bool, error) {
	f.txViews++
	f.mu.Lock()
	raw, ok := f.txInfosRaw[n]
	f.mu.Unlock()
	if !ok {
		return false, nil
	}
	return true, fn(raw)
}

func newFakeChain() *fakeChain {
	return &fakeChain{
		db:            memorydb.New(),
		blockRaw:      make(map[uint64][]byte),
		txInfosRaw:    make(map[uint64][]byte),
		stateRootRaw:  make(map[uint64][]byte),
		blockHashByNo: make(map[uint64]tcommon.Hash),
	}
}

func (f *fakeChain) LatestSolidifiedBlockNum() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.solidified
}

func (f *fakeChain) setSolidified(n int64) {
	f.mu.Lock()
	f.solidified = n
	f.mu.Unlock()
}

func (f *fakeChain) DB() ethdb.KeyValueStore { return f.db }

func (f *fakeChain) ReadBlockRaw(n uint64) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	if b, ok := f.blockRaw[n]; ok {
		return append([]byte(nil), b...) // defensive copy
	}
	return nil
}

func (f *fakeChain) ReadTransactionInfosRaw(n uint64) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	if b, ok := f.txInfosRaw[n]; ok {
		return append([]byte(nil), b...)
	}
	return nil
}

func (f *fakeChain) ReadBlockHash(n uint64, blockRaw []byte) tcommon.Hash {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !bytes.Equal(blockRaw, f.blockRaw[n]) {
		return tcommon.Hash{}
	}
	return f.blockHashByNo[n]
}

func (f *fakeChain) ReadBlockStateRootRaw(h tcommon.Hash) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Map hash → num via reverse lookup; tests always plant deterministic
	// hashes so this is cheap.
	for n, ph := range f.blockHashByNo {
		if ph == h {
			if b, ok := f.stateRootRaw[n]; ok {
				return append([]byte(nil), b...)
			}
			return nil
		}
	}
	return nil
}

// plantBlock seeds synthetic raw bytes for block num. The bytes are
// deterministic functions of num so test assertions can recompute
// expected values; the freezer just sees opaque bytes and appends them.
//
// Also writes the `b-<num>` and `tib-<num>` rows into the chain's KV so
// the runner's DeleteFrozenBlockRange phase has something to remove.
func (f *fakeChain) plantBlock(t *testing.T, n uint64) {
	t.Helper()
	blockBlob := blockBytes(n)
	txBlob := txInfosBytes(n)
	stateRoot := stateRootBytes(n)
	hash := blockHash(n)
	f.mu.Lock()
	f.blockRaw[n] = blockBlob
	f.txInfosRaw[n] = txBlob
	f.stateRootRaw[n] = stateRoot
	f.blockHashByNo[n] = hash
	f.mu.Unlock()
	// Mirror in Pebble so DeleteFrozenBlockRange has rows to drop and
	// the post-freeze KV-namespace size sample is realistic.
	if err := writeBlockKV(f.db, n, blockBlob); err != nil {
		t.Fatalf("plantBlock(%d): write block: %v", n, err)
	}
	if err := writeTxInfosKV(f.db, n, txBlob); err != nil {
		t.Fatalf("plantBlock(%d): write tx infos: %v", n, err)
	}
}

// writeBlockKV / writeTxInfosKV write through `b-<num>` / `tib-<num>` keys
// using the same encoding rawdb's accessors use. Mirrored locally because
// the rawdb helpers expect `*types.Block` / parsed protos; the freezer
// tests want raw bytes round-tripped untouched.
func writeBlockKV(db ethdb.KeyValueStore, n uint64, raw []byte) error {
	return db.Put(blockKVKey(n), raw)
}
func writeTxInfosKV(db ethdb.KeyValueStore, n uint64, raw []byte) error {
	return db.Put(txInfoBlockKVKey(n), raw)
}

// blockKVKey / txInfoBlockKVKey reproduce the rawdb schema's private
// key builders. They MUST match the prefixes the rawdb accessors use,
// or DeleteFrozenBlockRange in the runner won't clean up rows planted
// here.
func blockKVKey(n uint64) []byte {
	k := make([]byte, len("b-")+8)
	copy(k, "b-")
	putUint64BE(k[len("b-"):], n)
	return k
}
func txInfoBlockKVKey(n uint64) []byte {
	k := make([]byte, len("tib-")+8)
	copy(k, "tib-")
	putUint64BE(k[len("tib-"):], n)
	return k
}
func putUint64BE(b []byte, v uint64) {
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
}

func blockBytes(n uint64) []byte {
	out := append([]byte("block#"), byte(n))
	for i := 0; i < 8; i++ {
		out = append(out, byte(n>>(56-8*i)))
	}
	return out
}
func txInfosBytes(n uint64) []byte {
	return append([]byte("tib#"), byte(n))
}
func stateRootBytes(n uint64) []byte {
	out := make([]byte, 32)
	for i := 0; i < 8; i++ {
		out[i] = byte(n >> (56 - 8*i))
	}
	return out
}
func blockHash(n uint64) tcommon.Hash {
	var h tcommon.Hash
	for i := 0; i < 8; i++ {
		h[i] = byte(n >> (56 - 8*i))
	}
	h[31] = 0xAB // distinguish from zero hash
	return h
}

// newFreezer wires a temp-dir freezer with a 2 KiB shard size so even
// the small test loads exercise a shard rollover or two.
func newFreezer(t *testing.T) *rawdbfreezer.Freezer {
	t.Helper()
	dir := t.TempDir()
	f, err := rawdbfreezer.NewFreezer(dir, "", false, 2049, FreezerTableSet())
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// freezerWriter wraps *rawdbfreezer.Freezer to satisfy FreezerStore.
// The runner needs both AncientReader + AncientWriter; the slice-1
// Freezer implements both shapes but doesn't expose them as the
// composite interface, so the test fixture composes the read side via
// the public NewFreezerReader helper.
type freezerWriter struct {
	rawdb.AncientReader
	f *rawdbfreezer.Freezer
}

func wrapFreezer(f *rawdbfreezer.Freezer) FreezerStore {
	return &freezerWriter{AncientReader: rawdb.NewFreezerReader(f), f: f}
}

func (w *freezerWriter) ModifyAncients(fn func(rawdb.AncientWriteOp) error) (int64, error) {
	// rawdb.AncientWriteOp is a type alias to rawdbfreezer.AncientWriteOp
	// (see core/rawdb/accessors_ancient.go) so the function-value passed
	// to *Freezer.ModifyAncients is structurally compatible.
	return w.f.ModifyAncients(fn)
}
func (w *freezerWriter) TruncateHead(items uint64) (uint64, error) {
	return w.f.TruncateHead(items)
}
func (w *freezerWriter) Sync() error { return w.f.Sync() }

// TestOnePass_FreezesToMargin: chain with solidified=N; pass; ancient
// has 0..N-margin. Locks in the basic happy path.
func TestOnePass_FreezesToMargin(t *testing.T) {
	t.Parallel()
	fc := newFakeChain()
	for n := uint64(0); n < 50; n++ {
		fc.plantBlock(t, n)
	}
	fc.setSolidified(40)

	r := New(fc, wrapFreezer(newFreezer(t)), Config{
		Enabled:      true,
		MarginBlocks: 8,
		BatchBlocks:  1000, // big enough to do it in one pass
	})
	if r == nil {
		t.Fatal("New returned nil")
	}
	frozen, err := r.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	// solid=40, margin=8 → freezeTo=32 inclusive → 33 blocks (0..32).
	if frozen != 33 {
		t.Fatalf("frozen=%d, want 33", frozen)
	}
	// Verify ancient counts.
	for _, kind := range []string{rawdbAncientBlocks, rawdbAncientTxInfos, rawdbAncientStateRoots} {
		got, err := r.freezer.AncientCount(kind)
		if err != nil {
			t.Fatalf("AncientCount(%s): %v", kind, err)
		}
		if got != 33 {
			t.Fatalf("%s count=%d, want 33", kind, got)
		}
	}
	// Spot-check round-trip for one block.
	if data, err := r.freezer.Ancient(rawdbAncientBlocks, 7); err != nil {
		t.Fatalf("Ancient bodies[7]: %v", err)
	} else if string(data) != string(blockBytes(7)) {
		t.Fatalf("bodies[7] mismatch: %x", data)
	}
	if data, err := r.freezer.Ancient(rawdbAncientStateRoots, 7); err != nil {
		t.Fatalf("Ancient state_roots[7]: %v", err)
	} else if string(data) != string(stateRootBytes(7)) {
		t.Fatalf("state_roots[7] mismatch: %x", data)
	}
	// KV rows for frozen blocks should be gone.
	for n := uint64(0); n <= 32; n++ {
		if v, err := fc.db.Get(blockKVKey(n)); err == nil && len(v) > 0 {
			t.Fatalf("Pebble still has b-%d after freeze", n)
		}
		if v, err := fc.db.Get(txInfoBlockKVKey(n)); err == nil && len(v) > 0 {
			t.Fatalf("Pebble still has tib-%d after freeze", n)
		}
	}
	// KV rows for post-margin blocks should remain.
	for n := uint64(33); n < 50; n++ {
		if v, err := fc.db.Get(blockKVKey(n)); err != nil || len(v) == 0 {
			t.Fatalf("Pebble lost b-%d (should still be hot)", n)
		}
	}
}

func TestOnePass_UsesRawViewSourceWithoutFallbackCopies(t *testing.T) {
	base := newFakeChain()
	for n := uint64(0); n < 10; n++ {
		base.plantBlock(t, n)
	}
	base.setSolidified(8)
	chain := &viewingFakeChain{fakeChain: base}
	r := New(chain, wrapFreezer(newFreezer(t)), Config{
		Enabled:      true,
		MarginBlocks: 1,
		BatchBlocks:  100,
	})
	frozen, err := r.OnePass()
	if err != nil {
		t.Fatal(err)
	}
	if frozen != 8 {
		t.Fatalf("frozen = %d, want 8", frozen)
	}
	if chain.blockViews != 8 || chain.txViews != 8 {
		t.Fatalf("view calls = blocks:%d txInfos:%d, want 8/8", chain.blockViews, chain.txViews)
	}
	if chain.blockReads != 0 || chain.txReads != 0 {
		t.Fatalf("fallback reads = blocks:%d txInfos:%d, want 0/0", chain.blockReads, chain.txReads)
	}
}

// TestOnePass_BatchBound: solidified far ahead, batch=BatchBlocks should
// cap the pass at the configured limit.
func TestOnePass_BatchBound(t *testing.T) {
	t.Parallel()
	fc := newFakeChain()
	for n := uint64(0); n < 5_000; n++ {
		fc.plantBlock(t, n)
	}
	fc.setSolidified(4900)

	r := New(fc, wrapFreezer(newFreezer(t)), Config{
		Enabled:      true,
		MarginBlocks: 100,
		BatchBlocks:  1_000, // cap
	})
	frozen, err := r.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if frozen != 1_000 {
		t.Fatalf("frozen=%d, want 1000 (BatchBlocks cap)", frozen)
	}
	// Ancient should have exactly 1000 entries.
	got, _ := r.freezer.AncientCount(rawdbAncientBlocks)
	if got != 1000 {
		t.Fatalf("ancient count after capped pass: %d", got)
	}
}

// TestOnePass_Idempotent: two passes back-to-back; second is a no-op
// because freezeFrom catches up to freezeTo.
func TestOnePass_Idempotent(t *testing.T) {
	t.Parallel()
	fc := newFakeChain()
	for n := uint64(0); n < 50; n++ {
		fc.plantBlock(t, n)
	}
	fc.setSolidified(40)

	r := New(fc, wrapFreezer(newFreezer(t)), Config{
		Enabled:      true,
		MarginBlocks: 8,
		BatchBlocks:  1000,
	})
	first, _ := r.OnePass()
	if first == 0 {
		t.Fatal("first pass froze 0 blocks")
	}
	second, err := r.OnePass()
	if err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if second != 0 {
		t.Fatalf("second pass should be no-op, frozen=%d", second)
	}
	// Ancient count unchanged.
	count, _ := r.freezer.AncientCount(rawdbAncientBlocks)
	if count != first {
		t.Fatalf("ancient count changed across no-op pass: was %d, now %d", first, count)
	}
}

// TestOnePass_DisabledNoOp: Enabled=false → pass does nothing, no error.
func TestOnePass_DisabledNoOp(t *testing.T) {
	t.Parallel()
	fc := newFakeChain()
	for n := uint64(0); n < 10; n++ {
		fc.plantBlock(t, n)
	}
	fc.setSolidified(8)

	r := New(fc, wrapFreezer(newFreezer(t)), Config{
		Enabled:      false, // disabled
		MarginBlocks: 1,
		BatchBlocks:  100,
	})
	frozen, err := r.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if frozen != 0 {
		t.Fatalf("frozen=%d on disabled runner", frozen)
	}
	// Even KV is untouched.
	for n := uint64(0); n < 10; n++ {
		if v, err := fc.db.Get(blockKVKey(n)); err != nil || len(v) == 0 {
			t.Fatalf("disabled pass mutated KV at block %d", n)
		}
	}
}

// TestOnePass_BelowMargin: solidified < margin → no-op.
func TestOnePass_BelowMargin(t *testing.T) {
	t.Parallel()
	fc := newFakeChain()
	for n := uint64(0); n < 10; n++ {
		fc.plantBlock(t, n)
	}
	fc.setSolidified(5) // below margin

	r := New(fc, wrapFreezer(newFreezer(t)), Config{
		Enabled:      true,
		MarginBlocks: 100,
		BatchBlocks:  1000,
	})
	frozen, err := r.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if frozen != 0 {
		t.Fatalf("frozen=%d, want 0 (solid<margin)", frozen)
	}
}

// TestOnePass_MissingBlock: solidified block missing from KV → error.
// The freezer rolls back; ancient remains empty.
func TestOnePass_MissingBlock(t *testing.T) {
	t.Parallel()
	fc := newFakeChain()
	// Plant blocks 0..5 but skip block 3 in the chain source map.
	for n := uint64(0); n < 10; n++ {
		if n == 3 {
			continue
		}
		fc.plantBlock(t, n)
	}
	fc.setSolidified(8)

	r := New(fc, wrapFreezer(newFreezer(t)), Config{
		Enabled:      true,
		MarginBlocks: 0, // freeze everything ≤ solidified
		BatchBlocks:  1000,
	})
	// MarginBlocks=0 means the no-op check needs adjusting; OnePass uses
	// `< MarginBlocks` which is `< 0` for uint64 → never true. So this
	// just means freeze everything below solidified+1.
	frozen, err := r.OnePass()
	t.Logf("OnePass returned frozen=%d err=%v", frozen, err)
	if err == nil {
		t.Fatalf("OnePass: expected MissingBlockError, got nil (frozen=%d)", frozen)
	}
	var mbe *MissingBlockError
	if !errors.As(err, &mbe) {
		t.Fatalf("OnePass: error type: got %T, want *MissingBlockError", err)
	}
	if mbe.Number != 3 {
		t.Fatalf("MissingBlockError.Number=%d, want 3", mbe.Number)
	}
	// Atomic rollback: ancient stays empty.
	if got, _ := r.freezer.AncientCount(rawdbAncientBlocks); got != 0 {
		t.Fatalf("ancient count after rollback: %d", got)
	}
}

// TestOnePass_CrashRecovery: simulate a crash mid-pass by closing the
// freezer right after a successful pass-1. Reopen the freezer in a
// fresh runner and confirm pass-2 resumes from the saved head.
func TestOnePass_CrashRecovery(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fc := newFakeChain()
	for n := uint64(0); n < 100; n++ {
		fc.plantBlock(t, n)
	}
	fc.setSolidified(80)

	// First runner: freeze blocks 0..9 (BatchBlocks=10), then close.
	f1, err := rawdbfreezer.NewFreezer(dir, "", false, 2049, FreezerTableSet())
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}
	r1 := New(fc, &freezerWriter{AncientReader: rawdb.NewFreezerReader(f1), f: f1}, Config{
		Enabled:      true,
		MarginBlocks: 8,
		BatchBlocks:  10,
	})
	if frozen, err := r1.OnePass(); err != nil || frozen != 10 {
		t.Fatalf("pass-1: frozen=%d err=%v, want 10,nil", frozen, err)
	}
	if err := f1.Close(); err != nil {
		t.Fatalf("close f1: %v", err)
	}

	// Reopen the freezer in a new runner and run again.
	f2, err := rawdbfreezer.NewFreezer(dir, "", false, 2049, FreezerTableSet())
	if err != nil {
		t.Fatalf("reopen freezer: %v", err)
	}
	t.Cleanup(func() { _ = f2.Close() })

	// Confirm the reopen saw the prior pass's 10 rows.
	if got, _ := f2.AncientCount(rawdbAncientBlocks); got != 10 {
		t.Fatalf("count after reopen: %d, want 10", got)
	}

	r2 := New(fc, &freezerWriter{AncientReader: rawdb.NewFreezerReader(f2), f: f2}, Config{
		Enabled:      true,
		MarginBlocks: 8,
		BatchBlocks:  10,
	})
	if frozen, err := r2.OnePass(); err != nil || frozen != 10 {
		t.Fatalf("pass-2: frozen=%d err=%v, want 10,nil", frozen, err)
	}
	// Ancient now has 20 rows; verify block #15's bytes match the
	// original seed.
	got, err := f2.Ancient(rawdbAncientBlocks, 15)
	if err != nil {
		t.Fatalf("Ancient(15): %v", err)
	}
	if string(got) != string(blockBytes(15)) {
		t.Fatalf("Ancient(15) mismatch after resume: %x", got)
	}
}

// TestOnePass_CrashBetweenSyncAndDelete is the real crash-interleaving
// regression: a prior pass died after Phase 2 (ancient Sync) but before
// Phase 3 (Pebble DeleteRange), leaving blocks durably in ancient with
// their hot `b-`/`tib-` rows still in Pebble. Because passes only delete
// the range they freeze ([freezeFromN, cap)), no later pass would ever
// revisit those rows — they would leak disk space forever. The runner's
// once-per-process startup reconciliation must sweep them.
func TestOnePass_CrashBetweenSyncAndDelete(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fc := newFakeChain()
	for n := uint64(0); n < 100; n++ {
		fc.plantBlock(t, n)
	}
	fc.setSolidified(80)

	f, err := rawdbfreezer.NewFreezer(dir, "", false, 2049, FreezerTableSet())
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	// Simulate Phase 1+2 of a pass that then crashed: append blocks 0..9
	// to ancient and fsync, but DO NOT delete their Pebble rows.
	if _, err := f.ModifyAncients(func(op rawdb.AncientWriteOp) error {
		for n := uint64(0); n < 10; n++ {
			if err := op.AppendRaw(rawdbAncientBlocks, n, blockBytes(n)); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdbAncientTxInfos, n, nil); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdbAncientStateRoots, n, nil); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("simulate frozen append: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	// Crash state precondition: ancient holds 0..9, Pebble still holds b-5.
	if v, err := fc.db.Get(blockKVKey(5)); err != nil || len(v) == 0 {
		t.Fatal("precondition: b-5 should still be in Pebble (delete never ran)")
	}

	// Restart: a fresh runner over the same freezer. Its first pass must
	// reconcile the crash leftover before doing new work.
	r := New(fc, &freezerWriter{AncientReader: rawdb.NewFreezerReader(f), f: f}, Config{
		Enabled:      true,
		MarginBlocks: 8,
		BatchBlocks:  10,
	})
	if _, err := r.OnePass(); err != nil {
		t.Fatalf("OnePass after crash: %v", err)
	}

	// The leftover frozen rows b-0..b-9 must be gone from Pebble now.
	for n := uint64(0); n < 10; n++ {
		if v, err := fc.db.Get(blockKVKey(n)); err == nil && len(v) > 0 {
			t.Fatalf("crash leftover b-%d still in Pebble after reconciliation", n)
		}
	}
	// And ancient must not have grown duplicates for 0..9 — resume skips them.
	if got, _ := f.AncientCount(rawdbAncientBlocks); got < 10 {
		t.Fatalf("ancient count regressed: %d", got)
	}
	if got, err := f.Ancient(rawdbAncientBlocks, 5); err != nil || string(got) != string(blockBytes(5)) {
		t.Fatalf("ancient block #5 corrupted after reconciliation: %x err=%v", got, err)
	}
}

// TestRunner_StartStop: lifecycle plumbing. Idempotent stop + goroutine
// cleanup.
func TestRunner_StartStop(t *testing.T) {
	t.Parallel()
	fc := newFakeChain()
	r := New(fc, wrapFreezer(newFreezer(t)), Config{
		Enabled:      true,
		Interval:     100 * time.Millisecond,
		MarginBlocks: 8,
		BatchBlocks:  10,
	})
	if err := r.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Let one tick run.
	time.Sleep(150 * time.Millisecond)
	if err := r.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	// Idempotent.
	if err := r.Stop(); err != nil {
		t.Fatalf("Stop (2nd call): %v", err)
	}
}

// TestRunner_Snapshot: stats reflect pass outcomes.
func TestRunner_Snapshot(t *testing.T) {
	t.Parallel()
	fc := newFakeChain()
	for n := uint64(0); n < 30; n++ {
		fc.plantBlock(t, n)
	}
	fc.setSolidified(20)
	r := New(fc, wrapFreezer(newFreezer(t)), Config{
		Enabled:      true,
		MarginBlocks: 5,
		BatchBlocks:  100,
	})
	// Pre-pass snapshot.
	s0 := r.Snapshot()
	if s0.HasFrozen {
		t.Fatalf("pre-pass snapshot reports HasFrozen=true: %+v", s0)
	}
	if s0.BlocksFrozen != 0 || s0.PassesCompleted != 0 {
		t.Fatalf("pre-pass non-zero counters: %+v", s0)
	}

	frozen, err := r.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if frozen != 16 {
		t.Fatalf("frozen=%d, want 16", frozen)
	}

	s1 := r.Snapshot()
	if !s1.HasFrozen {
		t.Fatalf("post-pass HasFrozen=false: %+v", s1)
	}
	if s1.FrozenMax != 15 { // 0..15 inclusive
		t.Fatalf("FrozenMax=%d, want 15", s1.FrozenMax)
	}
	if s1.BlocksFrozen != 16 {
		t.Fatalf("BlocksFrozen=%d, want 16", s1.BlocksFrozen)
	}
	if s1.PassesCompleted != 1 {
		t.Fatalf("PassesCompleted=%d, want 1", s1.PassesCompleted)
	}
	if s1.LastPassAt.IsZero() {
		t.Fatalf("LastPassAt is zero after pass")
	}
	if s1.LastPassDuration == 0 {
		t.Fatalf("LastPassDuration is zero after pass")
	}
}

// TestNew_NilFreezer: defensive — passing a nil freezer returns nil so
// the caller's wiring layer can skip Lifecycle registration.
func TestNew_NilFreezer(t *testing.T) {
	t.Parallel()
	r := New(newFakeChain(), nil, Default())
	if r != nil {
		t.Fatalf("New(_, nil): want nil, got %v", r)
	}
}

// TestDefault_AppliesNonZero verifies the package defaults pour through
// applyDefaults so a zero Config still produces a runnable runner.
func TestDefault_AppliesNonZero(t *testing.T) {
	t.Parallel()
	d := Default()
	if d.Interval <= 0 || d.MarginBlocks == 0 || d.BatchBlocks == 0 {
		t.Fatalf("Default zero field: %+v", d)
	}
	// applyDefaults on a zero Config matches Default() apart from Enabled
	// and MarginBlocks: an explicit 0 margin is a valid "freeze up to
	// solidified" choice, so applyDefaults leaves it untouched (the 128
	// default is applied only by Default()).
	z := Config{}.applyDefaults()
	if z.Interval != defaultInterval ||
		z.BatchBlocks != defaultBatchBlocks {
		t.Fatalf("applyDefaults zero: %+v", z)
	}
	if z.MarginBlocks != 0 {
		t.Fatalf("applyDefaults clobbered explicit zero MarginBlocks: %+v", z)
	}
	// Enabled is intentionally left at the caller's value (zero = false).
	if z.Enabled {
		t.Fatalf("applyDefaults set Enabled=true from zero")
	}
}
