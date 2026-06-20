package freezer

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/metrics"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
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
	blockErr      map[uint64]error
	txInfosRaw    map[uint64][]byte
	txInfosErr    map[uint64]error
	stateRootRaw  map[uint64][]byte
	stateRootErr  map[uint64]error
	blockHashByNo map[uint64]tcommon.Hash
}

func newFakeChain() *fakeChain {
	return &fakeChain{
		db:            memorydb.New(),
		blockRaw:      make(map[uint64][]byte),
		blockErr:      make(map[uint64]error),
		txInfosRaw:    make(map[uint64][]byte),
		txInfosErr:    make(map[uint64]error),
		stateRootRaw:  make(map[uint64][]byte),
		stateRootErr:  make(map[uint64]error),
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

func (f *fakeChain) ReadBlockRawStrict(n uint64) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.blockErr[n]; err != nil {
		return nil, false, err
	}
	if b, ok := f.blockRaw[n]; ok {
		return append([]byte(nil), b...), true, nil // defensive copy
	}
	return nil, false, nil
}

func (f *fakeChain) ReadTransactionInfosRawStrict(n uint64) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.txInfosErr[n]; err != nil {
		return nil, false, err
	}
	if b, ok := f.txInfosRaw[n]; ok {
		return append([]byte(nil), b...), true, nil
	}
	return nil, false, nil
}

func (f *fakeChain) ReadBlockHashByNumber(n uint64) tcommon.Hash {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.blockHashByNo[n]
}

func (f *fakeChain) ReadBlockStateRootRaw(h tcommon.Hash) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Map hash → num via reverse lookup; tests always plant deterministic
	// hashes so this is cheap.
	for n, ph := range f.blockHashByNo {
		if ph == h {
			if err := f.stateRootErr[n]; err != nil {
				return nil, err
			}
			if b, ok := f.stateRootRaw[n]; ok {
				return append([]byte(nil), b...), nil
			}
			return nil, nil
		}
	}
	return nil, nil
}

// plantBlock seeds synthetic raw bytes for block num. The bytes are
// deterministic functions of num so test assertions can recompute
// expected values; the freezer just sees opaque bytes and appends them.
//
// Also writes a valid canonical `b-<num>` row plus the `tib-<num>` row into the
// chain's KV so the rawdb stage-progress verifier can recompute the block hash
// and the runner's DeleteFrozenBlockRange phase has rows to remove. The fake
// ChainSource methods still return the synthetic raw bytes above, so the
// freezer append path keeps testing opaque-byte round trips.
func (f *fakeChain) plantBlock(t *testing.T, n uint64) tcommon.Hash {
	t.Helper()
	blockBlob := blockBytes(n)
	txBlob := txInfosBytes(n)
	stateRoot := stateRootBytes(n)
	block := coretypes.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(n),
				Timestamp: int64(n) * 3000,
			},
		},
	})
	hash := block.Hash()
	f.mu.Lock()
	f.blockRaw[n] = blockBlob
	f.txInfosRaw[n] = txBlob
	f.stateRootRaw[n] = stateRoot
	f.blockHashByNo[n] = hash
	f.mu.Unlock()
	if err := rawdb.WriteBlock(f.db, block); err != nil {
		t.Fatalf("plantBlock(%d): write block: %v", n, err)
	}
	if err := writeTxInfosKV(f.db, n, txBlob); err != nil {
		t.Fatalf("plantBlock(%d): write tx infos: %v", n, err)
	}
	return hash
}

// writeTxInfosKV writes through `tib-<num>` keys using the same encoding
// rawdb's accessors use.
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
	block := coretypes.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(n),
				Timestamp: int64(n) * 3000,
			},
		},
	})
	out, err := block.Marshal()
	if err != nil {
		panic(err)
	}
	return out
}
func txInfosBytes(n uint64) []byte {
	out, err := proto.Marshal(&corepb.TransactionRet{
		BlockNumber:    int64(n),
		BlockTimeStamp: int64(n) + 1,
	})
	if err != nil {
		panic(err)
	}
	return out
}
func stateRootBytes(n uint64) []byte {
	out := make([]byte, 32)
	for i := 0; i < 8; i++ {
		out[i] = byte(n >> (56 - 8*i))
	}
	return out
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

func TestFreezerTableSetSupportsVirtualTail(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	f, err := rawdbfreezer.NewFreezer(dir, "", false, 2049, FreezerTableSet())
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}
	for i := uint64(0); i < 5; i++ {
		if _, err := f.ModifyAncients(func(op rawdb.AncientWriteOp) error {
			if err := op.AppendRaw(rawdb.AncientBlocksTable, i, blockBytes(i)); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdb.AncientTxInfosTable, i, txInfosBytes(i)); err != nil {
				return err
			}
			return op.AppendRaw(rawdb.AncientStateRootsTable, i, stateRootBytes(i))
		}); err != nil {
			t.Fatalf("ModifyAncients(%d): %v", i, err)
		}
	}
	if _, err := f.TruncateTail(3); err != nil {
		t.Fatalf("TruncateTail: %v", err)
	}
	if tail, err := f.Tail(); err != nil || tail != 3 {
		t.Fatalf("tail = %d/%v, want 3", tail, err)
	}
	if _, err := f.Ancient(rawdb.AncientBlocksTable, 2); !errors.Is(err, rawdbfreezer.ErrOutOfBounds) {
		t.Fatalf("read before tail = %v, want ErrOutOfBounds", err)
	}
	if _, err := f.Ancient(rawdb.AncientBlocksTable, 3); err != nil {
		t.Fatalf("read at tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := rawdbfreezer.NewFreezer(dir, "", false, 2049, FreezerTableSet())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if tail, err := reopened.Tail(); err != nil || tail != 3 {
		t.Fatalf("reopened tail = %d/%v, want 3", tail, err)
	}
	if _, err := reopened.Ancient(rawdb.AncientBlocksTable, 2); !errors.Is(err, rawdbfreezer.ErrOutOfBounds) {
		t.Fatalf("reopened read before tail = %v, want ErrOutOfBounds", err)
	}
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
	if got, ok, err := rawdb.ReadStageProgress(fc.db, rawdb.StageChainFreezer); err != nil || !ok || got != 32 {
		t.Fatalf("StageChainFreezer after freeze = %d ok=%v err=%v, want 32", got, ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(fc.db, rawdb.StageChainFreezer); err != nil || !ok || !row.HasBlockHash || row.BlockHash != fc.ReadBlockHashByNumber(32) {
		t.Fatalf("StageChainFreezer row after freeze = %+v ok=%v err=%v, want hash-bound block32", row, ok, err)
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

func TestOnePassRejectsMalformedBlockRawBeforeAppending(t *testing.T) {
	t.Parallel()
	fc := newFakeChain()
	for n := uint64(0); n < 3; n++ {
		fc.plantBlock(t, n)
	}
	fc.mu.Lock()
	fc.blockRaw[0] = []byte("not-a-block")
	fc.mu.Unlock()
	fc.setSolidified(2)

	f := newFreezer(t)
	r := New(fc, wrapFreezer(f), Config{
		Enabled:      true,
		MarginBlocks: 0,
		BatchBlocks:  1000,
	})
	frozen, err := r.OnePass()
	if err == nil || !strings.Contains(err.Error(), "decode block 0") {
		t.Fatalf("OnePass malformed block = frozen %d err %v, want decode error", frozen, err)
	}
	if frozen != 0 {
		t.Fatalf("frozen after malformed block = %d, want 0", frozen)
	}
	if count, err := f.AncientCount(rawdbAncientBlocks); err != nil || count != 0 {
		t.Fatalf("ancient bodies count after malformed block = %d/%v, want 0/nil", count, err)
	}
	if v, err := fc.db.Get(blockKVKey(0)); err != nil || len(v) == 0 {
		t.Fatalf("hot block row after malformed block = len %d err %v, want retained", len(v), err)
	}
}

func TestOnePassRejectsBlockRawReadErrorBeforeAppending(t *testing.T) {
	t.Parallel()
	fc := newFakeChain()
	for n := uint64(0); n < 3; n++ {
		fc.plantBlock(t, n)
	}
	fc.mu.Lock()
	fc.blockErr[0] = errors.New("block raw read failed")
	fc.mu.Unlock()
	fc.setSolidified(2)

	f := newFreezer(t)
	r := New(fc, wrapFreezer(f), Config{
		Enabled:      true,
		MarginBlocks: 0,
		BatchBlocks:  1000,
	})
	frozen, err := r.OnePass()
	if err == nil || !strings.Contains(err.Error(), "read block 0") || !strings.Contains(err.Error(), "block raw read failed") {
		t.Fatalf("OnePass block raw read error = frozen %d err %v, want read error", frozen, err)
	}
	if frozen != 0 {
		t.Fatalf("frozen after block raw read error = %d, want 0", frozen)
	}
	for _, kind := range []string{rawdbAncientBlocks, rawdbAncientTxInfos, rawdbAncientStateRoots} {
		if count, err := f.AncientCount(kind); err != nil || count != 0 {
			t.Fatalf("ancient %s count after block raw read error = %d/%v, want 0/nil", kind, count, err)
		}
	}
	if v, err := fc.db.Get(blockKVKey(0)); err != nil || len(v) == 0 {
		t.Fatalf("hot block row after block raw read error = len %d err %v, want retained", len(v), err)
	}
}

func TestOnePassRejectsMalformedTxInfosBeforeAppending(t *testing.T) {
	t.Parallel()
	fc := newFakeChain()
	for n := uint64(0); n < 3; n++ {
		fc.plantBlock(t, n)
	}
	fc.mu.Lock()
	fc.txInfosRaw[0] = []byte("not-a-transaction-ret")
	fc.mu.Unlock()
	fc.setSolidified(2)

	f := newFreezer(t)
	r := New(fc, wrapFreezer(f), Config{
		Enabled:      true,
		MarginBlocks: 0,
		BatchBlocks:  1000,
	})
	frozen, err := r.OnePass()
	if err == nil || !strings.Contains(err.Error(), "decode tx infos for block 0") {
		t.Fatalf("OnePass malformed tx infos = frozen %d err %v, want decode error", frozen, err)
	}
	if frozen != 0 {
		t.Fatalf("frozen after malformed tx infos = %d, want 0", frozen)
	}
	if count, err := f.AncientCount(rawdbAncientBlocks); err != nil || count != 0 {
		t.Fatalf("ancient bodies count after malformed tx infos = %d/%v, want 0/nil", count, err)
	}
	if v, err := fc.db.Get(txInfoBlockKVKey(0)); err != nil || len(v) == 0 {
		t.Fatalf("hot tx-info row after malformed tx infos = len %d err %v, want retained", len(v), err)
	}
}

func TestOnePassRejectsTxInfosReadErrorBeforeAppending(t *testing.T) {
	t.Parallel()
	fc := newFakeChain()
	for n := uint64(0); n < 3; n++ {
		fc.plantBlock(t, n)
	}
	fc.mu.Lock()
	fc.txInfosErr[0] = errors.New("tx infos raw read failed")
	fc.mu.Unlock()
	fc.setSolidified(2)

	f := newFreezer(t)
	r := New(fc, wrapFreezer(f), Config{
		Enabled:      true,
		MarginBlocks: 0,
		BatchBlocks:  1000,
	})
	frozen, err := r.OnePass()
	if err == nil || !strings.Contains(err.Error(), "read tx infos for block 0") || !strings.Contains(err.Error(), "tx infos raw read failed") {
		t.Fatalf("OnePass tx-info raw read error = frozen %d err %v, want read error", frozen, err)
	}
	if frozen != 0 {
		t.Fatalf("frozen after tx-info raw read error = %d, want 0", frozen)
	}
	for _, kind := range []string{rawdbAncientBlocks, rawdbAncientTxInfos, rawdbAncientStateRoots} {
		if count, err := f.AncientCount(kind); err != nil || count != 0 {
			t.Fatalf("ancient %s count after tx-info raw read error = %d/%v, want 0/nil", kind, count, err)
		}
	}
	if v, err := fc.db.Get(txInfoBlockKVKey(0)); err != nil || len(v) == 0 {
		t.Fatalf("hot tx-info row after tx-info raw read error = len %d err %v, want retained", len(v), err)
	}
}

func TestOnePassRejectsStateRootLookupErrorBeforeAppending(t *testing.T) {
	t.Parallel()
	fc := newFakeChain()
	for n := uint64(0); n < 3; n++ {
		fc.plantBlock(t, n)
	}
	fc.mu.Lock()
	fc.stateRootErr[0] = errors.New("state root lookup corrupt")
	fc.mu.Unlock()
	fc.setSolidified(2)

	f := newFreezer(t)
	r := New(fc, wrapFreezer(f), Config{
		Enabled:      true,
		MarginBlocks: 0,
		BatchBlocks:  1000,
	})
	frozen, err := r.OnePass()
	if err == nil || !strings.Contains(err.Error(), "read state root for block 0") || !strings.Contains(err.Error(), "state root lookup corrupt") {
		t.Fatalf("OnePass state-root lookup error = frozen %d err %v, want lookup error", frozen, err)
	}
	if frozen != 0 {
		t.Fatalf("frozen after state-root lookup error = %d, want 0", frozen)
	}
	for _, kind := range []string{rawdbAncientBlocks, rawdbAncientTxInfos, rawdbAncientStateRoots} {
		if count, err := f.AncientCount(kind); err != nil || count != 0 {
			t.Fatalf("ancient %s count after state-root lookup error = %d/%v, want 0/nil", kind, count, err)
		}
	}
	if v, err := fc.db.Get(blockKVKey(0)); err != nil || len(v) == 0 {
		t.Fatalf("hot block row after state-root lookup error = len %d err %v, want retained", len(v), err)
	}
}

func TestOnePass_CapsFreezeToVerifiedFinishStage(t *testing.T) {
	t.Parallel()
	fc := newFakeChain()
	for n := uint64(0); n < 50; n++ {
		fc.plantBlock(t, n)
	}
	fc.setSolidified(40)
	if err := rawdb.WriteStageProgressWithHash(fc.db, rawdb.StageFinish, 12, fc.ReadBlockHashByNumber(12)); err != nil {
		t.Fatalf("write finish stage: %v", err)
	}

	r := New(fc, wrapFreezer(newFreezer(t)), Config{
		Enabled:      true,
		MarginBlocks: 0,
		BatchBlocks:  1000,
	})
	frozen, err := r.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if frozen != 13 {
		t.Fatalf("frozen=%d, want 13 blocks through finish stage 12", frozen)
	}
	if got, ok, err := rawdb.ReadStageProgress(fc.db, rawdb.StageChainFreezer); err != nil || !ok || got != 12 {
		t.Fatalf("StageChainFreezer after finish cap = %d ok=%v err=%v, want 12", got, ok, err)
	}
	if v, err := fc.db.Get(blockKVKey(12)); err == nil && len(v) > 0 {
		t.Fatal("Pebble still has b-12 after finish-capped freeze")
	}
	if v, err := fc.db.Get(blockKVKey(13)); err != nil || len(v) == 0 {
		t.Fatalf("Pebble lost b-13 beyond finish stage: len=%d err=%v", len(v), err)
	}
}

func TestOnePass_VerifiesFinishStageThroughChainSourceHash(t *testing.T) {
	t.Parallel()
	fc := newFakeChain()
	for n := uint64(0); n < 20; n++ {
		fc.plantBlock(t, n)
	}
	fc.setSolidified(15)
	finishHash := fc.ReadBlockHashByNumber(10)
	if err := rawdb.WriteStageProgressWithHash(fc.db, rawdb.StageFinish, 10, finishHash); err != nil {
		t.Fatalf("write finish stage: %v", err)
	}
	if err := fc.db.Delete(blockKVKey(10)); err != nil {
		t.Fatalf("delete hot block row: %v", err)
	}

	r := New(fc, wrapFreezer(newFreezer(t)), Config{
		Enabled:      true,
		MarginBlocks: 0,
		BatchBlocks:  1000,
	})
	frozen, err := r.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if frozen != 11 {
		t.Fatalf("frozen=%d, want 11 blocks through finish stage 10", frozen)
	}
}

func TestOnePassRejectsFinishStageHashMismatch(t *testing.T) {
	t.Parallel()
	fc := newFakeChain()
	for n := uint64(0); n < 20; n++ {
		fc.plantBlock(t, n)
	}
	fc.setSolidified(15)
	if err := rawdb.WriteStageProgressWithHash(fc.db, rawdb.StageFinish, 10, tcommon.Hash{0xee}); err != nil {
		t.Fatalf("write mismatched finish stage: %v", err)
	}

	r := New(fc, wrapFreezer(newFreezer(t)), Config{
		Enabled:      true,
		MarginBlocks: 0,
		BatchBlocks:  1000,
	})
	frozen, err := r.OnePass()
	if err == nil || !strings.Contains(err.Error(), "finish stage 10 hash") {
		t.Fatalf("OnePass error = %v, want finish stage hash mismatch", err)
	}
	if frozen != 0 {
		t.Fatalf("frozen=%d, want 0 after finish hash mismatch", frozen)
	}
	if got, err := r.freezer.AncientCount(rawdbAncientBlocks); err != nil || got != 0 {
		t.Fatalf("ancient blocks after rejected pass = %d err=%v, want 0", got, err)
	}
	if v, err := fc.db.Get(blockKVKey(0)); err != nil || len(v) == 0 {
		t.Fatalf("Pebble lost b-0 despite rejected pass: len=%d err=%v", len(v), err)
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

func TestOnePass_BackfillsChainFreezerStageFromAncientHead(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fc := newFakeChain()
	fc.setSolidified(5)

	f, err := rawdbfreezer.NewFreezer(dir, "", false, 2049, FreezerTableSet())
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
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
		t.Fatalf("seed ancient rows: %v", err)
	}
	fc.mu.Lock()
	for n := uint64(0); n < 10; n++ {
		block, err := coretypes.UnmarshalBlock(blockBytes(n))
		if err != nil {
			fc.mu.Unlock()
			t.Fatalf("decode seeded ancient block %d: %v", n, err)
		}
		fc.blockHashByNo[n] = block.Hash()
	}
	fc.mu.Unlock()

	r := New(fc, &freezerWriter{AncientReader: rawdb.NewFreezerReader(f), f: f}, Config{
		Enabled:      true,
		MarginBlocks: 0,
		BatchBlocks:  10,
	})
	frozen, err := r.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if frozen != 0 {
		t.Fatalf("OnePass frozen=%d, want no new rows", frozen)
	}
	if got, ok, err := rawdb.ReadStageProgress(fc.db, rawdb.StageChainFreezer); err != nil || !ok || got != 9 {
		t.Fatalf("StageChainFreezer backfill = %d ok=%v err=%v, want 9", got, ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(fc.db, rawdb.StageChainFreezer); err != nil || !ok || !row.HasBlockHash || row.BlockHash != fc.ReadBlockHashByNumber(9) {
		t.Fatalf("StageChainFreezer backfill row = %+v ok=%v err=%v, want hash-bound block9", row, ok, err)
	}
}

func TestOnePassRejectsChainFreezerStageAheadOfAncientHead(t *testing.T) {
	t.Parallel()
	fc := newFakeChain()
	for n := uint64(0); n < 20; n++ {
		fc.plantBlock(t, n)
	}
	fc.setSolidified(15)
	f := newFreezer(t)
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
		t.Fatalf("seed ancient rows: %v", err)
	}
	if err := rawdb.DeleteFrozenBlockRange(fc.db, 0, 9); err != nil {
		t.Fatalf("delete hot frozen rows: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(fc.db, rawdb.StageChainFreezer, 12, fc.ReadBlockHashByNumber(12)); err != nil {
		t.Fatalf("write ahead ChainFreezer stage: %v", err)
	}

	r := New(fc, &freezerWriter{AncientReader: rawdb.NewFreezerReader(f), f: f}, Config{
		Enabled:      true,
		MarginBlocks: 0,
		BatchBlocks:  10,
	})
	frozen, err := r.OnePass()
	if err == nil || !strings.Contains(err.Error(), "ChainFreezer stage 12 is ahead of local ancient head 9") {
		t.Fatalf("OnePass frozen=%d err=%v, want ChainFreezer ahead rejection", frozen, err)
	}
	if frozen != 0 {
		t.Fatalf("frozen=%d, want 0 after ahead-stage rejection", frozen)
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
	if got, ok, err := rawdb.ReadStageProgress(fc.db, rawdb.StageChainFreezer); err != nil || !ok || got < 9 {
		t.Fatalf("StageChainFreezer after crash reconciliation = %d ok=%v err=%v, want at least 9", got, ok, err)
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

func TestRunner_MetricsUpdatedAfterPass(t *testing.T) {
	t.Parallel()
	namespace := normalizeMetricNamespace("test/chain/freezer/" + strings.ReplaceAll(t.Name(), "/", "_"))
	t.Cleanup(func() { unregisterRunnerMetricNamespace(namespace) })

	fc := newFakeChain()
	for n := uint64(0); n < 30; n++ {
		fc.plantBlock(t, n)
	}
	fc.setSolidified(20)
	r := New(fc, wrapFreezer(newFreezer(t)), Config{
		Enabled:          true,
		MarginBlocks:     5,
		BatchBlocks:      100,
		MetricsNamespace: namespace,
	})

	frozen, err := r.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if frozen != 16 {
		t.Fatalf("frozen=%d, want 16", frozen)
	}

	assertGauge := func(suffix string, want int64) {
		t.Helper()
		if got := runnerGaugeValue(t, namespace+suffix); got != want {
			t.Fatalf("gauge %s = %d, want %d", suffix, got, want)
		}
	}
	assertGauge("frozen/min", 0)
	assertGauge("frozen/max", 15)
	assertGauge("frozen/has", 1)
	assertGauge("blocks", 16)
	assertGauge("passes", 1)
	if got := runnerGaugeValue(t, namespace+"lastpass/time"); got <= 0 {
		t.Fatalf("lastpass/time = %d, want unix timestamp", got)
	}
	if got := runnerGaugeValue(t, namespace+"lastpass/duration"); got <= 0 {
		t.Fatalf("lastpass/duration = %d, want positive duration", got)
	}
	if got := runnerGaugeValue(t, namespace+"pebble/size"); got <= 0 {
		t.Fatalf("pebble/size = %d, want remaining hot block bytes", got)
	}
}

func runnerGaugeValue(t *testing.T, name string) int64 {
	t.Helper()
	gauge, ok := metrics.DefaultRegistry.Get(name).(*metrics.Gauge)
	if !ok {
		t.Fatalf("missing gauge %s", name)
	}
	return gauge.Snapshot().Value()
}

func unregisterRunnerMetricNamespace(namespace string) {
	for _, suffix := range []string{
		"frozen/min",
		"frozen/max",
		"frozen/has",
		"blocks",
		"passes",
		"lastpass/time",
		"lastpass/duration",
		"pebble/size",
	} {
		metrics.DefaultRegistry.Unregister(namespace + suffix)
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
