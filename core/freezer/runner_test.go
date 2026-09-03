package freezer

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/metrics"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/maintenance"
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
	db         ethdb.KeyValueStore
	solidified int64
	// Per-block synthetic content. plantBlock populates all three; the
	// runner asserts that what it appended to ancient matches what
	// plantBlock seeded.
	blockRaw           map[uint64][]byte
	blockErr           map[uint64]error
	txInfosRaw         map[uint64][]byte
	txInfosErr         map[uint64]error
	stateRootRaw       map[uint64][]byte
	stateRootErr       map[uint64]error
	blockHashByNo      map[uint64]tcommon.Hash
	blockHashErr       map[uint64]error
	receiptLogsCovered bool
	receiptLogsErr     error
	receiptLogRanges   [][2]uint64
}

type failNextBatchDB struct {
	ethdb.KeyValueStore
	fail          atomic.Bool
	err           error
	compactStarts [][]byte
}

type failNextWriteBatch struct {
	ethdb.Batch
	db *failNextBatchDB
}

func (db *failNextBatchDB) NewBatch() ethdb.Batch {
	return &failNextWriteBatch{Batch: db.KeyValueStore.NewBatch(), db: db}
}

func (db *failNextBatchDB) NewBatchWithSize(size int) ethdb.Batch {
	return &failNextWriteBatch{Batch: db.KeyValueStore.NewBatchWithSize(size), db: db}
}

func (db *failNextBatchDB) Compact(start, limit []byte) error {
	db.compactStarts = append(db.compactStarts, append([]byte(nil), start...))
	return db.KeyValueStore.Compact(start, limit)
}

func (b *failNextWriteBatch) Write() error {
	if b.db.fail.CompareAndSwap(true, false) {
		return b.db.err
	}
	return b.Batch.Write()
}

// blockingReadChain pauses one phase-1 block read so tests can deliver Stop
// while ModifyAncients owns an open atomic batch.
type blockingReadChain struct {
	ChainSource
	blockNum uint64
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
}

type syncTransitionReadChain struct {
	ChainSource
	blockNum uint64
	armed    atomic.Bool
	activate func()
	observed <-chan struct{}
}

func (c *syncTransitionReadChain) ReadBlockRawStrict(n uint64) ([]byte, bool, error) {
	if n == c.blockNum && c.armed.CompareAndSwap(true, false) {
		c.activate()
		select {
		case <-c.observed:
		case <-time.After(2 * time.Second):
			return nil, false, errors.New("timed out waiting for direct-V2 sync watcher")
		}
	}
	return c.ChainSource.ReadBlockRawStrict(n)
}

type syncTransitionFreezer struct {
	*rawdbfreezer.Freezer
	armed    atomic.Bool
	activate func()
	observed <-chan struct{}
}

type syncAfterDirectMigrateFreezer struct {
	*rawdbfreezer.Freezer
	armed    atomic.Bool
	activate func()
}

func (f *syncAfterDirectMigrateFreezer) MigrateV2(options rawdbfreezer.V2MigrationOptions) (rawdbfreezer.V2MigrationResult, error) {
	result, err := f.Freezer.MigrateV2(options)
	if err == nil && f.armed.CompareAndSwap(true, false) {
		f.activate()
	}
	return result, err
}

func (f *syncTransitionFreezer) Ancient(kind string, number uint64) ([]byte, error) {
	if kind == rawdbAncientBlocks && f.armed.CompareAndSwap(true, false) {
		f.activate()
		select {
		case <-f.observed:
		case <-time.After(2 * time.Second):
			return nil, errors.New("timed out waiting for sync watcher")
		}
	}
	return f.Freezer.Ancient(kind, number)
}

func (c *blockingReadChain) ReadBlockRawStrict(n uint64) ([]byte, bool, error) {
	if n == c.blockNum {
		c.once.Do(func() { close(c.entered) })
		<-c.release
	}
	return c.ChainSource.ReadBlockRawStrict(n)
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
		blockHashErr:  make(map[uint64]error),
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

func (f *fakeChain) ReceiptLogRangeCovered(fromBlock, toBlock uint64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.receiptLogRanges = append(f.receiptLogRanges, [2]uint64{fromBlock, toBlock})
	return f.receiptLogsCovered, f.receiptLogsErr
}

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

func (f *fakeChain) ReadBlockHashByNumberStrict(n uint64) (tcommon.Hash, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.blockHashErr[n]; err != nil {
		return tcommon.Hash{}, false, err
	}
	hash := f.blockHashByNo[n]
	if hash == (tcommon.Hash{}) {
		return tcommon.Hash{}, false, nil
	}
	return hash, true, nil
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
	if err := rawdb.WriteBlockStateRoot(f.db, hash, tcommon.BytesToHash(stateRoot)); err != nil {
		t.Fatalf("plantBlock(%d): write state root: %v", n, err)
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

func TestValidateFreezerTransactionInfosAllowsGenesisWithoutReceipts(t *testing.T) {
	block := coretypes.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 0}},
		Transactions: []*corepb.Transaction{
			{RawData: &corepb.TransactionRaw{}},
			{RawData: &corepb.TransactionRaw{}},
			{RawData: &corepb.TransactionRaw{}},
		},
	})
	if err := validateFreezerTransactionInfosRaw(0, block, nil); err != nil {
		t.Fatalf("genesis transaction infos: %v", err)
	}
	if err := validateFreezerTransactionInfosRaw(1, block, nil); err == nil || !strings.Contains(err.Error(), "missing transaction info coverage") {
		t.Fatalf("ordinary block missing transaction infos error = %v", err)
	}
}

func TestOnePassFreezesGenesisTransactionsWithoutReceipts(t *testing.T) {
	fc := newFakeChain()
	for n := uint64(0); n < 3; n++ {
		fc.plantBlock(t, n)
	}
	genesis := coretypes.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 0}},
		Transactions: []*corepb.Transaction{
			{RawData: &corepb.TransactionRaw{}},
			{RawData: &corepb.TransactionRaw{}},
			{RawData: &corepb.TransactionRaw{}},
		},
	})
	genesisRaw, err := genesis.Marshal()
	if err != nil {
		t.Fatalf("marshal genesis: %v", err)
	}
	fc.mu.Lock()
	fc.blockRaw[0] = genesisRaw
	delete(fc.txInfosRaw, 0)
	fc.blockHashByNo[0] = genesis.Hash()
	fc.mu.Unlock()
	if err := rawdb.WriteBlock(fc.db, genesis); err != nil {
		t.Fatalf("write genesis: %v", err)
	}
	if err := rawdb.WriteBlockStateRoot(fc.db, genesis.Hash(), tcommon.BytesToHash(stateRootBytes(0))); err != nil {
		t.Fatalf("write genesis state root: %v", err)
	}
	if err := fc.db.Delete(txInfoBlockKVKey(0)); err != nil {
		t.Fatalf("delete synthetic genesis tx infos: %v", err)
	}
	fc.setSolidified(2)

	r := New(fc, wrapFreezer(newFreezer(t)), Config{Enabled: true, BatchBlocks: 100})
	frozen, err := r.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if frozen != 3 {
		t.Fatalf("frozen=%d, want 3", frozen)
	}
	txInfos, err := r.freezer.Ancient(rawdbAncientTxInfos, 0)
	if err != nil {
		t.Fatalf("read frozen genesis tx infos: %v", err)
	}
	if len(txInfos) != 0 {
		t.Fatalf("frozen genesis tx infos len=%d, want 0", len(txInfos))
	}
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

type blockingV2Freezer struct {
	*freezerWriter
	started chan struct{}
}

type failingV2Freezer struct {
	*freezerWriter
	calls atomic.Uint64
	err   error
}

type interruptingDirectV2Freezer struct {
	*freezerWriter
	interrupted atomic.Bool
	err         error
}

func (f *blockingV2Freezer) MigrateV2(options rawdbfreezer.V2MigrationOptions) (rawdbfreezer.V2MigrationResult, error) {
	close(f.started)
	<-options.Context.Done()
	return rawdbfreezer.V2MigrationResult{}, options.Context.Err()
}

func (f *failingV2Freezer) MigrateV2(rawdbfreezer.V2MigrationOptions) (rawdbfreezer.V2MigrationResult, error) {
	f.calls.Add(1)
	return rawdbfreezer.V2MigrationResult{}, f.err
}

func (f *interruptingDirectV2Freezer) MigrateV2(options rawdbfreezer.V2MigrationOptions) (rawdbfreezer.V2MigrationResult, error) {
	if f.interrupted.CompareAndSwap(false, true) {
		options.BeforeTransactionIndexPublish = func() error { return f.err }
	}
	return f.freezerWriter.MigrateV2(options)
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
func (w *freezerWriter) V2Coverage() uint64 {
	return w.f.V2Coverage()
}
func (w *freezerWriter) CanAppendV2Direct(start uint64) bool {
	return w.f.CanAppendV2Direct(start)
}
func (w *freezerWriter) V1Tail() uint64 {
	return w.f.V1Tail()
}
func (w *freezerWriter) MigrateV2(options rawdbfreezer.V2MigrationOptions) (rawdbfreezer.V2MigrationResult, error) {
	return w.f.MigrateV2(options)
}
func (w *freezerWriter) AncientDatadir() (string, error) {
	return w.f.AncientDatadir()
}
func (w *freezerWriter) TransactionIndexCoverage() uint64 {
	return w.f.TransactionIndexCoverage()
}
func (w *freezerWriter) PublishTransactionIndexRun(result rawdbfreezer.TransactionIndexBuildResult) error {
	return w.f.PublishTransactionIndexRun(result)
}
func (w *freezerWriter) CompactTransactionIndexTail() (bool, error) {
	return w.f.CompactTransactionIndexTail()
}

func (w *freezerWriter) CompactTransactionIndexTailContext(ctx context.Context) (bool, error) {
	return w.f.CompactTransactionIndexTailContext(ctx)
}

func TestCompactV2OncePromotesCompleteAllTableSegment(t *testing.T) {
	f := newFreezer(t)
	for number := uint64(0); number < 72; number++ {
		if _, err := f.ModifyAncients(func(op rawdb.AncientWriteOp) error {
			if err := op.AppendRaw(rawdbAncientBlocks, number, blockBytes(number)); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdbAncientTxInfos, number, txInfosBytes(number)); err != nil {
				return err
			}
			return op.AppendRaw(rawdbAncientStateRoots, number, stateRootBytes(number))
		}); err != nil {
			t.Fatalf("append %d: %v", number, err)
		}
	}
	r := New(nil, wrapFreezer(f), Config{
		Enabled:         true,
		V2Enabled:       true,
		V2FrameBlocks:   8,
		V2SegmentBlocks: 64,
	})
	compacted, err := r.CompactV2Once()
	if err != nil {
		t.Fatalf("CompactV2Once: %v", err)
	}
	if compacted != 64 || f.V2Coverage() != 64 {
		t.Fatalf("compacted=%d coverage=%d, want 64/64", compacted, f.V2Coverage())
	}
	for _, kind := range []string{rawdbAncientBlocks, rawdbAncientTxInfos, rawdbAncientStateRoots} {
		for _, number := range []uint64{0, 63, 64, 71} {
			if _, err := f.Ancient(kind, number); err != nil {
				t.Fatalf("read %s[%d] after promotion: %v", kind, number, err)
			}
		}
	}
	stats := r.Snapshot()
	if stats.V2Coverage != 64 || stats.V2BlocksCompacted != 64 {
		t.Fatalf("runner stats = %+v", stats)
	}
}

func TestCompactV2OnceBootstrapsBoundedFreshSyncBacklog(t *testing.T) {
	f := newFreezer(t)
	for number := uint64(0); number < 200; number++ {
		if _, err := f.ModifyAncients(func(op rawdb.AncientWriteOp) error {
			if err := op.AppendRaw(rawdbAncientBlocks, number, blockBytes(number)); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdbAncientTxInfos, number, txInfosBytes(number)); err != nil {
				return err
			}
			return op.AppendRaw(rawdbAncientStateRoots, number, stateRootBytes(number))
		}); err != nil {
			t.Fatalf("append %d: %v", number, err)
		}
	}
	r := New(nil, wrapFreezer(f), Config{
		Enabled:                    true,
		V2Enabled:                  true,
		V2FrameBlocks:              8,
		V2SegmentBlocks:            64,
		V2CatchupMaxSegments:       1,
		SyncActive:                 func() bool { return true },
		CatchupMaintenanceInterval: time.Hour,
	})
	compacted, err := r.CompactV2Once()
	if err != nil {
		t.Fatalf("CompactV2Once: %v", err)
	}
	if compacted != 64 || f.V2Coverage() != 64 {
		t.Fatalf("compacted=%d coverage=%d, want one bounded bootstrap segment 64/64", compacted, f.V2Coverage())
	}
	if head, err := f.Ancients(); err != nil || head != 200 {
		t.Fatalf("ancient head=%d err=%v, want 200", head, err)
	}
	for _, number := range []uint64{0, 63, 64, 127, 128, 199} {
		if _, err := f.Ancient(rawdbAncientBlocks, number); err != nil {
			t.Fatalf("read bodies[%d] after bounded bootstrap: %v", number, err)
		}
	}
}

func TestCompactV2OnceUsesBoundedCatchupDutyCycle(t *testing.T) {
	f := newFreezer(t)
	appendRange := func(from, to uint64) {
		for number := from; number < to; number++ {
			if _, err := f.ModifyAncients(func(op rawdb.AncientWriteOp) error {
				if err := op.AppendRaw(rawdbAncientBlocks, number, blockBytes(number)); err != nil {
					return err
				}
				if err := op.AppendRaw(rawdbAncientTxInfos, number, txInfosBytes(number)); err != nil {
					return err
				}
				return op.AppendRaw(rawdbAncientStateRoots, number, stateRootBytes(number))
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	appendRange(0, 64)
	r := New(nil, wrapFreezer(f), Config{
		Enabled:                    true,
		V2Enabled:                  true,
		V2FrameBlocks:              8,
		V2SegmentBlocks:            64,
		SyncActive:                 func() bool { return true },
		CatchupMaintenanceInterval: time.Hour,
	})
	if compacted, err := r.CompactV2Once(); err != nil || compacted != 64 {
		t.Fatalf("first catch-up V2 = %d err=%v", compacted, err)
	}
	appendRange(64, 128)
	if compacted, err := r.CompactV2Once(); err != nil || compacted != 0 {
		t.Fatalf("rate-limited catch-up V2 = %d err=%v", compacted, err)
	}
	if got := r.Snapshot(); got.V2CatchupDeferred != 1 || got.V2BlocksCompacted != 64 {
		t.Fatalf("catch-up V2 stats = %+v", got)
	}
}

func TestCompactV2OnceAdaptsBatchToFastSyncBacklog(t *testing.T) {
	namespace := "test/freezer/v2-adaptive-backlog/"
	unregisterRunnerMetricNamespace(namespace)
	t.Cleanup(func() { unregisterRunnerMetricNamespace(namespace) })
	f := newFreezer(t)
	for number := uint64(0); number < 256; number++ {
		if _, err := f.ModifyAncients(func(op rawdb.AncientWriteOp) error {
			if err := op.AppendRaw(rawdbAncientBlocks, number, blockBytes(number)); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdbAncientTxInfos, number, txInfosBytes(number)); err != nil {
				return err
			}
			return op.AppendRaw(rawdbAncientStateRoots, number, stateRootBytes(number))
		}); err != nil {
			t.Fatal(err)
		}
	}
	r := New(nil, wrapFreezer(f), Config{
		Enabled:                    true,
		V2Enabled:                  true,
		V2FrameBlocks:              8,
		V2SegmentBlocks:            64,
		V2CatchupMaxSegments:       3,
		V2CatchupTimeBudget:        time.Hour,
		SyncActive:                 func() bool { return true },
		CatchupMaintenanceInterval: time.Hour,
		MetricsNamespace:           namespace,
	})
	if compacted, err := r.CompactV2Once(); err != nil || compacted != 192 {
		t.Fatalf("adaptive catch-up V2 = %d err=%v, want 192", compacted, err)
	}
	stats := r.Snapshot()
	if stats.V2Coverage != 192 || stats.V2BacklogBlocks != 64 || stats.V2BacklogSegments != 1 || stats.V2LastBatchSegments != 3 || stats.V2LastBatchDuration <= 0 {
		t.Fatalf("adaptive catch-up stats = %+v", stats)
	}
	if got := runnerGaugeValue(t, namespace+"v2/backlog/segments"); got != 1 {
		t.Fatalf("v2/backlog/segments = %d, want 1", got)
	}
}

func TestCompactV2OnceStopsAtTimeBudgetBoundary(t *testing.T) {
	namespace := "test/freezer/v2-time-budget/"
	unregisterRunnerMetricNamespace(namespace)
	t.Cleanup(func() { unregisterRunnerMetricNamespace(namespace) })
	f := newFreezer(t)
	for number := uint64(0); number < 192; number++ {
		if _, err := f.ModifyAncients(func(op rawdb.AncientWriteOp) error {
			if err := op.AppendRaw(rawdbAncientBlocks, number, blockBytes(number)); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdbAncientTxInfos, number, txInfosBytes(number)); err != nil {
				return err
			}
			return op.AppendRaw(rawdbAncientStateRoots, number, stateRootBytes(number))
		}); err != nil {
			t.Fatal(err)
		}
	}
	r := New(nil, wrapFreezer(f), Config{
		Enabled:              true,
		V2Enabled:            true,
		V2FrameBlocks:        8,
		V2SegmentBlocks:      64,
		V2CatchupMaxSegments: 16,
		V2CatchupTimeBudget:  time.Nanosecond,
		MetricsNamespace:     namespace,
	})
	if compacted, err := r.CompactV2Once(); err != nil || compacted != 64 {
		t.Fatalf("budgeted V2 = %d err=%v, want one segment", compacted, err)
	}
	stats := r.Snapshot()
	if stats.V2LastBatchSegments != 1 || stats.V2BudgetExhausted != 1 || stats.V2BacklogSegments != 2 {
		t.Fatalf("budgeted V2 stats = %+v", stats)
	}
	if got := runnerGaugeValue(t, namespace+"v2/batch/budget_exhausted"); got != 1 {
		t.Fatalf("v2/batch/budget_exhausted = %d, want 1", got)
	}
}

func TestCompactV2OnceCancelsWhenSyncStarts(t *testing.T) {
	f := newFreezer(t)
	for number := uint64(0); number < 64; number++ {
		if _, err := f.ModifyAncients(func(op rawdb.AncientWriteOp) error {
			if err := op.AppendRaw(rawdbAncientBlocks, number, blockBytes(number)); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdbAncientTxInfos, number, txInfosBytes(number)); err != nil {
				return err
			}
			return op.AppendRaw(rawdbAncientStateRoots, number, stateRootBytes(number))
		}); err != nil {
			t.Fatal(err)
		}
	}
	var syncing atomic.Bool
	store := &blockingV2Freezer{
		freezerWriter: &freezerWriter{AncientReader: rawdb.NewFreezerReader(f), f: f},
		started:       make(chan struct{}),
	}
	r := New(nil, store, Config{
		Enabled:                    true,
		V2Enabled:                  true,
		V2FrameBlocks:              8,
		V2SegmentBlocks:            64,
		SyncActive:                 syncing.Load,
		CatchupMaintenanceInterval: time.Hour,
	})
	result := make(chan error, 1)
	go func() {
		_, err := r.CompactV2Once()
		result <- err
	}()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("V2 migration did not start")
	}
	syncing.Store(true)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("dynamically deferred V2: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("V2 migration did not cancel after sync started")
	}
	if got := r.Snapshot(); got.V2CatchupDeferred != 1 || got.V2BlocksCompacted != 0 {
		t.Fatalf("dynamic V2 deferral stats = %+v", got)
	}
}

func TestCompactV2OnceCancelsCleanlyWhenRunnerStops(t *testing.T) {
	f := newFreezer(t)
	for number := uint64(0); number < 64; number++ {
		if _, err := f.ModifyAncients(func(op rawdb.AncientWriteOp) error {
			if err := op.AppendRaw(rawdbAncientBlocks, number, blockBytes(number)); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdbAncientTxInfos, number, txInfosBytes(number)); err != nil {
				return err
			}
			return op.AppendRaw(rawdbAncientStateRoots, number, stateRootBytes(number))
		}); err != nil {
			t.Fatal(err)
		}
	}
	store := &blockingV2Freezer{
		freezerWriter: &freezerWriter{AncientReader: rawdb.NewFreezerReader(f), f: f},
		started:       make(chan struct{}),
	}
	r := New(nil, store, Config{
		Enabled:         true,
		V2Enabled:       true,
		V2FrameBlocks:   8,
		V2SegmentBlocks: 64,
	})
	result := make(chan error, 1)
	go func() {
		_, err := r.CompactV2Once()
		result <- err
	}()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("V2 migration did not start")
	}
	r.BeginStop()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("stopped V2 error = %v, want context cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("V2 migration did not cancel on runner stop")
	}
	if got := r.Snapshot(); got.V2Errors != 0 {
		t.Fatalf("shutdown cancellation counted as failure: %+v", got)
	}
}

func TestCompactV2OnceDefersWhenHeavyWorkGateBusy(t *testing.T) {
	f := newFreezer(t)
	gate := maintenance.NewHeavyWorkGate()
	release, ok := gate.TryAcquire()
	if !ok {
		t.Fatal("hold maintenance gate")
	}
	defer release()
	r := New(nil, wrapFreezer(f), Config{Enabled: true, V2Enabled: true, HeavyWorkGate: gate})
	if compacted, err := r.CompactV2Once(); err != nil || compacted != 0 {
		t.Fatalf("resource-deferred V2 = %d err=%v", compacted, err)
	}
	if got := r.Snapshot(); got.V2ResourceDeferred != 1 {
		t.Fatalf("resource-deferred V2 stats = %+v", got)
	}
}

func TestCompactV2OnceBacksOffAfterPersistentError(t *testing.T) {
	namespace := "test/freezer/v2-error-backoff/"
	unregisterRunnerMetricNamespace(namespace)
	t.Cleanup(func() { unregisterRunnerMetricNamespace(namespace) })
	f := newFreezer(t)
	for number := uint64(0); number < 64; number++ {
		if _, err := f.ModifyAncients(func(op rawdb.AncientWriteOp) error {
			if err := op.AppendRaw(rawdbAncientBlocks, number, blockBytes(number)); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdbAncientTxInfos, number, txInfosBytes(number)); err != nil {
				return err
			}
			return op.AppendRaw(rawdbAncientStateRoots, number, stateRootBytes(number))
		}); err != nil {
			t.Fatal(err)
		}
	}
	wantErr := errors.New("persistent V2 failure")
	store := &failingV2Freezer{
		freezerWriter: &freezerWriter{AncientReader: rawdb.NewFreezerReader(f), f: f},
		err:           wantErr,
	}
	r := New(nil, store, Config{
		Enabled:                      true,
		V2Enabled:                    true,
		V2FrameBlocks:                8,
		V2SegmentBlocks:              64,
		HeavyMaintenanceErrorBackoff: time.Hour,
		MetricsNamespace:             namespace,
	})
	if compacted, err := r.CompactV2Once(); compacted != 0 || !errors.Is(err, wantErr) {
		t.Fatalf("first V2 attempt = %d/%v, want persistent error", compacted, err)
	}
	if compacted, err := r.CompactV2Once(); compacted != 0 || err != nil {
		t.Fatalf("backed-off V2 attempt = %d/%v", compacted, err)
	}
	stats := r.Snapshot()
	if store.calls.Load() != 1 || stats.V2Errors != 1 || stats.V2ErrorBackoffDeferred != 1 {
		t.Fatalf("V2 error backoff calls=%d stats=%+v", store.calls.Load(), stats)
	}
	if got := runnerGaugeValue(t, namespace+"v2/errors"); got != 1 {
		t.Fatalf("v2/errors = %d, want 1", got)
	}
	if got := runnerGaugeValue(t, namespace+"v2/deferred/error_backoff"); got != 1 {
		t.Fatalf("v2/deferred/error_backoff = %d, want 1", got)
	}
}

func TestCompactV2OnceStopsWhenV1SourceWasPruned(t *testing.T) {
	namespace := "test/freezer/v2-source-pruned/"
	unregisterRunnerMetricNamespace(namespace)
	t.Cleanup(func() { unregisterRunnerMetricNamespace(namespace) })
	f := newFreezer(t)
	for number := uint64(0); number < 128; number++ {
		if _, err := f.ModifyAncients(func(op rawdb.AncientWriteOp) error {
			if err := op.AppendRaw(rawdbAncientBlocks, number, blockBytes(number)); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdbAncientTxInfos, number, txInfosBytes(number)); err != nil {
				return err
			}
			return op.AppendRaw(rawdbAncientStateRoots, number, stateRootBytes(number))
		}); err != nil {
			t.Fatal(err)
		}
	}
	if result, err := f.MigrateV2(rawdbfreezer.V2MigrationOptions{
		Tables:        []string{rawdbAncientBlocks, rawdbAncientTxInfos, rawdbAncientStateRoots},
		SegmentBlocks: 64,
		FrameBlocks:   8,
		MaxSegments:   1,
		Online:        true,
	}); err != nil || result.End != 64 {
		t.Fatalf("seed V2 coverage = %+v/%v, want end 64", result, err)
	}
	if _, err := f.TruncateTail(80); err != nil {
		t.Fatalf("advance V1 tail beyond V2: %v", err)
	}
	store := &failingV2Freezer{
		freezerWriter: &freezerWriter{AncientReader: rawdb.NewFreezerReader(f), f: f},
		err:           errors.New("migration must not run"),
	}
	r := New(nil, store, Config{
		Enabled:          true,
		V2Enabled:        true,
		V2FrameBlocks:    8,
		V2SegmentBlocks:  64,
		MetricsNamespace: namespace,
	})
	if compacted, err := r.CompactV2Once(); err != nil || compacted != 0 {
		t.Fatalf("source-pruned V2 attempt = %d/%v, want no-op", compacted, err)
	}
	stats := r.Snapshot()
	if store.calls.Load() != 0 || stats.V2SourcePrunedDeferred != 1 || stats.V2Errors != 0 {
		t.Fatalf("source-pruned calls=%d stats=%+v", store.calls.Load(), stats)
	}
	if got := runnerGaugeValue(t, namespace+"v2/deferred/source_pruned"); got != 1 {
		t.Fatalf("v2/deferred/source_pruned = %d, want 1", got)
	}
}

func TestOnePassPublishesFreshSyncDirectlyToV2(t *testing.T) {
	fc := newFakeChain()
	for number := uint64(0); number < 8; number++ {
		fc.plantBlock(t, number)
	}
	fc.setSolidified(7)
	fz := newFreezer(t)
	r := New(fc, wrapFreezer(fz), Config{
		Enabled:                 true,
		MarginBlocks:            0,
		BatchBlocks:             2, // Direct V2 deliberately ignores the smaller V1 batch cap.
		V2Enabled:               true,
		DirectV2:                true,
		V2FrameBlocks:           2,
		V2SegmentBlocks:         4,
		TransactionIndexEnabled: false,
	})
	for pass, wantCoverage := range []uint64{4, 8} {
		frozen, err := r.OnePass()
		if err != nil {
			t.Fatalf("direct V2 pass %d: %v", pass+1, err)
		}
		if frozen != 4 || fz.V2Coverage() != wantCoverage {
			t.Fatalf("direct V2 pass %d frozen=%d coverage=%d, want 4/%d", pass+1, frozen, fz.V2Coverage(), wantCoverage)
		}
	}
	if count, err := fz.AncientCount(rawdbAncientBlocks); err != nil || count != 8 {
		t.Fatalf("direct V2 logical head=%d err=%v, want 8", count, err)
	}
	stats, err := fz.Stats()
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range stats.Tables {
		if table.Head != 0 || table.HiddenTail != 0 || table.V2Size == 0 {
			t.Fatalf("direct V2 table stats=%+v, want empty V1 and non-empty V2", table)
		}
	}
	for number := uint64(0); number < 8; number++ {
		if _, err := fz.Ancient(rawdbAncientBlocks, number); err != nil {
			t.Fatalf("read direct V2 body %d: %v", number, err)
		}
	}
}

func TestOnePassDirectV2ExternalizesReceiptsOnlyAfterEventLogCoverage(t *testing.T) {
	fc := newFakeChain()
	for number := uint64(0); number < 4; number++ {
		fc.plantBlock(t, number)
	}
	fc.setSolidified(3)
	fz := newFreezer(t)
	r := New(fc, wrapFreezer(fz), Config{
		Enabled:                  true,
		MarginBlocks:             0,
		V2Enabled:                true,
		DirectV2:                 true,
		V2FrameBlocks:            2,
		V2SegmentBlocks:          4,
		TransactionIndexEnabled:  false,
		ExternalizeV2ReceiptLogs: true,
	})
	if frozen, err := r.OnePass(); err != nil || frozen != 0 {
		t.Fatalf("uncovered direct V2 frozen=%d err=%v, want 0/nil", frozen, err)
	}
	if coverage := fz.V2Coverage(); coverage != 0 {
		t.Fatalf("uncovered direct V2 coverage=%d, want 0", coverage)
	}
	fc.mu.Lock()
	fc.receiptLogsCovered = true
	fc.mu.Unlock()
	if frozen, err := r.OnePass(); err != nil || frozen != 4 {
		t.Fatalf("covered direct V2 frozen=%d err=%v, want 4/nil", frozen, err)
	}
	fc.mu.Lock()
	ranges := append([][2]uint64(nil), fc.receiptLogRanges...)
	fc.mu.Unlock()
	if len(ranges) != 2 || ranges[0] != [2]uint64{1, 3} || ranges[1] != [2]uint64{1, 3} {
		t.Fatalf("receipt coverage ranges=%v, want two [1,3] checks", ranges)
	}
}

func TestOnePassDirectV2KeepsIncompleteSegmentHot(t *testing.T) {
	fc := newFakeChain()
	for number := uint64(0); number < 3; number++ {
		fc.plantBlock(t, number)
	}
	fc.setSolidified(2)
	fz := newFreezer(t)
	r := New(fc, wrapFreezer(fz), Config{
		Enabled:         true,
		MarginBlocks:    0,
		V2Enabled:       true,
		DirectV2:        true,
		V2FrameBlocks:   2,
		V2SegmentBlocks: 4,
	})
	if frozen, err := r.OnePass(); err != nil || frozen != 0 {
		t.Fatalf("incomplete direct V2 frozen=%d err=%v, want 0/nil", frozen, err)
	}
	if count, err := fz.AncientCount(rawdbAncientBlocks); err != nil || count != 0 || fz.V2Coverage() != 0 {
		t.Fatalf("incomplete direct V2 ancient=%d coverage=%d err=%v", count, fz.V2Coverage(), err)
	}
	if raw, ok, err := rawdb.ReadBlockRawStrict(fc.db, 2); err != nil || !ok || len(raw) == 0 {
		t.Fatalf("incomplete segment hot block missing ok=%t len=%d err=%v", ok, len(raw), err)
	}
}

func TestOnePassDirectV2HonorsPromotionAdmission(t *testing.T) {
	fc := newFakeChain()
	for number := uint64(0); number < 4; number++ {
		fc.plantBlock(t, number)
	}
	fc.setSolidified(3)
	fz := newFreezer(t)
	allowed := false
	r := New(fc, wrapFreezer(fz), Config{
		Enabled:         true,
		MarginBlocks:    0,
		V2Enabled:       true,
		DirectV2:        true,
		V2FrameBlocks:   2,
		V2SegmentBlocks: 4,
		V2PromotionAllowed: func() bool {
			return allowed
		},
	})
	if frozen, err := r.OnePass(); err != nil || frozen != 0 {
		t.Fatalf("deferred direct V2 frozen=%d err=%v, want 0/nil", frozen, err)
	}
	count, err := fz.AncientCount(rawdbAncientBlocks)
	if err != nil {
		t.Fatal(err)
	}
	if coverage := fz.V2Coverage(); coverage != 0 || count != 0 {
		t.Fatalf("deferred direct V2 coverage/count=%d/%d, want 0/0", coverage, count)
	}
	stats, err := fz.Stats()
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range stats.Tables {
		if table.Head != 0 || table.HiddenTail != 0 || table.V2Size != 0 {
			t.Fatalf("deferred direct V2 wrote ancient table: %+v", table)
		}
	}
	if raw, ok, err := rawdb.ReadBlockRawStrict(fc.db, 3); err != nil || !ok || len(raw) == 0 {
		t.Fatalf("deferred direct V2 removed hot block ok=%t len=%d err=%v", ok, len(raw), err)
	}

	allowed = true
	if frozen, err := r.OnePass(); err != nil || frozen != 4 {
		t.Fatalf("admitted direct V2 frozen=%d err=%v, want 4/nil", frozen, err)
	}
	if coverage := fz.V2Coverage(); coverage != 4 {
		t.Fatalf("admitted direct V2 coverage=%d, want 4", coverage)
	}
}

func TestOnePassDirectV2KillSwitchPausesExistingLayout(t *testing.T) {
	fc := newFakeChain()
	for number := uint64(0); number < 8; number++ {
		fc.plantBlock(t, number)
	}
	fc.setSolidified(7)
	fz := newFreezer(t)
	r := New(fc, wrapFreezer(fz), Config{
		Enabled:         true,
		MarginBlocks:    0,
		V2Enabled:       true,
		DirectV2:        true,
		V2FrameBlocks:   2,
		V2SegmentBlocks: 4,
	})
	if frozen, err := r.OnePass(); err != nil || frozen != 4 {
		t.Fatalf("initial direct segment frozen=%d err=%v", frozen, err)
	}
	r.cfg.DirectV2 = false
	if frozen, err := r.OnePass(); err != nil || frozen != 0 {
		t.Fatalf("disabled direct layout frozen=%d err=%v, want safe pause", frozen, err)
	}
	if count, err := fz.AncientCount(rawdbAncientBlocks); err != nil || count != 4 {
		t.Fatalf("disabled direct layout count=%d err=%v, want 4", count, err)
	}
	if value, err := fc.db.Get(blockKVKey(7)); err != nil || len(value) == 0 {
		t.Fatalf("kill switch removed unfrozen hot block: %x err=%v", value, err)
	}
}

func TestDirectV2BacklogIncludesEligibleHotSegments(t *testing.T) {
	fc := newFakeChain()
	for number := uint64(0); number < 12; number++ {
		fc.plantBlock(t, number)
	}
	fc.setSolidified(11)
	r := New(fc, wrapFreezer(newFreezer(t)), Config{
		Enabled:         true,
		MarginBlocks:    0,
		V2Enabled:       true,
		DirectV2:        true,
		V2FrameBlocks:   2,
		V2SegmentBlocks: 4,
	})
	stats := r.Snapshot()
	if stats.V2Coverage != 0 || stats.V2BacklogBlocks != 12 || stats.V2BacklogSegments != 3 {
		t.Fatalf("direct backlog stats=%+v, want coverage=0 blocks=12 segments=3", stats)
	}
}

func TestDirectV2Phase3FastForwardsStateRootPruneCursor(t *testing.T) {
	fc := newFakeChain()
	for number := uint64(0); number < 5; number++ {
		fc.plantBlock(t, number)
	}
	fc.setSolidified(4)
	fz := newFreezer(t)
	r := New(fc, wrapFreezer(fz), Config{
		Enabled:         true,
		MarginBlocks:    0,
		BatchBlocks:     2,
		V2Enabled:       true,
		DirectV2:        true,
		V2FrameBlocks:   2,
		V2SegmentBlocks: 4,
	})
	if frozen, err := r.OnePass(); err != nil || frozen != 4 {
		t.Fatalf("initial direct segment frozen=%d err=%v", frozen, err)
	}
	row, ok, err := rawdb.ReadStageProgressRow(fc.db, rawdb.StageChainFreezerStateRootPrune)
	if err != nil || !ok || row.BlockNum != 3 {
		t.Fatalf("state-root prune cursor=%+v ok=%v err=%v, want block 3", row, ok, err)
	}
	if frozen, err := r.OnePass(); err != nil || frozen != 0 {
		t.Fatalf("incomplete direct pass frozen=%d err=%v", frozen, err)
	}
	again, ok, err := rawdb.ReadStageProgressRow(fc.db, rawdb.StageChainFreezerStateRootPrune)
	if err != nil || !ok || again.BlockNum != 3 {
		t.Fatalf("incomplete pass regressed cursor=%+v ok=%v err=%v", again, ok, err)
	}
}

func TestTransactionIndexPruneProgressStartsWithMultipleDirectSegments(t *testing.T) {
	fc := newFakeChain()
	fz := newFreezer(t)
	r := New(fc, wrapFreezer(fz), Config{
		Enabled:         true,
		V2Enabled:       true,
		V2SegmentBlocks: 4,
	})
	var hash [32]byte
	hash[0] = 0x42
	if err := rawdb.WriteTransactionIndex(fc.db, hash[:], 0); err != nil {
		t.Fatal(err)
	}
	progress, initialized, err := r.transactionIndexPruneProgress(8)
	if err != nil || !initialized || progress != 0 {
		t.Fatalf("multi-segment prune initialization=%d/%t err=%v, want 0/true/nil", progress, initialized, err)
	}
}

func TestOnePassDirectV2SameProcessRecoveryDeletesHotStateRoots(t *testing.T) {
	fc := newFakeChain()
	for number := uint64(0); number < 4; number++ {
		fc.plantBlock(t, number)
	}
	fc.setSolidified(3)
	fz := newFreezer(t)
	injected := errors.New("interrupt after V2 manifest")
	store := &interruptingDirectV2Freezer{
		freezerWriter: &freezerWriter{AncientReader: rawdb.NewFreezerReader(fz), f: fz},
		err:           injected,
	}
	r := New(fc, store, Config{
		Enabled:                    true,
		MarginBlocks:               0,
		V2Enabled:                  true,
		DirectV2:                   true,
		V2FrameBlocks:              2,
		V2SegmentBlocks:            4,
		TransactionIndexEnabled:    true,
		TransactionIndexPrefixBits: 8,
	})
	if frozen, err := r.OnePass(); !errors.Is(err, injected) || frozen != 0 {
		t.Fatalf("interrupted direct V2 frozen=%d err=%v, want 0/%v", frozen, err, injected)
	}
	if coverage := fz.V2Coverage(); coverage != 0 {
		t.Fatalf("interrupted live V2 coverage=%d, want 0", coverage)
	}
	for number := uint64(0); number < 4; number++ {
		hash := fc.ReadBlockHashByNumber(number)
		if root := rawdb.ReadBlockStateRootRaw(fc.db, hash); len(root) == 0 {
			t.Fatalf("interrupted pass removed hot state root %d", number)
		}
	}

	if frozen, err := r.OnePass(); err != nil || frozen != 4 {
		t.Fatalf("same-process recovery frozen=%d err=%v, want 4/nil", frozen, err)
	}
	if coverage := fz.V2Coverage(); coverage != 4 {
		t.Fatalf("same-process recovery coverage=%d, want 4", coverage)
	}
	if coverage := fz.TransactionIndexCoverage(); coverage != 4 {
		t.Fatalf("same-process recovery transaction-index coverage=%d, want 4", coverage)
	}
	for number := uint64(0); number < 4; number++ {
		hash := fc.ReadBlockHashByNumber(number)
		if root := rawdb.ReadBlockStateRootRaw(fc.db, hash); len(root) != 0 {
			t.Fatalf("same-process recovery leaked hot state root %d: %x", number, root)
		}
	}
}

func TestOnlineTransactionIndexPublishesPrunesAndMerges(t *testing.T) {
	fc := newFakeChain()
	fz := newFreezer(t)
	r := New(fc, wrapFreezer(fz), Config{
		Enabled:                    true,
		MarginBlocks:               0,
		BatchBlocks:                4,
		V2Enabled:                  true,
		DirectV2:                   true,
		V2FrameBlocks:              2,
		V2SegmentBlocks:            4,
		TransactionIndexEnabled:    true,
		TransactionIndexPrefixBits: 8,
		MetricsNamespace:           "test/online-tx-index/",
	})
	var hashes [][32]byte
	plant := func(number uint64) {
		blockPB := &corepb.Block{
			BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(number), Timestamp: int64(number) * 3000}},
			Transactions: []*corepb.Transaction{
				{RawData: &corepb.TransactionRaw{Timestamp: int64(number*10 + 1)}},
				{RawData: &corepb.TransactionRaw{Timestamp: int64(number*10 + 2)}},
			},
		}
		body, err := proto.Marshal(blockPB)
		if err != nil {
			t.Fatal(err)
		}
		block := coretypes.NewBlockFromPB(blockPB)
		infos := make([]*corepb.TransactionInfo, 0, len(blockPB.Transactions))
		for ordinal, tx := range block.Transactions() {
			hash := tx.Hash()
			hashes = append(hashes, hash)
			infos = append(infos, &corepb.TransactionInfo{Id: hash[:]})
			if err := rawdb.WriteTransactionLocation(fc.db, hash[:], number, ordinal); err != nil {
				t.Fatal(err)
			}
		}
		ret, err := proto.Marshal(&corepb.TransactionRet{Transactioninfo: infos})
		if err != nil {
			t.Fatal(err)
		}
		fc.mu.Lock()
		fc.blockRaw[number] = body
		fc.txInfosRaw[number] = ret
		fc.stateRootRaw[number] = stateRootBytes(number)
		fc.blockHashByNo[number] = block.Hash()
		fc.mu.Unlock()
		if err := rawdb.WriteBlock(fc.db, block); err != nil {
			t.Fatal(err)
		}
		if err := writeTxInfosKV(fc.db, number, ret); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteBlockStateRoot(fc.db, block.Hash(), tcommon.BytesToHash(stateRootBytes(number))); err != nil {
			t.Fatal(err)
		}
	}
	hotExists := func(hash [32]byte) bool {
		key := append([]byte("tx-"), hash[:]...)
		_, err := fc.db.Get(key)
		return err == nil
	}

	for number := uint64(0); number < 4; number++ {
		plant(number)
	}
	fc.setSolidified(3)
	if frozen, err := r.OnePass(); err != nil || frozen != 4 {
		t.Fatalf("first freeze=%d err=%v", frozen, err)
	}
	if compacted, err := r.CompactV2Once(); err != nil || compacted != 0 {
		t.Fatalf("first direct V2 left migration backlog=%d err=%v", compacted, err)
	}
	if got := fz.TransactionIndexCoverage(); got != 4 {
		t.Fatalf("coverage after fused V2 publish=%d, want 4", got)
	}
	if progress, ok, err := rawdb.ReadStageProgress(fc.db, rawdb.StageFreezerTxIndexPrune); err != nil || !ok || progress != 4 {
		t.Fatalf("fused direct prune progress=%d ok=%v err=%v, want 4", progress, ok, err)
	}
	chainDBBeforePrune := rawdb.NewChainDB(fc.db, rawdb.NewFreezerReader(fz))
	for i, hash := range hashes {
		wantBlock := uint64(i / 2)
		if got := rawdb.ReadTransactionIndex(chainDBBeforePrune, hash[:]); got == nil || *got != wantBlock {
			t.Fatalf("cold lookup after fused prune %d=%v, want %d", i, got, wantBlock)
		}
	}
	ancientDir, err := fz.AncientDatadir()
	if err != nil {
		t.Fatal(err)
	}
	runPath := rawdbfreezer.TransactionIndexRunPath(ancientDir, 0, 4)
	run, err := rawdbfreezer.OpenTransactionIndexRun(runPath)
	if err != nil {
		t.Fatal(err)
	}
	republished := rawdbfreezer.TransactionIndexBuildResult{
		Path: runPath, Rows: run.Rows(), StartBlock: run.StartBlock(), EndBlock: run.EndBlock(),
		PrefixBits: run.PrefixBits(), FileBytes: run.Size(),
	}
	if err := run.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fz.PublishTransactionIndexRun(republished); err != nil {
		t.Fatalf("idempotent publication after manifest commit: %v", err)
	}
	for _, hash := range hashes {
		if hotExists(hash) {
			t.Fatalf("fused direct publication left hot row: %x", hash[:8])
		}
		if candidates, err := fz.TransactionIndexCandidates(hash); err != nil || len(candidates) != 1 {
			t.Fatalf("cold candidates=%v err=%v", candidates, err)
		}
	}

	for number := uint64(4); number < 8; number++ {
		plant(number)
	}
	fc.setSolidified(7)
	if frozen, err := r.OnePass(); err != nil || frozen != 4 {
		t.Fatalf("second freeze=%d err=%v", frozen, err)
	}
	if compacted, err := r.CompactV2Once(); err != nil || compacted != 0 {
		t.Fatalf("second direct V2 left migration backlog=%d err=%v", compacted, err)
	}
	if progress, ok, err := rawdb.ReadStageProgress(fc.db, rawdb.StageFreezerTxIndexPrune); err != nil || !ok || progress != 8 {
		t.Fatalf("second fused direct prune progress=%d ok=%v err=%v, want 8", progress, ok, err)
	}
	if changed, err := r.MaintainTransactionIndexOnce(); err != nil || !changed {
		t.Fatalf("tail merge changed=%v err=%v", changed, err)
	}
	if got := fz.TransactionIndexCoverage(); got != 8 {
		t.Fatalf("coverage after merge=%d, want 8", got)
	}
	chainDB := rawdb.NewChainDB(fc.db, rawdb.NewFreezerReader(fz))
	for i, hash := range hashes {
		wantBlock := uint64(i / 2)
		if got := rawdb.ReadTransactionIndex(chainDB, hash[:]); got == nil || *got != wantBlock {
			t.Fatalf("historical transaction index %d=%v, want %d", i, got, wantBlock)
		}
		if got := rawdb.ReadTransactionInfo(chainDB, hash[:]); got == nil || tcommon.BytesToHash(got.Id) != hash {
			t.Fatalf("historical transaction info %d=%+v", i, got)
		}
	}
	stats := r.Snapshot()
	if stats.TransactionIndexCoverage != 8 || stats.TransactionIndexPruned != 8 || stats.TransactionIndexRowsArchived != 16 || stats.TransactionIndexRowsPruned != 16 {
		t.Fatalf("transaction index stats=%+v", stats)
	}
}

func TestDirectV2RepaysTransactionIndexGapBeforePublishingNextSegment(t *testing.T) {
	fc := newFakeChain()
	fz := newFreezer(t)
	r := New(fc, wrapFreezer(fz), Config{
		Enabled:                    true,
		MarginBlocks:               0,
		V2Enabled:                  true,
		DirectV2:                   true,
		V2FrameBlocks:              2,
		V2SegmentBlocks:            4,
		TransactionIndexEnabled:    false,
		TransactionIndexPrefixBits: 8,
	})
	var hashes [][32]byte
	for number := uint64(0); number < 12; number++ {
		blockPB := &corepb.Block{
			BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(number), Timestamp: int64(number) * 3000}},
			Transactions: []*corepb.Transaction{{RawData: &corepb.TransactionRaw{Timestamp: int64(number + 1)}}},
		}
		raw, err := proto.Marshal(blockPB)
		if err != nil {
			t.Fatal(err)
		}
		block := coretypes.NewBlockFromPB(blockPB)
		hash := block.Transactions()[0].Hash()
		hashes = append(hashes, hash)
		ret, err := proto.Marshal(&corepb.TransactionRet{Transactioninfo: []*corepb.TransactionInfo{{Id: hash[:]}}})
		if err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteBlock(fc.db, block); err != nil {
			t.Fatal(err)
		}
		if err := writeTxInfosKV(fc.db, number, ret); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteTransactionLocation(fc.db, hash[:], number, 0); err != nil {
			t.Fatal(err)
		}
		fc.mu.Lock()
		fc.blockRaw[number] = raw
		fc.txInfosRaw[number] = ret
		fc.stateRootRaw[number] = stateRootBytes(number)
		fc.blockHashByNo[number] = block.Hash()
		fc.mu.Unlock()
	}
	fc.setSolidified(7)
	for pass := 0; pass < 2; pass++ {
		if frozen, err := r.OnePass(); err != nil || frozen != 4 {
			t.Fatalf("seed direct pass %d frozen=%d err=%v", pass, frozen, err)
		}
	}
	if got := fz.V2Coverage(); got != 8 {
		t.Fatalf("seed V2 coverage=%d, want 8", got)
	}

	r.cfg.TransactionIndexEnabled = true
	fc.setSolidified(11)
	// Direct debt service builds and prunes one bounded segment in the same
	// admitted pass, so the two existing segments require two passes.
	for pass := 0; pass < 2; pass++ {
		if frozen, err := r.OnePass(); err != nil || frozen != 0 {
			t.Fatalf("debt step %d frozen=%d err=%v, want 0/nil", pass, frozen, err)
		}
		if got := fz.V2Coverage(); got != 8 {
			t.Fatalf("debt step %d advanced V2 coverage=%d before index debt cleared", pass, got)
		}
	}
	if got := fz.TransactionIndexCoverage(); got != 8 {
		t.Fatalf("debt repayment index coverage=%d, want 8", got)
	}
	if progress, ok, err := rawdb.ReadStageProgress(fc.db, rawdb.StageFreezerTxIndexPrune); err != nil || !ok || progress != 8 {
		t.Fatalf("debt repayment prune progress=%d ok=%v err=%v, want 8", progress, ok, err)
	}
	if frozen, err := r.OnePass(); err != nil || frozen != 4 {
		t.Fatalf("post-debt direct pass frozen=%d err=%v, want 4/nil", frozen, err)
	}
	if got := fz.TransactionIndexCoverage(); got != 12 {
		t.Fatalf("final index coverage=%d, want 12", got)
	}
	if progress, ok, err := rawdb.ReadStageProgress(fc.db, rawdb.StageFreezerTxIndexPrune); err != nil || !ok || progress != 12 {
		t.Fatalf("final tx prune progress=%d ok=%v err=%v, want 12", progress, ok, err)
	}
	for i, hash := range hashes {
		key := append([]byte("tx-"), hash[:]...)
		if value, err := fc.db.Get(key); err == nil && len(value) > 0 {
			t.Fatalf("hot tx index %d survived direct debt repayment: %x", i, value)
		}
	}
}

func TestDirectV2DefersTransactionIndexWithoutBlockingActiveSync(t *testing.T) {
	fc := newFakeChain()
	fz := newFreezer(t)
	var syncing atomic.Bool
	syncing.Store(true)
	namespace := "test/direct-v2-sync-debt/"
	unregisterRunnerMetricNamespace(namespace)
	t.Cleanup(func() { unregisterRunnerMetricNamespace(namespace) })
	r := New(fc, wrapFreezer(fz), Config{
		Enabled:                    true,
		MarginBlocks:               0,
		V2Enabled:                  true,
		DirectV2:                   true,
		V2FrameBlocks:              2,
		V2SegmentBlocks:            4,
		TransactionIndexEnabled:    true,
		TransactionIndexPrefixBits: 8,
		SyncActive:                 syncing.Load,
		MetricsNamespace:           namespace,
	})
	hashes := make([]tcommon.Hash, 0, 8)
	for number := uint64(0); number < 8; number++ {
		blockPB := &corepb.Block{
			BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(number), Timestamp: int64(number) * 3000}},
			Transactions: []*corepb.Transaction{{RawData: &corepb.TransactionRaw{Timestamp: int64(number + 1)}}},
		}
		raw, err := proto.Marshal(blockPB)
		if err != nil {
			t.Fatal(err)
		}
		block := coretypes.NewBlockFromPB(blockPB)
		hash := block.Transactions()[0].Hash()
		hashes = append(hashes, hash)
		ret, err := proto.Marshal(&corepb.TransactionRet{Transactioninfo: []*corepb.TransactionInfo{{Id: hash[:]}}})
		if err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteBlock(fc.db, block); err != nil {
			t.Fatal(err)
		}
		if err := writeTxInfosKV(fc.db, number, ret); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteTransactionLocation(fc.db, hash[:], number, 0); err != nil {
			t.Fatal(err)
		}
		fc.mu.Lock()
		fc.blockRaw[number] = raw
		fc.txInfosRaw[number] = ret
		fc.stateRootRaw[number] = stateRootBytes(number)
		fc.blockHashByNo[number] = block.Hash()
		fc.mu.Unlock()
	}
	fc.setSolidified(7)
	for pass := 0; pass < 2; pass++ {
		if frozen, err := r.OnePass(); err != nil || frozen != 4 {
			t.Fatalf("active-sync pass %d frozen=%d err=%v, want 4/nil", pass, frozen, err)
		}
	}
	if got := fz.V2Coverage(); got != 8 {
		t.Fatalf("active-sync V2 coverage=%d, want 8", got)
	}
	if got := fz.TransactionIndexCoverage(); got != 0 {
		t.Fatalf("active-sync transaction-index coverage=%d, want deferred", got)
	}
	chainDB := rawdb.NewChainDB(fc.db, rawdb.NewFreezerReader(fz))
	for number, hash := range hashes {
		if got := rawdb.ReadTransactionIndex(chainDB, hash[:]); got == nil || *got != uint64(number) {
			t.Fatalf("hot fallback lookup %d=%v", number, got)
		}
	}
	stats := r.Snapshot()
	if stats.TransactionIndexDebtBlocks != 8 || stats.TransactionIndexSyncDeferred != 2 || stats.TransactionIndexMaintenanceDeferred != 0 {
		t.Fatalf("active-sync debt stats=%+v", stats)
	}

	syncing.Store(false)
	// Each admitted maintenance publishes one bounded immutable range and then
	// prunes its matching hot-index rows, so the two V2 segments need two passes.
	for pass := 0; pass < 2; pass++ {
		if changed, err := r.MaintainTransactionIndexOnce(); err != nil || !changed {
			t.Fatalf("idle index catch-up pass %d changed=%v err=%v", pass, changed, err)
		}
	}
	if got := fz.TransactionIndexCoverage(); got != 8 {
		t.Fatalf("idle catch-up coverage=%d, want 8", got)
	}
	if progress, ok, err := rawdb.ReadStageProgress(fc.db, rawdb.StageFreezerTxIndexPrune); err != nil || !ok || progress != 8 {
		t.Fatalf("idle catch-up prune progress=%d ok=%v err=%v", progress, ok, err)
	}
	for number, hash := range hashes {
		key := append([]byte("tx-"), hash[:]...)
		if value, err := fc.db.Get(key); err == nil && len(value) > 0 {
			t.Fatalf("idle catch-up left hot tx index %d: %x", number, value)
		}
		if got := rawdb.ReadTransactionIndex(chainDB, hash[:]); got == nil || *got != uint64(number) {
			t.Fatalf("cold lookup after catch-up %d=%v", number, got)
		}
	}
}

func TestDirectV2CancelsIdleDebtWhenSyncBecomesActive(t *testing.T) {
	fc := newFakeChain()
	base := newFreezer(t)
	var syncing atomic.Bool
	observed := make(chan struct{})
	var observedOnce sync.Once
	store := &syncTransitionFreezer{
		Freezer: base,
		activate: func() {
			syncing.Store(true)
		},
		observed: observed,
	}
	namespace := "test/direct-v2-idle-active-transition/"
	unregisterRunnerMetricNamespace(namespace)
	t.Cleanup(func() { unregisterRunnerMetricNamespace(namespace) })
	r := New(fc, store, Config{
		Enabled:                    true,
		MarginBlocks:               0,
		V2Enabled:                  true,
		DirectV2:                   true,
		V2FrameBlocks:              2,
		V2SegmentBlocks:            4,
		TransactionIndexEnabled:    false,
		TransactionIndexPrefixBits: 8,
		SyncActive: func() bool {
			active := syncing.Load()
			if active {
				observedOnce.Do(func() { close(observed) })
			}
			return active
		},
		MetricsNamespace: namespace,
	})
	hashes := make([]tcommon.Hash, 0, 8)
	for number := uint64(0); number < 8; number++ {
		blockPB := &corepb.Block{
			BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(number), Timestamp: int64(number) * 3000}},
			Transactions: []*corepb.Transaction{{RawData: &corepb.TransactionRaw{Timestamp: int64(number + 1)}}},
		}
		raw, err := proto.Marshal(blockPB)
		if err != nil {
			t.Fatal(err)
		}
		block := coretypes.NewBlockFromPB(blockPB)
		hash := block.Transactions()[0].Hash()
		hashes = append(hashes, hash)
		ret, err := proto.Marshal(&corepb.TransactionRet{Transactioninfo: []*corepb.TransactionInfo{{Id: hash[:]}}})
		if err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteBlock(fc.db, block); err != nil {
			t.Fatal(err)
		}
		if err := writeTxInfosKV(fc.db, number, ret); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteTransactionLocation(fc.db, hash[:], number, 0); err != nil {
			t.Fatal(err)
		}
		fc.mu.Lock()
		fc.blockRaw[number] = raw
		fc.txInfosRaw[number] = ret
		fc.stateRootRaw[number] = stateRootBytes(number)
		fc.blockHashByNo[number] = block.Hash()
		fc.mu.Unlock()
	}
	fc.setSolidified(3)
	if frozen, err := r.OnePass(); err != nil || frozen != 4 {
		t.Fatalf("seed direct pass frozen=%d err=%v", frozen, err)
	}
	r.cfg.TransactionIndexEnabled = true
	store.armed.Store(true)
	fc.setSolidified(7)
	if frozen, err := r.OnePass(); err != nil || frozen != 4 {
		t.Fatalf("sync-transition direct pass frozen=%d err=%v, want continued V2 publication", frozen, err)
	}
	if got := base.V2Coverage(); got != 8 {
		t.Fatalf("sync-transition V2 coverage=%d, want 8", got)
	}
	if got := base.TransactionIndexCoverage(); got != 0 {
		t.Fatalf("canceled debt build published coverage=%d, want 0", got)
	}
	stats := r.Snapshot()
	if stats.TransactionIndexSyncDeferred != 1 || stats.TransactionIndexMaintenanceDeferred != 0 || stats.TransactionIndexErrors != 0 {
		t.Fatalf("sync-transition stats=%+v", stats)
	}
	chainDB := rawdb.NewChainDB(fc.db, rawdb.NewFreezerReader(base))
	for number, hash := range hashes {
		key := append([]byte("tx-"), hash[:]...)
		if value, err := fc.db.Get(key); err != nil || len(value) == 0 {
			t.Fatalf("sync-transition lost hot tx index %d: value=%x err=%v", number, value, err)
		}
		if got := rawdb.ReadTransactionIndex(chainDB, hash[:]); got == nil || *got != uint64(number) {
			t.Fatalf("sync-transition lookup %d=%v", number, got)
		}
	}
}

func TestDirectV2SyncTransitionAfterFusedAppendRetainsHotIndex(t *testing.T) {
	fc := newFakeChain()
	base := newFreezer(t)
	var syncing atomic.Bool
	store := &syncAfterDirectMigrateFreezer{
		Freezer:  base,
		activate: func() { syncing.Store(true) },
	}
	namespace := "test/direct-v2-post-append-transition/"
	unregisterRunnerMetricNamespace(namespace)
	t.Cleanup(func() { unregisterRunnerMetricNamespace(namespace) })
	r := New(fc, store, Config{
		Enabled:                    true,
		MarginBlocks:               0,
		V2Enabled:                  true,
		DirectV2:                   true,
		V2FrameBlocks:              2,
		V2SegmentBlocks:            4,
		TransactionIndexEnabled:    true,
		TransactionIndexPrefixBits: 8,
		SyncActive:                 syncing.Load,
		MetricsNamespace:           namespace,
	})
	hashes := make([]tcommon.Hash, 0, 4)
	for number := uint64(0); number < 4; number++ {
		blockPB := &corepb.Block{
			BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(number), Timestamp: int64(number) * 3000}},
			Transactions: []*corepb.Transaction{{RawData: &corepb.TransactionRaw{Timestamp: int64(number + 1)}}},
		}
		raw, err := proto.Marshal(blockPB)
		if err != nil {
			t.Fatal(err)
		}
		block := coretypes.NewBlockFromPB(blockPB)
		hash := block.Transactions()[0].Hash()
		hashes = append(hashes, hash)
		ret, err := proto.Marshal(&corepb.TransactionRet{Transactioninfo: []*corepb.TransactionInfo{{Id: hash[:]}}})
		if err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteBlock(fc.db, block); err != nil {
			t.Fatal(err)
		}
		if err := writeTxInfosKV(fc.db, number, ret); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteTransactionLocation(fc.db, hash[:], number, 0); err != nil {
			t.Fatal(err)
		}
		fc.mu.Lock()
		fc.blockRaw[number] = raw
		fc.txInfosRaw[number] = ret
		fc.stateRootRaw[number] = stateRootBytes(number)
		fc.blockHashByNo[number] = block.Hash()
		fc.mu.Unlock()
	}
	store.armed.Store(true)
	fc.setSolidified(3)
	if frozen, err := r.OnePass(); err != nil || frozen != 4 {
		t.Fatalf("post-append transition frozen=%d err=%v", frozen, err)
	}
	if got := base.TransactionIndexCoverage(); got != 4 {
		t.Fatalf("fused index coverage=%d, want 4", got)
	}
	if progress, ok, err := rawdb.ReadStageProgress(fc.db, rawdb.StageFreezerTxIndexPrune); err != nil || ok || progress != 0 {
		t.Fatalf("post-append transition prune progress=%d ok=%v err=%v, want retained hot rows", progress, ok, err)
	}
	stats := r.Snapshot()
	if stats.TransactionIndexSyncDeferred != 1 || stats.TransactionIndexMaintenanceDeferred != 0 || stats.TransactionIndexErrors != 0 {
		t.Fatalf("post-append transition stats=%+v", stats)
	}
	chainDB := rawdb.NewChainDB(fc.db, rawdb.NewFreezerReader(base))
	for number, hash := range hashes {
		key := append([]byte("tx-"), hash[:]...)
		if value, err := fc.db.Get(key); err != nil || len(value) == 0 {
			t.Fatalf("post-append transition lost hot tx index %d: value=%x err=%v", number, value, err)
		}
		if got := rawdb.ReadTransactionIndex(chainDB, hash[:]); got == nil || *got != uint64(number) {
			t.Fatalf("post-append transition lookup %d=%v", number, got)
		}
	}
}

func TestDirectV2CancelsFusedIndexBuildAndRetriesWithoutIndex(t *testing.T) {
	fc := newFakeChain()
	base := newFreezer(t)
	var syncing atomic.Bool
	observed := make(chan struct{})
	var observedOnce sync.Once
	chain := &syncTransitionReadChain{
		ChainSource: fc,
		blockNum:    0,
		activate:    func() { syncing.Store(true) },
		observed:    observed,
	}
	namespace := "test/direct-v2-fused-cancel-retry/"
	unregisterRunnerMetricNamespace(namespace)
	t.Cleanup(func() { unregisterRunnerMetricNamespace(namespace) })
	r := New(chain, wrapFreezer(base), Config{
		Enabled:                    true,
		MarginBlocks:               0,
		V2Enabled:                  true,
		DirectV2:                   true,
		V2FrameBlocks:              2,
		V2SegmentBlocks:            4,
		TransactionIndexEnabled:    true,
		TransactionIndexPrefixBits: 8,
		SyncActive: func() bool {
			active := syncing.Load()
			if active {
				observedOnce.Do(func() { close(observed) })
			}
			return active
		},
		MetricsNamespace: namespace,
	})
	hashes := make([]tcommon.Hash, 0, 4)
	for number := uint64(0); number < 4; number++ {
		blockPB := &corepb.Block{
			BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(number), Timestamp: int64(number) * 3000}},
			Transactions: []*corepb.Transaction{{RawData: &corepb.TransactionRaw{Timestamp: int64(number + 1)}}},
		}
		raw, err := proto.Marshal(blockPB)
		if err != nil {
			t.Fatal(err)
		}
		block := coretypes.NewBlockFromPB(blockPB)
		hash := block.Transactions()[0].Hash()
		hashes = append(hashes, hash)
		ret, err := proto.Marshal(&corepb.TransactionRet{Transactioninfo: []*corepb.TransactionInfo{{Id: hash[:]}}})
		if err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteBlock(fc.db, block); err != nil {
			t.Fatal(err)
		}
		if err := writeTxInfosKV(fc.db, number, ret); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteTransactionLocation(fc.db, hash[:], number, 0); err != nil {
			t.Fatal(err)
		}
		fc.mu.Lock()
		fc.blockRaw[number] = raw
		fc.txInfosRaw[number] = ret
		fc.stateRootRaw[number] = stateRootBytes(number)
		fc.blockHashByNo[number] = block.Hash()
		fc.mu.Unlock()
	}
	chain.armed.Store(true)
	fc.setSolidified(3)
	if frozen, err := r.OnePass(); err != nil || frozen != 4 {
		t.Fatalf("fused cancel/retry frozen=%d err=%v", frozen, err)
	}
	if got := base.V2Coverage(); got != 4 {
		t.Fatalf("fused cancel/retry V2 coverage=%d, want 4", got)
	}
	if got := base.TransactionIndexCoverage(); got != 0 {
		t.Fatalf("canceled fused build transaction-index coverage=%d, want 0", got)
	}
	stats := r.Snapshot()
	if stats.TransactionIndexSyncDeferred != 1 || stats.TransactionIndexMaintenanceDeferred != 0 || stats.TransactionIndexErrors != 0 {
		t.Fatalf("fused cancel/retry stats=%+v", stats)
	}
	chainDB := rawdb.NewChainDB(fc.db, rawdb.NewFreezerReader(base))
	for number, hash := range hashes {
		key := append([]byte("tx-"), hash[:]...)
		if value, err := fc.db.Get(key); err != nil || len(value) == 0 {
			t.Fatalf("fused cancel/retry lost hot tx index %d: value=%x err=%v", number, value, err)
		}
		if got := rawdb.ReadTransactionIndex(chainDB, hash[:]); got == nil || *got != uint64(number) {
			t.Fatalf("fused cancel/retry lookup %d=%v", number, got)
		}
	}
}

// blockingSyncFreezer pauses only after the real fsync has succeeded. It
// models shutdown in the intentional recovery window between durable ancient
// append and hot-row deletion.
type blockingSyncFreezer struct {
	FreezerStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	syncs   uint64
}

func (f *blockingSyncFreezer) Sync() error {
	if err := f.FreezerStore.Sync(); err != nil {
		return err
	}
	f.syncs++
	f.once.Do(func() { close(f.entered) })
	<-f.release
	return nil
}

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
	if row, ok, err := rawdb.ReadStageProgressRow(fc.db, rawdb.StageChainFreezerStateRootPrune); err != nil || !ok || row.BlockNum != 32 || !row.HasBlockHash || row.BlockHash != fc.ReadBlockHashByNumber(32) {
		t.Fatalf("StageChainFreezerStateRootPrune row after freeze = %+v ok=%v err=%v, want hash-bound block32", row, ok, err)
	}
	// KV rows for frozen blocks should be gone.
	for n := uint64(0); n <= 32; n++ {
		if v, err := fc.db.Get(blockKVKey(n)); err == nil && len(v) > 0 {
			t.Fatalf("Pebble still has b-%d after freeze", n)
		}
		if v, err := fc.db.Get(txInfoBlockKVKey(n)); err == nil && len(v) > 0 {
			t.Fatalf("Pebble still has tib-%d after freeze", n)
		}
		if root := rawdb.ReadBlockStateRootRaw(fc.db, fc.ReadBlockHashByNumber(n)); root != nil {
			t.Fatalf("Pebble still has state root for block %d after freeze: %x", n, root)
		}
	}
	chainDB := rawdb.NewChainDB(fc.db, r.freezer)
	if got := rawdb.ReadBlockStateRoot(chainDB, fc.ReadBlockHashByNumber(7)); got != tcommon.BytesToHash(stateRootBytes(7)) {
		t.Fatalf("cold state root after freeze = %x, want %x", got, tcommon.BytesToHash(stateRootBytes(7)))
	}
	// KV rows for post-margin blocks should remain.
	for n := uint64(33); n < 50; n++ {
		if v, err := fc.db.Get(blockKVKey(n)); err != nil || len(v) == 0 {
			t.Fatalf("Pebble lost b-%d (should still be hot)", n)
		}
		if root := rawdb.ReadBlockStateRootRaw(fc.db, fc.ReadBlockHashByNumber(n)); root == nil {
			t.Fatalf("Pebble lost state root for block %d (should still be hot)", n)
		}
	}
}

func TestOnePassPrunesLegacyFrozenStateRoots(t *testing.T) {
	t.Parallel()
	fc := newFakeChain()
	const blocks = uint64(5)
	for n := uint64(0); n < blocks; n++ {
		fc.plantBlock(t, n)
	}
	fc.setSolidified(int64(blocks - 1))

	f := newFreezer(t)
	if _, err := f.ModifyAncients(func(op rawdb.AncientWriteOp) error {
		for n := uint64(0); n < blocks; n++ {
			if err := op.AppendRaw(rawdbAncientBlocks, n, blockBytes(n)); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdbAncientTxInfos, n, txInfosBytes(n)); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdbAncientStateRoots, n, stateRootBytes(n)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed ancient rows: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync seeded ancient rows: %v", err)
	}
	// Model a database written by the previous freezer: block and tx-info
	// rows have left Pebble, but the duplicate hash-keyed roots remain.
	if err := rawdb.DeleteFrozenBlockRange(fc.db, 0, blocks-1); err != nil {
		t.Fatalf("delete legacy frozen block rows: %v", err)
	}

	r := New(fc, wrapFreezer(f), Config{
		Enabled:      true,
		MarginBlocks: 0,
		BatchBlocks:  2,
	})
	for _, wantStage := range []uint64{1, 3, 4} {
		frozen, err := r.OnePass()
		if err != nil {
			t.Fatalf("OnePass through legacy root migration to %d: %v", wantStage, err)
		}
		if frozen != 0 {
			t.Fatalf("legacy root migration frozen=%d, want 0", frozen)
		}
		row, ok, err := rawdb.ReadStageProgressRow(fc.db, rawdb.StageChainFreezerStateRootPrune)
		if err != nil || !ok || row.BlockNum != wantStage || !row.HasBlockHash || row.BlockHash != fc.ReadBlockHashByNumber(wantStage) {
			t.Fatalf("legacy root stage after migration to %d = %+v ok=%v err=%v", wantStage, row, ok, err)
		}
		for n := uint64(0); n < blocks; n++ {
			root := rawdb.ReadBlockStateRootRaw(fc.db, fc.ReadBlockHashByNumber(n))
			if n <= wantStage && root != nil {
				t.Fatalf("legacy root for block %d remains after stage %d: %x", n, wantStage, root)
			}
			if n > wantStage && root == nil {
				t.Fatalf("legacy root for block %d removed before stage %d", n, wantStage)
			}
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
	if err := fc.db.Put(blockKVKey(0), []byte("not-a-block")); err != nil {
		t.Fatal(err)
	}
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
	if err := fc.db.Put(txInfoBlockKVKey(0), []byte("not-a-transaction-ret")); err != nil {
		t.Fatal(err)
	}
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
	if root := rawdb.ReadBlockStateRootRaw(fc.db, fc.ReadBlockHashByNumber(0)); root == nil {
		t.Fatal("hot state root after state-root lookup error is missing, want retained")
	}
}

func TestOnePassRejectsMalformedStateRootBeforeAppending(t *testing.T) {
	t.Parallel()
	fc := newFakeChain()
	for n := uint64(0); n < 3; n++ {
		fc.plantBlock(t, n)
	}
	fc.mu.Lock()
	fc.stateRootRaw[0] = []byte{0x01}
	fc.mu.Unlock()
	fc.setSolidified(2)

	f := newFreezer(t)
	r := New(fc, wrapFreezer(f), Config{
		Enabled:      true,
		MarginBlocks: 0,
		BatchBlocks:  1000,
	})
	frozen, err := r.OnePass()
	if err == nil || !strings.Contains(err.Error(), "state root for block 0 has length 1") {
		t.Fatalf("OnePass malformed state root = frozen %d err %v, want length error", frozen, err)
	}
	if frozen != 0 {
		t.Fatalf("frozen after malformed state root = %d, want 0", frozen)
	}
	for _, kind := range []string{rawdbAncientBlocks, rawdbAncientTxInfos, rawdbAncientStateRoots} {
		if count, err := f.AncientCount(kind); err != nil || count != 0 {
			t.Fatalf("ancient %s count after malformed state root = %d/%v, want 0/nil", kind, count, err)
		}
	}
	if v, err := fc.db.Get(blockKVKey(0)); err != nil || len(v) == 0 {
		t.Fatalf("hot block row after malformed state root = len %d err %v, want retained", len(v), err)
	}
	if root := rawdb.ReadBlockStateRootRaw(fc.db, fc.ReadBlockHashByNumber(0)); root == nil {
		t.Fatal("hot state root after malformed state root is missing, want retained")
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

func TestOnePassPropagatesFinishStageHashLookupError(t *testing.T) {
	t.Parallel()
	fc := newFakeChain()
	for n := uint64(0); n < 20; n++ {
		fc.plantBlock(t, n)
	}
	fc.setSolidified(15)
	if err := rawdb.WriteStageProgressWithHash(fc.db, rawdb.StageFinish, 10, fc.ReadBlockHashByNumber(10)); err != nil {
		t.Fatalf("write finish stage: %v", err)
	}
	fc.mu.Lock()
	fc.blockHashErr[10] = errors.New("canonical hash corrupt")
	fc.mu.Unlock()

	r := New(fc, wrapFreezer(newFreezer(t)), Config{
		Enabled:      true,
		MarginBlocks: 0,
		BatchBlocks:  1000,
	})
	frozen, err := r.OnePass()
	if err == nil || !strings.Contains(err.Error(), "finish stage 10 canonical hash lookup") || !strings.Contains(err.Error(), "canonical hash corrupt") {
		t.Fatalf("OnePass error = %v, want finish stage hash lookup error", err)
	}
	if frozen != 0 {
		t.Fatalf("frozen=%d, want 0 after finish hash lookup error", frozen)
	}
	if got, err := r.freezer.AncientCount(rawdbAncientBlocks); err != nil || got != 0 {
		t.Fatalf("ancient blocks after rejected pass = %d err=%v, want 0", got, err)
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
	advanced := 0
	var hookStage uint64
	hookStageOK := false
	r.AddChainFreezerAdvanceHook(func() {
		stage, ok, err := rawdb.ReadStageProgress(fc.db, rawdb.StageChainFreezer)
		if err != nil {
			t.Errorf("read ChainFreezer stage from hook: %v", err)
			return
		}
		advanced++
		hookStage = stage
		hookStageOK = ok
	})
	frozen, err := r.OnePass()
	if err != nil {
		t.Fatalf("OnePass: %v", err)
	}
	if frozen != 0 {
		t.Fatalf("OnePass frozen=%d, want no new rows", frozen)
	}
	if advanced != 1 {
		t.Fatalf("ChainFreezer advance hooks = %d, want 1 after stage backfill", advanced)
	}
	if !hookStageOK || hookStage != 9 {
		t.Fatalf("ChainFreezer stage from hook = %d/%v, want 9/true", hookStage, hookStageOK)
	}
	if got, ok, err := rawdb.ReadStageProgress(fc.db, rawdb.StageChainFreezer); err != nil || !ok || got != 9 {
		t.Fatalf("StageChainFreezer backfill = %d ok=%v err=%v, want 9", got, ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(fc.db, rawdb.StageChainFreezer); err != nil || !ok || !row.HasBlockHash || row.BlockHash != fc.ReadBlockHashByNumber(9) {
		t.Fatalf("StageChainFreezer backfill row = %+v ok=%v err=%v, want hash-bound block9", row, ok, err)
	}
}

func TestRunnerRequestPassRunsBeforeInterval(t *testing.T) {
	fc := newFakeChain()
	for n := uint64(0); n < 2; n++ {
		fc.plantBlock(t, n)
	}
	f := newFreezer(t)
	r := New(fc, wrapFreezer(f), Config{
		Enabled:      true,
		Interval:     time.Hour,
		MarginBlocks: 0,
		BatchBlocks:  10,
	})
	advanced := make(chan struct{}, 1)
	r.AddChainFreezerAdvanceHook(func() { advanced <- struct{}{} })
	if err := r.Start(); err != nil {
		t.Fatalf("start runner: %v", err)
	}
	defer func() {
		if err := r.Stop(); err != nil {
			t.Fatalf("stop runner: %v", err)
		}
	}()

	waitForRunnerPasses(t, r, 1)
	fc.setSolidified(1)
	r.RequestPass()
	r.RequestPass()
	select {
	case <-advanced:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for requested freezer pass")
	}
	if got, ok, err := rawdb.ReadStageProgress(fc.db, rawdb.StageChainFreezer); err != nil || !ok || got != 1 {
		t.Fatalf("StageChainFreezer after requested pass = %d ok=%v err=%v, want 1", got, ok, err)
	}
}

func waitForRunnerPasses(t *testing.T, r *Runner, want uint64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if r.Snapshot().PassesCompleted >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d freezer passes", want)
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

func TestOnePassPropagatesChainFreezerStageHashLookupError(t *testing.T) {
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
	fc.mu.Lock()
	fc.blockHashErr[9] = errors.New("canonical hash corrupt")
	fc.mu.Unlock()

	r := New(fc, &freezerWriter{AncientReader: rawdb.NewFreezerReader(f), f: f}, Config{
		Enabled:      true,
		MarginBlocks: 0,
		BatchBlocks:  10,
	})
	frozen, err := r.OnePass()
	if err == nil || !strings.Contains(err.Error(), "ChainFreezer stage 9") || !strings.Contains(err.Error(), "canonical hash corrupt") {
		t.Fatalf("OnePass frozen=%d err=%v, want ChainFreezer hash lookup error", frozen, err)
	}
	if frozen != 0 {
		t.Fatalf("frozen=%d, want 0 after ChainFreezer hash lookup error", frozen)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(fc.db, rawdb.StageChainFreezer); err != nil || ok || row.BlockNum != 0 {
		t.Fatalf("ChainFreezer stage after hash lookup error = %+v ok=%v err=%v, want absent", row, ok, err)
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

func TestOnePassCrashReconciliationDetectsTxInfoOnlyLeftover(t *testing.T) {
	dir := t.TempDir()
	fc := newFakeChain()
	for n := uint64(0); n < 20; n++ {
		fc.plantBlock(t, n)
	}
	// Keep the normal append range empty so this test isolates startup
	// reconciliation of the already-frozen prefix.
	fc.setSolidified(15)

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
			if err := op.AppendRaw(rawdbAncientTxInfos, n, txInfosBytes(n)); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdbAncientStateRoots, n, stateRootBytes(n)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed ancient rows: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}

	// Reproduce an old non-atomic cleanup that deleted b-* but crashed before
	// deleting tib-*. Probing only the highest block row would miss this state.
	if err := fc.db.DeleteRange(blockKVKey(0), blockKVKey(10)); err != nil {
		t.Fatal(err)
	}
	if _, err := fc.db.Get(blockKVKey(9)); err == nil {
		t.Fatal("precondition: highest hot block survived")
	}
	if value, err := fc.db.Get(txInfoBlockKVKey(9)); err != nil || len(value) == 0 {
		t.Fatalf("precondition: highest tx info missing: %x err=%v", value, err)
	}
	r := New(fc, &freezerWriter{AncientReader: rawdb.NewFreezerReader(f), f: f}, Config{
		Enabled:      true,
		MarginBlocks: 8,
		BatchBlocks:  10,
	})
	if frozen, err := r.OnePass(); err != nil || frozen != 0 {
		t.Fatalf("tx-only reconciliation frozen=%d err=%v, want 0/nil", frozen, err)
	}
	if value, err := fc.db.Get(txInfoBlockKVKey(5)); err == nil && len(value) > 0 {
		t.Fatal("reconciliation left frozen hot tx info tib-5 behind")
	}
}

func TestOnePassReconcilesLaterPhase3DeleteFailureInSameProcess(t *testing.T) {
	fc := newFakeChain()
	for n := uint64(0); n < 8; n++ {
		fc.plantBlock(t, n)
	}
	fc.setSolidified(7)
	injected := errors.New("injected phase-3 batch failure")
	failingDB := &failNextBatchDB{KeyValueStore: fc.db, err: injected}
	fc.db = failingDB
	fz := newFreezer(t)
	r := New(fc, wrapFreezer(fz), Config{
		Enabled:         true,
		MarginBlocks:    0,
		BatchBlocks:     4,
		V2Enabled:       true,
		DirectV2:        true,
		V2FrameBlocks:   2,
		V2SegmentBlocks: 4,
	})

	if frozen, err := r.OnePass(); frozen != 4 || err != nil {
		t.Fatalf("first direct segment frozen=%d err=%v, want 4/nil", frozen, err)
	}
	failingDB.fail.Store(true)
	if frozen, err := r.OnePass(); frozen != 0 || !errors.Is(err, injected) {
		t.Fatalf("second-segment delete pass frozen=%d err=%v, want 0/%v", frozen, err, injected)
	}
	if count, err := fz.AncientCount(rawdbAncientBlocks); err != nil || count != 8 {
		t.Fatalf("durable ancient count=%d err=%v, want 8", count, err)
	}
	if value, err := fc.db.Get(blockKVKey(7)); err != nil || len(value) == 0 {
		t.Fatalf("failed phase-3 delete unexpectedly removed b-7: %x err=%v", value, err)
	}
	if value, err := fc.db.Get(txInfoBlockKVKey(7)); err != nil || len(value) == 0 {
		t.Fatalf("failed phase-3 delete unexpectedly removed tib-7: %x err=%v", value, err)
	}

	if frozen, err := r.OnePass(); err != nil || frozen != 0 {
		t.Fatalf("same-process reconciliation frozen=%d err=%v, want 0/nil", frozen, err)
	}
	if _, err := fc.db.Get(blockKVKey(7)); err == nil {
		t.Fatal("same-process reconciliation left b-7")
	}
	if _, err := fc.db.Get(txInfoBlockKVKey(7)); err == nil {
		t.Fatal("same-process reconciliation left tib-7")
	}
	compactedBlocks, compactedTxInfos := false, false
	for _, start := range failingDB.compactStarts {
		compactedBlocks = compactedBlocks || strings.HasPrefix(string(start), "b-")
		compactedTxInfos = compactedTxInfos || strings.HasPrefix(string(start), "tib-")
	}
	if !compactedBlocks || !compactedTxInfos {
		t.Fatalf("compacted prefixes blocks=%t txInfos=%t starts=%q", compactedBlocks, compactedTxInfos, failingDB.compactStarts)
	}
}

func TestRunnerStopRollsBackOpenAncientBatchAndRestartResumes(t *testing.T) {
	fc := newFakeChain()
	for n := uint64(0); n < 10; n++ {
		fc.plantBlock(t, n)
	}
	fc.setSolidified(9)

	f := newFreezer(t)
	blocked := &blockingReadChain{
		ChainSource: fc,
		blockNum:    3,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	r := New(blocked, wrapFreezer(f), Config{
		Enabled:      true,
		Interval:     time.Hour,
		MarginBlocks: 0,
		BatchBlocks:  10,
	})
	if err := r.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for phase-1 block read")
	}

	// Queue another pass to reproduce the shutdown race where both wake and
	// quit are ready after the current pass returns.
	r.RequestPass()
	stopDone := make(chan error, 1)
	go func() { stopDone <- r.Stop() }()
	select {
	case <-r.pauseCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for runner cancellation")
	}
	close(blocked.release)
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after releasing phase-1 read")
	}

	// ModifyAncients must roll all three tables back together. The queued wake
	// must not start a second pass after cancellation.
	for _, kind := range []string{rawdbAncientBlocks, rawdbAncientTxInfos, rawdbAncientStateRoots} {
		if got, err := f.AncientCount(kind); err != nil || got != 0 {
			t.Fatalf("%s count after interrupted batch = %d/%v, want 0/nil", kind, got, err)
		}
	}
	if got := r.Snapshot().PassesCompleted; got != 0 {
		t.Fatalf("completed passes after interrupted batch = %d, want 0", got)
	}

	// A fresh runner resumes from AncientCount=0 and freezes the full range.
	r2 := New(fc, wrapFreezer(f), Config{
		Enabled:      true,
		MarginBlocks: 0,
		BatchBlocks:  10,
	})
	if frozen, err := r2.OnePass(); err != nil || frozen != 10 {
		t.Fatalf("restart pass = frozen %d err %v, want 10/nil", frozen, err)
	}
	for _, kind := range []string{rawdbAncientBlocks, rawdbAncientTxInfos, rawdbAncientStateRoots} {
		if got, err := f.AncientCount(kind); err != nil || got != 10 {
			t.Fatalf("%s count after restart = %d/%v, want 10/nil", kind, got, err)
		}
	}
}

func TestRunnerStopAfterAncientSyncRestartsFromDurableHead(t *testing.T) {
	dir := t.TempDir()
	fc := newFakeChain()
	for n := uint64(0); n < 10; n++ {
		fc.plantBlock(t, n)
	}
	fc.setSolidified(9)

	f1, err := rawdbfreezer.NewFreezer(dir, "", false, 2049, FreezerTableSet())
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}
	baseStore := &freezerWriter{AncientReader: rawdb.NewFreezerReader(f1), f: f1}
	blocked := &blockingSyncFreezer{
		FreezerStore: baseStore,
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
	}
	r := New(fc, blocked, Config{
		Enabled:      true,
		Interval:     time.Hour,
		MarginBlocks: 0,
		BatchBlocks:  10,
	})
	if err := r.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ancient fsync")
	}

	// The ancient append is durable but hot deletion has not begun. Stop must
	// leave that recoverable overlap intact and ignore the queued second pass.
	r.RequestPass()
	stopDone := make(chan error, 1)
	go func() { stopDone <- r.Stop() }()
	select {
	case <-r.pauseCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for runner cancellation")
	}
	close(blocked.release)
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after durable ancient sync")
	}
	if blocked.syncs != 1 {
		t.Fatalf("ancient sync calls = %d, want 1 (no post-stop pass)", blocked.syncs)
	}
	for _, kind := range []string{rawdbAncientBlocks, rawdbAncientTxInfos, rawdbAncientStateRoots} {
		if got, err := f1.AncientCount(kind); err != nil || got != 10 {
			t.Fatalf("%s durable count before reopen = %d/%v, want 10/nil", kind, got, err)
		}
	}
	if hot, err := fc.db.Get(blockKVKey(9)); err != nil || len(hot) == 0 {
		t.Fatalf("hot overlap before reopen = len %d err %v, want retained", len(hot), err)
	}
	if err := f1.Close(); err != nil {
		t.Fatalf("close first freezer: %v", err)
	}

	// Reopen the real freezer files to prove the fsync boundary survives a
	// process lifetime, then let a fresh runner reconcile hot duplicates.
	f2, err := rawdbfreezer.NewFreezer(dir, "", false, 2049, FreezerTableSet())
	if err != nil {
		t.Fatalf("reopen freezer: %v", err)
	}
	t.Cleanup(func() { _ = f2.Close() })
	store2 := &freezerWriter{AncientReader: rawdb.NewFreezerReader(f2), f: f2}
	r2 := New(fc, store2, Config{
		Enabled:      true,
		MarginBlocks: 0,
		BatchBlocks:  10,
	})
	if frozen, err := r2.OnePass(); err != nil || frozen != 0 {
		t.Fatalf("restart reconciliation = frozen %d err %v, want 0/nil", frozen, err)
	}
	if got, err := f2.AncientCount(rawdbAncientBlocks); err != nil || got != 10 {
		t.Fatalf("ancient count after reconciliation = %d/%v, want 10/nil", got, err)
	}
	for n := uint64(0); n < 10; n++ {
		if hot, err := fc.db.Get(blockKVKey(n)); err == nil && len(hot) > 0 {
			t.Fatalf("hot block %d remains after restart reconciliation", n)
		}
		if root := rawdb.ReadBlockStateRootRaw(fc.db, fc.ReadBlockHashByNumber(n)); root != nil {
			t.Fatalf("hot state root %d remains after restart reconciliation: %x", n, root)
		}
	}
	if got, err := f2.Ancient(rawdbAncientBlocks, 9); err != nil || string(got) != string(blockBytes(9)) {
		t.Fatalf("ancient block 9 after restart = %x/%v, want original", got, err)
	}
	if stage, ok, err := rawdb.ReadStageProgress(fc.db, rawdb.StageChainFreezer); err != nil || !ok || stage != 9 {
		t.Fatalf("ChainFreezer stage after restart = %d ok=%v err=%v, want 9", stage, ok, err)
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
		"pebble/size/rows",
		"pebble/size/complete",
		"pebble/size/sampled_at",
		"v2/coverage",
		"v2/blocks",
		"v2/backlog/blocks",
		"v2/backlog/segments",
		"v2/batch/segments",
		"v2/batch/duration",
		"v2/batch/budget_exhausted",
		"v2/deferred/catchup",
		"v2/deferred/resource",
		"v2/deferred/error_backoff",
		"v2/deferred/source_pruned",
		"v2/errors",
		"txindex/coverage",
		"txindex/pruned",
		"txindex/rows/archived",
		"txindex/rows/pruned",
		"txindex/debt/blocks",
		"txindex/prune/blocks",
		"txindex/prune/rows",
		"txindex/prune/duration",
		"txindex/maintenance/admitted",
		"txindex/maintenance/deferred",
		"txindex/deferred/catchup",
		"txindex/deferred/resource",
		"txindex/deferred/error_backoff",
		"txindex/deferred/sync",
		"txindex/errors",
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
	if d.Interval <= 0 || d.MarginBlocks == 0 || d.BatchBlocks == 0 || d.HeavyMaintenanceErrorBackoff <= 0 || d.V2CatchupTimeBudget <= 0 || d.V2CatchupMaxSegments <= 1 {
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
