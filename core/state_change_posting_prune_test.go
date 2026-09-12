package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type postingPruneGuardDB struct {
	ethdb.KeyValueStore
	beforeWrite func() error
	readErr     error
}

func (db *postingPruneGuardDB) Get(key []byte) ([]byte, error) {
	if db.readErr != nil {
		return nil, db.readErr
	}
	return db.KeyValueStore.Get(key)
}

func (db *postingPruneGuardDB) NewBatch() ethdb.Batch {
	return &postingPruneGuardBatch{Batch: db.KeyValueStore.NewBatch(), db: db}
}

type postingPruneGuardBatch struct {
	ethdb.Batch
	db *postingPruneGuardDB
}

func (b *postingPruneGuardBatch) Write() error {
	if b.db.beforeWrite != nil {
		if err := b.db.beforeWrite(); err != nil {
			return err
		}
	}
	return b.Batch.Write()
}

type postingPruneGuardFixture struct {
	bc     *BlockChain
	db     *postingPruneGuardDB
	blocks []*types.Block
}

func newPostingPruneGuardFixture(t *testing.T) postingPruneGuardFixture {
	t.Helper()
	store := ethrawdb.NewMemoryDatabase()
	cfg := cloneMainnetChainConfig()
	cfg.HistoryEnabled = true
	_, _, err := SetupGenesisBlock(store, &params.Genesis{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	bc, err := NewBlockChain(store, state.NewDatabase(store), cfg)
	if err != nil {
		t.Fatal(err)
	}
	db := &postingPruneGuardDB{KeyValueStore: store}
	bc.db, bc.chaindb = db, rawdb.NewChainDB(db, nil)
	t.Cleanup(func() {
		db.readErr, db.beforeWrite = nil, nil
		if err := bc.Close(); err != nil {
			t.Errorf("close chain: %v", err)
		}
		_ = store.Close()
	})
	blocks := []*types.Block{bc.CurrentBlock()}
	for height := uint64(1); height <= 6; height++ {
		block := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(height), Timestamp: int64(height * 3000), ParentHash: blocks[height-1].Hash().Bytes(),
		}}})
		if err := rawdb.WriteBlock(store, block); err != nil {
			t.Fatal(err)
		}
		blocks = append(blocks, block)
	}
	bc.currentBlock.Store(blocks[6])
	dp := bc.cachedDynProps()
	dp.SetLatestSolidifiedBlockNum(6)
	bc.storeDynPropsCache(dp)
	for _, stage := range []rawdb.StageID{rawdb.StageFinish, rawdb.StageStateHistoryIndex} {
		if err := rawdb.WriteStageProgressWithHash(store, stage, 6, blocks[6].Hash()); err != nil {
			t.Fatal(err)
		}
	}
	for _, height := range []uint64{1, 2, 5} {
		if err := putPostingPruneGuardRow(store, height); err != nil {
			t.Fatal(err)
		}
	}
	return postingPruneGuardFixture{bc: bc, db: db, blocks: blocks}
}

func putPostingPruneGuardRow(db ethdb.KeyValueWriter, height uint64) error {
	return rawdb.WriteStateDomainChangePostingIndex(db, &rawdb.StateDomainChange{
		BlockNum: height, FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: common.Address{0x41, 1},
	})
}

func postingPruneGuardLimits() rawdb.StateChangePostingPruneLimits {
	return rawdb.StateChangePostingPruneLimits{MaxScannedRows: 10, MaxScannedBytes: 1 << 20, MaxDeleteBytes: 1 << 20, MaxDuration: time.Second}
}

func (f postingPruneGuardFixture) prune(ctx context.Context, anchor common.Hash, cursor []byte, limits rawdb.StateChangePostingPruneLimits) (rawdb.StateChangePostingPruneChunkResult, common.Hash, error) {
	return f.bc.PruneStateChangePostingChunk(ctx, 2, 4, f.blocks[4].Hash(), anchor, cursor, limits)
}

func postingPruneGuardRows(t *testing.T, db ethdb.Iteratee) [][]byte {
	t.Helper()
	prefix, _, _, _ := rawdb.StateHistoryPostingKeyspaceBounds()
	it := db.NewIterator(prefix, nil)
	defer it.Release()
	var rows [][]byte
	for it.Next() {
		rows = append(rows, bytes.Clone(it.Key()))
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestStateChangePostingPruneGuardSuccessAndNewerWriter(t *testing.T) {
	f := newPostingPruneGuardFixture(t)
	limits := postingPruneGuardLimits()
	limits.MaxScannedRows = 1
	first, anchor, err := f.prune(context.Background(), common.Hash{}, nil, limits)
	if err != nil || first.RowsDeleted != 1 || anchor != f.blocks[2].Hash() || first.Complete {
		t.Fatalf("first chunk=%+v anchor=%x err=%v", first, anchor, err)
	}
	// The async writer need not take chainmu: its strictly newer singleton uses
	// a different immutable key even for the same account digest.
	f.db.beforeWrite = func() error { return putPostingPruneGuardRow(f.db.KeyValueStore, 6) }
	second, nextAnchor, err := f.prune(context.Background(), anchor, first.NextCursor, postingPruneGuardLimits())
	if err != nil || second.RowsDeleted != 1 || !second.Complete || nextAnchor != anchor {
		t.Fatalf("second chunk=%+v anchor=%x err=%v", second, nextAnchor, err)
	}
	if rows := postingPruneGuardRows(t, f.db); len(rows) != 2 {
		t.Fatalf("remaining postings=%d, want both newer rows", len(rows))
	}
}

func TestStateChangePostingPruneGuardDefersWithoutMutation(t *testing.T) {
	for _, name := range []string{"index-lock", "closed", "head", "solidified", "missing-finish", "missing-index", "finish-behind-proof", "index-behind-H"} {
		t.Run(name, func(t *testing.T) {
			f := newPostingPruneGuardFixture(t)
			switch name {
			case "index-lock":
				f.bc.stateHistoryIndexMu.Lock()
				defer f.bc.stateHistoryIndexMu.Unlock()
			case "closed":
				f.bc.closed.Store(true)
				defer f.bc.closed.Store(false)
			case "head":
				f.bc.currentBlock.Store(f.blocks[3])
			case "solidified":
				dp := f.bc.cachedDynProps()
				dp.SetLatestSolidifiedBlockNum(1)
				f.bc.storeDynPropsCache(dp)
			case "missing-finish", "missing-index":
				stage := rawdb.StageFinish
				if name == "missing-index" {
					stage = rawdb.StageStateHistoryIndex
				}
				if err := rawdb.DeleteStageProgress(f.db, stage); err != nil {
					t.Fatal(err)
				}
			case "finish-behind-proof", "index-behind-H":
				stage, height := rawdb.StageFinish, uint64(3)
				if name == "index-behind-H" {
					stage, height = rawdb.StageStateHistoryIndex, 1
				}
				if err := rawdb.WriteStageProgressWithHash(f.db, stage, height, f.blocks[height].Hash()); err != nil {
					t.Fatal(err)
				}
			}
			cursor := postingPruneGuardRows(t, f.db)[0]
			result, anchor, err := f.prune(context.Background(), f.blocks[2].Hash(), cursor, postingPruneGuardLimits())
			if !errors.Is(err, ErrStateChangePostingPruneDeferred) || result.RowsDeleted != 0 || result.RowsScanned != 0 || result.Complete || !bytes.Equal(result.NextCursor, cursor) || anchor != f.blocks[2].Hash() {
				t.Fatalf("deferred chunk=%+v anchor=%x err=%v", result, anchor, err)
			}
			if len(postingPruneGuardRows(t, f.db)) != 3 {
				t.Fatal("deferred chunk mutated postings")
			}
		})
	}
}

func TestStateChangePostingPruneGuardRejectsProofAndStageErrors(t *testing.T) {
	for _, name := range []string{"proof-replaced", "anchor-replaced", "finish-hash", "index-hash", "unbound-finish", "unbound-index", "read-error", "canceled"} {
		t.Run(name, func(t *testing.T) {
			f := newPostingPruneGuardFixture(t)
			ctx := context.Background()
			anchor := common.Hash{}
			var want error
			switch name {
			case "proof-replaced":
				replacement := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 4, Timestamp: 99}}})
				if err := rawdb.WriteBlock(f.db, replacement); err != nil {
					t.Fatal(err)
				}
				want = ErrStateChangePostingPruneBoundaryChanged
			case "anchor-replaced":
				anchor, want = common.Hash{0xee}, ErrStateChangePostingPruneBoundaryChanged
			case "finish-hash", "index-hash", "unbound-finish", "unbound-index":
				stage := rawdb.StageFinish
				if name == "index-hash" || name == "unbound-index" {
					stage = rawdb.StageStateHistoryIndex
				}
				var err error
				if name == "unbound-finish" || name == "unbound-index" {
					err = rawdb.WriteStageProgress(f.db, stage, 6)
				} else {
					err = rawdb.WriteStageProgressWithHash(f.db, stage, 6, common.Hash{0xee})
				}
				if err != nil {
					t.Fatal(err)
				}
			case "read-error":
				want = errors.New("injected read error")
				f.db.readErr = want
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = context.Canceled
			}
			result, returnedAnchor, err := f.prune(ctx, anchor, nil, postingPruneGuardLimits())
			if err == nil || want != nil && !errors.Is(err, want) || errors.Is(err, ErrStateChangePostingPruneDeferred) || result.RowsScanned != 0 || returnedAnchor != anchor {
				t.Fatalf("rejected chunk=%+v anchor=%x err=%v want=%v", result, returnedAnchor, err, want)
			}
			f.db.readErr = nil
			if len(postingPruneGuardRows(t, f.db)) != 3 {
				t.Fatal("rejected proof mutated postings")
			}
		})
	}
}

func TestStateChangePostingPruneGuardHoldsLocksThroughWriteAndClose(t *testing.T) {
	f := newPostingPruneGuardFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.db.beforeWrite = func() error {
		if f.bc.chainmu.TryLock() {
			f.bc.chainmu.Unlock()
			return errors.New("chain lock released before batch write")
		}
		if f.bc.stateHistoryIndexMu.TryLock() {
			f.bc.stateHistoryIndexMu.Unlock()
			return errors.New("index lock released before batch write")
		}
		once.Do(func() { close(entered); <-release })
		return nil
	}
	pruned := make(chan error, 1)
	go func() {
		_, _, err := f.prune(context.Background(), common.Hash{}, nil, postingPruneGuardLimits())
		pruned <- err
	}()
	select {
	case <-entered:
	case err := <-pruned:
		t.Fatalf("prune returned before barrier: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("prune did not reach batch write")
	}
	_, _, err := f.prune(context.Background(), common.Hash{}, nil, postingPruneGuardLimits())
	if !errors.Is(err, ErrStateChangePostingPruneDeferred) {
		close(release)
		t.Fatalf("concurrent chunk err=%v", err)
	}
	closed := make(chan error, 1)
	go func() { closed <- f.bc.Close() }()
	// Close must acquire the same outer lock; the active write's direct lock
	// assertions above establish exclusion without a timing-based sleep.
	close(release)
	for name, ch := range map[string]<-chan error{"prune": pruned, "close": closed} {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s did not finish", name)
		}
	}
}

func TestStateChangePostingPruneGuardBatchFailureAndRewind(t *testing.T) {
	f := newPostingPruneGuardFixture(t)
	limits := postingPruneGuardLimits()
	limits.MaxScannedRows = 1
	first, anchor, err := f.prune(context.Background(), common.Hash{}, nil, limits)
	if err != nil {
		t.Fatal(err)
	}
	fail := errors.New("injected batch error")
	f.db.beforeWrite = func() error { return fail }
	failed, failedAnchor, err := f.prune(context.Background(), anchor, first.NextCursor, limits)
	if !errors.Is(err, fail) || failed.RowsDeleted != 0 || !bytes.Equal(failed.NextCursor, first.NextCursor) || failedAnchor != anchor {
		t.Fatalf("failed chunk=%+v anchor=%x err=%v", failed, failedAnchor, err)
	}
	f.db.beforeWrite = nil
	f.bc.chainmu.Lock()
	err = f.bc.rewindStateHistoryIndexStageLocked(1, f.blocks[1].Hash())
	f.bc.chainmu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = f.prune(context.Background(), anchor, first.NextCursor, limits)
	if !errors.Is(err, ErrStateChangePostingPruneDeferred) || len(postingPruneGuardRows(t, f.db)) != 2 {
		t.Fatalf("rewound stage allowed prune: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(f.db, rawdb.StageStateHistoryIndex, 6, f.blocks[6].Hash()); err != nil {
		t.Fatal(err)
	}
	retried, _, err := f.prune(context.Background(), anchor, first.NextCursor, limits)
	if err != nil || retried.RowsDeleted != 1 {
		t.Fatalf("retry chunk=%+v err=%v", retried, err)
	}
}

func TestStateChangePostingPruneGuardInvalidProof(t *testing.T) {
	f := newPostingPruneGuardFixture(t)
	cursor := postingPruneGuardRows(t, f.db)[0]
	if result, _, err := f.prune(context.Background(), common.Hash{}, cursor, postingPruneGuardLimits()); err == nil || result.RowsScanned != 0 || !bytes.Equal(result.NextCursor, cursor) {
		t.Fatalf("unanchored resume chunk=%+v err=%v", result, err)
	}
	for _, proof := range []struct {
		h, head uint64
		hash    common.Hash
	}{{0, 4, f.blocks[4].Hash()}, {3, 2, f.blocks[2].Hash()}, {2, 4, common.Hash{}}} {
		t.Run(fmt.Sprintf("%d-%d-%x", proof.h, proof.head, proof.hash), func(t *testing.T) {
			result, _, err := f.bc.PruneStateChangePostingChunk(context.Background(), proof.h, proof.head, proof.hash, common.Hash{}, nil, postingPruneGuardLimits())
			if err == nil || result.RowsScanned != 0 || errors.Is(err, ErrStateChangePostingPruneDeferred) {
				t.Fatalf("invalid proof chunk=%+v err=%v", result, err)
			}
		})
	}
}

type postingPruneGuardCallResult struct {
	chunk  rawdb.StateChangePostingPruneChunkResult
	anchor common.Hash
	err    error
	timing StateChangePostingPruneTimings
}

func TestStateChangePostingPruneGuardAdmissionAfterQueuedLock(t *testing.T) {
	for _, mode := range []string{"success", "gate-lost", "pressure-changed", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			f := newPostingPruneGuardFixture(t)
			gate := maintenance.NewHeavyWorkGate()
			if !gate.CanTryAcquire() {
				t.Fatal("initial gate hint rejected")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var calls atomic.Int32
			var pressureReady atomic.Bool
			pressureReady.Store(true)
			admit := func() (func(), bool) {
				calls.Add(1)
				if f.bc.chainmu.TryLock() {
					f.bc.chainmu.Unlock()
					t.Error("admission ran without chain lock")
				}
				if f.bc.stateHistoryIndexMu.TryLock() {
					f.bc.stateHistoryIndexMu.Unlock()
					t.Error("admission ran without index lock")
				}
				if !pressureReady.Load() {
					return nil, false
				}
				return gate.TryAcquire()
			}
			writes := 0
			f.db.beforeWrite = func() error {
				writes++
				if gate.CanTryAcquire() {
					t.Error("lease ended before batch write")
				}
				return nil
			}
			done := make(chan postingPruneGuardCallResult, 1)
			f.bc.chainmu.Lock()
			unlocked, joined := false, false
			var otherRelease func()
			defer func() {
				cancel()
				if !unlocked {
					f.bc.chainmu.Unlock()
				}
				if otherRelease != nil {
					otherRelease()
				}
				if !joined {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("admission waiter leaked")
					}
				}
			}()
			awaitPostingPruneGuardWaiter(t, f, done, func() {
				go func() {
					chunk, anchor, timing, err := f.bc.PruneStateChangePostingChunkWithAdmission(ctx, 2, 4, f.blocks[4].Hash(), common.Hash{}, nil, postingPruneGuardLimits(), admit)
					done <- postingPruneGuardCallResult{chunk: chunk, anchor: anchor, err: err, timing: timing}
				}()
			})
			if calls.Load() != 0 {
				t.Fatal("admission acquired a lease before the chain handoff")
			}
			var ok bool
			otherRelease, ok = gate.TryAcquire()
			if !ok {
				t.Fatal("queued chain waiter prevented other maintenance admission")
			}
			// The gate owner may itself need chainmu. The posting waiter must not
			// wait for that owner while holding chainmu; it must decline immediately.
			if mode != "gate-lost" {
				otherRelease()
				otherRelease = nil
			}
			if mode == "pressure-changed" {
				pressureReady.Store(false)
			}
			if mode == "canceled" {
				cancel()
			}
			f.bc.chainmu.Unlock()
			unlocked = true
			var out postingPruneGuardCallResult
			select {
			case out = <-done:
				joined = true
			case <-time.After(5 * time.Second):
				t.Fatal("gate owner and chain waiter failed to make progress")
			}
			if out.timing.ChainWait <= 0 || out.timing.ChainHeld <= 0 {
				t.Fatalf("missing lock timing: %+v", out.timing)
			}
			if mode == "success" {
				if out.err != nil || out.chunk.RowsDeleted != 2 || writes != 1 || out.timing.GateHeld <= 0 || out.timing.Proof <= 0 || out.chunk.ScanDuration <= 0 || out.chunk.WriteDuration <= 0 {
					t.Fatalf("successful observed chunk=%+v writes=%d", out, writes)
				}
			} else {
				want := ErrStateChangePostingPruneDeferred
				if mode == "canceled" {
					want = context.Canceled
					if calls.Load() != 0 {
						t.Fatal("canceled waiter performed admission")
					}
				}
				if !errors.Is(out.err, want) || out.chunk.RowsScanned != 0 || out.chunk.RowsDeleted != 0 || writes != 0 || out.timing.Proof != 0 || out.timing.GateHeld != 0 || out.chunk.ScanDuration != 0 || out.chunk.WriteDuration != 0 || len(postingPruneGuardRows(t, f.db)) != 3 {
					t.Fatalf("rejected observed chunk=%+v writes=%d", out, writes)
				}
			}
			if otherRelease != nil {
				otherRelease()
				otherRelease = nil
			}
			if !gate.CanTryAcquire() || !f.bc.stateHistoryIndexMu.TryLock() {
				t.Fatal("admission leaked gate or index lock")
			}
			f.bc.stateHistoryIndexMu.Unlock()
			if err := f.bc.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStateChangePostingPruneGuardObservedFailureReleasesLease(t *testing.T) {
	for _, mode := range []string{"proof-read", "write", "missing-admission", "missing-lease"} {
		t.Run(mode, func(t *testing.T) {
			f := newPostingPruneGuardFixture(t)
			gate := maintenance.NewHeavyWorkGate()
			boom := errors.New("injected observed failure")
			admit := StateChangePostingPruneAdmission(gate.TryAcquire)
			switch mode {
			case "proof-read":
				f.db.readErr = boom
			case "write":
				f.db.beforeWrite = func() error { return boom }
			case "missing-admission":
				admit = nil
			case "missing-lease":
				admit = func() (func(), bool) { return nil, true }
			}
			out, _, timing, err := f.bc.PruneStateChangePostingChunkWithAdmission(context.Background(), 2, 4, f.blocks[4].Hash(), common.Hash{}, nil, postingPruneGuardLimits(), admit)
			if err == nil || out.RowsDeleted != 0 || len(out.NextCursor) != 0 || !gate.CanTryAcquire() {
				t.Fatalf("failure leaked progress/lease: out=%+v timing=%+v err=%v", out, timing, err)
			}
			if mode == "proof-read" && (!errors.Is(err, boom) || timing.Proof <= 0 || timing.GateHeld <= 0 || out.ScanDuration != 0 || out.WriteDuration != 0) {
				t.Fatalf("proof failure timing=%+v chunk=%+v err=%v", timing, out, err)
			}
			if mode == "write" && (!errors.Is(err, boom) || out.ScanDuration <= 0 || out.WriteDuration <= 0 || timing.GateHeld <= 0) {
				t.Fatalf("write failure timing=%+v chunk=%+v err=%v", timing, out, err)
			}
		})
	}
}

// The caller holds chainmu. Ownership of the outer mutex establishes that the
// worker passed the initial context check and is now entering/waiting for the
// chain lock. The probe itself can briefly make the outer TryLock fail; retry
// only that completed admission instead of depending on a scheduling sleep.
func awaitPostingPruneGuardWaiter(t *testing.T, f postingPruneGuardFixture, done chan postingPruneGuardCallResult, start func()) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	start()
	for {
		if !f.bc.stateHistoryIndexMu.TryLock() {
			return
		}
		f.bc.stateHistoryIndexMu.Unlock()
		select {
		case out := <-done:
			if !errors.Is(out.err, ErrStateChangePostingPruneDeferred) {
				done <- out // Preserve the result for the caller's cleanup.
				t.Fatalf("call returned before acquiring the held chain lock: %v", out.err)
			}
			start()
		case <-deadline.C:
			t.Fatal("prune did not reach the held chain lock")
		default:
			runtime.Gosched()
		}
	}
}

func TestStateChangePostingPruneGuardQueuedHandoffAndCancellation(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled=%v", canceled), func(t *testing.T) {
			f := newPostingPruneGuardFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			writes := 0
			f.db.beforeWrite = func() error { writes++; return nil }
			done := make(chan postingPruneGuardCallResult, 1)
			f.bc.chainmu.Lock()
			released, joined := false, false
			defer func() {
				cancel()
				if !released {
					f.bc.chainmu.Unlock()
				}
				if !joined {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("queued prune did not exit during cleanup")
					}
				}
			}()
			awaitPostingPruneGuardWaiter(t, f, done, func() {
				go func() {
					chunk, anchor, err := f.prune(ctx, common.Hash{}, nil, postingPruneGuardLimits())
					done <- postingPruneGuardCallResult{chunk: chunk, anchor: anchor, err: err}
				}()
			})
			if canceled {
				cancel()
			}
			// The existing holder finishes. The admitted waiter must take this
			// boundary instead of returning the former immediate chain deferral.
			f.bc.chainmu.Unlock()
			released = true
			var out postingPruneGuardCallResult
			select {
			case out = <-done:
				joined = true
			case <-time.After(5 * time.Second):
				t.Fatal("queued prune did not finish after the holder released")
			}
			if canceled {
				if !errors.Is(out.err, context.Canceled) || out.chunk.RowsScanned != 0 || out.chunk.RowsDeleted != 0 || out.chunk.Complete || out.anchor != (common.Hash{}) || writes != 0 || len(postingPruneGuardRows(t, f.db)) != 3 {
					t.Fatalf("canceled waiter=%+v writes=%d", out, writes)
				}
			} else if out.err != nil || out.chunk.RowsDeleted != 2 || !out.chunk.Complete || out.anchor != f.blocks[2].Hash() || writes != 1 || len(postingPruneGuardRows(t, f.db)) != 1 {
				t.Fatalf("queued waiter=%+v writes=%d", out, writes)
			}
			if !f.bc.stateHistoryIndexMu.TryLock() {
				t.Fatal("queued call leaked the index lock")
			}
			f.bc.stateHistoryIndexMu.Unlock()
			if !f.bc.chainmu.TryLock() {
				t.Fatal("queued call leaked the chain lock")
			}
			f.bc.chainmu.Unlock()
			closed := make(chan error, 1)
			go func() { closed <- f.bc.Close() }()
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not finish after the queued call")
			}
		})
	}
}
