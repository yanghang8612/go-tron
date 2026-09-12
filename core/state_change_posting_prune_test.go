package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
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
	for _, name := range []string{"index-lock", "chain-lock", "closed", "head", "solidified", "missing-finish", "missing-index", "finish-behind-proof", "index-behind-H"} {
		t.Run(name, func(t *testing.T) {
			f := newPostingPruneGuardFixture(t)
			switch name {
			case "index-lock":
				f.bc.stateHistoryIndexMu.Lock()
				defer f.bc.stateHistoryIndexMu.Unlock()
			case "chain-lock":
				f.bc.chainmu.Lock()
				defer f.bc.chainmu.Unlock()
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
