package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestStateDomainChangePruneGuardOnlyBusyAllowsFallback(t *testing.T) {
	for _, name := range []string{"index", "chain"} {
		t.Run(name, func(t *testing.T) {
			f := newPostingPruneGuardFixture(t)
			unlock := f.bc.stateHistoryIndexMu.Unlock
			if name == "index" {
				f.bc.stateHistoryIndexMu.Lock()
			} else {
				f.bc.chainmu.Lock()
				unlock = f.bc.chainmu.Unlock
			}
			called := false
			ran, err := f.bc.TryWithStateDomainChangePruneGuard(context.Background(), 2, 4, f.blocks[4].Hash(), func() error { called = true; return nil })
			unlock()
			if ran || err != nil || called {
				t.Fatalf("busy ran=%v err=%v called=%v", ran, err, called)
			}
			assertStateDomainChangeGuardUnlocked(t, f)
			ran, err = f.bc.TryWithStateDomainChangePruneGuard(context.Background(), 2, 4, f.blocks[4].Hash(), func() error { called = true; return nil })
			if !ran || err != nil || !called {
				t.Fatalf("retry ran=%v err=%v called=%v", ran, err, called)
			}
		})
	}
}

func TestStateDomainChangePruneGuardRejectsUnprovenWork(t *testing.T) {
	for _, name := range []string{"zero-through", "proof-behind", "zero-hash", "nil-work", "closed", "history-disabled", "nil-config", "head", "solidified", "missing-proof", "proof-replaced", "missing-through", "missing-finish", "missing-index", "finish-behind", "index-behind", "finish-hash", "index-hash", "unbound-finish", "unbound-index", "read-error", "canceled"} {
		t.Run(name, func(t *testing.T) {
			f := newPostingPruneGuardFixture(t)
			ctx := context.Background()
			through, proofHead, proofHash := uint64(2), uint64(4), f.blocks[4].Hash()
			called := false
			work := func() error { called = true; return nil }
			switch name {
			case "zero-through":
				through = 0
			case "proof-behind":
				proofHead = 1
			case "zero-hash":
				proofHash = common.Hash{}
			case "nil-work":
				work = nil
			case "closed":
				f.bc.closed.Store(true)
				defer f.bc.closed.Store(false)
			case "history-disabled":
				f.bc.config.HistoryEnabled = false
			case "nil-config":
				cfg := f.bc.config
				f.bc.config = nil
				defer func() { f.bc.config = cfg }()
			case "head":
				f.bc.currentBlock.Store(f.blocks[3])
			case "solidified":
				dp := f.bc.cachedDynProps()
				dp.SetLatestSolidifiedBlockNum(1)
				f.bc.storeDynPropsCache(dp)
			case "missing-proof", "missing-through":
				store := rawdb.NewMemoryDatabase()
				defer func() { _ = store.Close() }()
				for _, block := range f.blocks {
					if name == "missing-proof" && block.Number() == 4 || name == "missing-through" && block.Number() == 2 {
						continue
					}
					if err := rawdb.WriteBlock(store, block); err != nil {
						t.Fatal(err)
					}
				}
				f.bc.chaindb = rawdb.NewChainDB(store, nil)
			case "proof-replaced":
				replacement := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 4, Timestamp: 99}}})
				if err := rawdb.WriteBlock(f.db, replacement); err != nil {
					t.Fatal(err)
				}
			case "read-error":
				f.db.readErr = errors.New("injected proof read error")
				defer func() { f.db.readErr = nil }()
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			default:
				stage := rawdb.StageFinish
				if name == "missing-index" || name == "index-behind" || name == "index-hash" || name == "unbound-index" {
					stage = rawdb.StageStateHistoryIndex
				}
				var err error
				switch name {
				case "missing-finish", "missing-index":
					err = rawdb.DeleteStageProgress(f.db, stage)
				case "finish-behind", "index-behind":
					err = rawdb.WriteStageProgressWithHash(f.db, stage, 1, f.blocks[1].Hash())
				case "finish-hash", "index-hash":
					err = rawdb.WriteStageProgressWithHash(f.db, stage, 6, common.Hash{0xee})
				case "unbound-finish", "unbound-index":
					err = rawdb.WriteStageProgress(f.db, stage, 6)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			ran, err := f.bc.TryWithStateDomainChangePruneGuard(ctx, through, proofHead, proofHash, work)
			if ran || err == nil || called {
				t.Fatalf("rejected ran=%v err=%v called=%v", ran, err, called)
			}
			assertStateDomainChangeGuardUnlocked(t, f)
		})
	}
	var bc *BlockChain
	if ran, err := bc.TryWithStateDomainChangePruneGuard(context.Background(), 2, 4, common.Hash{1}, func() error { return nil }); ran || err == nil {
		t.Fatalf("nil chain ran=%v err=%v", ran, err)
	}
}

func assertStateDomainChangeGuardUnlocked(t *testing.T, f postingPruneGuardFixture) {
	t.Helper()
	if !f.bc.stateHistoryIndexMu.TryLock() {
		t.Fatal("index lock leaked")
	}
	f.bc.stateHistoryIndexMu.Unlock()
	if !f.bc.chainmu.TryLock() {
		t.Fatal("chain lock leaked")
	}
	f.bc.chainmu.Unlock()
}

func TestStateDomainChangePruneGuardHoldsLocksThroughFlush(t *testing.T) {
	f := newPostingPruneGuardFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	f.db.beforeWrite = func() error {
		if f.bc.chainmu.TryLock() {
			f.bc.chainmu.Unlock()
			return errors.New("chain lock missing at write")
		}
		if f.bc.stateHistoryIndexMu.TryLock() {
			f.bc.stateHistoryIndexMu.Unlock()
			return errors.New("index lock missing at write")
		}
		close(entered)
		<-release
		// A later async writer can publish an unrelated immutable block key
		// without taking chainmu. The selected old range must leave it intact.
		return f.db.Put([]byte("newer-history-fixture"), []byte{9})
	}
	done := make(chan error, 1)
	go func() {
		ran, err := f.bc.TryWithStateDomainChangePruneGuard(context.Background(), 2, 4, f.blocks[4].Hash(), func() error {
			batch := f.db.NewBatch()
			if err := batch.Put([]byte("guard-flush-fixture"), []byte{1}); err != nil {
				return err
			}
			return batch.Write()
		})
		if !ran && err == nil {
			err = errors.New("work not entered")
		}
		done <- err
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("guard before write: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("guard did not reach write")
	}
	closed, oldWriter := make(chan error, 1), make(chan struct{})
	go func() { closed <- f.bc.Close() }()
	go func() { f.bc.chainmu.Lock(); close(oldWriter); f.bc.chainmu.Unlock() }()
	// Direct TryLock assertions inside the blocked Write prove exclusion;
	// completion channels check both waiters can finish after work returns.
	unblock()
	for name, ch := range map[string]<-chan error{"guard": done, "close": closed} {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s blocked", name)
		}
	}
	select {
	case <-oldWriter:
	case <-time.After(5 * time.Second):
		t.Fatal("old writer lock leaked")
	}
	if value, err := f.db.Get([]byte("guard-flush-fixture")); err != nil || len(value) != 1 {
		t.Fatalf("work flush value=%x err=%v", value, err)
	}
	if value, err := f.db.Get([]byte("newer-history-fixture")); err != nil || len(value) != 1 {
		t.Fatalf("newer row value=%x err=%v", value, err)
	}
}

type stateDomainChangeGuardReadHookDB struct {
	ethdb.KeyValueStore
	once      sync.Once
	beforeGet func()
}

func (db *stateDomainChangeGuardReadHookDB) Get(key []byte) ([]byte, error) {
	db.once.Do(db.beforeGet)
	return db.KeyValueStore.Get(key)
}

func TestStateDomainChangePruneGuardCancelAndBatchErrorReleaseLocks(t *testing.T) {
	for _, name := range []string{"cancel-during-proof", "batch-error", "callback-canceled"} {
		t.Run(name, func(t *testing.T) {
			f := newPostingPruneGuardFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := errors.New("injected batch failure")
			switch name {
			case "cancel-during-proof":
				db := &stateDomainChangeGuardReadHookDB{KeyValueStore: f.db, beforeGet: cancel}
				f.bc.db, f.bc.chaindb = db, rawdb.NewChainDB(db, nil)
				want = context.Canceled
			case "callback-canceled":
				want = context.Canceled
			}
			f.db.beforeWrite = func() error { return want }
			called := false
			ran, err := f.bc.TryWithStateDomainChangePruneGuard(ctx, 2, 4, f.blocks[4].Hash(), func() error {
				called = true
				if name == "callback-canceled" {
					cancel()
					return ctx.Err()
				}
				batch := f.db.NewBatch()
				if err := batch.Put([]byte("failed-fixture"), []byte{1}); err != nil {
					return err
				}
				return batch.Write()
			})
			if !errors.Is(err, want) || ran != called || called == (name == "cancel-during-proof") {
				t.Fatalf("ran=%v called=%v err=%v want=%v", ran, called, err, want)
			}
			if ok, err := f.db.Has([]byte("failed-fixture")); err != nil || ok {
				t.Fatalf("failed write visible=%v err=%v", ok, err)
			}
			assertStateDomainChangeGuardUnlocked(t, f)
		})
	}
}

func TestStateDomainChangePruneGuardRejectsUnsettledOrFailedPrefix(t *testing.T) {
	for _, name := range []string{"inflight", "committed", "commit-error", "flush-error", "nil-buffer", "newer-layer"} {
		t.Run(name, func(t *testing.T) {
			f := newPostingPruneGuardFixture(t)
			fail := errors.New("injected async failure")
			switch name {
			case "inflight", "committed", "newer-layer":
				height := uint64(2)
				if name == "newer-layer" {
					height = 5
				}
				f.bc.buffer.BeginBlock(f.blocks[height].Hash(), height)
				if name != "inflight" {
					f.bc.buffer.CommitBlock()
				}
				defer f.bc.buffer.Discard()
			case "commit-error":
				f.bc.commitErr.Store(&fail)
				defer f.bc.commitErr.Store(nil)
			case "flush-error":
				f.bc.flushErr.Store(&fail)
				defer f.bc.flushErr.Store(nil)
			case "nil-buffer":
				buffer := f.bc.buffer
				f.bc.buffer = nil
				defer func() { f.bc.buffer = buffer }()
			}
			// Fixture Finish and index are already durably visible at block 6;
			// this alone must not authorize deletion across a retained old layer.
			called := false
			ran, err := f.bc.TryWithStateDomainChangePruneGuard(context.Background(), 2, 4, f.blocks[4].Hash(), func() error { called = true; return nil })
			if name == "newer-layer" {
				if !ran || !called || err != nil {
					t.Fatalf("newer layer rejected: ran=%v called=%v err=%v", ran, called, err)
				}
			} else if ran || called || err == nil {
				t.Fatalf("unsafe prefix ran=%v called=%v err=%v", ran, called, err)
			}
			assertStateDomainChangeGuardUnlocked(t, f)
		})
	}
}

type stateDomainChangeGuardBatchStore struct {
	rawdb.StateKVLatestStore
	batch ethdb.Batch
}

func (s stateDomainChangeGuardBatchStore) Put(key, value []byte) error {
	return s.batch.Put(key, value)
}
func (s stateDomainChangeGuardBatchStore) Delete(key []byte) error { return s.batch.Delete(key) }
func (s stateDomainChangeGuardBatchStore) DeleteRange(start, end []byte) error {
	return s.batch.DeleteRange(start, end)
}

func TestStateDomainChangePruneGuardRangeLeavesNewerAsyncRows(t *testing.T) {
	f := newPostingPruneGuardFixture(t)
	write := func(height uint64) error {
		return rawdb.WriteStateDomainChangeBlockRows(f.db.KeyValueStore, []*rawdb.StateDomainChange{{
			BlockNum: height, Seq: 1, TxNum: height, FlatDomain: rawdb.StateFlatDomainAccountLatest, Owner: common.Address{0x41, 1},
		}})
	}
	for _, height := range []uint64{1, 2, 5} {
		if err := write(height); err != nil {
			t.Fatal(err)
		}
	}
	f.db.beforeWrite = func() error { return write(6) }
	var stats rawdb.StateDomainChangeDeleteStats
	ran, err := f.bc.TryWithStateDomainChangePruneGuard(context.Background(), 2, 4, f.blocks[4].Hash(), func() error {
		batch := f.db.NewBatch()
		defer batch.Close()
		var err error
		stats, err = rawdb.DeleteStateDomainChangeBlocksWithOptions(stateDomainChangeGuardBatchStore{StateKVLatestStore: f.db, batch: batch}, []uint64{1, 2}, rawdb.StateDomainChangeDeleteOptions{EnableRangeDelete: true, MinRangeBlocks: 2})
		if err != nil {
			return err
		}
		return batch.Write()
	})
	if !ran || err != nil || stats.RangeRuns != 1 || stats.RangeRows != 2 {
		t.Fatalf("range ran=%v stats=%+v err=%v", ran, stats, err)
	}
	for _, height := range []uint64{1, 2, 5, 6} {
		_, exists, err := rawdb.ReadStateDomainChange(f.db, height, 1)
		if err != nil || exists != (height > 2) {
			t.Fatalf("block %d exists=%v err=%v", height, exists, err)
		}
	}
}
