package core

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type stateDomainChangeQueueResult struct {
	admitted bool
	err      error
}

func awaitStateDomainChangeQueueBusy(t *testing.T, observation *stateDomainChangePruneGuardMetrics, done chan stateDomainChangeQueueResult) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for observation.queuedChainBusy.Snapshot().Count() == 0 {
		select {
		case result := <-done:
			done <- result // The caller's cleanup must still join this result.
			t.Fatalf("queued guard returned before reaching held chain lock: %+v", result)
		case <-deadline.C:
			t.Fatal("queued guard did not reach held chain lock")
		default:
			runtime.Gosched()
		}
	}
}

func awaitStateDomainChangeQueueResult(t *testing.T, done <-chan stateDomainChangeQueueResult) stateDomainChangeQueueResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("queued guard did not finish")
		return stateDomainChangeQueueResult{}
	}
}

func TestStateDomainChangePruneQueueRechecksAfterHandoff(t *testing.T) {
	for _, name := range []string{"success", "cancel", "proof-replaced", "finish-rewound", "index-rewound", "flush-error", "commit-error", "prefix-inflight", "prefix-committed"} {
		t.Run(name, func(t *testing.T) {
			f := newPostingPruneGuardFixture(t)
			observation := newStateDomainChangePruneGuardMetrics(metrics.NewRegistry())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			defer f.bc.commitErr.Store(nil)
			defer f.bc.flushErr.Store(nil)
			defer f.bc.buffer.Discard()
			called, writes := false, 0
			f.db.beforeWrite = func() error { writes++; return nil }
			done := make(chan stateDomainChangeQueueResult, 1)
			f.bc.chainmu.Lock()
			var releaseOnce sync.Once
			releaseChain := func() { releaseOnce.Do(f.bc.chainmu.Unlock) }
			joined := false
			defer func() {
				cancel()
				releaseChain()
				if !joined {
					awaitStateDomainChangeQueueResult(t, done)
				}
			}()
			go func() {
				admitted, err := f.bc.withStateDomainChangePruneGuard(ctx, 2, 4, f.blocks[4].Hash(), func() error {
					called = true
					batch := f.db.NewBatch()
					defer batch.Close()
					if err := batch.Put([]byte("queued-guard-write"), []byte{1}); err != nil {
						return err
					}
					return batch.Write()
				}, observation, true)
				done <- stateDomainChangeQueueResult{admitted: admitted, err: err}
			}()
			awaitStateDomainChangeQueueBusy(t, observation, done)
			// The chain holder has not released. Unlike the old Try entry, this
			// attempt is registered for handoff and cannot return point fallback.
			select {
			case result := <-done:
				done <- result
				t.Fatalf("returned while chain held: %+v", result)
			default:
			}
			if observation.attempts.Snapshot().Count() != 1 || observation.queuedAttempts.Snapshot().Count() != 1 || observation.busyChain.Snapshot().Count() != 0 || observation.admitted.Snapshot().Count() != 0 {
				t.Fatal("queued contention classified as completed fallback/admission")
			}
			if observation.queuedChainWaitTotal.Snapshot().Count() != 0 || observation.queuedChainHeldTotal.Snapshot().Count() != 0 {
				t.Fatal("unfinished wait/held duration published")
			}
			wantOutcome := "admitted"
			injected := errors.New("async failure while guard waits")
			switch name {
			case "cancel":
				cancel()
				wantOutcome = "other_errors"
			case "proof-replaced":
				replacement := types.NewBlockFromPB(&corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 4, Timestamp: 991}}})
				if err := rawdb.WriteBlock(f.db, replacement); err != nil {
					t.Fatal(err)
				}
				wantOutcome = "proof_errors"
			case "finish-rewound", "index-rewound":
				stage := rawdb.StageFinish
				if name == "index-rewound" {
					stage = rawdb.StageStateHistoryIndex
				}
				if err := rawdb.WriteStageProgressWithHash(f.db, stage, 1, f.blocks[1].Hash()); err != nil {
					t.Fatal(err)
				}
				wantOutcome = "proof_errors"
			case "flush-error":
				f.bc.flushErr.Store(&injected)
				wantOutcome = "other_errors"
			case "commit-error":
				f.bc.commitErr.Store(&injected)
				wantOutcome = "other_errors"
			case "prefix-inflight", "prefix-committed":
				f.bc.buffer.BeginBlock(f.blocks[2].Hash(), 2)
				if name == "prefix-committed" {
					f.bc.buffer.CommitBlock()
				}
				wantOutcome = "prefix_unsettled"
			}
			releaseChain()
			result := awaitStateDomainChangeQueueResult(t, done)
			joined = true
			wantWork := name == "success"
			if result.admitted != wantWork || called != wantWork || (result.err == nil) != wantWork || writes != boolToStateDomainChangeQueueInt(wantWork) {
				t.Fatalf("result=%+v called=%v writes=%d", result, called, writes)
			}
			if name == "cancel" && !errors.Is(result.err, context.Canceled) {
				t.Fatalf("cancel err=%v", result.err)
			}
			if name == "flush-error" || name == "commit-error" {
				if !errors.Is(result.err, injected) {
					t.Fatalf("async err=%v", result.err)
				}
			}
			present, err := f.db.Has([]byte("queued-guard-write"))
			if err != nil || present != wantWork {
				t.Fatalf("write visible=%v err=%v", present, err)
			}
			if stateDomainChangeGuardMetricCounts(observation)[wantOutcome] != 1 {
				t.Fatalf("outcome counters=%+v", stateDomainChangeGuardMetricCounts(observation))
			}
			assertStateDomainChangeQueueTimings(t, observation)
			assertStateDomainChangeGuardMetricPartition(t, observation)
			assertStateDomainChangeGuardUnlocked(t, f)
		})
	}
}

func boolToStateDomainChangeQueueInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func assertStateDomainChangeQueueTimings(t *testing.T, observation *stateDomainChangePruneGuardMetrics) {
	t.Helper()
	wait := observation.queuedChainWaitTotal.Snapshot().Count()
	held := observation.queuedChainHeldTotal.Snapshot().Count()
	if wait <= 0 || held <= 0 || observation.queuedChainWaitMax.Snapshot().Value() > wait || observation.queuedChainHeldMax.Snapshot().Value() > held {
		t.Fatalf("wait total/max=%d/%d held total/max=%d/%d", wait, observation.queuedChainWaitMax.Snapshot().Value(), held, observation.queuedChainHeldMax.Snapshot().Value())
	}
}

func TestStateDomainChangePruneQueueIndexBusyStillReturnsImmediately(t *testing.T) {
	f := newPostingPruneGuardFixture(t)
	observation := newStateDomainChangePruneGuardMetrics(metrics.NewRegistry())
	f.bc.stateHistoryIndexMu.Lock()
	defer f.bc.stateHistoryIndexMu.Unlock()
	for _, invoke := range []func(context.Context, uint64, uint64, common.Hash, func() error) (bool, error){
		f.bc.WithStateDomainChangePruneGuard,
		func(ctx context.Context, through, head uint64, hash common.Hash, work func() error) (bool, error) {
			return f.bc.withStateDomainChangePruneGuard(ctx, through, head, hash, work, observation, true)
		},
	} {
		called := false
		admitted, err := invoke(context.Background(), 2, 4, f.blocks[4].Hash(), func() error { called = true; return nil })
		if admitted || err != nil || called {
			t.Fatalf("busy index admitted=%v err=%v called=%v", admitted, err, called)
		}
	}
	if observation.queuedAttempts.Snapshot().Count() != 1 || observation.busyIndex.Snapshot().Count() != 1 || observation.queuedChainBusy.Snapshot().Count() != 0 || observation.queuedChainWaitTotal.Snapshot().Count() != 0 || observation.queuedChainHeldTotal.Snapshot().Count() != 0 {
		t.Fatal("index rejection entered/timed chain queue")
	}
	assertStateDomainChangeGuardMetricPartition(t, observation)
}

func TestStateDomainChangePruneQueueFlushLockLifetimeAndTiming(t *testing.T) {
	for _, writeFails := range []bool{false, true} {
		name := "success"
		if writeFails {
			name = "write-error"
		}
		t.Run(name, func(t *testing.T) {
			f := newPostingPruneGuardFixture(t)
			observation := newStateDomainChangePruneGuardMetrics(metrics.NewRegistry())
			entered, releaseWork := make(chan struct{}), make(chan struct{})
			var workOnce sync.Once
			unblockWork := func() { workOnce.Do(func() { close(releaseWork) }) }
			defer unblockWork()
			injected := errors.New("queued batch failure")
			f.db.beforeWrite = func() error {
				if f.bc.chainmu.TryLock() {
					f.bc.chainmu.Unlock()
					return errors.New("chain lock missing during batch Write")
				}
				if f.bc.stateHistoryIndexMu.TryLock() {
					f.bc.stateHistoryIndexMu.Unlock()
					return errors.New("index lock missing during batch Write")
				}
				close(entered)
				<-releaseWork
				if writeFails {
					return injected
				}
				return nil
			}
			f.bc.chainmu.Lock()
			var chainOnce sync.Once
			releaseChain := func() { chainOnce.Do(f.bc.chainmu.Unlock) }
			done := make(chan stateDomainChangeQueueResult, 1)
			joined := false
			defer func() {
				releaseChain()
				unblockWork()
				if !joined {
					awaitStateDomainChangeQueueResult(t, done)
				}
			}()
			go func() {
				admitted, err := f.bc.withStateDomainChangePruneGuard(context.Background(), 2, 4, f.blocks[4].Hash(), func() error {
					batch := f.db.NewBatch()
					defer batch.Close()
					if err := batch.Put([]byte("queued-lifetime"), []byte{1}); err != nil {
						return err
					}
					return batch.Write()
				}, observation, true)
				done <- stateDomainChangeQueueResult{admitted: admitted, err: err}
			}()
			awaitStateDomainChangeQueueBusy(t, observation, done)
			releaseChain()
			select {
			case <-entered:
			case result := <-done:
				done <- result
				t.Fatalf("guard never reached Write: %+v", result)
			case <-time.After(5 * time.Second):
				t.Fatal("Write did not start")
			}
			waitAtWork := observation.queuedChainWaitTotal.Snapshot().Count()
			if waitAtWork <= 0 || observation.queuedChainHeldTotal.Snapshot().Count() != 0 || observation.admitted.Snapshot().Count() != 1 {
				t.Fatal("wait not finalized at handoff, or held published before work finished")
			}
			closed := make(chan error, 1)
			go func() { closed <- f.bc.Close() }()
			// Both lock assertions ran in the actual batch Write. Close requires
			// the same outer mutex and must finish only after this work releases.
			unblockWork()
			result := awaitStateDomainChangeQueueResult(t, done)
			joined = true
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Close blocked after queued work")
			}
			if !result.admitted || (result.err != nil) != writeFails || writeFails && !errors.Is(result.err, injected) {
				t.Fatalf("write result=%+v", result)
			}
			if observation.queuedChainWaitTotal.Snapshot().Count() != waitAtWork {
				t.Fatal("callback work was included in queue wait")
			}
			if observation.workErrors.Snapshot().Count() != int64(boolToStateDomainChangeQueueInt(writeFails)) {
				t.Fatal("failed admitted callback classification")
			}
			assertStateDomainChangeQueueTimings(t, observation)
			if observation.queuedChainHeldMax.Snapshot().Value() != observation.queuedChainHeldTotal.Snapshot().Count() || observation.queuedChainWaitMax.Snapshot().Value() != waitAtWork {
				t.Fatal("single call max differs from total")
			}
			assertStateDomainChangeGuardMetricPartition(t, observation)
		})
	}
}
