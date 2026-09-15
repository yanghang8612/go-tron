package rawdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func TestStateHistoryPipelineWorkersFrozenOracle(t *testing.T) {
	for _, workers := range []int{2, 4, 8} {
		for _, bad := range []bool{false, true} {
			t.Run(fmt.Sprintf("workers%d/bad%v", workers, bad), func(t *testing.T) {
				modes := []string{"shared", "snappy-shared", "raw", "nested-snappy", "shared", "snappy", "shared", "shared", "shared"}
				if bad {
					modes[2] = "bad-digest"
					modes[7] = "missing"
				}
				db, _ := pipelineFixture(t, modes...)
				view, release, err := AcquireStateHistoryReadView(db)
				if err != nil {
					t.Fatal(err)
				}
				defer release()
				want, we := pipelineCollect(t, view, false, [4]uint64{1, 9, 0, 99})
				var got []*StateDomainChange
				ge := IterateStateDomainChangesByBlockTxRangePipelinedWithWorkers(context.Background(), view, 1, 9, 0, 99, workers, func(row *StateDomainChange) (bool, error) {
					got = append(got, cloneStateDomainChange(row))
					return true, nil
				})
				if fmt.Sprint(we) != fmt.Sprint(ge) || !reflect.DeepEqual(want, got) {
					t.Fatalf("error %v/%v rows %d/%d", we, ge, len(want), len(got))
				}
			})
		}
	}
}

func TestStateHistoryPipelineWorkersBoundedQueueAndCancelJoin(t *testing.T) {
	for _, workers := range []int{2, 4, 8} {
		for _, cancelRun := range []bool{false, true} {
			t.Run(fmt.Sprintf("workers%d/cancel%v", workers, cancelRun), func(t *testing.T) {
				modes := make([]string, workers+1)
				for i := range modes {
					modes[i] = "shared"
				}
				db, keys := pipelineFixture(t, modes...)
				view, release, err := AcquireStateHistoryReadView(db)
				if err != nil {
					t.Fatal(err)
				}
				started := make([]chan struct{}, workers)
				onces := make([]sync.Once, workers)
				for i := range started {
					started[i] = make(chan struct{})
				}
				allowReads, extraRead := make(chan struct{}), make(chan struct{})
				var extraOnce sync.Once
				v := &pipelineClosingView{pipelineTestView: &pipelineTestView{StateHistoryReadView: view, hook: func(op string, key []byte) error {
					if op != "Get" {
						return nil
					}
					for i := range started {
						if bytes.Equal(key, keys[i]) {
							onces[i].Do(func() { close(started[i]) })
							<-allowReads
							return nil
						}
					}
					if bytes.Equal(key, keys[workers]) {
						extraOnce.Do(func() { close(extraRead) })
					}
					return nil
				}}, closeFn: release}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				inCallback, allowCallback := make(chan struct{}), make(chan struct{})
				seen := 0
				done := make(chan error, 1)
				go func() {
					done <- IterateStateDomainChangesByBlockTxRangePipelinedWithWorkers(ctx, &pipelineFactory{Iteratee: db, snapshot: v}, 1, uint64(workers+1), 0, 999, workers, func(row *StateDomainChange) (bool, error) {
						seen++
						if seen == 1 {
							close(inCallback)
							<-allowCallback
						}
						return true, nil
					})
				}()
				for _, ch := range started {
					pipelineAwait(t, ch)
				}
				if int(v.active.Load()) != workers {
					t.Fatalf("active reads=%d want%d", v.active.Load(), workers)
				}
				select {
				case <-extraRead:
					t.Fatal("more jobs than slots")
				default:
				}
				if cancelRun {
					cancel()
					select {
					case err := <-done:
						t.Fatalf("did not join blocked workers: %v", err)
					default:
					}
					if v.closes.Load() != 0 {
						t.Fatal("closed before join")
					}
					close(allowReads)
					close(allowCallback)
				} else {
					close(allowReads)
					pipelineAwait(t, inCallback)
					select {
					case <-extraRead:
						t.Fatal("consuming block released slot early")
					default:
					}
					close(allowCallback)
				}
				err = <-done
				if cancelRun {
					if !errors.Is(err, context.Canceled) || seen != 0 {
						t.Fatalf("cancel error=%v rows=%d", err, seen)
					}
				} else if err != nil || seen != workers+1 {
					t.Fatalf("success error=%v rows=%d", err, seen)
				}
				if v.closes.Load() != 1 || v.closeActive.Load() != 0 || v.active.Load() != 0 {
					t.Fatalf("close=%d active=%d atclose=%d", v.closes.Load(), v.active.Load(), v.closeActive.Load())
				}
			})
		}
	}
}

func TestStateHistoryPipelineWorkersFutureErrorOrder(t *testing.T) {
	for _, workers := range []int{4, 8} {
		for _, callbackErr := range []error{nil, errors.New("earlier callback failure")} {
			t.Run(fmt.Sprintf("workers%d/callback%v", workers, callbackErr), func(t *testing.T) {
				modes := make([]string, workers)
				for i := range modes {
					modes[i] = "shared"
				}
				db, keys := pipelineFixture(t, modes...)
				view, release, err := AcquireStateHistoryReadView(db)
				if err != nil {
					t.Fatal(err)
				}
				defer release()
				first, future, allowFirst := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var firstOnce, futureOnce sync.Once
				v := &pipelineTestView{StateHistoryReadView: view, hook: func(op string, key []byte) error {
					if op == "Get" && bytes.Equal(key, keys[0]) {
						firstOnce.Do(func() { close(first) })
						<-allowFirst
					}
					if op == "Get" && bytes.Equal(key, keys[workers-1]) {
						futureOnce.Do(func() { close(future) })
						return errors.New("later read failure")
					}
					return nil
				}}
				seen := 0
				done := make(chan error, 1)
				go func() {
					done <- IterateStateDomainChangesByBlockTxRangePipelinedWithWorkers(context.Background(), v, 1, uint64(workers), 0, 999, workers, func(*StateDomainChange) (bool, error) { seen++; return false, callbackErr })
				}()
				pipelineAwait(t, first)
				pipelineAwait(t, future)
				select {
				case err := <-done:
					t.Fatalf("future error overtook earlier block: %v", err)
				default:
				}
				close(allowFirst)
				if err := <-done; !errors.Is(err, callbackErr) || seen != 1 || v.active.Load() != 0 {
					t.Fatalf("error=%v rows=%d active=%d", err, seen, v.active.Load())
				}
			})
		}
	}
}

func TestStateHistoryPipelineWorkersBudgetAndValidation(t *testing.T) {
	for _, workers := range []int{2, 4, 8} {
		var used uint64
		piece := uint64(256 / workers)
		// Two maximum-sized pieces are deliberately exclusive; use smaller
		// pieces for the two-slot case. Four/eight may fill all 256 units.
		if workers == 2 {
			piece = 127
		}
		for count := 0; count < workers; count++ {
			if !historyPipelineCanAdmit(used, count, piece, 256, workers, false) {
				t.Fatalf("small pieces rejected: workers=%d count=%d used=%d", workers, count, used)
			}
			used += piece
		}
		if historyPipelineCanAdmit(used, workers, 1, 256, workers, false) {
			t.Fatal("slot limit exceeded")
		}
		if workers > 2 && used != 256 {
			t.Fatal("aggregate budget not fully available")
		}
		if historyPipelineCanAdmit(256, 1, 1, 256, workers, false) || historyPipelineCanAdmit(1, 1, 128, 256, workers, false) || historyPipelineCanAdmit(128, 1, 1, 256, workers, true) || !historyPipelineCanAdmit(0, 0, 128, 256, workers, false) {
			t.Fatal("budget or large-exclusive semantics changed")
		}
		if historyPipelineCanAdmit(0, 0, 129, 256, workers, false) {
			t.Fatal("oversized decoded block admitted")
		}
	}
	db, _ := pipelineFixture(t, "shared")
	for _, workers := range []int{-1, 0, 1, 3, 5, 9, 256} {
		if err := IterateStateDomainChangesByBlockTxRangePipelinedWithWorkers(context.Background(), db, 1, 1, 0, 99, workers, borrowedStateDomainChangeNoop); !errors.Is(err, ErrStateHistoryPipelineWorkers) {
			t.Fatalf("workers=%d: %v", workers, err)
		}
	}
}
