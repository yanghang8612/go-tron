package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestRepairParallelOrderedPreservesTaskOrder(t *testing.T) {
	for _, workers := range []int{1, 2, 4, 8} {
		t.Run(string(rune('0'+workers)), func(t *testing.T) {
			gate := make(chan struct{})
			var laterTaskCompleted atomic.Bool
			count := max(8, workers+1)
			results, err := repairParallelOrdered(context.Background(), workers, count, func(_ context.Context, index int) (int, error) {
				if workers > 1 {
					if index == 0 {
						<-gate
					} else if index == workers {
						// Reaching the next queued task proves that an earlier
						// sibling finished while task zero was still held.
						laterTaskCompleted.Store(true)
						close(gate)
					}
				}
				return index * 13, nil
			})
			if err != nil || len(results) != count {
				t.Fatalf("ordered work: %v, %v", results, err)
			}
			for i, value := range results {
				if value != i*13 {
					t.Fatalf("result[%d]=%d", i, value)
				}
			}
			if workers > 1 && !laterTaskCompleted.Load() {
				t.Fatal("task zero was not overtaken by a later completed task")
			}
		})
	}
}

func TestRepairParallelOrderedCancelsAndJoinsBeforeReturning(t *testing.T) {
	var joined atomic.Bool
	started := make(chan struct{})
	failed := errors.New("independent TARGET build failed")
	_, err := repairParallelOrdered(context.Background(), 2, 2, func(ctx context.Context, index int) (int, error) {
		if index == 0 {
			<-started
			return 0, failed
		}
		close(started)
		<-ctx.Done()
		joined.Store(true)
		return 0, ctx.Err()
	})
	if !errors.Is(err, failed) || !joined.Load() {
		t.Fatalf("first error %v returned before sibling joined=%t", err, joined.Load())
	}
}

func TestRepairParallelOrderedBoundsActiveWorkers(t *testing.T) {
	for _, workers := range []int{2, 4, 8} {
		started := make(chan struct{}, workers)
		release := make(chan struct{})
		finished := make(chan error, 1)
		var active, peak atomic.Int32
		go func() {
			_, err := repairParallelOrdered(context.Background(), workers, workers*3, func(ctx context.Context, index int) (int, error) {
				current := active.Add(1)
				for {
					old := peak.Load()
					if current <= old || peak.CompareAndSwap(old, current) {
						break
					}
				}
				if index < workers {
					started <- struct{}{}
					<-release
				}
				active.Add(-1)
				return index, ctx.Err()
			})
			finished <- err
		}()
		for range workers {
			<-started
		}
		close(release)
		if err := <-finished; err != nil || peak.Load() != int32(workers) || active.Load() != 0 {
			t.Fatalf("workers=%d: err=%v peak=%d active=%d", workers, err, peak.Load(), active.Load())
		}
	}
}

func TestRepairParallelOrderedExternalCancellationJoins(t *testing.T) {
	for _, workers := range []int{2, 8} {
		t.Run(string(rune('0'+workers)), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started := make(chan struct{}, workers)
			finished := make(chan error, 1)
			var joined atomic.Int32
			go func() {
				_, err := repairParallelOrdered(ctx, workers, workers*2, func(ctx context.Context, _ int) (int, error) {
					started <- struct{}{}
					<-ctx.Done()
					joined.Add(1)
					return 0, ctx.Err()
				})
				finished <- err
			}()
			for range workers {
				<-started
			}
			cancel()
			if err := <-finished; !errors.Is(err, context.Canceled) || joined.Load() != int32(workers) {
				t.Fatalf("workers=%d: cancel returned %v before %d workers joined", workers, err, joined.Load())
			}
		})
	}
}
